package sessionrestore

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strings"

	"github.com/yasyf/cc-sync/internal/claudenative"
)

// PathRule maps the absolute source path From, and everything below it, to To.
type PathRule struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// PathMap relocates source-layout paths; the longest matching From wins.
type PathMap []PathRule

// Map returns p relocated by the longest rule whose From equals p or is a
// directory prefix of it, and false when no rule covers p.
func (m PathMap) Map(p string) (string, bool) {
	best := -1
	for i, r := range m {
		if (p == r.From || strings.HasPrefix(p, r.From+"/")) && (best < 0 || len(r.From) > len(m[best].From)) {
			best = i
		}
	}
	if best < 0 {
		return p, false
	}
	return m[best].To + p[len(m[best].From):], true
}

func (m PathMap) identity() bool {
	for _, r := range m {
		if r.From != r.To {
			return false
		}
	}
	return true
}

func (m PathMap) sorted() PathMap {
	seen := make(map[string]bool, len(m))
	out := make(PathMap, 0, len(m))
	for _, r := range m {
		if !seen[r.From] {
			seen[r.From] = true
			out = append(out, PathRule{From: filepath.Clean(r.From), To: filepath.Clean(r.To)})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].From < out[j].From })
	return out
}

// Unmapped is an absolute path left verbatim: a typed path field outside
// every PathMap root, or a source-layout path inside a record type the
// adapter does not know.
type Unmapped struct {
	File       string `json:"file"`
	Line       int    `json:"line,omitempty"`
	RecordType string `json:"record_type,omitempty"`
	Path       string `json:"path"`
}

type field struct {
	path bool
	keys bool
	each *field
	sub  map[string]*field
}

func object(sub map[string]*field) *field { return &field{sub: sub} }

func array(each *field) *field { return &field{each: each} }

var (
	pathField      = &field{path: true}
	cwdOnly        = object(map[string]*field{"cwd": pathField})
	planFile       = object(map[string]*field{"planFilePath": pathField})
	filenameOnly   = object(map[string]*field{"filename": pathField})
	workflowFields = object(map[string]*field{"scriptPath": pathField})

	recordFields = map[string]*field{
		"user": object(map[string]*field{
			"cwd": pathField,
			"serverClassifierContext": object(map[string]*field{"context": object(map[string]*field{
				"live_cwd":  pathField,
				"git_state": object(map[string]*field{"root": pathField, "cwd": pathField}),
			})}),
			"toolUseResult": object(map[string]*field{"persistedOutputPath": pathField}),
		}),
		"assistant": cwdOnly,
		"system":    cwdOnly,
		"worktree-state": object(map[string]*field{"worktreeSession": object(map[string]*field{
			"originalCwd":         pathField,
			"preEnterOriginalCwd": pathField,
			"worktreePath":        pathField,
		})}),
		"file-history-snapshot": object(map[string]*field{"snapshot": object(map[string]*field{
			"trackedFileBackups": {keys: true, each: object(map[string]*field{"realParentDir": pathField})},
		})}),
		"file-history-delta": object(map[string]*field{
			"trackingPath": pathField,
			"backup":       object(map[string]*field{"realParentDir": pathField}),
		}),
	}

	attachmentFields = map[string]*field{
		"plan_mode":           planFile,
		"plan_file_reference": planFile,
		"plan_mode_exit":      planFile,
		"plan_mode_reentry":   planFile,
		"task_status":         object(map[string]*field{"outputFilePath": pathField}),
		"task_reminder": object(map[string]*field{"content": array(object(map[string]*field{
			"metadata": object(map[string]*field{"worktree": pathField}),
		}))}),
		"file": object(map[string]*field{
			"filename": pathField,
			"content":  object(map[string]*field{"file": object(map[string]*field{"filePath": pathField})}),
		}),
		"compact_file_reference": filenameOnly,
		"edited_text_file":       filenameOnly,
		"instructions":           object(map[string]*field{"files": array(object(map[string]*field{"path": pathField}))}),
	}

	knownTypes = map[string]bool{
		"attachment": true, "summary": true, "last-prompt": true, "mode": true, "permission-mode": true,
		"ai-title": true, "custom-title": true, "tag": true, "relocated": true, "agent-name": true,
		"agent-color": true, "agent-setting": true, "atis-latch": true, "cost-state": true, "pr-link": true,
		"bridge-session": true, "queue-operation": true,
	}
)

type relocation struct {
	Rewritten        int
	Stripped         int
	DroppedTailBytes int64
	Unmapped         []Unmapped
	sessionRecords   int
}

type relocator struct {
	paths   PathMap
	fromID  claudenative.SessionID
	toID    claudenative.SessionID
	strip   bool
	stats   *relocation
	file    string
	line    int
	recType string
	dropped map[string]*string
}

func (r *relocator) forking() bool { return r.fromID != r.toID }

func (r *relocator) begin(file string) {
	r.file, r.line, r.recType = file, 0, ""
	r.dropped = map[string]*string{}
}

func (r *relocator) unmapped(p string) {
	r.stats.Unmapped = append(r.stats.Unmapped, Unmapped{File: r.file, Line: r.line, RecordType: r.recType, Path: p})
}

func (r *relocator) jsonl(src io.Reader, dst io.Writer) error {
	br := bufio.NewReaderSize(src, 1<<16)
	for {
		b, err := br.ReadBytes('\n')
		if errors.Is(err, io.EOF) {
			r.stats.DroppedTailBytes += int64(len(b))
			return nil
		}
		if err != nil {
			return fmt.Errorf("read %s: %w", r.file, err)
		}
		out, keep := r.record(b[:len(b)-1])
		if !keep {
			continue
		}
		if _, err := dst.Write(append(out, '\n')); err != nil {
			return fmt.Errorf("write %s: %w", r.file, err)
		}
	}
}

func (r *relocator) document(src io.Reader, dst io.Writer, f *field) error {
	b, err := io.ReadAll(src)
	if err != nil {
		return fmt.Errorf("read %s: %w", r.file, err)
	}
	out, _ := r.walk(b, f)
	if _, err := dst.Write(out); err != nil {
		return fmt.Errorf("write %s: %w", r.file, err)
	}
	return nil
}

func (r *relocator) link(target string) string {
	if !filepath.IsAbs(target) {
		return target
	}
	to, ok := r.paths.Map(target)
	if !ok {
		r.unmapped(target)
	}
	return to
}

func (r *relocator) record(b []byte) ([]byte, bool) {
	r.line++
	r.recType = ""
	if !json.Valid(b) {
		return b, true
	}
	members, ok := objectMembers(b)
	if !ok {
		return b, true
	}
	get := func(name string) []byte {
		for _, m := range members {
			if m.name == name {
				return b[m.val.start:m.val.end]
			}
		}
		return nil
	}
	r.recType, _ = decodeString(get("type"))
	if sid, _ := decodeString(get("sessionId")); sid == string(r.fromID) {
		r.stats.sessionRecords++
	}
	f := recordFields[r.recType]
	if r.recType == "attachment" {
		kind, _ := decodeString(memberValue(get("attachment"), "type"))
		if r.strip && kind == "prompt_snapshot" {
			uuid, _ := decodeString(get("uuid"))
			r.dropped[uuid] = parentOf(get("parentUuid"))
			r.stats.Stripped++
			return nil, false
		}
		f = object(map[string]*field{"cwd": pathField, "attachment": attachmentFields[kind]})
	}
	if f == nil && !knownTypes[r.recType] {
		r.reportSourcePaths(b)
	}
	out, changed := r.walk(b, f)
	out, relinked := r.relink(out)
	if changed || relinked {
		r.stats.Rewritten++
	}
	return out, true
}

func parentOf(raw []byte) *string {
	s, ok := decodeString(raw)
	if !ok {
		return nil
	}
	return &s
}

func (r *relocator) relink(b []byte) ([]byte, bool) {
	if len(r.dropped) == 0 {
		return b, false
	}
	members, _ := objectMembers(b)
	var edits []edit
	for _, m := range members {
		if m.name != "parentUuid" && m.name != "logicalParentUuid" {
			continue
		}
		id, ok := decodeString(b[m.val.start:m.val.end])
		if _, gone := r.dropped[id]; !ok || !gone {
			continue
		}
		edits = append(edits, edit{m.val, r.ancestor(id)})
	}
	if len(edits) == 0 {
		return b, false
	}
	return splice(b, edits), true
}

func (r *relocator) ancestor(id string) []byte {
	for {
		parent := r.dropped[id]
		if parent == nil {
			return []byte("null")
		}
		if _, gone := r.dropped[*parent]; !gone {
			return quote(*parent)
		}
		id = *parent
	}
}

func (r *relocator) walk(b []byte, f *field) ([]byte, bool) {
	if f != nil && f.path {
		return r.mapPath(b)
	}
	if f == nil && !r.forking() {
		return b, false
	}
	i := skipSpace(b, 0)
	if i == len(b) {
		return b, false
	}
	switch b[i] {
	case '{':
		return r.walkObject(b, f)
	case '[':
		return r.walkArray(b, f)
	}
	return b, false
}

func (r *relocator) walkObject(b []byte, f *field) ([]byte, bool) {
	members, _ := objectMembers(b)
	var edits []edit
	for _, m := range members {
		val := b[m.val.start:m.val.end]
		if r.forking() && (m.name == "sessionId" || m.name == "session_id") {
			if s, ok := decodeString(val); ok && s == string(r.fromID) {
				edits = append(edits, edit{m.val, quote(string(r.toID))})
				continue
			}
		}
		var child *field
		switch {
		case f != nil && f.keys:
			if out, ok := r.mapPath(b[m.key.start:m.key.end]); ok {
				edits = append(edits, edit{m.key, out})
			}
			child = f.each
		case f != nil:
			child = f.sub[m.name]
		}
		if out, ok := r.walk(val, child); ok {
			edits = append(edits, edit{m.val, out})
		}
	}
	if len(edits) == 0 {
		return b, false
	}
	return splice(b, edits), true
}

func (r *relocator) walkArray(b []byte, f *field) ([]byte, bool) {
	var child *field
	if f != nil {
		child = f.each
	}
	elems, _ := arrayElements(b)
	var edits []edit
	for _, e := range elems {
		if out, ok := r.walk(b[e.start:e.end], child); ok {
			edits = append(edits, edit{e, out})
		}
	}
	if len(edits) == 0 {
		return b, false
	}
	return splice(b, edits), true
}

func (r *relocator) mapPath(raw []byte) ([]byte, bool) {
	p, ok := decodeString(raw)
	if !ok || !filepath.IsAbs(p) {
		return raw, false
	}
	to, ok := r.paths.Map(p)
	if !ok {
		r.unmapped(p)
		return raw, false
	}
	if to == p {
		return raw, false
	}
	return quote(to), true
}

func (r *relocator) reportSourcePaths(b []byte) {
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		return
	}
	var visit func(v any)
	visit = func(v any) {
		switch v := v.(type) {
		case string:
			if to, ok := r.paths.Map(v); ok && filepath.IsAbs(v) && to != v {
				r.unmapped(v)
			}
		case []any:
			for _, e := range v {
				visit(e)
			}
		case map[string]any:
			keys := make([]string, 0, len(v))
			for k := range v {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				visit(v[k])
			}
		}
	}
	visit(v)
}

func relocatedRecord(id claudenative.SessionID, cwd string) []byte {
	out := []byte(`{"type":"relocated","sessionId":`)
	out = append(out, quote(string(id))...)
	out = append(out, `,"relocatedCwd":`...)
	out = append(out, quote(cwd)...)
	return append(out, "}\n"...)
}
