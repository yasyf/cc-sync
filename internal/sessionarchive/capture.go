// Package sessionarchive captures one Claude Code session's native storage —
// its transcript, sidecar tree, file history, task lists, plans, paste-cache
// blobs, and scratchpad — into the synckit artifact store.
package sessionarchive

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/yasyf/synckit/artifact"
)

// Store is the part of the synckit artifact store Capture writes through.
type Store interface {
	Put(ctx context.Context, r io.Reader, media string) (artifact.Ref, error)
	PutGroup(ctx context.Context, media string, deps []artifact.Ref) (artifact.Ref, error)
}

// Source is one session's native paths and metadata, filled by the capture
// job from the claudenative inventory. TaskListDirs, PlanFiles, PasteFiles,
// and ScratchpadDir are the items the session references; each one absent at
// the source is reported in Completeness.Missing. CapturedAt stamps a
// manifest whose content changed.
type Source struct {
	SessionID      string
	SourceHost     string
	ConfigDir      string
	ProjectDirName string
	TranscriptPath string
	SessionDir     string
	FileHistoryDir string
	TaskListDirs   []string
	PlanFiles      []string
	PasteFiles     []string
	ScratchpadDir  string
	Cwd            string
	OriginalCwd    string
	GitBranch      string
	Title          string
	ClaudeVersion  string
	LastHuman      time.Time
	LastAutonomous time.Time
	CapturedAt     time.Time
}

var excludedNames = map[string]bool{
	"sessions":          true,
	"session-env":       true,
	"shell-snapshots":   true,
	"statsig":           true,
	".credentials.json": true,
	"credentials.json":  true,
}

var backupFileNameMarker = []byte(`"backupFileName":"`)

type tree struct {
	root     Root
	base     string
	start    string
	required bool
}

func (t tree) label() string {
	if t.start == "." {
		return string(t.root)
	}
	return string(t.root) + "/" + t.start
}

type capturer struct {
	src      Source
	store    Store
	prev     map[string]Entry
	roots    map[Root]string
	entries  []Entry
	missing  []string
	deferred []string
}

// Capture archives src into store and returns the stored Archive. Each JSONL
// transcript (the main one and every subagents/**/*.jsonl) is frozen at its
// last newline so a partial record is never stored; every other file is stored
// whole; symlinks are recorded verbatim and never followed. A file whose
// inode, size, and mtime match its prev entry is reused without being read,
// and a capture that changes nothing returns *prev without writing. A file
// that changes under two consecutive reads is deferred: its prev entry, if
// any, stands in and Completeness.Deferred names it.
func Capture(ctx context.Context, src Source, prev *Archive, store Store) (Archive, error) {
	c := &capturer{src: src, store: store, prev: map[string]Entry{}, roots: map[Root]string{}}
	if prev != nil {
		for _, e := range prev.Manifest.Entries {
			c.prev[e.Key()] = e
		}
	}
	if err := c.transcript(ctx); err != nil {
		return Archive{}, err
	}
	trees, err := c.trees()
	if err != nil {
		return Archive{}, err
	}
	for _, t := range trees {
		if err := c.walk(ctx, t); err != nil {
			return Archive{}, err
		}
	}
	m := c.manifest()
	if prev != nil {
		same, err := sameContent(m, prev.Manifest)
		if err != nil {
			return Archive{}, err
		}
		if same {
			return *prev, nil
		}
	}
	return publish(ctx, store, m)
}

func (c *capturer) transcript(ctx context.Context) error {
	base, name := filepath.Dir(c.src.TranscriptPath), filepath.Base(c.src.TranscriptPath)
	root, err := os.OpenRoot(base)
	if err != nil {
		return fmt.Errorf("sessionarchive: open transcript dir: %w", err)
	}
	defer func() { _ = root.Close() }()
	info, err := root.Lstat(name)
	if err != nil {
		return fmt.Errorf("sessionarchive: stat transcript: %w", err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("sessionarchive: transcript %s is %s, not a regular file", c.src.TranscriptPath, info.Mode().Type())
	}
	c.roots[RootTranscript] = base
	return c.file(ctx, root, RootTranscript, name, info)
}

func (c *capturer) trees() ([]tree, error) {
	var trees []tree
	for _, t := range []tree{
		{root: RootSession, base: c.src.SessionDir, start: "."},
		{root: RootFileHistory, base: c.src.FileHistoryDir, start: "."},
		{root: RootScratchpad, base: c.src.ScratchpadDir, start: ".", required: true},
	} {
		if t.base != "" {
			trees = append(trees, t)
		}
	}
	for _, items := range []struct {
		root  Root
		paths []string
	}{
		{RootTasks, c.src.TaskListDirs},
		{RootPlans, c.src.PlanFiles},
		{RootPasteCache, c.src.PasteFiles},
	} {
		for _, p := range slices.Compact(slices.Sorted(slices.Values(items.paths))) {
			trees = append(trees, tree{root: items.root, base: filepath.Dir(p), start: filepath.Base(p), required: true})
		}
	}
	for _, t := range trees {
		if base, ok := c.roots[t.root]; ok && base != t.base {
			return nil, fmt.Errorf("sessionarchive: %s items span %s and %s", t.root, base, t.base)
		}
		c.roots[t.root] = t.base
	}
	return trees, nil
}

func (c *capturer) walk(ctx context.Context, t tree) error {
	root, err := os.OpenRoot(t.base)
	if errors.Is(err, fs.ErrNotExist) {
		c.absent(t)
		return nil
	}
	if err != nil {
		return fmt.Errorf("sessionarchive: open %s: %w", t.root, err)
	}
	defer func() { _ = root.Close() }()
	return fs.WalkDir(root.FS(), t.start, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if p == t.start && errors.Is(err, fs.ErrNotExist) {
				c.absent(t)
				return nil
			}
			return fmt.Errorf("sessionarchive: walk %s/%s: %w", t.root, p, err)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if p == "." {
			return nil
		}
		if excludedName(d.Name()) {
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return fmt.Errorf("sessionarchive: stat %s/%s: %w", t.root, p, err)
		}
		switch {
		case info.IsDir():
			c.entries = append(c.entries, Entry{Root: t.root, Path: p, Mode: info.Mode()})
		case info.Mode()&fs.ModeSymlink != 0:
			link, err := root.Readlink(filepath.FromSlash(p))
			if err != nil {
				return fmt.Errorf("sessionarchive: readlink %s/%s: %w", t.root, p, err)
			}
			c.entries = append(c.entries, Entry{Root: t.root, Path: p, Mode: info.Mode(), Link: link})
		case info.Mode().IsRegular():
			return c.file(ctx, root, t.root, p, info)
		}
		return nil
	})
}

func (c *capturer) absent(t tree) {
	if t.required {
		c.missing = append(c.missing, t.label())
	}
}

func (c *capturer) file(ctx context.Context, root *os.Root, r Root, p string, info fs.FileInfo) error {
	key := Entry{Root: r, Path: p}.Key()
	old, hasOld := c.prev[key]
	if hasOld && old.Mode == info.Mode() && old.Stat.same(statOf(info)) {
		c.entries = append(c.entries, old)
		return nil
	}
	for range 2 {
		e, torn, err := c.read(ctx, root, r, p)
		if err != nil {
			return err
		}
		if !torn {
			c.entries = append(c.entries, e)
			return nil
		}
	}
	slog.Debug("sessionarchive: deferring a file that changed under every read", "session", c.src.SessionID, "path", key)
	c.deferred = append(c.deferred, key)
	if hasOld {
		c.entries = append(c.entries, old)
	}
	return nil
}

func (c *capturer) read(ctx context.Context, root *os.Root, r Root, p string) (Entry, bool, error) {
	f, err := root.Open(filepath.FromSlash(p))
	if err != nil {
		return Entry{}, false, fmt.Errorf("sessionarchive: open %s/%s: %w", r, p, err)
	}
	defer func() { _ = f.Close() }()
	before, err := f.Stat()
	if err != nil {
		return Entry{}, false, fmt.Errorf("sessionarchive: stat %s/%s: %w", r, p, err)
	}
	e := Entry{Root: r, Path: p, Mode: before.Mode(), Size: before.Size(), Stat: statOf(before)}
	frozen := freezes(r, p)
	media := MediaFile
	var scanner *referenceScanner
	if frozen {
		complete, err := lastNewline(f, before.Size())
		if errors.Is(err, io.EOF) {
			return Entry{}, true, nil
		}
		if err != nil {
			return Entry{}, false, fmt.Errorf("sessionarchive: freeze %s/%s: %w", r, p, err)
		}
		e.Size, e.DroppedTailBytes = complete, before.Size()-complete
		media = MediaTranscript
		scanner = c.scanner()
	}
	body := io.LimitReader(f, e.Size)
	if scanner != nil {
		body = io.TeeReader(body, scanner)
	}
	ref, err := c.store.Put(ctx, body, media)
	if err != nil {
		return Entry{}, false, fmt.Errorf("sessionarchive: put %s/%s: %w", r, p, err)
	}
	after, err := f.Stat()
	if err != nil {
		return Entry{}, false, fmt.Errorf("sessionarchive: restat %s/%s: %w", r, p, err)
	}
	if ref.Size != e.Size || torn(before, after, frozen) {
		return Entry{}, true, nil
	}
	e.Ref = ref
	if scanner != nil {
		e.References = scanner.sorted()
	}
	return e, false, nil
}

func (c *capturer) scanner() *referenceScanner {
	s := &referenceScanner{found: map[string]struct{}{}}
	if c.src.SessionDir != "" {
		s.toolResults = []byte(c.src.SessionDir + "/tool-results/")
	}
	s.fileHistory = c.src.FileHistoryDir != ""
	return s
}

func (c *capturer) manifest() Manifest {
	present := make(map[string]struct{}, len(c.entries))
	for _, e := range c.entries {
		present[e.Key()] = struct{}{}
	}
	missing := c.missing
	for _, e := range c.entries {
		for _, ref := range e.References {
			if _, ok := present[ref]; !ok {
				missing = append(missing, ref)
			}
		}
	}
	slices.SortFunc(c.entries, func(a, b Entry) int {
		return cmp.Or(cmp.Compare(a.Root, b.Root), cmp.Compare(a.Path, b.Path))
	})
	slices.Sort(missing)
	slices.Sort(c.deferred)
	return Manifest{
		Format:         Format,
		SessionID:      c.src.SessionID,
		SourceHost:     c.src.SourceHost,
		CapturedAt:     c.src.CapturedAt.UTC(),
		ClaudeVersion:  c.src.ClaudeVersion,
		ConfigDir:      c.src.ConfigDir,
		ProjectDirName: c.src.ProjectDirName,
		Cwd:            c.src.Cwd,
		OriginalCwd:    c.src.OriginalCwd,
		GitBranch:      c.src.GitBranch,
		Title:          c.src.Title,
		LastHuman:      c.src.LastHuman.UTC(),
		LastAutonomous: c.src.LastAutonomous.UTC(),
		Roots:          c.roots,
		Entries:        c.entries,
		Completeness:   Completeness{Missing: slices.Compact(missing), Deferred: c.deferred},
	}
}

func sameContent(m, prev Manifest) (bool, error) {
	m.CapturedAt = prev.CapturedAt
	a, err := json.Marshal(m)
	if err != nil {
		return false, fmt.Errorf("sessionarchive: encode manifest: %w", err)
	}
	b, err := json.Marshal(prev)
	if err != nil {
		return false, fmt.Errorf("sessionarchive: encode previous manifest: %w", err)
	}
	return bytes.Equal(a, b), nil
}

func publish(ctx context.Context, store Store, m Manifest) (Archive, error) {
	body, err := json.Marshal(m)
	if err != nil {
		return Archive{}, fmt.Errorf("sessionarchive: encode manifest: %w", err)
	}
	manifestRef, err := store.Put(ctx, bytes.NewReader(body), MediaManifest)
	if err != nil {
		return Archive{}, fmt.Errorf("sessionarchive: put manifest: %w", err)
	}
	deps := m.Refs()
	for len(deps)+1 > artifact.MaxDeps {
		groups := make([]artifact.Ref, 0, (len(deps)+artifact.MaxDeps-1)/artifact.MaxDeps)
		for chunk := range slices.Chunk(deps, artifact.MaxDeps) {
			ref, err := store.PutGroup(ctx, MediaSessionDeps, chunk)
			if err != nil {
				return Archive{}, fmt.Errorf("sessionarchive: put dependency group: %w", err)
			}
			groups = append(groups, ref)
		}
		deps = groups
	}
	ref, err := store.PutGroup(ctx, MediaSession, append([]artifact.Ref{manifestRef}, deps...))
	if err != nil {
		return Archive{}, fmt.Errorf("sessionarchive: put session group: %w", err)
	}
	return Archive{Ref: ref, ManifestRef: manifestRef, Manifest: m}, nil
}

func excludedName(name string) bool {
	return excludedNames[name] || strings.HasSuffix(name, ".key") || strings.HasSuffix(name, ".lock")
}

func freezes(r Root, p string) bool {
	return r == RootTranscript || r == RootSession && strings.HasPrefix(p, "subagents/") && strings.HasSuffix(p, ".jsonl")
}

func statOf(info fs.FileInfo) Stat {
	return Stat{Inode: info.Sys().(*syscall.Stat_t).Ino, Size: info.Size(), ModTime: info.ModTime().UTC()}
}

func torn(before, after fs.FileInfo, frozen bool) bool {
	if frozen {
		return after.Size() < before.Size()
	}
	return after.Size() != before.Size() || !after.ModTime().Equal(before.ModTime())
}

func lastNewline(f io.ReaderAt, size int64) (int64, error) {
	buf := make([]byte, 64<<10)
	for end := size; end > 0; {
		start := max(end-int64(len(buf)), 0)
		window := buf[:end-start]
		if _, err := f.ReadAt(window, start); err != nil {
			return 0, err
		}
		if i := bytes.LastIndexByte(window, '\n'); i >= 0 {
			return start + int64(i) + 1, nil
		}
		end = start
	}
	return 0, nil
}

type referenceScanner struct {
	toolResults []byte
	fileHistory bool
	line        []byte
	found       map[string]struct{}
}

func (s *referenceScanner) Write(p []byte) (int, error) {
	n := len(p)
	for {
		i := bytes.IndexByte(p, '\n')
		if i < 0 {
			s.line = append(s.line, p...)
			return n, nil
		}
		if len(s.line) == 0 {
			s.scan(p[:i])
		} else {
			s.line = append(s.line, p[:i]...)
			s.scan(s.line)
			s.line = s.line[:0]
		}
		p = p[i+1:]
	}
}

func (s *referenceScanner) scan(line []byte) {
	if len(s.toolResults) > 0 {
		s.collect(line, s.toolResults, string(RootSession)+"/tool-results/", isPathByte)
	}
	if s.fileHistory {
		s.collect(line, backupFileNameMarker, string(RootFileHistory)+"/", func(b byte) bool { return b != '"' })
	}
}

func (s *referenceScanner) collect(line, marker []byte, prefix string, keep func(byte) bool) {
	for {
		i := bytes.Index(line, marker)
		if i < 0 {
			return
		}
		line = line[i+len(marker):]
		end := 0
		for end < len(line) && keep(line[end]) {
			end++
		}
		if name := strings.TrimRight(string(line[:end]), "./"); name != "" {
			s.found[prefix+name] = struct{}{}
		}
		line = line[end:]
	}
}

func (s *referenceScanner) sorted() []string {
	return slices.Sorted(maps.Keys(s.found))
}

func isPathByte(b byte) bool {
	return 'a' <= b && b <= 'z' || 'A' <= b && b <= 'Z' || '0' <= b && b <= '9' || b == '.' || b == '_' || b == '-' || b == '/'
}
