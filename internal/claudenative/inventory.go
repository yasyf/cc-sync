package claudenative

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"
)

// SidecarSet names the native state a session owns outside its main transcript.
type SidecarSet struct {
	SessionDir  bool
	Subagents   int
	ToolResults int
	Workflows   int
	FileHistory bool
	// TaskLists are the existing tasks/<listId> dirs of the session: its id,
	// "session-" plus the id's first 8 hex digits, and team names its
	// tool calls passed as team_name.
	TaskLists []string
	// Plans are absolute plan files from planFilePath attachments that
	// report the plan exists, plus plans/<slug>.md for the record slug when present.
	Plans []string
	// PasteHashes name paste-cache/<hash>.txt blobs from history.jsonl.
	PasteHashes []string
	Scratchpad  string
}

// TranscriptStat describes the main transcript as last scanned. CompleteSize
// ends at the last complete, parseable record; PartialTail reports bytes past it.
type TranscriptStat struct {
	Size         int64
	CompleteSize int64
	ModTime      time.Time
	Inode        uint64
	PartialTail  bool
	Records      int
	LeafUUID     string
}

// Session is one native Claude session as seen by Scan.
type Session struct {
	ID             SessionID
	ConfigDir      string
	ProjectDirName string
	TranscriptPath string
	// Cwd is Claude's effective cwd: the last relocated cwd, else OriginalCwd.
	Cwd         string
	OriginalCwd string
	GitBranch   string
	Version     string
	Title       string
	Entrypoint  string
	// Repo is nil for an unprotected session: outside every registered
	// repo, inside a LocalOnly one, or scanned without a RepoResolver.
	Repo                   *RepoBinding
	LastHumanInput         time.Time
	LastAutonomousActivity time.Time
	LastActivity           time.Time
	Live                   *LiveProcess
	// Duplicates are other projects/*/<id>.jsonl copies that make resume ambiguous.
	Duplicates []string
	Transcript TranscriptStat
	Sidecars   SidecarSet
}

// RepoBinding places a session cwd inside a registered reposync repo.
type RepoBinding struct {
	Origin       string
	Relpath      string
	RegistryPath string
	CheckoutRoot string
	WorktreeRel  string
	Linked       bool
}

// RepoResolver maps a cwd to its registered repo through the git common dir;
// ok is false outside every registered repo and inside a LocalOnly one.
type RepoResolver interface {
	Resolve(ctx context.Context, cwd string) (RepoBinding, bool, error)
}

// FileCursor is Scan's memory of one transcript: its identity at the last
// scan, how far complete records were folded, and what they said. Boundary
// hashes the ends of the folded prefix, so a same-inode rewrite forces a
// reparse; SidecarStamp hashes the stats of the session's sidecar roots, so a
// sidecar change is discovered without a transcript change.
type FileCursor struct {
	Inode        uint64
	Size         int64
	ModTime      time.Time
	ChangeTime   time.Time
	ParsedOffset int64
	Boundary     Digest
	Summary      TranscriptSummary
	Sidecars     SidecarSet
	SidecarStamp Digest
}

// HistoryState is what history.jsonl has said about one session so far.
type HistoryState struct {
	LastInput   time.Time
	PasteHashes []string
}

// Cursor carries Scan state between runs; the zero Cursor forces a full scan.
// HistoryBoundary hashes the ends of history.jsonl's folded prefix.
type Cursor struct {
	Files           map[string]FileCursor
	HistoryOffset   int64
	HistoryInode    uint64
	HistoryBoundary Digest
	History         map[SessionID]HistoryState
}

// ScanOptions configure Scan. Repos and Live are optional: without them every
// session is reported unprotected and not live.
type ScanOptions struct {
	Layout Layout
	Repos  RepoResolver
	Live   map[SessionID]LiveProcess
	Only   []SessionID
}

type transcriptFile struct {
	path    string
	dirName string
	info    fs.FileInfo
}

type scannedCopy struct {
	transcriptFile
	cursor FileCursor
}

type historyLine struct {
	SessionID      string `json:"sessionId"`
	Timestamp      int64  `json:"timestamp"`
	PastedContents map[string]struct {
		ContentHash string `json:"contentHash"`
	} `json:"pastedContents"`
}

// Scan inventories every projects/*/<uuid>.jsonl, reading only transcript
// bytes past each cursor's ParsedOffset and history.jsonl past HistoryOffset;
// an inode change, a shrink below the parsed prefix, or rewritten bytes at
// either end of that prefix force a reparse. It returns one Session per
// id, sorted by id, preferring the copy in the project dir Claude would derive
// from the session cwd, then the newest; the rest are reported as Duplicates.
func Scan(ctx context.Context, opts ScanOptions, prev Cursor) ([]Session, Cursor, error) {
	l := opts.Layout
	next := Cursor{Files: make(map[string]FileCursor)}
	if err := tailHistory(l, prev, &next); err != nil {
		return nil, Cursor{}, err
	}
	copies, err := listTranscripts(l)
	if err != nil {
		return nil, Cursor{}, err
	}
	only := make(map[SessionID]bool, len(opts.Only))
	for _, id := range opts.Only {
		only[id] = true
	}
	repos := make(map[string]*RepoBinding)
	var sessions []Session
	for _, id := range slices.Sorted(maps.Keys(copies)) {
		if err := ctx.Err(); err != nil {
			return nil, Cursor{}, fmt.Errorf("scan sessions: %w", err)
		}
		if len(only) > 0 && !only[id] {
			for _, f := range copies[id] {
				if fc, ok := prev.Files[f.path]; ok {
					next.Files[f.path] = fc
				}
			}
			continue
		}
		scanned := make([]scannedCopy, 0, len(copies[id]))
		for _, f := range copies[id] {
			fc, err := refreshTranscript(l, id, f, prev.Files[f.path])
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			if err != nil {
				return nil, Cursor{}, err
			}
			next.Files[f.path] = fc
			scanned = append(scanned, scannedCopy{transcriptFile: f, cursor: fc})
		}
		if len(scanned) == 0 {
			continue
		}
		s, err := assemble(ctx, opts, id, scanned, next.History[id], repos)
		if err != nil {
			return nil, Cursor{}, err
		}
		sessions = append(sessions, s)
	}
	for id := range next.History {
		if _, ok := copies[id]; !ok {
			delete(next.History, id)
		}
	}
	return sessions, next, nil
}

func listTranscripts(l Layout) (map[SessionID][]transcriptFile, error) {
	root := ProjectsDir(l.ConfigDir)
	dirs, err := os.ReadDir(root)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read projects dir: %w", err)
	}
	copies := make(map[SessionID][]transcriptFile)
	for _, d := range dirs {
		if !d.IsDir() {
			continue
		}
		entries, err := os.ReadDir(filepath.Join(root, d.Name()))
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read project dir %s: %w", d.Name(), err)
		}
		for _, e := range entries {
			stem, ok := strings.CutSuffix(e.Name(), ".jsonl")
			if !ok || !e.Type().IsRegular() {
				continue
			}
			id, err := ParseSessionID(stem)
			if err != nil || string(id) != stem {
				continue
			}
			info, err := e.Info()
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			if err != nil {
				return nil, fmt.Errorf("stat transcript %s: %w", e.Name(), err)
			}
			path := filepath.Join(root, d.Name(), e.Name())
			copies[id] = append(copies[id], transcriptFile{path: path, dirName: d.Name(), info: info})
		}
	}
	return copies, nil
}

func refreshTranscript(l Layout, id SessionID, f transcriptFile, prev FileCursor) (FileCursor, error) {
	inode, size, mtime, ctime := inodeOf(f.info), f.info.Size(), f.info.ModTime(), changeTime(f.info)
	fc := prev
	changed := prev.Inode != inode || prev.Size != size || !prev.ModTime.Equal(mtime) || !prev.ChangeTime.Equal(ctime)
	if changed {
		var err error
		if fc, err = refold(f.path, inode, size, prev); err != nil {
			return FileCursor{}, err
		}
		fc.Inode, fc.Size, fc.ModTime, fc.ChangeTime = inode, size, mtime, ctime
	}
	sessionDir := filepath.Join(filepath.Dir(f.path), string(id))
	stamp, err := sidecarStamp(l, id, sessionDir, fc.Summary)
	if err != nil {
		return FileCursor{}, err
	}
	if changed || stamp != prev.SidecarStamp {
		sidecars, err := discoverSidecars(l, id, sessionDir, fc.Summary)
		if err != nil {
			return FileCursor{}, err
		}
		fc.Sidecars, fc.SidecarStamp = sidecars, stamp
	}
	return fc, nil
}

func refold(path string, inode uint64, size int64, prev FileCursor) (FileCursor, error) {
	f, err := os.Open(path) //nolint:gosec // G304: a transcript under the scanned Claude config dir, opened read-only.
	if err != nil {
		return FileCursor{}, fmt.Errorf("open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()
	intact := prev.Inode == inode
	if intact {
		if intact, err = prefixIntact(f, size, prev.ParsedOffset, prev.Boundary); err != nil {
			return FileCursor{}, err
		}
	}
	fc := prev
	if !intact {
		fc = FileCursor{}
	}
	summary := fc.Summary.clone()
	end, err := foldCompleteLines(f, fc.ParsedOffset, size, func(line []byte) error {
		r, ts, err := decodeRecord(line)
		if err != nil {
			return err
		}
		return summary.fold(&r, ts, line)
	})
	if err != nil {
		return FileCursor{}, err
	}
	boundary, err := boundaryDigest(f, end)
	if err != nil {
		return FileCursor{}, err
	}
	fc.Summary, fc.ParsedOffset, fc.Boundary = summary, end, boundary
	return fc, nil
}

func assemble(ctx context.Context, opts ScanOptions, id SessionID, copies []scannedCopy, hist HistoryState, repos map[string]*RepoBinding) (Session, error) {
	slices.SortFunc(copies, func(a, b scannedCopy) int {
		aHome := a.dirName == ProjectDirName(a.cursor.Summary.Cwd())
		bHome := b.dirName == ProjectDirName(b.cursor.Summary.Cwd())
		switch {
		case aHome != bHome && aHome:
			return -1
		case aHome != bHome:
			return 1
		}
		return cmp.Or(b.info.ModTime().Compare(a.info.ModTime()), strings.Compare(a.path, b.path))
	})
	c := copies[0]
	sum := c.cursor.Summary
	s := Session{
		ID:                     id,
		ConfigDir:              opts.Layout.ConfigDir,
		ProjectDirName:         c.dirName,
		TranscriptPath:         c.path,
		Cwd:                    sum.Cwd(),
		OriginalCwd:            sum.OriginalCwd,
		GitBranch:              sum.GitBranch,
		Version:                sum.Version,
		Title:                  sum.Title(),
		Entrypoint:             sum.Entrypoint,
		LastHumanInput:         later(sum.LastHuman, hist.LastInput),
		LastAutonomousActivity: sum.LastAutonomous,
		Transcript: TranscriptStat{
			Size:         c.cursor.Size,
			CompleteSize: c.cursor.ParsedOffset,
			ModTime:      c.cursor.ModTime,
			Inode:        c.cursor.Inode,
			PartialTail:  c.cursor.Size > c.cursor.ParsedOffset,
			Records:      sum.Records,
			LeafUUID:     sum.LeafUUID,
		},
		Sidecars: c.cursor.Sidecars,
	}
	s.LastActivity = later(s.LastHumanInput, s.LastAutonomousActivity)
	for _, dup := range copies[1:] {
		s.Duplicates = append(s.Duplicates, dup.path)
	}
	slices.Sort(s.Duplicates)
	s.Sidecars.PasteHashes = slices.Clone(hist.PasteHashes)
	scratchpad, err := findScratchpad(opts.Layout, id, sum)
	if err != nil {
		return Session{}, err
	}
	s.Sidecars.Scratchpad = scratchpad
	if p, ok := opts.Live[id]; ok {
		s.Live = &p
	}
	if opts.Repos != nil && s.Cwd != "" {
		binding, seen := repos[s.Cwd]
		if !seen {
			b, ok, err := opts.Repos.Resolve(ctx, s.Cwd)
			if err != nil {
				return Session{}, fmt.Errorf("resolve repo for session %s: %w", id, err)
			}
			if ok {
				binding = &b
			}
			repos[s.Cwd] = binding
		}
		s.Repo = binding
	}
	return s, nil
}

func discoverSidecars(l Layout, id SessionID, sessionDir string, sum TranscriptSummary) (SidecarSet, error) {
	var s SidecarSet
	var err error
	if s.SessionDir, err = isDir(sessionDir); err != nil {
		return SidecarSet{}, err
	}
	if s.SessionDir {
		if err := countSessionTree(sessionDir, &s); err != nil {
			return SidecarSet{}, err
		}
	}
	if s.FileHistory, err = isDir(l.fileHistoryDir(id)); err != nil {
		return SidecarSet{}, err
	}
	for _, listID := range taskListCandidates(id, sum) {
		ok, err := isDir(l.taskListDir(listID))
		if err != nil {
			return SidecarSet{}, err
		}
		if ok {
			s.TaskLists = insertSorted(s.TaskLists, listID)
		}
	}
	s.Plans = slices.Clone(sum.PlanFiles)
	for _, slug := range planSlugs(sum) {
		ok, err := isRegular(l.planPath(slug))
		if err != nil {
			return SidecarSet{}, err
		}
		if ok {
			s.Plans = insertSorted(s.Plans, l.planPath(slug))
		}
	}
	return s, nil
}

func taskListCandidates(id SessionID, sum TranscriptSummary) []string {
	var ids []string
	for _, listID := range append([]string{string(id), "session-" + string(id)[:8]}, sum.TeamNames...) {
		if isPathElement(listID) {
			ids = append(ids, listID)
		}
	}
	return ids
}

func planSlugs(sum TranscriptSummary) []string {
	var slugs []string
	for _, slug := range sum.Slugs {
		if isPathElement(slug) {
			slugs = append(slugs, slug)
		}
	}
	return slugs
}

func sidecarStamp(l Layout, id SessionID, sessionDir string, sum TranscriptSummary) (Digest, error) {
	roots := []string{sessionDir}
	for _, dir := range []string{sessionDir, filepath.Join(sessionDir, "subagents", "workflows")} {
		subs, err := subdirs(dir)
		if err != nil {
			return Digest{}, err
		}
		roots = append(roots, subs...)
	}
	roots = append(roots, l.fileHistoryDir(id))
	for _, listID := range taskListCandidates(id, sum) {
		roots = append(roots, l.taskListDir(listID))
	}
	for _, slug := range planSlugs(sum) {
		roots = append(roots, l.planPath(slug))
	}
	h := sha256.New()
	for _, root := range roots {
		info, err := os.Stat(root)
		switch {
		case errors.Is(err, fs.ErrNotExist):
			_, _ = fmt.Fprintf(h, "%s\x00absent\n", root)
		case err != nil:
			return Digest{}, fmt.Errorf("stat %s: %w", root, err)
		default:
			_, _ = fmt.Fprintf(h, "%s\x00%d\x00%d\x00%s\n", root, inodeOf(info), info.ModTime().UnixNano(), info.Mode())
		}
	}
	var d Digest
	h.Sum(d[:0])
	return d, nil
}

func subdirs(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", dir, err)
	}
	var dirs []string
	for _, e := range entries {
		if e.IsDir() {
			dirs = append(dirs, filepath.Join(dir, e.Name()))
		}
	}
	return dirs, nil
}

func countSessionTree(dir string, s *SidecarSet) error {
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		parts := strings.Split(filepath.ToSlash(rel), "/")
		switch {
		case parts[0] == "subagents" && strings.HasPrefix(d.Name(), "agent-") && strings.HasSuffix(d.Name(), ".jsonl"):
			s.Subagents++
		case parts[0] == "tool-results":
			s.ToolResults++
		case parts[0] == "workflows" && len(parts) == 2 && strings.HasSuffix(d.Name(), ".json"):
			s.Workflows++
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("walk session dir %s: %w", dir, err)
	}
	return nil
}

func findScratchpad(l Layout, id SessionID, sum TranscriptSummary) (string, error) {
	for _, cwd := range slices.Compact([]string{sum.Cwd(), sum.OriginalCwd}) {
		if cwd == "" {
			continue
		}
		dir := ScratchpadDir(l.TmpRoot, l.UID, cwd, id)
		ok, err := isDir(dir)
		if err != nil {
			return "", err
		}
		if ok {
			return dir, nil
		}
	}
	return "", nil
}

func tailHistory(l Layout, prev Cursor, next *Cursor) error {
	f, err := os.Open(l.historyPath())
	if errors.Is(err, fs.ErrNotExist) {
		next.History = make(map[SessionID]HistoryState)
		return nil
	}
	if err != nil {
		return fmt.Errorf("open history: %w", err)
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat history: %w", err)
	}
	inode, size := inodeOf(info), info.Size()
	from := prev.HistoryOffset
	next.History = maps.Clone(prev.History)
	intact := next.History != nil && inode == prev.HistoryInode
	if intact {
		if intact, err = prefixIntact(f, size, from, prev.HistoryBoundary); err != nil {
			return err
		}
	}
	if !intact {
		from = 0
		next.History = make(map[SessionID]HistoryState)
	}
	end, err := foldCompleteLines(f, from, size, func(line []byte) error {
		var h historyLine
		if err := json.Unmarshal(line, &h); err != nil {
			return fmt.Errorf("decode history line: %w", err)
		}
		id, err := ParseSessionID(h.SessionID)
		if err != nil {
			return nil
		}
		state := next.History[id]
		state.LastInput = later(state.LastInput, time.UnixMilli(h.Timestamp))
		for _, p := range h.PastedContents {
			if p.ContentHash != "" {
				state.PasteHashes = insertSorted(slices.Clone(state.PasteHashes), p.ContentHash)
			}
		}
		next.History[id] = state
		return nil
	})
	if err != nil {
		return err
	}
	boundary, err := boundaryDigest(f, end)
	if err != nil {
		return err
	}
	next.HistoryOffset, next.HistoryInode, next.HistoryBoundary = end, inode, boundary
	return nil
}

func isPathElement(name string) bool {
	return name != "" && name != "." && name != ".." && !strings.ContainsRune(name, filepath.Separator)
}

func inodeOf(info fs.FileInfo) uint64 {
	return info.Sys().(*syscall.Stat_t).Ino
}

func isDir(path string) (bool, error) {
	info, err := os.Stat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("stat %s: %w", path, err)
	}
	return info.IsDir(), nil
}

func isRegular(path string) (bool, error) {
	info, err := os.Stat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("stat %s: %w", path, err)
	}
	return info.Mode().IsRegular(), nil
}
