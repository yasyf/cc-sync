// Package sessionrestore installs a picked-up Claude Code session into the
// destination's native store so a plain `claude --resume <id>` continues it
// in the recovered worktree.
//
// Its input is a replica directory that pickup materializes from the archive
// at <ReplicaRoot>/<origin>/<session id>/<checkpoint>/:
//
//	meta.json          Meta: the source layout and the checkpoint identity
//	transcript.jsonl   the main transcript
//	session/           the <session id>/ tree: subagents/, tool-results/, workflows/
//	file-history/      the contents of file-history/<session id>/
//	tasks/<listId>/    task lists the session used
//	plans/<slug>.md    plan files the session referenced
//	paste-cache/       paste blobs referenced from history.jsonl
//	scratchpad/        the <tmp>/claude-<uid>/<project>/<session id>/ tree, symlinks kept
//
// Everything but meta.json and transcript.jsonl is optional. Prepare reads the
// replica and the native store and returns a Plan without writing anything;
// Apply stages the plan under <config>/.cc-sync-staging/<nonce> and renames it
// into place behind a journal, sidecars first and the transcript last, so a
// failure or crash at any point rolls back.
package sessionrestore

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/yasyf/cc-sync/internal/claudenative"
)

const (
	metaFile       = "meta.json"
	transcriptFile = "transcript.jsonl"
)

// ErrLiveLocal reports that the session is running on this host; pickup never overrides it.
var ErrLiveLocal = errors.New("session is live locally")

// Meta is a replica's meta.json.
type Meta struct {
	SessionID       claudenative.SessionID `json:"session_id"`
	SourceHost      string                 `json:"source_host"`
	SourceConfigDir string                 `json:"source_config_dir"`
	SourceTmpRoot   string                 `json:"source_tmp_root"`
	SourceUID       int                    `json:"source_uid"`
	SourceCwd       string                 `json:"source_cwd"`
	SourceHome      string                 `json:"source_home"`
	CheckpointID    string                 `json:"checkpoint_id"`
	CapturedAt      time.Time              `json:"captured_at"`
	LeafUUID        string                 `json:"leaf_uuid"`
	PrefixDigest    string                 `json:"prefix_digest"`
	ClaudeVersion   string                 `json:"claude_version"`
}

// ReadMeta reads and validates <replica>/meta.json.
func ReadMeta(replica string) (Meta, error) {
	path := filepath.Join(replica, metaFile)
	b, err := os.ReadFile(path) //nolint:gosec // G304: the replica pickup materialized under ReplicaRoot.
	if err != nil {
		return Meta{}, fmt.Errorf("read replica meta: %w", err)
	}
	var m Meta
	if err := json.Unmarshal(b, &m); err != nil {
		return Meta{}, fmt.Errorf("decode %s: %w", path, err)
	}
	if m.SessionID, err = claudenative.ParseSessionID(string(m.SessionID)); err != nil {
		return Meta{}, fmt.Errorf("%s: %w", path, err)
	}
	for name, p := range map[string]string{
		"source_config_dir": m.SourceConfigDir, "source_tmp_root": m.SourceTmpRoot,
		"source_cwd": m.SourceCwd, "source_home": m.SourceHome,
	} {
		if !filepath.IsAbs(p) {
			return Meta{}, fmt.Errorf("%s: %s %q is not absolute", path, name, p)
		}
	}
	return m, nil
}

// Target is where the session lands: the destination Claude layout, home,
// the recovered cwd, and the checkout roots reposync restored.
type Target struct {
	Layout    claudenative.Layout
	Home      string
	Cwd       string
	Checkouts PathMap
}

// Divergence is the --on-divergence choice for a local copy that holds
// records the picked checkpoint lacks.
type Divergence string

// Divergence choices; the zero value refuses.
const (
	DivergenceRefuse    Divergence = "refuse"
	DivergenceKeepLocal Divergence = "keep-local"
	DivergenceReplace   Divergence = "replace"
	DivergenceFork      Divergence = "fork"
)

// ParseDivergence validates an --on-divergence value.
func ParseDivergence(s string) (Divergence, error) {
	switch d := Divergence(s); d {
	case DivergenceRefuse, DivergenceKeepLocal, DivergenceReplace, DivergenceFork:
		return d, nil
	}
	return "", fmt.Errorf("invalid --on-divergence %q: want refuse, keep-local, replace, or fork", s)
}

// Options steers Prepare.
type Options struct {
	OnDivergence Divergence
	// Now dates the checkpoint age and the displaced-copy directory.
	Now time.Time
	// DisplacedRoot is <Dir>/displaced; replace moves local copies below it.
	DisplacedRoot string
	Procs         claudenative.ProcessLister
	Capabilities  Capabilities
	// OmitRecoveryPrompt drops --append-system-prompt from the launch even
	// when the destination claude advertises it.
	OmitRecoveryPrompt bool
}

// Mode is how the picked history lands.
type Mode string

// Modes.
const (
	ModeFresh       Mode = "fresh"
	ModeFastForward Mode = "fast-forward"
	ModeReplace     Mode = "replace"
	ModeFork        Mode = "fork"
	ModeKeepLocal   Mode = "keep-local"
)

// DivergentLocalError reports a local copy holding records the picked
// checkpoint lacks; both histories stay untouched.
type DivergentLocalError struct {
	SessionID         claudenative.SessionID
	LocalPath         string
	LocalLeafUUID     string
	LocalLastActivity time.Time
	PickedLeafUUID    string
	PickedCapturedAt  time.Time
}

func (e *DivergentLocalError) Error() string {
	return fmt.Sprintf("session %s: local copy %s (leaf %s, last activity %s) diverges from the picked checkpoint (leaf %s, captured %s)",
		e.SessionID, e.LocalPath, e.LocalLeafUUID, e.LocalLastActivity.Format(time.RFC3339),
		e.PickedLeafUUID, e.PickedCapturedAt.Format(time.RFC3339))
}

// UnitKind names one installable piece of a session.
type UnitKind string

// Unit kinds, in install order.
const (
	UnitFileHistory UnitKind = "file-history"
	UnitTaskList    UnitKind = "tasks"
	UnitPlan        UnitKind = "plan"
	UnitPasteCache  UnitKind = "paste-cache"
	UnitScratchpad  UnitKind = "scratchpad"
	UnitSession     UnitKind = "session"
	UnitTranscript  UnitKind = "transcript"
)

// Install moves one replica file or tree to its native path. Replace swaps
// out whatever is there; otherwise an existing Dest is kept and the unit skipped.
type Install struct {
	Kind    UnitKind `json:"kind"`
	Source  string   `json:"source"`
	Dest    string   `json:"dest"`
	Replace bool     `json:"replace"`
}

// Displacement moves a divergent local copy or one of its sidecars aside intact.
type Displacement struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// Launch is how to resume the installed session.
type Launch struct {
	Argv     []string          `json:"argv"`
	Dir      string            `json:"dir"`
	EnvUnset []string          `json:"env_unset"`
	EnvSet   map[string]string `json:"env_set"`
}

// Plan is everything Apply does and pickup reports.
type Plan struct {
	SessionID       claudenative.SessionID `json:"session_id"`
	SourceSessionID claudenative.SessionID `json:"source_session_id"`
	Mode            Mode                   `json:"mode"`
	Meta            Meta                   `json:"meta"`
	Replica         string                 `json:"replica"`
	Layout          claudenative.Layout    `json:"layout"`
	Cwd             string                 `json:"cwd"`
	PathMap         PathMap                `json:"path_map"`
	Transcript      string                 `json:"transcript"`
	Installs        []Install              `json:"installs"`
	Displace        []Displacement         `json:"displace,omitempty"`
	DisplacedDir    string                 `json:"displaced_dir,omitempty"`
	// Duplicates are native copies of the session outside the destination
	// project dir; they are reported and never touched.
	Duplicates       []string   `json:"duplicates,omitempty"`
	Rewritten        int        `json:"rewritten"`
	Stripped         int        `json:"stripped"`
	DroppedTailBytes int64      `json:"dropped_tail_bytes"`
	Unmapped         []Unmapped `json:"unmapped,omitempty"`
	RecoveryContext  string     `json:"recovery_context"`
	Launch           Launch     `json:"launch"`
}

type relation uint8

const (
	relDivergent relation = iota
	relPrefix
	relIdentical
)

// Prepare plans the restore of replica into t. It reads the native store but
// never writes to it.
func Prepare(ctx context.Context, replica string, t Target, opts Options) (Plan, error) {
	if !opts.Capabilities.Resume {
		return Plan{}, &IncompatibleError{Capability: "resume", Detail: "`claude --help` does not advertise --resume"}
	}
	meta, err := ReadMeta(replica)
	if err != nil {
		return Plan{}, err
	}
	if err := probeProjectDirs(t.Layout.ConfigDir); err != nil {
		return Plan{}, err
	}
	sid := meta.SessionID
	live, err := claudenative.LiveSessions(ctx, t.Layout, opts.Procs)
	if err != nil {
		return Plan{}, fmt.Errorf("check live sessions: %w", err)
	}
	if proc, ok := live[sid]; ok {
		return Plan{}, fmt.Errorf("%w: %s (pid %d)", ErrLiveLocal, sid, proc.PID)
	}
	copies, err := filepath.Glob(filepath.Join(claudenative.ProjectsDir(t.Layout.ConfigDir), "*", string(sid)+".jsonl"))
	if err != nil {
		return Plan{}, fmt.Errorf("find local copies: %w", err)
	}
	p := Plan{
		SessionID: sid, SourceSessionID: sid, Meta: meta, Replica: replica,
		Layout: t.Layout, Cwd: t.Cwd, Transcript: claudenative.TranscriptPath(t.Layout.ConfigDir, t.Cwd, sid),
	}
	p.setIdentity(t, sid)
	relations, err := p.classify(copies)
	if err != nil {
		return Plan{}, err
	}
	var divergent []string
	for _, c := range copies {
		if relations[c] == relDivergent {
			divergent = append(divergent, c)
		}
	}
	switch {
	case len(divergent) == 0 && relations[p.Transcript] > relDivergent:
		p.Mode = ModeFastForward
	case len(divergent) == 0:
		p.Mode = ModeFresh
	case opts.OnDivergence == DivergenceKeepLocal:
		p.Mode = ModeKeepLocal
	case opts.OnDivergence == DivergenceReplace:
		p.Mode = ModeReplace
		p.DisplacedDir = filepath.Join(opts.DisplacedRoot, string(sid), opts.Now.UTC().Format(time.RFC3339))
		if p.Displace, err = displacements(t.Layout, sid, divergent, p.DisplacedDir); err != nil {
			return Plan{}, err
		}
	case opts.OnDivergence == DivergenceFork:
		p.Mode = ModeFork
		p.setIdentity(t, newSessionID())
	default:
		return Plan{}, divergentError(meta, divergent[0])
	}
	if p.Mode != ModeFork {
		for _, c := range copies {
			if c != p.Transcript && (p.Mode != ModeReplace || relations[c] != relDivergent) {
				p.Duplicates = append(p.Duplicates, c)
			}
		}
	}
	if p.Mode == ModeKeepLocal {
		p.Launch = launch(p, t, opts, false)
		return p, nil
	}
	if p.Installs, err = installs(replica, t, sid, p.SessionID); err != nil {
		return Plan{}, err
	}
	stats := relocation{}
	rel := p.relocator(&stats)
	for _, u := range p.Installs {
		if err := p.materialize(u, discardSink{}, rel); err != nil {
			return Plan{}, err
		}
	}
	if stats.sessionRecords == 0 {
		return Plan{}, &IncompatibleError{Capability: "transcript-format", Detail: fmt.Sprintf("no record in %s carries sessionId %s", filepath.Join(replica, transcriptFile), sid)}
	}
	p.Rewritten, p.Stripped, p.DroppedTailBytes, p.Unmapped = stats.Rewritten, stats.Stripped, stats.DroppedTailBytes, stats.Unmapped
	p.RecoveryContext = recoveryContext(p, opts.Now)
	p.Launch = launch(p, t, opts, true)
	return p, nil
}

func (p *Plan) setIdentity(t Target, id claudenative.SessionID) {
	p.SessionID = id
	p.Transcript = claudenative.TranscriptPath(t.Layout.ConfigDir, t.Cwd, id)
	p.PathMap = pathMap(p.Meta, t, id)
}

func (p *Plan) relocatedSuffix() []byte {
	if p.Cwd == p.Meta.SourceCwd {
		return nil
	}
	return relocatedRecord(p.SessionID, p.Cwd)
}

func pathMap(m Meta, t Target, to claudenative.SessionID) PathMap {
	from := m.SessionID
	cfg := t.Layout.ConfigDir
	srcProject := filepath.Join(claudenative.ProjectsDir(m.SourceConfigDir), claudenative.ProjectDirName(m.SourceCwd))
	dstProject := filepath.Join(claudenative.ProjectsDir(cfg), claudenative.ProjectDirName(t.Cwd))
	rules := append(PathMap{}, t.Checkouts...)
	rules = append(rules,
		PathRule{m.SourceCwd, t.Cwd},
		PathRule{srcProject, dstProject},
		PathRule{m.SourceConfigDir, cfg},
		PathRule{claudenative.ScratchpadDir(m.SourceTmpRoot, m.SourceUID, m.SourceCwd, from), claudenative.ScratchpadDir(t.Layout.TmpRoot, t.Layout.UID, t.Cwd, to)},
		PathRule{filepath.Join(m.SourceTmpRoot, "claude-"+strconv.Itoa(m.SourceUID)), filepath.Join(t.Layout.TmpRoot, "claude-"+strconv.Itoa(t.Layout.UID))},
		PathRule{m.SourceHome, t.Home},
	)
	if from != to {
		rules = append(rules,
			PathRule{filepath.Join(srcProject, string(from)), filepath.Join(dstProject, string(to))},
			PathRule{filepath.Join(m.SourceConfigDir, "file-history", string(from)), filepath.Join(cfg, "file-history", string(to))},
			PathRule{filepath.Join(m.SourceConfigDir, "tasks", string(from)), filepath.Join(cfg, "tasks", string(to))},
			PathRule{filepath.Join(m.SourceConfigDir, "tasks", sessionListID(from)), filepath.Join(cfg, "tasks", sessionListID(to))},
		)
	}
	return rules.sorted()
}

func sessionListID(id claudenative.SessionID) string {
	return "session-" + string(id)[:8]
}

func (p *Plan) relocator(stats *relocation) *relocator {
	return &relocator{paths: p.PathMap, fromID: p.SourceSessionID, toID: p.SessionID, strip: !p.PathMap.identity(), stats: stats}
}

func (p *Plan) classify(copies []string) (map[string]relation, error) {
	out := make(map[string]relation, len(copies))
	if len(copies) == 0 {
		return out, nil
	}
	meters := make([]*prefixMeter, len(copies))
	writers := make([]io.Writer, len(copies))
	for i, c := range copies {
		f, err := os.Open(c) //nolint:gosec // G304: a native copy of the session under the destination projects dir.
		if err != nil {
			return nil, fmt.Errorf("open local copy: %w", err)
		}
		defer func() { _ = f.Close() }()
		info, err := f.Stat()
		if err != nil {
			return nil, fmt.Errorf("stat local copy: %w", err)
		}
		meters[i] = &prefixMeter{f: f, size: info.Size()}
		writers[i] = meters[i]
	}
	src, err := os.Open(filepath.Join(p.Replica, transcriptFile))
	if err != nil {
		return nil, fmt.Errorf("open replica transcript: %w", err)
	}
	defer func() { _ = src.Close() }()
	rel := p.relocator(&relocation{})
	rel.begin(transcriptFile)
	body := &countingWriter{w: io.MultiWriter(writers...)}
	if err := rel.jsonl(src, body); err != nil {
		return nil, err
	}
	suffix := p.relocatedSuffix()
	if _, err := body.w.Write(suffix); err != nil {
		return nil, err
	}
	for i, c := range copies {
		r, err := meters[i].relation(suffix, body.n)
		if err != nil {
			return nil, err
		}
		out[c] = r
	}
	return out, nil
}

type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(b []byte) (int, error) {
	n, err := c.w.Write(b)
	c.n += int64(n)
	return n, err
}

type prefixMeter struct {
	f      *os.File
	size   int64
	common int64
	total  int64
	split  bool
	buf    []byte
}

func (m *prefixMeter) Write(b []byte) (int, error) {
	m.total += int64(len(b))
	if m.split {
		return len(b), nil
	}
	if cap(m.buf) < len(b) {
		m.buf = make([]byte, len(b))
	}
	local := m.buf[:len(b)]
	n, err := m.f.ReadAt(local, m.common)
	if err != nil && !errors.Is(err, io.EOF) {
		return 0, fmt.Errorf("read local copy: %w", err)
	}
	for i := range n {
		if local[i] != b[i] {
			m.common += int64(i)
			m.split = true
			return len(b), nil
		}
	}
	m.common += int64(n)
	m.split = n < len(b)
	return len(b), nil
}

func (m *prefixMeter) relation(suffix []byte, bodyLen int64) (relation, error) {
	switch {
	case m.common == m.size && m.size == m.total:
		return relIdentical, nil
	case m.common == m.size:
		return relPrefix, nil
	}
	cut := m.size - int64(len(suffix))
	if len(suffix) == 0 || cut < 0 || cut >= bodyLen || m.common < cut {
		return relDivergent, nil
	}
	tail := make([]byte, len(suffix)+1)
	if cut == 0 {
		tail = tail[1:]
		if _, err := m.f.ReadAt(tail, 0); err != nil {
			return 0, fmt.Errorf("read local copy: %w", err)
		}
		return relationOf(bytes.Equal(tail, suffix)), nil
	}
	if _, err := m.f.ReadAt(tail, cut-1); err != nil {
		return 0, fmt.Errorf("read local copy: %w", err)
	}
	return relationOf(tail[0] == '\n' && bytes.Equal(tail[1:], suffix)), nil
}

func relationOf(prefix bool) relation {
	if prefix {
		return relPrefix
	}
	return relDivergent
}

func divergentError(m Meta, local string) error {
	e := &DivergentLocalError{SessionID: m.SessionID, LocalPath: local, PickedLeafUUID: m.LeafUUID, PickedCapturedAt: m.CapturedAt}
	if info, err := os.Stat(local); err == nil {
		e.LocalLastActivity = info.ModTime()
	}
	e.LocalLeafUUID = leafUUID(local)
	return e
}

func leafUUID(path string) string {
	b, err := os.ReadFile(path) //nolint:gosec // G304: a native copy of the session under the destination projects dir.
	if err != nil {
		return ""
	}
	lines := bytes.Split(bytes.TrimRight(b, "\n"), []byte("\n"))
	for i := len(lines) - 1; i >= 0; i-- {
		if id, ok := decodeString(memberValue(lines[i], "uuid")); ok {
			return id
		}
	}
	return ""
}

func displacements(l claudenative.Layout, sid claudenative.SessionID, divergent []string, dir string) ([]Displacement, error) {
	items := make([]string, 0, 2*len(divergent)+3)
	for _, c := range divergent {
		items = append(items, c, strings.TrimSuffix(c, ".jsonl"))
	}
	items = append(items,
		filepath.Join(l.ConfigDir, "file-history", string(sid)),
		filepath.Join(l.ConfigDir, "tasks", string(sid)),
		filepath.Join(l.ConfigDir, "tasks", sessionListID(sid)),
	)
	tmpUser := filepath.Join(l.TmpRoot, "claude-"+strconv.Itoa(l.UID))
	scratch, err := filepath.Glob(filepath.Join(tmpUser, "*", string(sid)))
	if err != nil {
		return nil, fmt.Errorf("find scratchpads: %w", err)
	}
	var out []Displacement
	for _, item := range append(items, scratch...) {
		if _, err := os.Lstat(item); errors.Is(err, fs.ErrNotExist) {
			continue
		} else if err != nil {
			return nil, fmt.Errorf("stat %s: %w", item, err)
		}
		rel, err := filepath.Rel(l.ConfigDir, item)
		if strings.HasPrefix(item, tmpUser+string(filepath.Separator)) {
			rel, err = filepath.Rel(tmpUser, item)
			rel = filepath.Join("tmp", rel)
		}
		if err != nil {
			return nil, fmt.Errorf("displace %s: %w", item, err)
		}
		out = append(out, Displacement{From: item, To: filepath.Join(dir, rel)})
	}
	return out, nil
}

func installs(replica string, t Target, from, to claudenative.SessionID) ([]Install, error) {
	cfg := t.Layout.ConfigDir
	var out []Install
	add := func(kind UnitKind, rel, dest string, replace bool) error {
		src := filepath.Join(replica, rel)
		if _, err := os.Lstat(src); errors.Is(err, fs.ErrNotExist) {
			return nil
		} else if err != nil {
			return fmt.Errorf("stat replica %s: %w", rel, err)
		}
		out = append(out, Install{Kind: kind, Source: src, Dest: dest, Replace: replace})
		return nil
	}
	children := func(dir string) ([]string, error) {
		entries, err := os.ReadDir(filepath.Join(replica, dir))
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		if err != nil {
			return nil, fmt.Errorf("read replica %s: %w", dir, err)
		}
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		return names, nil
	}
	if err := add(UnitFileHistory, "file-history", filepath.Join(cfg, "file-history", string(to)), true); err != nil {
		return nil, err
	}
	lists, err := children("tasks")
	if err != nil {
		return nil, err
	}
	for _, list := range lists {
		dest, owned := list, true
		switch list {
		case string(from):
			dest = string(to)
		case sessionListID(from):
			dest = sessionListID(to)
		default:
			owned = false
		}
		if err := add(UnitTaskList, filepath.Join("tasks", list), filepath.Join(cfg, "tasks", dest), owned); err != nil {
			return nil, err
		}
	}
	for _, dir := range []struct {
		kind    UnitKind
		name    string
		replace bool
	}{{UnitPlan, "plans", true}, {UnitPasteCache, "paste-cache", false}} {
		names, err := children(dir.name)
		if err != nil {
			return nil, err
		}
		for _, name := range names {
			if err := add(dir.kind, filepath.Join(dir.name, name), filepath.Join(cfg, dir.name, name), dir.replace); err != nil {
				return nil, err
			}
		}
	}
	if err := add(UnitScratchpad, "scratchpad", claudenative.ScratchpadDir(t.Layout.TmpRoot, t.Layout.UID, t.Cwd, to), true); err != nil {
		return nil, err
	}
	if err := add(UnitSession, "session", claudenative.SessionDir(cfg, t.Cwd, to), true); err != nil {
		return nil, err
	}
	out = append(out, Install{Kind: UnitTranscript, Source: filepath.Join(replica, transcriptFile), Dest: claudenative.TranscriptPath(cfg, t.Cwd, to), Replace: true})
	return out, nil
}

func recoveryContext(p Plan, now time.Time) string {
	var b strings.Builder
	m := p.Meta
	fmt.Fprintf(&b, "cc-sync restored this Claude Code session from host %s (checkpoint %s, captured %s, %s before this pickup).\n",
		m.SourceHost, m.CheckpointID, m.CapturedAt.UTC().Format(time.RFC3339), now.Sub(m.CapturedAt).Round(time.Minute))
	fmt.Fprintf(&b, "It now runs in %s on this host.\n", p.Cwd)
	b.WriteString("Absolute paths earlier in this conversation refer to the source host's layout. Translate them with this map (source -> destination, longest prefix wins):\n")
	for _, r := range p.PathMap {
		if r.From != r.To {
			fmt.Fprintf(&b, "- %s -> %s\n", r.From, r.To)
		}
	}
	srcResults := filepath.Join(claudenative.SessionDir(m.SourceConfigDir, m.SourceCwd, p.SourceSessionID), "tool-results")
	dstResults := filepath.Join(claudenative.SessionDir(p.Layout.ConfigDir, p.Cwd, p.SessionID), "tool-results")
	fmt.Fprintf(&b, "Spilled tool results saved under %s now live under %s.\n", srcResults, dstResults)
	if p.Mode == ModeFork {
		fmt.Fprintf(&b, "This is a fork of session %s under the new id %s.\n", p.SourceSessionID, p.SessionID)
	}
	if len(p.Unmapped) > 0 {
		fmt.Fprintf(&b, "%d absolute paths outside the map were left as they were on the source host.\n", len(p.Unmapped))
	}
	return b.String()
}

func launch(p Plan, t Target, opts Options, relocated bool) Launch {
	l := Launch{
		Argv:     []string{"claude", "--resume", string(p.SessionID)},
		Dir:      t.Cwd,
		EnvUnset: []string{"CLAUDECODE", "CLAUDE_CODE_ENTRYPOINT"},
	}
	if relocated && opts.Capabilities.AppendSystemPrompt && !opts.OmitRecoveryPrompt {
		l.Argv = append(l.Argv, "--append-system-prompt", p.RecoveryContext)
	}
	if t.Layout.ConfigDir == filepath.Join(t.Home, ".claude") {
		l.EnvUnset = append(l.EnvUnset, "CLAUDE_CONFIG_DIR")
	} else {
		l.EnvSet = map[string]string{"CLAUDE_CONFIG_DIR": t.Layout.ConfigDir}
	}
	return l
}

func newSessionID() claudenative.SessionID {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return claudenative.SessionID(fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]))
}
