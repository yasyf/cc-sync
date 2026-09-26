package sessionrestore

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-sync/internal/claudenative"
)

const (
	srcHome    = "/Users/alice"
	srcCwd     = "/Users/alice/Code/proj"
	srcConfig  = "/Users/alice/.claude"
	srcProject = "/Users/alice/.claude/projects/-Users-alice-Code-proj"
	partial    = `{"parentUuid":"p1","type":"assi`
)

var (
	testNow      = time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	testCaptured = time.Date(2026, 9, 26, 11, 0, 0, 0, time.UTC)

	sourceLines = []string{
		`{"type":"file-history-snapshot","messageId":"m0","snapshot":{"messageId":"m0","trackedFileBackups":{"/Users/alice/Code/proj/a.go":{"backupFileName":"h1@v1","version":1,"realParentDir":"/Users/alice/Code/proj"}},"timestamp":"2026-09-26T10:00:00.000Z"},"isSnapshotUpdate":false}`,
		`{"parentUuid":null,"type":"user","message":{"role":"user","content":"edit /Users/alice/Code/proj/a.go"},"uuid":"u1","cwd":"/Users/alice/Code/proj","sessionId":"SID"}`,
		`{"parentUuid":"u1","type":"attachment","attachment":{"type":"prompt_snapshot","systemPrompt":["memory: /Users/alice/.claude/projects/-Users-alice-Code-proj/memory/"]},"uuid":"s1","cwd":"/Users/alice/Code/proj","sessionId":"SID"}`,
		`{"parentUuid":"s1","type":"attachment","attachment":{"type":"environment","snapshot":{"workingDirectory":"/Users/alice/Code/proj"},"rendered":"Primary working directory: /Users/alice/Code/proj"},"uuid":"e1","cwd":"/Users/alice/Code/proj","sessionId":"SID"}`,
		`{"parentUuid":"e1","type":"assistant","message":{"role":"assistant","content":[{"type":"tool_use","id":"t1","name":"Read","input":{"file_path":"/Users/alice/Code/proj/a.go"}}]},"uuid":"a1","cwd":"/Users/alice/Code/proj","sessionId":"SID"}`,
		`{"parentUuid":"a1","type":"user","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"t1","content":"saved to /Users/alice/.claude/projects/-Users-alice-Code-proj/SID/tool-results/r1.txt"}]},"toolUseResult":{"persistedOutputPath":"/Users/alice/.claude/projects/-Users-alice-Code-proj/SID/tool-results/r1.txt"},"uuid":"u2","cwd":"/Users/alice/Code/proj","sessionId":"SID"}`,
		`{"parentUuid":"u2","type":"attachment","attachment":{"type":"plan_mode","planFilePath":"/Users/alice/.claude/plans/twinkly-puffin.md"},"uuid":"p1","cwd":"/Users/alice/Code/proj","sessionId":"SID"}`,
		`{"type":"future-record","note":"/Users/alice/Code/proj/x.txt","sessionId":"SID"}`,
		`{"type":"last-prompt","lastPrompt":"edit","leafUuid":"p1","sessionId":"SID"}`,
	}
	wantLines = []string{
		`{"type":"file-history-snapshot","messageId":"m0","snapshot":{"messageId":"m0","trackedFileBackups":{"{CO}/a.go":{"backupFileName":"h1@v1","version":1,"realParentDir":"{CO}"}},"timestamp":"2026-09-26T10:00:00.000Z"},"isSnapshotUpdate":false}`,
		`{"parentUuid":null,"type":"user","message":{"role":"user","content":"edit /Users/alice/Code/proj/a.go"},"uuid":"u1","cwd":"{CO}","sessionId":"SID"}`,
		`{"parentUuid":"u1","type":"attachment","attachment":{"type":"environment","snapshot":{"workingDirectory":"/Users/alice/Code/proj"},"rendered":"Primary working directory: /Users/alice/Code/proj"},"uuid":"e1","cwd":"{CO}","sessionId":"SID"}`,
		`{"parentUuid":"e1","type":"assistant","message":{"role":"assistant","content":[{"type":"tool_use","id":"t1","name":"Read","input":{"file_path":"/Users/alice/Code/proj/a.go"}}]},"uuid":"a1","cwd":"{CO}","sessionId":"SID"}`,
		`{"parentUuid":"a1","type":"user","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"t1","content":"saved to /Users/alice/.claude/projects/-Users-alice-Code-proj/SID/tool-results/r1.txt"}]},"toolUseResult":{"persistedOutputPath":"{PROJ}/SID/tool-results/r1.txt"},"uuid":"u2","cwd":"{CO}","sessionId":"SID"}`,
		`{"parentUuid":"u2","type":"attachment","attachment":{"type":"plan_mode","planFilePath":"{CFG}/plans/twinkly-puffin.md"},"uuid":"p1","cwd":"{CO}","sessionId":"SID"}`,
		`{"type":"future-record","note":"/Users/alice/Code/proj/x.txt","sessionId":"SID"}`,
		`{"type":"last-prompt","lastPrompt":"edit","leafUuid":"p1","sessionId":"SID"}`,
		`{"type":"relocated","sessionId":"SID","relocatedCwd":"{CO}"}`,
	}
)

type fakeProcs []claudenative.Process

func (f fakeProcs) Processes(context.Context) ([]claudenative.Process, error) { return f, nil }

type world struct {
	t       *testing.T
	root    string
	replica string
	target  Target
	project string
}

func newWorld(t *testing.T) *world {
	t.Helper()
	root := t.TempDir()
	home := filepath.Join(root, "home", "bob")
	co := filepath.Join(home, ".cc-sync", "checkouts", "proj-alice")
	w := &world{
		t:       t,
		root:    root,
		replica: filepath.Join(root, "replicas", "alice-mbp", string(testSID), "ck1"),
		target: Target{
			Layout:    claudenative.Layout{ConfigDir: filepath.Join(home, ".claude"), TmpRoot: filepath.Join(root, "tmp"), UID: 502},
			Home:      home,
			Cwd:       co,
			Checkouts: PathMap{{From: srcCwd, To: co}},
		},
	}
	w.project = filepath.Join(w.target.Layout.ConfigDir, "projects", claudenative.ProjectDirName(co))
	meta, err := json.Marshal(Meta{
		SessionID: testSID, SourceHost: "alice-mbp", SourceConfigDir: srcConfig, SourceTmpRoot: "/private/tmp",
		SourceUID: 501, SourceCwd: srcCwd, SourceHome: srcHome, CheckpointID: "ck1", CapturedAt: testCaptured,
		LeafUUID: "p1", PrefixDigest: "sha256:abc", ClaudeVersion: "2.1.283",
	})
	if err != nil {
		t.Fatal(err)
	}
	w.write("meta.json", string(meta))
	w.transcript(sourceLines)
	w.write("session/subagents/agent-a1.jsonl", withSID(`{"type":"user","isSidechain":true,"uuid":"x1","parentUuid":null,"cwd":"/Users/alice/Code/proj","sessionId":"SID","agentId":"a1"}`)+"\n")
	w.write("session/subagents/agent-a1.meta.json", `{"agentType":"general-purpose","toolUseId":"t9"}`)
	w.write("session/tool-results/r1.txt", "result body")
	w.write("session/workflows/wf_1.json", withSID(`{"id":"wf_1","scriptPath":"/Users/alice/.claude/projects/-Users-alice-Code-proj/SID/workflows/scripts/w.js"}`))
	w.write("session/workflows/scripts/w.js", "run()")
	w.write("file-history/h1@v1", "old a.go")
	w.write("tasks/SID/1.json", `{"id":"1"}`)
	w.write("tasks/team-x/1.json", `{"id":"t"}`)
	w.write("plans/twinkly-puffin.md", "# plan")
	w.write("paste-cache/abc.txt", "pasted")
	w.write("scratchpad/scratchpad/notes.txt", "notes")
	if err := os.MkdirAll(filepath.Join(w.replica, "scratchpad", "tasks"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(withSID(srcProject+"/SID/subagents/agent-a1.jsonl"), filepath.Join(w.replica, "scratchpad", "tasks", "a1.output")); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(w.target.Layout.ConfigDir, "sessions"), 0o700); err != nil {
		t.Fatal(err)
	}
	return w
}

func (w *world) write(rel, body string) {
	w.t.Helper()
	path := filepath.Join(w.replica, withSID(rel))
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		w.t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		w.t.Fatal(err)
	}
}

func (w *world) transcript(lines []string) {
	w.write(transcriptFile, withSID(strings.Join(lines, "\n")+"\n"+partial))
}

func (w *world) expand(s string) string {
	return strings.NewReplacer("{CO}", w.target.Cwd, "{PROJ}", w.project, "{CFG}", w.target.Layout.ConfigDir, "SID", string(testSID)).Replace(s)
}

func (w *world) want(lines []string) string {
	return w.expand(strings.Join(lines, "\n") + "\n")
}

func (w *world) opts() Options {
	return Options{
		Now:           testNow,
		DisplacedRoot: filepath.Join(w.root, "cc-sync", "displaced"),
		Procs:         fakeProcs{},
		Capabilities:  Capabilities{Version: "2.1.283", Resume: true, AppendSystemPrompt: true},
	}
}

func (w *world) prepare(opts Options) Plan {
	w.t.Helper()
	p, err := Prepare(context.Background(), w.replica, w.target, opts)
	if err != nil {
		w.t.Fatalf("Prepare() = %v", err)
	}
	return p
}

func (w *world) apply(p Plan) Result {
	w.t.Helper()
	res, err := Apply(context.Background(), p)
	if err != nil {
		w.t.Fatalf("Apply() = %v", err)
	}
	return res
}

func (w *world) native() string {
	return filepath.Join(w.project, string(testSID)+".jsonl")
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path) //nolint:gosec // G304: a fixture under t.TempDir.
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func snapshot(t *testing.T, roots ...string) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, root := range roots {
		err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
			switch {
			case errors.Is(err, fs.ErrNotExist) && p == root:
				return fs.SkipAll
			case err != nil:
				return err
			case d.Type()&fs.ModeSymlink != 0:
				target, err := os.Readlink(p)
				out[p] = "link:" + target
				return err
			case d.IsDir():
				out[p] = "dir"
				return nil
			}
			out[p] = readFile(t, p)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	return out
}

func TestPrepareApplyFresh(t *testing.T) {
	w := newWorld(t)
	p := w.prepare(w.opts())
	wantUnmapped := []Unmapped{{File: transcriptFile, Line: 8, RecordType: "future-record", Path: srcCwd + "/x.txt"}}
	if p.Mode != ModeFresh || p.SessionID != testSID || p.Stripped != 1 || p.DroppedTailBytes != int64(len(partial)) || !reflect.DeepEqual(p.Unmapped, wantUnmapped) {
		t.Fatalf("plan = mode %s id %s stripped %d dropped %d unmapped %+v", p.Mode, p.SessionID, p.Stripped, p.DroppedTailBytes, p.Unmapped)
	}
	wantLaunch := Launch{
		Argv:     []string{"claude", "--resume", string(testSID), "--append-system-prompt", p.RecoveryContext},
		Dir:      w.target.Cwd,
		EnvUnset: []string{"CLAUDECODE", "CLAUDE_CODE_ENTRYPOINT", "CLAUDE_CONFIG_DIR"},
	}
	if !reflect.DeepEqual(p.Launch, wantLaunch) {
		t.Errorf("Launch = %+v, want %+v", p.Launch, wantLaunch)
	}
	for _, s := range []string{
		"from host alice-mbp (checkpoint ck1, captured 2026-09-26T11:00:00Z, 1h0m0s before this pickup)",
		"It now runs in " + w.target.Cwd,
		"- " + srcCwd + " -> " + w.target.Cwd,
		"Spilled tool results saved under " + withSID(srcProject+"/SID/tool-results") + " now live under " + w.expand("{PROJ}/SID/tool-results"),
	} {
		if !strings.Contains(p.RecoveryContext, s) {
			t.Errorf("RecoveryContext lacks %q:\n%s", s, p.RecoveryContext)
		}
	}
	res := w.apply(p)
	if len(res.Installed) != len(p.Installs) || len(res.Skipped) != 0 {
		t.Errorf("Result = %+v", res)
	}
	cfg := w.target.Layout.ConfigDir
	scratch := claudenative.ScratchpadDir(w.target.Layout.TmpRoot, 502, w.target.Cwd, testSID)
	files := map[string]string{
		w.native(): w.want(wantLines),
		w.expand("{PROJ}/SID/subagents/agent-a1.jsonl"):     w.expand(`{"type":"user","isSidechain":true,"uuid":"x1","parentUuid":null,"cwd":"{CO}","sessionId":"SID","agentId":"a1"}` + "\n"),
		w.expand("{PROJ}/SID/subagents/agent-a1.meta.json"): `{"agentType":"general-purpose","toolUseId":"t9"}`,
		w.expand("{PROJ}/SID/tool-results/r1.txt"):          "result body",
		w.expand("{PROJ}/SID/workflows/wf_1.json"):          w.expand(`{"id":"wf_1","scriptPath":"{PROJ}/SID/workflows/scripts/w.js"}`),
		w.expand("{PROJ}/SID/workflows/scripts/w.js"):       "run()",
		w.expand("{CFG}/file-history/SID/h1@v1"):            "old a.go",
		w.expand("{CFG}/tasks/SID/1.json"):                  `{"id":"1"}`,
		filepath.Join(cfg, "tasks", "team-x", "1.json"):     `{"id":"t"}`,
		filepath.Join(cfg, "plans", "twinkly-puffin.md"):    "# plan",
		filepath.Join(cfg, "paste-cache", "abc.txt"):        "pasted",
		filepath.Join(scratch, "scratchpad", "notes.txt"):   "notes",
	}
	for path, want := range files {
		if got := readFile(t, path); got != want {
			t.Errorf("%s =\n%s\nwant\n%s", path, got, want)
		}
	}
	if got, err := os.Readlink(filepath.Join(scratch, "tasks", "a1.output")); err != nil || got != w.expand("{PROJ}/SID/subagents/agent-a1.jsonl") {
		t.Errorf("scratchpad link = %q, %v", got, err)
	}
	for _, dir := range []string{filepath.Join(cfg, stagingDirName), filepath.Join(w.target.Layout.TmpRoot, "claude-502", stagingDirName)} {
		if _, err := os.Lstat(dir); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("staging %s left behind: %v", dir, err)
		}
	}
}

func TestPrepareLaunchOptions(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Options)
		want   int
	}{
		{"append advertised", func(*Options) {}, 5},
		{"caller omits the prompt", func(o *Options) { o.OmitRecoveryPrompt = true }, 3},
		{"append not advertised", func(o *Options) { o.Capabilities.AppendSystemPrompt = false }, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := newWorld(t)
			opts := w.opts()
			tt.mutate(&opts)
			if p := w.prepare(opts); len(p.Launch.Argv) != tt.want {
				t.Errorf("Argv = %q, want %d args", p.Launch.Argv, tt.want)
			}
		})
	}
}

func TestPrepareRefusals(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Options)
		check  func(error) bool
	}{
		{"no resume capability", func(o *Options) { o.Capabilities = Capabilities{Version: "9.0.0"} }, func(err error) bool {
			var inc *IncompatibleError
			return errors.As(err, &inc) && inc.Capability == "resume"
		}},
		{"live locally", func(o *Options) {
			o.Procs = fakeProcs{{PID: 4242, Start: "Sat Sep 26 10:00:00 2026", Argv: []string{"/usr/local/bin/claude", "--resume", string(testSID)}}}
		}, func(err error) bool { return errors.Is(err, ErrLiveLocal) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := newWorld(t)
			opts := w.opts()
			tt.mutate(&opts)
			if _, err := Prepare(context.Background(), w.replica, w.target, opts); !tt.check(err) {
				t.Errorf("Prepare() = %v", err)
			}
		})
	}
}

func TestPrepareFastForward(t *testing.T) {
	tests := []struct {
		name  string
		setup func(w *world)
	}{
		{"identical local copy", func(w *world) { w.apply(w.prepare(w.opts())) }},
		{"earlier pickup with its relocated record", func(w *world) {
			w.transcript(sourceLines[:5])
			w.apply(w.prepare(w.opts()))
			w.transcript(sourceLines)
		}},
		{"plain prefix", func(w *world) {
			if err := os.MkdirAll(w.project, 0o700); err != nil {
				w.t.Fatal(err)
			}
			if err := os.WriteFile(w.native(), []byte(w.want(wantLines[:3])), 0o600); err != nil {
				w.t.Fatal(err)
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := newWorld(t)
			tt.setup(w)
			p := w.prepare(w.opts())
			if p.Mode != ModeFastForward || len(p.Duplicates) != 0 {
				t.Fatalf("Mode = %s, Duplicates = %v", p.Mode, p.Duplicates)
			}
			w.apply(p)
			if got := readFile(t, w.native()); got != w.want(wantLines) {
				t.Errorf("transcript =\n%s", got)
			}
		})
	}
}

func TestPrepareDuplicateElsewhere(t *testing.T) {
	w := newWorld(t)
	other := filepath.Join(w.target.Layout.ConfigDir, "projects", "-elsewhere", string(testSID)+".jsonl")
	if err := os.MkdirAll(filepath.Dir(other), 0o700); err != nil {
		t.Fatal(err)
	}
	body := w.want(wantLines[:1])
	if err := os.WriteFile(other, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	p := w.prepare(w.opts())
	if p.Mode != ModeFresh || !reflect.DeepEqual(p.Duplicates, []string{other}) {
		t.Fatalf("Mode = %s, Duplicates = %v", p.Mode, p.Duplicates)
	}
	w.apply(p)
	if got := readFile(t, other); got != body {
		t.Errorf("duplicate changed: %s", got)
	}
	if got := readFile(t, w.native()); got != w.want(wantLines) {
		t.Errorf("transcript =\n%s", got)
	}
}

func divergedWorld(t *testing.T) (*world, string) {
	t.Helper()
	w := newWorld(t)
	w.apply(w.prepare(w.opts()))
	local := readFile(t, w.native()) + w.expand(`{"type":"user","uuid":"local1","parentUuid":"p1","cwd":"{CO}","sessionId":"SID"}`) + "\n"
	if err := os.WriteFile(w.native(), []byte(local), 0o600); err != nil {
		t.Fatal(err)
	}
	return w, local
}

func TestDivergentRefuse(t *testing.T) {
	w, local := divergedWorld(t)
	_, err := Prepare(context.Background(), w.replica, w.target, w.opts())
	var div *DivergentLocalError
	if !errors.As(err, &div) {
		t.Fatalf("Prepare() = %v, want *DivergentLocalError", err)
	}
	info, statErr := os.Stat(w.native())
	if statErr != nil {
		t.Fatal(statErr)
	}
	want := DivergentLocalError{SessionID: testSID, LocalPath: w.native(), LocalLeafUUID: "local1", LocalLastActivity: info.ModTime(), PickedLeafUUID: "p1", PickedCapturedAt: testCaptured}
	if *div != want {
		t.Errorf("error = %+v, want %+v", *div, want)
	}
	if got := readFile(t, w.native()); got != local {
		t.Errorf("local copy changed")
	}
}

func TestDivergentKeepLocal(t *testing.T) {
	w, local := divergedWorld(t)
	opts := w.opts()
	opts.OnDivergence = DivergenceKeepLocal
	p := w.prepare(opts)
	if p.Mode != ModeKeepLocal || len(p.Installs) != 0 || !reflect.DeepEqual(p.Launch.Argv, []string{"claude", "--resume", string(testSID)}) {
		t.Fatalf("plan = mode %s installs %d argv %q", p.Mode, len(p.Installs), p.Launch.Argv)
	}
	w.apply(p)
	if got := readFile(t, w.native()); got != local {
		t.Errorf("local copy changed")
	}
}

func TestDivergentReplace(t *testing.T) {
	w, local := divergedWorld(t)
	scratch := claudenative.ScratchpadDir(w.target.Layout.TmpRoot, 502, w.target.Cwd, testSID)
	if err := os.WriteFile(filepath.Join(scratch, "scratchpad", "local.txt"), []byte("local notes"), 0o600); err != nil {
		t.Fatal(err)
	}
	opts := w.opts()
	opts.OnDivergence = DivergenceReplace
	p := w.prepare(opts)
	wantDir := filepath.Join(opts.DisplacedRoot, string(testSID), "2026-09-26T12:00:00Z")
	if p.Mode != ModeReplace || p.DisplacedDir != wantDir || len(p.Displace) != 5 {
		t.Fatalf("plan = mode %s dir %s displace %+v", p.Mode, p.DisplacedDir, p.Displace)
	}
	w.apply(p)
	enc := claudenative.ProjectDirName(w.target.Cwd)
	displaced := map[string]string{
		filepath.Join(wantDir, "projects", enc, string(testSID)+".jsonl"):                  local,
		filepath.Join(wantDir, "projects", enc, string(testSID), "tool-results", "r1.txt"): "result body",
		filepath.Join(wantDir, "file-history", string(testSID), "h1@v1"):                   "old a.go",
		filepath.Join(wantDir, "tasks", string(testSID), "1.json"):                         `{"id":"1"}`,
		filepath.Join(wantDir, "tmp", enc, string(testSID), "scratchpad", "local.txt"):     "local notes",
		w.native(): w.want(wantLines),
	}
	for path, want := range displaced {
		if got := readFile(t, path); got != want {
			t.Errorf("%s =\n%s\nwant\n%s", path, got, want)
		}
	}
	if _, err := os.Lstat(filepath.Join(scratch, "scratchpad", "local.txt")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("local scratch file survived in place: %v", err)
	}
}

func TestDivergentFork(t *testing.T) {
	w, local := divergedWorld(t)
	opts := w.opts()
	opts.OnDivergence = DivergenceFork
	p := w.prepare(opts)
	forked := p.SessionID
	if _, err := claudenative.ParseSessionID(string(forked)); err != nil || forked == testSID || p.Mode != ModeFork || p.SourceSessionID != testSID {
		t.Fatalf("plan = mode %s id %s source %s", p.Mode, forked, p.SourceSessionID)
	}
	if p.Launch.Argv[2] != string(forked) {
		t.Errorf("Argv = %q", p.Launch.Argv)
	}
	w.apply(p)
	if got := readFile(t, w.native()); got != local {
		t.Errorf("local copy changed")
	}
	cfg := w.target.Layout.ConfigDir
	transcript := readFile(t, claudenative.TranscriptPath(cfg, w.target.Cwd, forked))
	if n := strings.Count(transcript, `"sessionId":"`+string(forked)+`"`); n != 8 || strings.Contains(transcript, `"sessionId":"`+string(testSID)+`"`) {
		t.Errorf("forked transcript carries %d new ids:\n%s", n, transcript)
	}
	if !strings.Contains(transcript, `"persistedOutputPath":"`+filepath.Join(w.project, string(forked), "tool-results", "r1.txt")+`"`) {
		t.Errorf("persistedOutputPath not forked:\n%s", transcript)
	}
	sub := readFile(t, filepath.Join(w.project, string(forked), "subagents", "agent-a1.jsonl"))
	if !strings.Contains(sub, `"sessionId":"`+string(forked)+`"`) {
		t.Errorf("subagent not forked: %s", sub)
	}
	for _, path := range []string{
		filepath.Join(cfg, "file-history", string(forked), "h1@v1"),
		filepath.Join(cfg, "tasks", string(forked), "1.json"),
		filepath.Join(cfg, "tasks", "team-x", "1.json"),
	} {
		readFile(t, path)
	}
	scratch := claudenative.ScratchpadDir(w.target.Layout.TmpRoot, 502, w.target.Cwd, forked)
	if got, err := os.Readlink(filepath.Join(scratch, "tasks", "a1.output")); err != nil || got != filepath.Join(w.project, string(forked), "subagents", "agent-a1.jsonl") {
		t.Errorf("forked scratchpad link = %q, %v", got, err)
	}
}

func TestApplyRollsBack(t *testing.T) {
	errInjected := errors.New("injected rename failure")
	for _, crash := range []bool{false, true} {
		for failAt := 0; ; failAt++ {
			w := newWorld(t)
			w.transcript(sourceLines[:5])
			w.apply(w.prepare(w.opts()))
			w.transcript(sourceLines)
			p := w.prepare(w.opts())
			roots := []string{w.target.Layout.ConfigDir, w.target.Layout.TmpRoot}
			before := snapshot(t, roots...)
			calls := 0
			rename := func(from, to string) error {
				calls++
				if calls-1 == failAt {
					return errInjected
				}
				return os.Rename(from, to)
			}
			_, err := applier{rename: rename}.apply(context.Background(), p, !crash)
			if err == nil {
				if failAt < 2 {
					t.Fatalf("apply succeeded after only %d renames", failAt)
				}
				break
			}
			if !errors.Is(err, errInjected) {
				t.Fatalf("crash=%v failAt=%d: apply() = %v", crash, failAt, err)
			}
			if crash {
				if err := Recover(context.Background(), w.target.Layout); err != nil {
					t.Fatalf("Recover() = %v", err)
				}
			}
			if after := snapshot(t, roots...); !reflect.DeepEqual(after, before) {
				t.Errorf("crash=%v failAt=%d: native state changed:\nbefore %v\nafter  %v", crash, failAt, before, after)
			}
		}
	}
}
