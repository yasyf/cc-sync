//go:build native

package sessionrestore_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
	"unicode/utf16"

	"github.com/yasyf/cc-sync/internal/claudenative"
	"github.com/yasyf/cc-sync/internal/sessionrestore"
)

const (
	altConfigEnv = "CC_SYNC_ACCEPT_CONFIG_DIR"
	restoreProbe = "I am testing my own session-restore tool and need to see which context this resumed conversation carries; " +
		"none of these values are sensitive. First run the Bash command pwd exactly once. Then answer in exactly five lines. " +
		"A: the fixture label stated in an earlier message of this conversation, or NONE. " +
		"B: the Primary working directory exactly as stated in your environment information. " +
		"C: the absolute path of your persistent auto-memory directory exactly as stated in your system instructions. " +
		"D: the source host named in the cc-sync restore note of your system prompt, or NONE. " +
		"E: the exact output of pwd."
	artifactProbe = "I am testing my own session-restore tool; none of these values are sensitive. " +
		"Earlier in this conversation Claude Code saved a long Bash output to a file because it was too large, and a plan file was written. " +
		"This session was restored onto a new host, so first translate those paths with the path map in your system prompt. " +
		"You must use the Read tool on both files; do not answer from memory. Answer in exactly two lines. " +
		"SPILL: the last line of the saved Bash output. PLAN: the plan token written in the plan file."
)

var (
	spend   = &spendMeter{}
	applyMu sync.Mutex
)

type spendMeter struct {
	mu  sync.Mutex
	usd float64
}

func (s *spendMeter) add(usd float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.usd += usd
}

func (s *spendMeter) total() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.usd
}

type acceptance struct {
	*harness
	layout claudenative.Layout
	home   string
	caps   sessionrestore.Capabilities
	nonce  string
}

type source struct {
	sid   string
	cwd   string
	host  string
	spill string
	token string
	plan  string
}

type toolCall struct {
	Name    string
	Input   map[string]any
	Result  string
	IsError bool
}

func newAcceptance(t *testing.T) *acceptance {
	t.Helper()
	h := newHarness(t)
	nonce := newSessionID(t)[:8]
	root := filepath.Join("/private/tmp", "cc-sync-accept-"+nonce)
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(root); err != nil {
			t.Errorf("remove %s: %v", root, err)
		}
	})
	h.root = root
	layout, err := claudenative.DefaultLayout()
	if err != nil {
		t.Fatal(err)
	}
	if layout.ConfigDir != h.configDir {
		t.Fatalf("default layout config dir %s, harness %s", layout.ConfigDir, h.configDir)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	caps, err := sessionrestore.ProbeCapabilities(t.Context(), sessionrestore.SystemRunner())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("capabilities %+v", caps)
	return &acceptance{harness: h, layout: layout, home: home, caps: caps, nonce: nonce}
}

func (a *acceptance) track(t *testing.T, configDir, sid string) {
	t.Helper()
	t.Cleanup(func() {
		todos, _ := filepath.Glob(filepath.Join(configDir, "todos", sid+"*"))
		for _, p := range append(todos,
			filepath.Join(configDir, "file-history", sid),
			filepath.Join(configDir, "tasks", sid),
			filepath.Join(configDir, "tasks", "session-"+sid[:8]),
			filepath.Join(configDir, "session-env", sid),
		) {
			if err := os.RemoveAll(p); err != nil {
				t.Errorf("remove %s: %v", p, err)
			}
		}
	})
}

func (a *acceptance) artifactSource(t *testing.T, name string) source {
	t.Helper()
	s := source{cwd: a.cwd(t, filepath.Join("src", name)), host: "orchard-" + a.nonce, spill: "SPILL-END-" + a.nonce, token: "PLANTOKEN-" + a.nonce}
	var big strings.Builder
	for i := range 400 {
		fmt.Fprintf(&big, "row %04d %s\n", i, strings.Repeat("x", 120))
	}
	big.WriteString(s.spill + "\n")
	if err := os.WriteFile(filepath.Join(s.cwd, "big.txt"), []byte(big.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	s.sid = a.sessionID(t)
	a.track(t, a.configDir, s.sid)
	a.mustRun(t, s.cwd, "The fixture label for this conversation is "+fixtureLabel+". Run the Bash command `cat big.txt` exactly once, then reply only OK.",
		"--session-id", s.sid, "--tools", "Bash", "--allowedTools", "Bash(cat big.txt)", "--max-turns", "4")
	spilled, err := filepath.Glob(filepath.Join(claudenative.SessionDir(a.configDir, s.cwd, claudenative.SessionID(s.sid)), "tool-results", "*"))
	if err != nil || len(spilled) == 0 {
		t.Fatalf("the large Bash output was not spilled to tool-results (err %v)", err)
	}
	a.mustRun(t, s.cwd, "Plan how to add a file named hello.txt to this project. Write your plan to the plan file named in the plan mode instructions "+
		"and include the exact plan token "+s.token+" in it. Do not implement anything and do not exit plan mode; after writing the plan file reply only OK.",
		"--resume", s.sid, "--permission-mode", "plan", "--tools", "Write", "--allowedTools", "Write", "--max-turns", "6")
	for _, p := range planPaths(t, readFile(t, a.transcript(s.cwd, s.sid))) {
		t.Cleanup(func() { _ = os.Remove(p) })
		if b, err := os.ReadFile(p); err == nil && bytes.Contains(b, []byte(s.token)) {
			s.plan = p
		}
	}
	if s.plan == "" {
		t.Fatalf("no plan file carrying %s was written", s.token)
	}
	return s
}

func (a *acceptance) capture(t *testing.T, s source) string {
	t.Helper()
	sid := claudenative.SessionID(s.sid)
	replica := filepath.Join(a.root, "replica", s.sid, "ck1")
	transcript := readFile(t, a.transcript(s.cwd, s.sid))
	writeFile(t, filepath.Join(replica, "transcript.jsonl"), transcript)
	copyTree(t, claudenative.SessionDir(a.configDir, s.cwd, sid), filepath.Join(replica, "session"))
	copyTree(t, filepath.Join(a.configDir, "file-history", s.sid), filepath.Join(replica, "file-history"))
	for _, list := range []string{s.sid, "session-" + s.sid[:8]} {
		copyTree(t, filepath.Join(a.configDir, "tasks", list), filepath.Join(replica, "tasks", list))
	}
	for _, p := range planPaths(t, transcript) {
		copyTree(t, p, filepath.Join(replica, "plans", filepath.Base(p)))
	}
	copyTree(t, claudenative.ScratchpadDir(a.layout.TmpRoot, a.layout.UID, s.cwd, sid), filepath.Join(replica, "scratchpad"))
	digest := sha256.Sum256(transcript)
	meta, err := json.Marshal(sessionrestore.Meta{
		SessionID: sid, SourceHost: s.host, SourceConfigDir: a.configDir, SourceTmpRoot: a.layout.TmpRoot,
		SourceUID: a.layout.UID, SourceCwd: s.cwd, SourceHome: a.home, CheckpointID: "ck1", CapturedAt: time.Now().Add(-time.Hour),
		LeafUUID: lastUUID(transcript), PrefixDigest: "sha256:" + hex.EncodeToString(digest[:]), ClaudeVersion: a.caps.Version,
	})
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(replica, "meta.json"), meta)
	return replica
}

func (a *acceptance) removeSource(t *testing.T, s source) {
	t.Helper()
	for _, p := range []string{
		s.cwd,
		filepath.Join(claudenative.ProjectsDir(a.configDir), claudenative.ProjectDirName(s.cwd)),
		filepath.Dir(claudenative.ScratchpadDir(a.layout.TmpRoot, a.layout.UID, s.cwd, claudenative.SessionID(s.sid))),
		filepath.Join(a.configDir, "file-history", s.sid),
		filepath.Join(a.configDir, "tasks", s.sid),
		filepath.Join(a.configDir, "tasks", "session-"+s.sid[:8]),
		filepath.Join(a.configDir, "session-env", s.sid),
		s.plan,
	} {
		if p == "" {
			continue
		}
		if err := os.RemoveAll(p); err != nil {
			t.Fatal(err)
		}
	}
	if got := copiesIn(t, a.configDir, s.sid); len(got) != 0 {
		t.Fatalf("source copies survived removal: %v", got)
	}
}

func (a *acceptance) restore(t *testing.T, replica string, layout claudenative.Layout, dst string) sessionrestore.Plan {
	t.Helper()
	target := sessionrestore.Target{Layout: layout, Home: a.home, Cwd: dst}
	plan, err := sessionrestore.Prepare(t.Context(), replica, target, sessionrestore.Options{
		OnDivergence: sessionrestore.DivergenceRefuse, Now: time.Now(), DisplacedRoot: filepath.Join(a.root, "displaced"),
		Procs: claudenative.SystemProcesses(), Capabilities: a.caps,
	})
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	t.Cleanup(func() {
		for _, u := range plan.Installs {
			_ = os.RemoveAll(u.Dest)
		}
		_ = os.RemoveAll(filepath.Join(layout.ConfigDir, "session-env", string(plan.SessionID)))
	})
	if plan.Mode != sessionrestore.ModeFresh || string(plan.SessionID) != string(plan.Meta.SessionID) {
		t.Fatalf("plan mode %s id %s, want a fresh install under the source id %s", plan.Mode, plan.SessionID, plan.Meta.SessionID)
	}
	staging := filepath.Join(layout.ConfigDir, ".cc-sync-staging")
	var res sessionrestore.Result
	func() {
		applyMu.Lock()
		defer applyMu.Unlock()
		before := entryNames(t, staging)
		var err error
		if res, err = sessionrestore.Apply(t.Context(), plan); err != nil {
			t.Fatalf("apply: %v", err)
		}
		if after := entryNames(t, staging); (before == nil) != (after == nil) || !slices.Equal(before, after) {
			t.Errorf("apply changed the staging entries under %s from %q to %q", staging, before, after)
		}
	}()
	t.Logf("installed %d units, rewritten %d, stripped %d, dropped tail %d, unmapped %d", len(res.Installed), plan.Rewritten, plan.Stripped, plan.DroppedTailBytes, len(plan.Unmapped))
	return plan
}

func (a *acceptance) launchEnv(t *testing.T, l sessionrestore.Launch) []string {
	t.Helper()
	env := slices.DeleteFunc(slices.Clone(a.env), func(kv string) bool {
		name, _, _ := strings.Cut(kv, "=")
		_, set := l.EnvSet[name]
		return set || slices.Contains(l.EnvUnset, name)
	})
	for k, v := range l.EnvSet {
		env = append(env, k+"="+v)
	}
	return env
}

func (a *acceptance) resume(t *testing.T, plan sessionrestore.Plan, prompt string, extra ...string) (runResult, []byte) {
	t.Helper()
	if plan.Launch.Argv[0] != "claude" || plan.Launch.Argv[1] != "--resume" || plan.Launch.Argv[2] != string(plan.SessionID) {
		t.Fatalf("launch argv %q does not resume %s", plan.Launch.Argv, plan.SessionID)
	}
	before := len(readFile(t, plan.Transcript))
	h := *a.harness
	h.env = a.launchEnv(t, plan.Launch)
	res := h.mustRun(t, plan.Launch.Dir, prompt, slices.Concat(plan.Launch.Argv[1:], extra)...)
	if res.SessionID != string(plan.SessionID) {
		t.Errorf("resumed session id = %s, want %s", res.SessionID, plan.SessionID)
	}
	after := readFile(t, plan.Transcript)
	if len(after) <= before {
		t.Fatalf("the resumed run appended nothing to %s", plan.Transcript)
	}
	appended := after[before:]
	for i, rec := range records(t, appended) {
		if id, ok := rec["sessionId"].(string); ok && id != string(plan.SessionID) {
			t.Errorf("appended record %d carries sessionId %s", i, id)
		}
		if cwd, ok := rec["cwd"].(string); ok && cwd != plan.Cwd {
			t.Errorf("appended record %d carries cwd %s, want %s", i, cwd, plan.Cwd)
		}
	}
	if got := copiesIn(t, plan.Layout.ConfigDir, string(plan.SessionID)); len(got) != 1 || got[0] != plan.Transcript {
		t.Errorf("native copies = %v, want only %s", got, plan.Transcript)
	}
	return res, appended
}

func TestNativeAcceptance(t *testing.T) {
	a := newAcceptance(t)
	start := time.Now()
	t.Cleanup(func() {
		t.Logf("acceptance wall time %s, cumulative -p spend $%.4f", time.Since(start).Round(time.Second), spend.total())
	})

	t.Run("cross-cwd source unavailable with relocated artifacts", func(t *testing.T) {
		t.Parallel()
		a.artifactScenario(t, a.layout, "cross-cwd "+unicodeDir)
	})

	t.Run("cross-config-root", func(t *testing.T) {
		t.Parallel()
		alt := os.Getenv(altConfigEnv)
		if alt == "" {
			t.Skipf("NOT-COVERED: %s names no authenticated alternate CLAUDE_CONFIG_DIR; cc-pool lists no accounts and discovering one needs keychain access this harness never takes", altConfigEnv)
		}
		if sameDir(t, alt, a.configDir) {
			t.Fatalf("%s=%s resolves to the source config dir %s; the cross-config scenario needs a distinct authenticated CLAUDE_CONFIG_DIR", altConfigEnv, alt, a.configDir)
		}
		a.artifactScenario(t, claudenative.Layout{ConfigDir: alt, TmpRoot: a.layout.TmpRoot, UID: a.layout.UID}, "cross-config "+unicodeDir)
	})

	t.Run("long unicode cwd with partial tail", func(t *testing.T) {
		t.Parallel()
		long := filepath.Join(strings.Repeat("l", 90), "😀 ünï (x)+"+strings.Repeat("o", 90), "deep")
		s := source{cwd: a.cwd(t, filepath.Join("src", long)), host: "orchard-" + a.nonce}
		dst := a.cwd(t, filepath.Join("dst", long))
		for _, p := range []string{s.cwd, dst} {
			if n := len(utf16.Encode([]rune(p))); n <= 200 {
				t.Fatalf("cwd %s has %d UTF-16 units, want > 200", p, n)
			}
		}
		s.sid, _ = a.source(t, s.cwd)
		a.track(t, a.configDir, s.sid)
		replica := a.capture(t, s)
		data := readFile(t, filepath.Join(replica, "transcript.jsonl"))
		last := bytes.LastIndexByte(data[:len(data)-1], '\n') + 1
		cut := last + (len(data)-last)/2
		writeFile(t, filepath.Join(replica, "transcript.jsonl"), data[:cut])
		a.removeSource(t, s)
		plan := a.restore(t, replica, a.layout, dst)
		if plan.DroppedTailBytes != int64(cut-last) {
			t.Errorf("dropped tail bytes = %d, want %d", plan.DroppedTailBytes, cut-last)
		}
		installed := readFile(t, plan.Transcript)
		if bad := invalidLines(installed); len(bad) != 0 || installed[len(installed)-1] != '\n' {
			t.Fatalf("installed transcript has invalid lines %v or no trailing newline", bad)
		}
		res, _ := a.resume(t, plan, probe, "--tools", "")
		enc := projectDirName(dst)
		for _, want := range []string{fixtureLabel, dst, enc[strings.LastIndexByte(enc, '-'):] + "/memory"} {
			if !strings.Contains(res.Result, want) {
				t.Errorf("answer %q does not contain %q", res.Result, want)
			}
		}
		if bad := invalidLines(readFile(t, plan.Transcript)); len(bad) != 0 {
			t.Errorf("resumed transcript has invalid lines %v", bad)
		}
		if from := environmentChangeFrom(t, plan.Transcript); from != s.cwd {
			t.Errorf("environment update from = %q, want %q", from, s.cwd)
		}
	})

	t.Run("file history and rewind", func(t *testing.T) {
		t.Parallel()
		a.rewindScenario(t)
	})
}

func (a *acceptance) artifactScenario(t *testing.T, dstLayout claudenative.Layout, name string) {
	s := a.artifactSource(t, name)
	dst := a.cwd(t, filepath.Join("dst", name))
	if dstLayout.ConfigDir != a.configDir {
		t.Cleanup(func() {
			_ = os.RemoveAll(filepath.Join(claudenative.ProjectsDir(dstLayout.ConfigDir), claudenative.ProjectDirName(dst)))
		})
		a.track(t, dstLayout.ConfigDir, s.sid)
	}
	replica := a.capture(t, s)
	sourceTranscript := readFile(t, filepath.Join(replica, "transcript.jsonl"))
	sourceEnv := environmentLines(sourceTranscript)
	a.removeSource(t, s)
	plan := a.restore(t, replica, dstLayout, dst)
	dstProject := filepath.Join(claudenative.ProjectsDir(dstLayout.ConfigDir), claudenative.ProjectDirName(dst))
	dstResults := filepath.Join(claudenative.SessionDir(dstLayout.ConfigDir, dst, plan.SessionID), "tool-results")
	dstPlan := filepath.Join(dstLayout.ConfigDir, "plans", filepath.Base(s.plan))

	installed := readFile(t, plan.Transcript)
	t.Run("adapter output", func(t *testing.T) {
		if plan.Stripped == 0 || bytes.Contains(installed, []byte(`"prompt_snapshot"`)) {
			t.Errorf("stripped %d snapshot records, installed transcript still carries prompt_snapshot = %v", plan.Stripped, bytes.Contains(installed, []byte(`"prompt_snapshot"`)))
		}
		if len(sourceEnv) == 0 {
			t.Fatal("the source transcript carries no environment record")
		}
		installedEnv := environmentLines(installed)
		for _, env := range sourceEnv {
			if !slices.Contains(installedEnv, env) {
				t.Errorf("environment payload not installed verbatim: %s", env)
			}
		}
		srcResults := filepath.Join(claudenative.SessionDir(a.configDir, s.cwd, claudenative.SessionID(s.sid)), "tool-results")
		relocatedValues(t, sourceTranscript, installed, "persistedOutputPath", srcResults, dstResults)
		relocatedValues(t, sourceTranscript, installed, "planFilePath", filepath.Join(a.configDir, "plans"), filepath.Join(dstLayout.ConfigDir, "plans"))
		for _, want := range []string{dstPlan, dstResults} {
			if _, err := os.Stat(want); err != nil {
				t.Errorf("relocated artifact missing: %v", err)
			}
		}
		i := slices.Index(plan.Launch.Argv, "--append-system-prompt")
		if !a.caps.AppendSystemPrompt || i < 0 || plan.Launch.Argv[i+1] != plan.RecoveryContext {
			t.Errorf("launch argv %q does not append the recovery context", plan.Launch.Argv)
		}
		for _, want := range []string{s.host, dstResults} {
			if !strings.Contains(plan.RecoveryContext, want) {
				t.Errorf("recovery context does not name %s:\n%s", want, plan.RecoveryContext)
			}
		}
	})

	t.Run("resume reports destination", func(t *testing.T) {
		res, appended := a.resume(t, plan, restoreProbe, "--tools", "Bash", "--allowedTools", "Bash(pwd)", "--max-turns", "6")
		for _, want := range []string{fixtureLabel, dst, dstProject + "/memory", s.host} {
			if !strings.Contains(res.Result, want) {
				t.Errorf("answer %q does not contain %q", res.Result, want)
			}
		}
		pwd := slices.IndexFunc(toolCalls(appended), func(c toolCall) bool {
			return c.Name == "Bash" && !c.IsError && strings.TrimSpace(c.Result) == dst
		})
		if pwd < 0 {
			t.Errorf("no appended Bash call printed %s: %+v", dst, toolCalls(appended))
		}
		if from := environmentChangeFrom(t, plan.Transcript); from != s.cwd {
			t.Errorf("environment update from = %q, want %q", from, s.cwd)
		}
		if !bytes.Contains(appended, []byte(`"prompt_snapshot"`)) {
			t.Errorf("the resumed run recorded no fresh prompt snapshot")
		}
		after := environmentLines(readFile(t, plan.Transcript))
		for _, env := range sourceEnv {
			if !slices.Contains(after, env) {
				t.Errorf("environment payload changed after resume: %s", env)
			}
		}
	})

	t.Run("relocated artifacts are readable", func(t *testing.T) {
		res, appended := a.resume(t, plan, artifactProbe, "--tools", "Read", "--allowedTools", "Read", "--max-turns", "10")
		calls := toolCalls(appended)
		t.Logf("read calls: %+v", readSummary(calls))
		for _, want := range []string{s.spill, s.token} {
			if !strings.Contains(res.Result, want) {
				t.Errorf("answer %q does not contain %q", res.Result, want)
			}
		}
		read := func(prefix, marker string) bool {
			return slices.ContainsFunc(calls, func(c toolCall) bool {
				p, _ := c.Input["file_path"].(string)
				return c.Name == "Read" && !c.IsError && strings.HasPrefix(p, prefix) && strings.Contains(c.Result, marker)
			})
		}
		if !read(dstResults+"/", s.spill) {
			t.Errorf("no successful Read of the spilled result under %s", dstResults)
		}
		if !read(dstPlan, s.token) {
			t.Errorf("no successful Read of the plan at %s", dstPlan)
		}
	})
}

func (a *acceptance) rewindScenario(t *testing.T) {
	src := a.cwd(t, filepath.Join("src", "rewind"))
	dst := a.cwd(t, filepath.Join("dst", "rewind"))
	original := "ORIGINAL-" + a.nonce + "\n"
	edited := "EDITED-" + a.nonce + "\n"
	token := "REWIND-" + a.nonce
	writeFile(t, filepath.Join(src, "target.txt"), []byte(original))
	s := source{cwd: src, sid: a.sessionID(t), host: "orchard-" + a.nonce}
	a.track(t, a.configDir, s.sid)
	flags := []string{"--model", "haiku", "--setting-sources", "project", "--strict-mcp-config", "--permission-mode", "acceptEdits", "--tools", "Edit,Read", "--allowedTools", "Edit,Read"}

	term := startTerminal(t, src, a.env, slices.Concat([]string{a.claude, token + ": use the Edit tool to replace the text ORIGINAL-" + a.nonce + " with EDITED-" + a.nonce + " in target.txt, then reply only DONE."}, flags, []string{"--session-id", s.sid})...)
	term.until(t, 3*time.Minute, func() bool {
		return strings.TrimSpace(string(readFile(t, filepath.Join(src, "target.txt")))) == strings.TrimSpace(edited)
	})
	term.quiet(t, 3*time.Second, time.Minute)
	term.exit(t)
	backups, err := os.ReadDir(filepath.Join(a.configDir, "file-history", s.sid))
	if err != nil || len(backups) == 0 {
		t.Fatalf("interactive edit wrote no file-history backups (err %v)", err)
	}
	replica := a.capture(t, s)
	writeFile(t, filepath.Join(dst, "target.txt"), readFile(t, filepath.Join(src, "target.txt")))
	a.removeSource(t, s)
	plan := a.restore(t, replica, a.layout, dst)

	installedBackups, err := os.ReadDir(filepath.Join(a.configDir, "file-history", s.sid))
	if err != nil || len(installedBackups) != len(backups) {
		t.Fatalf("installed file-history %v (err %v), want %d backups", installedBackups, err, len(backups))
	}
	keys := trackedFiles(t, readFile(t, plan.Transcript))
	t.Logf("tracked files %v", keys)
	if len(keys) == 0 {
		t.Fatal("the installed transcript tracks no file backups")
	}
	for _, k := range keys {
		if filepath.IsAbs(k) && k != dst && !strings.HasPrefix(k, dst+"/") {
			t.Errorf("tracked file %s not mapped under %s", k, dst)
		}
	}

	res := startTerminal(t, plan.Launch.Dir, a.launchEnv(t, plan.Launch), slices.Concat([]string{a.claude}, plan.Launch.Argv[1:], flags)...)
	res.quiet(t, 3*time.Second, 2*time.Minute)
	mark := res.mark()
	res.send(t, "/rewind")
	res.send(t, "\r")
	res.waitFor(t, mark, regexp.MustCompile(`(?i)rewind|restore`), 30*time.Second)
	res.quiet(t, 1500*time.Millisecond, 30*time.Second)
	list := res.screen(mark)
	t.Logf("rewind screen:\n%s", tail(list, 3000))
	if !regexp.MustCompile(regexp.QuoteMeta(token) + `(?s:.*)target\.txt \+1 -1`).MatchString(list) {
		t.Fatalf("the /rewind checkpoint list does not show the pre-edit checkpoint %s with its target.txt change", token)
	}
	if !res.selectLine(t, token, "\x1b[A", 8) {
		t.Fatalf("could not move the /rewind selection onto %s", token)
	}
	mark = res.mark()
	res.send(t, "\r")
	res.waitFor(t, mark, regexp.MustCompile(`(?i)restore code`), 30*time.Second)
	res.quiet(t, 1500*time.Millisecond, 30*time.Second)
	options := res.screen(mark)
	t.Logf("restore options:\n%s", tail(options, 2000))
	if !strings.Contains(options, "The code will be restored +1 -1 in target.txt") {
		t.Errorf("the rewind confirmation does not offer to restore target.txt")
	}
	if !res.selectLine(t, "Restore code", "\x1b[B", 6) {
		t.Fatal("could not select a restore option that restores code")
	}
	res.send(t, "\r")
	res.until(t, time.Minute, func() bool { return string(readFile(t, filepath.Join(dst, "target.txt"))) == original })
	res.quiet(t, 2*time.Second, 30*time.Second)
	res.exit(t)
	forks, err := filepath.Glob(filepath.Join(claudenative.ProjectsDir(a.configDir), claudenative.ProjectDirName(dst), "*.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range forks {
		if id := strings.TrimSuffix(filepath.Base(f), ".jsonl"); id != s.sid {
			t.Logf("rewind forked the conversation into %s", id)
			a.track(t, a.configDir, id)
		}
	}
}

type terminal struct {
	cmd  *exec.Cmd
	in   io.WriteCloser
	mu   sync.Mutex
	raw  []byte
	done chan struct{}
}

var ansi = regexp.MustCompile(`\x1b\[[0-9;?<>=]*[ -/]*[@-~]|\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)|\x1b[()][0-9A-Za-z]|\x1b[=>78]`)

func startTerminal(t *testing.T, dir string, env []string, argv ...string) *terminal {
	t.Helper()
	cmd := exec.Command("script", slices.Concat([]string{"-q", "/dev/null", "/bin/sh", "-c", `stty cols 200 rows 60; exec "$0" "$@"`}, argv)...)
	cmd.Dir = dir
	cmd.Env = append(slices.Clone(env), "TERM=xterm-256color")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	in, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	m := &terminal{cmd: cmd, in: in, done: make(chan struct{})}
	go func() {
		buf := make([]byte, 8192)
		for {
			n, err := out.Read(buf)
			m.mu.Lock()
			m.raw = append(m.raw, buf[:n]...)
			m.mu.Unlock()
			if err != nil {
				_ = cmd.Wait()
				close(m.done)
				return
			}
		}
	}()
	t.Cleanup(func() {
		select {
		case <-m.done:
		default:
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			<-m.done
		}
	})
	return m
}

func (m *terminal) mark() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.raw)
}

func (m *terminal) screen(from int) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return strings.ReplaceAll(ansi.ReplaceAllString(string(m.raw[from:]), ""), "\r", "\n")
}

func (m *terminal) send(t *testing.T, keys string) {
	t.Helper()
	if _, err := io.WriteString(m.in, keys); err != nil {
		t.Fatalf("send %q: %v", keys, err)
	}
	time.Sleep(400 * time.Millisecond)
}

var trustPrompt = regexp.MustCompile(`(?i)do you trust|trust the files|Yes, proceed`)

func (m *terminal) until(t *testing.T, timeout time.Duration, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	trusted := 0
	for !ok() {
		if s := m.screen(trusted); trustPrompt.MatchString(s) {
			t.Logf("answering the workspace trust prompt")
			trusted = m.mark()
			m.send(t, "\r")
		}
		select {
		case <-m.done:
			t.Fatalf("claude exited early; screen:\n%s", tail(m.screen(0), 4000))
		case <-time.After(300 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out; screen:\n%s", tail(m.screen(0), 4000))
		}
	}
}

func (m *terminal) waitFor(t *testing.T, from int, re *regexp.Regexp, timeout time.Duration) {
	t.Helper()
	m.until(t, timeout, func() bool { return re.MatchString(m.screen(from)) })
}

func (m *terminal) quiet(t *testing.T, idle, timeout time.Duration) {
	t.Helper()
	last, since := m.mark(), time.Now()
	m.until(t, timeout, func() bool {
		if n := m.mark(); n != last {
			last, since = n, time.Now()
		}
		return time.Since(since) >= idle
	})
}

func (m *terminal) selectLine(t *testing.T, want, key string, tries int) bool {
	t.Helper()
	for range tries {
		if strings.Contains(selectedLine(m.screen(0)), want) {
			return true
		}
		mark := m.mark()
		m.send(t, key)
		m.quiet(t, 800*time.Millisecond, 10*time.Second)
		t.Logf("after %q selected %q", key, selectedLine(m.screen(mark)))
	}
	return strings.Contains(selectedLine(m.screen(0)), want)
}

func selectedLine(screen string) string {
	i := strings.LastIndex(screen, "❯")
	if i < 0 {
		return ""
	}
	line, _, _ := strings.Cut(screen[i:], "\n")
	return line
}

func (m *terminal) exit(t *testing.T) {
	t.Helper()
	m.send(t, "\x03")
	m.send(t, "\x03")
	select {
	case <-m.done:
	case <-time.After(20 * time.Second):
		t.Fatalf("claude did not exit after two ^C; screen:\n%s", tail(m.screen(0), 2000))
	}
}

func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}

func sameDir(t *testing.T, a, b string) bool {
	t.Helper()
	ai, err := os.Stat(a)
	if err != nil {
		t.Fatal(err)
	}
	bi, err := os.Stat(b)
	if err != nil {
		t.Fatal(err)
	}
	return os.SameFile(ai, bi)
}

func entryNames(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	names := []string{}
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

func copiesIn(t *testing.T, configDir, sid string) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(claudenative.ProjectsDir(configDir), "*", sid+".jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	return matches
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func writeFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func copyTree(t *testing.T, from, to string) {
	t.Helper()
	err := filepath.WalkDir(from, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(from, p)
		if err != nil {
			return err
		}
		dest := filepath.Join(to, rel)
		switch {
		case d.Type()&fs.ModeSymlink != 0:
			target, err := os.Readlink(p)
			if err != nil {
				return err
			}
			if err := os.MkdirAll(filepath.Dir(dest), 0o700); err != nil {
				return err
			}
			return os.Symlink(target, dest)
		case d.IsDir():
			return os.MkdirAll(dest, 0o700)
		}
		data, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		writeFile(t, dest, data)
		return nil
	})
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("copy %s: %v", from, err)
	}
}

func records(t *testing.T, data []byte) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range bytes.Split(data, []byte("\n")) {
		var rec map[string]any
		if json.Unmarshal(line, &rec) == nil {
			out = append(out, rec)
		}
	}
	return out
}

func lastUUID(data []byte) string {
	id := ""
	for _, line := range bytes.Split(data, []byte("\n")) {
		var rec struct {
			UUID string `json:"uuid"`
		}
		if json.Unmarshal(line, &rec) == nil && rec.UUID != "" {
			id = rec.UUID
		}
	}
	return id
}

func environmentLines(data []byte) []string {
	var out []string
	for _, line := range bytes.Split(data, []byte("\n")) {
		var rec struct {
			UUID       string          `json:"uuid"`
			Attachment json.RawMessage `json:"attachment"`
			Rendered   json.RawMessage `json:"rendered"`
		}
		var kind struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(line, &rec) == nil && json.Unmarshal(rec.Attachment, &kind) == nil && kind.Type == "environment" {
			out = append(out, rec.UUID+" "+string(rec.Attachment)+" "+string(rec.Rendered))
		}
	}
	return out
}

func stringsAt(t *testing.T, data []byte, key string) []string {
	t.Helper()
	var out []string
	var visit func(v any)
	visit = func(v any) {
		switch v := v.(type) {
		case []any:
			for _, e := range v {
				visit(e)
			}
		case map[string]any:
			for k, e := range v {
				if s, ok := e.(string); ok && k == key {
					out = append(out, s)
				}
				visit(e)
			}
		}
	}
	for _, rec := range records(t, data) {
		visit(rec)
	}
	return out
}

func relocatedValues(t *testing.T, source, installed []byte, key, from, to string) {
	t.Helper()
	var want []string
	for _, p := range stringsAt(t, source, key) {
		rel, ok := strings.CutPrefix(p, from+"/")
		if !ok {
			t.Errorf("source %s %s is not under %s", key, p, from)
			continue
		}
		want = append(want, to+"/"+rel)
	}
	got := stringsAt(t, installed, key)
	slices.Sort(want)
	slices.Sort(got)
	if len(want) == 0 || !slices.Equal(got, want) {
		t.Errorf("installed %s values %q, want %q: every source value relocated from %s to %s", key, got, want, from, to)
	}
}

func planPaths(t *testing.T, transcript []byte) []string {
	t.Helper()
	var out []string
	for _, p := range stringsAt(t, transcript, "planFilePath") {
		if !slices.Contains(out, p) {
			out = append(out, p)
		}
	}
	return out
}

func trackedFiles(t *testing.T, data []byte) []string {
	t.Helper()
	var keys []string
	add := func(k string) {
		if k != "" && !slices.Contains(keys, k) {
			keys = append(keys, k)
		}
	}
	for _, line := range bytes.Split(data, []byte("\n")) {
		var rec struct {
			Type     string `json:"type"`
			Snapshot struct {
				TrackedFileBackups map[string]struct {
					RealParentDir string `json:"realParentDir"`
				} `json:"trackedFileBackups"`
			} `json:"snapshot"`
			TrackingPath string `json:"trackingPath"`
			Backup       struct {
				RealParentDir string `json:"realParentDir"`
			} `json:"backup"`
		}
		if json.Unmarshal(line, &rec) != nil || !strings.HasPrefix(rec.Type, "file-history-") {
			continue
		}
		t.Logf("%s", tail(string(line), 600))
		for k, b := range rec.Snapshot.TrackedFileBackups {
			add(k)
			add(b.RealParentDir)
		}
		add(rec.TrackingPath)
		add(rec.Backup.RealParentDir)
	}
	return keys
}

func toolCalls(data []byte) []toolCall {
	var calls []toolCall
	index := map[string]int{}
	for _, line := range bytes.Split(data, []byte("\n")) {
		var rec struct {
			Message struct {
				Content json.RawMessage `json:"content"`
			} `json:"message"`
		}
		if json.Unmarshal(line, &rec) != nil {
			continue
		}
		var blocks []struct {
			Type      string          `json:"type"`
			ID        string          `json:"id"`
			Name      string          `json:"name"`
			Input     map[string]any  `json:"input"`
			ToolUseID string          `json:"tool_use_id"`
			Content   json.RawMessage `json:"content"`
			IsError   bool            `json:"is_error"`
		}
		if json.Unmarshal(rec.Message.Content, &blocks) != nil {
			continue
		}
		for _, b := range blocks {
			switch b.Type {
			case "tool_use":
				index[b.ID] = len(calls)
				calls = append(calls, toolCall{Name: b.Name, Input: b.Input})
			case "tool_result":
				if i, ok := index[b.ToolUseID]; ok {
					calls[i].Result, calls[i].IsError = blockText(b.Content), b.IsError
				}
			}
		}
	}
	return calls
}

func blockText(raw json.RawMessage) string {
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	var parts []struct {
		Text string `json:"text"`
	}
	_ = json.Unmarshal(raw, &parts)
	var b strings.Builder
	for _, p := range parts {
		b.WriteString(p.Text)
	}
	return b.String()
}

func readSummary(calls []toolCall) []string {
	out := make([]string, 0, len(calls))
	for _, c := range calls {
		out = append(out, fmt.Sprintf("%s %v error=%v", c.Name, c.Input["file_path"], c.IsError))
	}
	return out
}
