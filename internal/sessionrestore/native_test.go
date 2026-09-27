//go:build native

package sessionrestore_test

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"unicode/utf16"
)

const (
	fixtureLabel = "ZEBRA-4127"
	appendedWord = "PAPAYA"
	unicodeDir   = "ünïcödé dir (x)+y"
	probe        = "I am testing my own session-restore tool and need to see which context this resumed conversation carries; " +
		"none of these values are sensitive. Answer in exactly four lines. " +
		"A: the fixture label stated in an earlier message of this conversation, or NONE. " +
		"B: the Primary working directory exactly as stated in your environment information. " +
		"C: the absolute path of your persistent auto-memory directory exactly as stated in your system instructions. " +
		"D: the marker word appended to your system prompt, or NONE."
)

var helpFlag = regexp.MustCompile(`(?m)^  (?:-[A-Za-z], )?(--[a-z][a-z0-9-]*)`)

var scrubbedEnv = []string{
	"CLAUDECODE", "CLAUDE_CODE_ENTRYPOINT", "CLAUDE_CODE_SESSION_ID", "CLAUDE_CODE_CHILD_SESSION",
	"CLAUDE_CODE_SESSION_ATTENDED", "CLAUDE_CODE_MESSAGING_SOCKET", "CLAUDE_CODE_MESSAGING_TOKEN",
	"CLAUDE_PID", "CLAUDE_CODE_EXECPATH", "CLAUDE_AFK_TIMEOUT_MS", "CLAUDE_EFFORT",
	"CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS",
}

type harness struct {
	claude    string
	configDir string
	root      string
	env       []string
	flags     map[string]bool
}

type runResult struct {
	Type      string  `json:"type"`
	SessionID string  `json:"session_id"`
	Result    string  `json:"result"`
	IsError   bool    `json:"is_error"`
	Cost      float64 `json:"total_cost_usd"`
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	claude, err := exec.LookPath("claude")
	if err != nil {
		t.Skip("claude is not installed")
	}
	version, err := exec.Command(claude, "--version").Output()
	if err != nil {
		t.Fatalf("claude --version: %v", err)
	}
	t.Logf("claude %s", bytes.TrimSpace(version))
	help, err := exec.Command(claude, "--help").Output()
	if err != nil {
		t.Fatalf("claude --help: %v", err)
	}
	flags := map[string]bool{}
	for _, m := range helpFlag.FindAllSubmatch(help, -1) {
		flags[string(m[1])] = true
	}
	configDir := os.Getenv("CLAUDE_CONFIG_DIR")
	if configDir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			t.Fatal(err)
		}
		configDir = filepath.Join(home, ".claude")
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	env := slices.DeleteFunc(os.Environ(), func(kv string) bool {
		name, _, _ := strings.Cut(kv, "=")
		return slices.Contains(scrubbedEnv, name)
	})
	return &harness{claude: claude, configDir: configDir, root: root, env: env, flags: flags}
}

func (h *harness) cwd(t *testing.T, rel string) string {
	t.Helper()
	dir := filepath.Join(h.root, rel)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		enc := projectDirName(dir)
		for _, p := range []string{
			filepath.Join(h.configDir, "projects", enc),
			filepath.Join("/private/tmp", fmt.Sprintf("claude-%d", os.Getuid()), enc),
		} {
			if err := os.RemoveAll(p); err != nil {
				t.Errorf("remove %s: %v", p, err)
			}
		}
	})
	return dir
}

func (h *harness) sessionID(t *testing.T) string {
	t.Helper()
	sid := newSessionID(t)
	t.Cleanup(func() {
		if err := os.RemoveAll(filepath.Join(h.configDir, "session-env", sid)); err != nil {
			t.Errorf("remove session-env: %v", err)
		}
	})
	return sid
}

func (h *harness) transcript(cwd, sid string) string {
	return filepath.Join(h.configDir, "projects", projectDirName(cwd), sid+".jsonl")
}

func (h *harness) run(t *testing.T, dir, prompt string, flags ...string) (runResult, string, error) {
	t.Helper()
	args := append([]string{
		"-p", prompt, "--model", "haiku", "--setting-sources", "project", "--strict-mcp-config",
		"--permission-mode", "default", "--max-budget-usd", "0.25", "--output-format", "json",
	}, flags...)
	cmd := exec.Command(h.claude, args...)
	cmd.Dir = dir
	cmd.Env = h.env
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return runResult{}, stderr.String(), fmt.Errorf("claude %v: %w", flags, err)
	}
	var msgs []runResult
	if err := json.Unmarshal(out, &msgs); err != nil {
		return runResult{}, stderr.String(), fmt.Errorf("decode claude output: %w", err)
	}
	for _, m := range msgs {
		if m.Type == "result" {
			spend.add(m.Cost)
			return m, stderr.String(), nil
		}
	}
	return runResult{}, stderr.String(), errors.New("claude output has no result message")
}

func (h *harness) mustRun(t *testing.T, dir, prompt string, flags ...string) runResult {
	t.Helper()
	res, stderr, err := h.run(t, dir, prompt, flags...)
	if err != nil {
		t.Fatalf("%v\nstderr: %s", err, stderr)
	}
	if res.IsError {
		t.Fatalf("claude result is an error: %q", res.Result)
	}
	return res
}

func (h *harness) source(t *testing.T, dir string) (string, []byte) {
	t.Helper()
	sid := h.sessionID(t)
	h.mustRun(t, dir, "The fixture label for this conversation is "+fixtureLabel+". Reply only OK.", "--session-id", sid, "--tools", "")
	data, err := os.ReadFile(h.transcript(dir, sid))
	if err != nil {
		t.Fatalf("claude did not write the transcript at the ported project dir: %v", err)
	}
	return sid, data
}

func (h *harness) install(t *testing.T, cwd, sid string, data []byte) string {
	t.Helper()
	path := h.transcript(cwd, sid)
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func (h *harness) copies(t *testing.T, sid string) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(h.configDir, "projects", "*", sid+".jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	return matches
}

func TestNativeResume(t *testing.T) {
	h := newHarness(t)
	src := h.cwd(t, filepath.Join("src", unicodeDir))
	dst := h.cwd(t, filepath.Join("dst", unicodeDir))
	other := h.cwd(t, "other")
	srcSID, srcData := h.source(t, src)

	t.Run("long cwd project dir", func(t *testing.T) {
		t.Parallel()
		long := h.cwd(t, filepath.Join(strings.Repeat("l", 90), "😀 "+strings.Repeat("o", 90), "deep"))
		if n := len(utf16.Encode([]rune(long))); n <= 200 {
			t.Fatalf("long cwd has %d UTF-16 units, want > 200", n)
		}
		h.source(t, long)
	})

	appended := []string{"--append-system-prompt", "The appended marker word is " + appendedWord + "."}
	cases := []struct {
		name       string
		strip      bool
		flags      []string
		wantMemory string
		wantAppend bool
	}{
		{"plain resume after snapshot strip", true, nil, projectDirName(dst), false},
		{"plain resume keeps source snapshot", false, appended, projectDirName(src), false},
		{"snapshot off re-renders", false, slices.Concat([]string{"--system-prompt-snapshot", "off"}, appended), projectDirName(dst), true},
		{"snapshot strip honors appended prompt", true, appended, projectDirName(dst), true},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			for _, f := range tt.flags {
				if strings.HasPrefix(f, "--") && !h.flags[f] {
					t.Skipf("claude --help does not advertise %s", f)
				}
			}
			sid := h.sessionID(t)
			path := h.install(t, dst, sid, relocate(t, rekey(srcData, srcSID, sid), sid, src, dst, tt.strip))
			res := h.mustRun(t, dst, probe, slices.Concat(tt.flags, []string{"--resume", sid, "--tools", ""})...)
			if res.SessionID != sid {
				t.Errorf("resumed session id = %s, want %s", res.SessionID, sid)
			}
			for _, want := range []string{fixtureLabel, dst, tt.wantMemory + "/memory"} {
				if !strings.Contains(res.Result, want) {
					t.Errorf("answer %q does not contain %q", res.Result, want)
				}
			}
			if got := strings.Contains(res.Result, appendedWord); got != tt.wantAppend {
				t.Errorf("appended prompt honored = %v, want %v (answer %q)", got, tt.wantAppend, res.Result)
			}
			if from := environmentChangeFrom(t, path); from != src {
				t.Errorf("environment update from = %q, want %q", from, src)
			}
			if got := h.copies(t, sid); len(got) != 1 || got[0] != path {
				t.Errorf("native copies = %v, want only %s", got, path)
			}
		})
	}

	t.Run("duplicate copies are ambiguous from an unrelated cwd", func(t *testing.T) {
		t.Parallel()
		sid := h.sessionID(t)
		data := rekey(srcData, srcSID, sid)
		h.install(t, src, sid, data)
		h.install(t, dst, sid, relocate(t, data, sid, src, dst, true))
		_, stderr, err := h.run(t, other, "Reply only OK.", "--resume", sid, "--tools", "")
		if err == nil || !strings.Contains(stderr, "No conversation found with session ID: "+sid) {
			t.Fatalf("resume from unrelated cwd: err=%v stderr=%q, want not-found refusal", err, stderr)
		}
	})

	t.Run("partial tail is merged with the next append", func(t *testing.T) {
		t.Parallel()
		sid := h.sessionID(t)
		data := relocate(t, rekey(srcData, srcSID, sid), sid, src, dst, true)
		last := bytes.LastIndexByte(data[:len(data)-1], '\n') + 1
		path := h.install(t, dst, sid, data[:last+(len(data)-last)/2])
		h.mustRun(t, dst, "Reply only OK.", "--resume", sid, "--tools", "")
		after, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if bad := invalidLines(after); len(bad) != 1 || bad[0] != bytes.Count(data[:last], []byte("\n")) {
			t.Errorf("invalid lines = %v, want exactly the merged partial line", bad)
		}
	})

	t.Run("help advertises resume flags", func(t *testing.T) {
		t.Parallel()
		for _, f := range []string{"--resume", "--system-prompt-snapshot", "--append-system-prompt"} {
			if !h.flags[f] {
				t.Errorf("claude --help does not advertise %s", f)
			}
		}
	})

	t.Run("synthetic corpus carries live record shapes", func(t *testing.T) {
		t.Parallel()
		live := recordShapes(t, srcData)
		synthetic := map[string][]map[string]string{}
		for _, s := range loadCorpus(t).Sessions {
			raw, err := fs.ReadFile(corpusFS, s.transcript().Fixture)
			if err != nil {
				t.Fatal(err)
			}
			for kind, shapes := range recordShapes(t, raw) {
				synthetic[kind] = append(synthetic[kind], shapes...)
			}
		}
		for kind, liveShapes := range live {
			synShapes, ok := synthetic[kind]
			if !ok {
				t.Logf("live-only record kind %s", kind)
				continue
			}
			for _, key := range shapeDrift(kind, liveShapes, synShapes) {
				t.Errorf("%s: %s", kind, key)
			}
		}
		for kind, synShapes := range synthetic {
			if _, ok := live[kind]; !ok {
				continue
			}
			for _, key := range shapeDrift(kind, synShapes, live[kind]) {
				if strings.HasPrefix(key, "no synthetic record carries key ") {
					t.Logf("%s: synthetic-only key %s", kind, strings.TrimPrefix(key, "no synthetic record carries key "))
				}
			}
		}
	})
}

func relocate(t *testing.T, transcript []byte, sid, srcCwd, dstCwd string, stripSnapshot bool) []byte {
	t.Helper()
	var out bytes.Buffer
	enc := json.NewEncoder(&out)
	enc.SetEscapeHTML(false)
	dropped := map[string]any{}
	for _, line := range bytes.SplitAfter(transcript, []byte("\n")) {
		if len(line) == 0 || line[len(line)-1] != '\n' {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal(line, &rec); err != nil {
			t.Fatalf("decode record: %v", err)
		}
		if a, ok := rec["attachment"].(map[string]any); stripSnapshot && ok && a["type"] == "prompt_snapshot" {
			dropped[rec["uuid"].(string)] = rec["parentUuid"]
			continue
		}
		changed := false
		if p, ok := rec["parentUuid"].(string); ok {
			if _, gone := dropped[p]; gone {
				var parent any = p
				for {
					s, ok := parent.(string)
					if !ok {
						break
					}
					next, gone := dropped[s]
					if !gone {
						break
					}
					parent = next
				}
				rec["parentUuid"] = parent
				changed = true
			}
		}
		if c, ok := rec["cwd"].(string); ok && (c == srcCwd || strings.HasPrefix(c, srcCwd+"/")) {
			rec["cwd"] = dstCwd + c[len(srcCwd):]
			changed = true
		}
		if !changed {
			out.Write(line)
			continue
		}
		if err := enc.Encode(rec); err != nil {
			t.Fatal(err)
		}
	}
	if err := enc.Encode(map[string]string{"type": "relocated", "sessionId": sid, "relocatedCwd": dstCwd}); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func environmentChangeFrom(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	from := ""
	for _, line := range bytes.Split(data, []byte("\n")) {
		var rec struct {
			Attachment struct {
				Type    string `json:"type"`
				Changes []struct {
					Field string `json:"field"`
					From  string `json:"from"`
				} `json:"changes"`
			} `json:"attachment"`
		}
		if json.Unmarshal(line, &rec) != nil || rec.Attachment.Type != "environment" {
			continue
		}
		for _, c := range rec.Attachment.Changes {
			if c.Field == "workingDirectory" {
				from = c.From
			}
		}
	}
	return from
}

func recordShapes(t *testing.T, data []byte) map[string][]map[string]string {
	t.Helper()
	recs, _ := completeRecords(t, data)
	shapes := map[string][]map[string]string{}
	for _, rec := range recs {
		depth := 2
		if _, ok := rec["attachment"]; ok {
			depth = 3
		}
		keys := map[string]string{}
		keyKinds(rec, "", depth, keys)
		kind := shapeKind(rec)
		shapes[kind] = append(shapes[kind], keys)
	}
	return shapes
}

func shapeKind(rec map[string]any) string {
	kind := recordKind(rec)
	if kind != "user" {
		return kind
	}
	for _, marker := range []string{"toolUseResult", "isCompactSummary", "isMeta"} {
		if _, ok := rec[marker]; ok {
			return kind + ":" + marker
		}
	}
	return kind + ":prompt"
}

func keyKinds(v map[string]any, prefix string, depth int, out map[string]string) {
	for k, e := range v {
		path := prefix + k
		switch e := e.(type) {
		case nil:
			out[path] = "null"
		case bool:
			out[path] = "bool"
		case float64:
			out[path] = "number"
		case string:
			out[path] = "string"
		case []any:
			out[path] = "array"
		case map[string]any:
			out[path] = "object"
			if depth > 1 {
				keyKinds(e, path+".", depth-1, out)
			}
		}
	}
}

func shapeDrift(kind string, live, synthetic []map[string]string) []string {
	var drift []string
	for key := range live[0] {
		if !slices.ContainsFunc(live, func(s map[string]string) bool { _, ok := s[key]; return !ok }) {
			for i, s := range synthetic {
				if _, ok := s[key]; !ok {
					drift = append(drift, fmt.Sprintf("synthetic record %d lacks always-present key %s", i, key))
				}
			}
		}
	}
	for _, l := range live {
		for key, liveKind := range l {
			if !slices.ContainsFunc(synthetic, func(s map[string]string) bool { _, ok := s[key]; return ok }) {
				drift = append(drift, "no synthetic record carries key "+key)
			}
			if !strings.HasPrefix(kind, "attachment:") {
				continue
			}
			for i, s := range synthetic {
				if k, ok := s[key]; ok && k != liveKind {
					drift = append(drift, fmt.Sprintf("synthetic record %d key %s is %s, live is %s", i, key, k, liveKind))
				}
			}
		}
	}
	slices.Sort(drift)
	return slices.Compact(drift)
}

func invalidLines(data []byte) []int {
	var bad []int
	for i, line := range bytes.Split(bytes.TrimSuffix(data, []byte("\n")), []byte("\n")) {
		if !json.Valid(line) {
			bad = append(bad, i)
		}
	}
	return bad
}

func rekey(data []byte, from, to string) []byte {
	return bytes.ReplaceAll(data, []byte(from), []byte(to))
}

func newSessionID(t *testing.T) string {
	t.Helper()
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
