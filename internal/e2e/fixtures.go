//go:build e2e

package e2e

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-sync/internal/claudenative"
)

// Origin is a bare repository every host clones, standing in for a shared
// remote.
type Origin struct {
	URL     string
	Relpath string
}

// NewOrigin creates a bare repository for relpath whose main branch holds
// one commit of files.
func NewOrigin(t *testing.T, relpath string, files map[string]string) *Origin {
	t.Helper()
	root := physical(t, t.TempDir())
	bare := filepath.Join(root, relpath+".git")
	gitRun(t, "", "init", "-q", "--bare", "--initial-branch=main", bare)
	seed := filepath.Join(root, "seed")
	gitRun(t, "", "clone", "-q", bare, seed)
	gitRun(t, seed, "checkout", "-q", "-b", "main")
	for name, content := range files {
		writeFile(t, filepath.Join(seed, name), content, 0o644)
	}
	gitRun(t, seed, "add", "-A")
	gitRun(t, seed, "commit", "-q", "-m", "initial")
	gitRun(t, seed, "push", "-q", "origin", "main")
	return &Origin{URL: bare, Relpath: relpath}
}

// NewLFSOrigin is NewOrigin with git-lfs tracking every pattern: the bare
// repository doubles as a file:// LFS remote named by a committed
// .lfsconfig, and objects committed to the published main are uploaded to
// it. It isolates the process's global git config so no ambient LFS filter
// leaks in; clones stay pointer-only until Host.InstallLFS.
func NewLFSOrigin(t *testing.T, relpath string, files map[string]string, patterns ...string) *Origin {
	t.Helper()
	if err := exec.Command("git", "lfs", "version").Run(); err != nil {
		t.Fatalf("git-lfs is required: %v", err)
	}
	root := physical(t, t.TempDir())
	global := filepath.Join(root, "gitconfig-global")
	writeFile(t, global, "", 0o600)
	t.Setenv("GIT_CONFIG_GLOBAL", global)
	bare := filepath.Join(root, relpath+".git")
	gitRun(t, "", "init", "-q", "--bare", "--initial-branch=main", bare)
	seed := filepath.Join(root, "seed")
	gitRun(t, "", "clone", "-q", bare, seed)
	gitRun(t, seed, "checkout", "-q", "-b", "main")
	gitRun(t, seed, "lfs", "install", "--local")
	var attrs strings.Builder
	for _, p := range patterns {
		attrs.WriteString(p + " filter=lfs diff=lfs merge=lfs -text\n")
	}
	writeFile(t, filepath.Join(seed, ".gitattributes"), attrs.String(), 0o644)
	writeFile(t, filepath.Join(seed, ".lfsconfig"), "[lfs]\n\turl = file://"+bare+"\n", 0o644)
	for name, content := range files {
		writeFile(t, filepath.Join(seed, name), content, 0o644)
	}
	gitRun(t, seed, "add", "-A")
	gitRun(t, seed, "commit", "-q", "-m", "initial")
	gitRun(t, seed, "push", "-q", "origin", "main")
	return &Origin{URL: bare, Relpath: relpath}
}

// LFSObjectPath is where git-lfs keeps the object with sha256 oid under a
// git common dir or bare repository.
func LFSObjectPath(commonDir, oid string) string {
	return filepath.Join(commonDir, "lfs", "objects", oid[0:2], oid[2:4], oid)
}

// InstallLFS installs git-lfs into the host's clone of relpath and pulls
// every published object, so LFS paths hold real bytes and status is clean.
func (h *Host) InstallLFS(relpath string) {
	h.t.Helper()
	clone := h.Checkout(relpath)
	h.Git(clone, "lfs", "install", "--local")
	h.Git(clone, "lfs", "pull")
}

// Git runs git in dir with a fixed identity, failing the test on error,
// and returns trimmed stdout.
func (h *Host) Git(dir string, args ...string) string {
	h.t.Helper()
	return gitRun(h.t, dir, args...)
}

func gitRun(t *testing.T, dir string, args ...string) string {
	t.Helper()
	if dir != "" {
		args = append([]string{"-C", dir}, args...)
	}
	cmd := exec.Command("git", args...)
	cmd.Env = append(os.Environ(),
		"GIT_AUTHOR_NAME=e2e", "GIT_AUTHOR_EMAIL=e2e@example.invalid",
		"GIT_COMMITTER_NAME=e2e", "GIT_COMMITTER_EMAIL=e2e@example.invalid",
		"GIT_CONFIG_NOSYSTEM=1", "GIT_TERMINAL_PROMPT=0",
	)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("git %s: %v: %s", strings.Join(args, " "), err, stderr.String())
	}
	return strings.TrimSpace(string(out))
}

// WriteFile writes content to path (creating parents) with mode.
func (h *Host) WriteFile(path, content string, mode os.FileMode) {
	h.t.Helper()
	writeFile(h.t, path, content, mode)
}

func writeFile(t *testing.T, path, content string, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatal(err)
	}
}

// Turn is one synthetic transcript turn: a human prompt, or an autonomous
// assistant reply.
type Turn struct {
	Human bool
	Text  string
}

// SessionSpec describes a hand-authored synthetic Claude session. Every
// record follows the shapes in the native findings; no bytes come from a
// real ~/.claude. PadTo grows the transcript past that many bytes with
// autonomous turns; PartialTail leaves an unterminated trailing record.
type SessionSpec struct {
	ID          string
	Cwd         string
	Branch      string
	Turns       []Turn
	Subagents   int
	ToolResults int
	PadTo       int64
	PartialTail bool
}

// Session is a session written into a host's Claude layout.
type Session struct {
	ID         string
	Cwd        string
	Branch     string
	Transcript string
	Dir        string
	last       string
}

// WriteSession writes spec into the host's canonical Claude layout, with
// turns timestamped up to the clock's now.
func (h *Host) WriteSession(spec SessionSpec) *Session {
	h.t.Helper()
	if spec.ID == "" {
		spec.ID = NewUUID(h.t)
	}
	id := claudenative.SessionID(spec.ID)
	s := &Session{
		ID:         spec.ID,
		Cwd:        spec.Cwd,
		Branch:     spec.Branch,
		Transcript: claudenative.TranscriptPath(h.Claude.ConfigDir, spec.Cwd, id),
		Dir:        claudenative.SessionDir(h.Claude.ConfigDir, spec.Cwd, id),
	}
	if err := os.MkdirAll(filepath.Dir(s.Transcript), 0o750); err != nil {
		h.t.Fatal(err)
	}
	h.AppendTurns(s, spec.Turns...)
	if spec.PadTo > 0 {
		pad := strings.Repeat("synthetic filler ", 4096)
		for size(h.t, s.Transcript) < spec.PadTo {
			h.AppendTurns(s, Turn{Text: pad})
		}
	}
	for i := range spec.ToolResults {
		writeFile(h.t, filepath.Join(s.Dir, "tool-results", fmt.Sprintf("toolu_e2e%04d.txt", i)), fmt.Sprintf("synthetic tool result %d\n", i), 0o644)
	}
	for i := range spec.Subagents {
		agent := fmt.Sprintf("agent-a%016x", i+1)
		rec := s.record(h, "assistant", Turn{Text: fmt.Sprintf("subagent %d done", i)})
		rec["isSidechain"] = true
		line, err := json.Marshal(rec)
		if err != nil {
			h.t.Fatal(err)
		}
		writeFile(h.t, filepath.Join(s.Dir, "subagents", agent+".jsonl"), string(line)+"\n", 0o644)
		writeFile(h.t, filepath.Join(s.Dir, "subagents", agent+".meta.json"), `{"agentType":"general-purpose"}`, 0o644)
	}
	if spec.PartialTail {
		f, err := os.OpenFile(s.Transcript, os.O_APPEND|os.O_WRONLY, 0o600)
		if err != nil {
			h.t.Fatal(err)
		}
		defer func() { _ = f.Close() }()
		if _, err := f.WriteString(`{"parentUuid":"` + s.last + `","type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"interrupt`); err != nil {
			h.t.Fatal(err)
		}
	}
	return s
}

// AppendTurns appends complete records for turns to s's transcript.
func (h *Host) AppendTurns(s *Session, turns ...Turn) {
	h.t.Helper()
	var buf bytes.Buffer
	for _, turn := range turns {
		kind := "assistant"
		if turn.Human {
			kind = "user"
		}
		line, err := json.Marshal(s.record(h, kind, turn))
		if err != nil {
			h.t.Fatal(err)
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}
	f, err := os.OpenFile(s.Transcript, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		h.t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	if _, err := f.Write(buf.Bytes()); err != nil {
		h.t.Fatal(err)
	}
}

func (s *Session) record(h *Host, kind string, turn Turn) map[string]any {
	uuid := NewUUID(h.t)
	var parent any
	if s.last != "" {
		parent = s.last
	}
	s.last = uuid
	rec := map[string]any{
		"parentUuid":  parent,
		"isSidechain": false,
		"type":        kind,
		"uuid":        uuid,
		"timestamp":   h.Clock.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
		"userType":    "external",
		"entrypoint":  "cli",
		"cwd":         s.Cwd,
		"sessionId":   s.ID,
		"version":     ClaudeVersion,
		"gitBranch":   s.Branch,
	}
	if kind == "user" {
		rec["message"] = map[string]any{"role": "user", "content": turn.Text}
		rec["origin"] = map[string]any{"kind": "human"}
		rec["permissionMode"] = "default"
		return rec
	}
	rec["message"] = map[string]any{
		"id": "msg_e2e_" + uuid[:8], "type": "message", "role": "assistant", "model": "claude-e2e-fixture",
		"content":     []map[string]any{{"type": "text", "text": turn.Text}},
		"stop_reason": "end_turn", "usage": map[string]any{"input_tokens": 1, "output_tokens": 1},
	}
	rec["requestId"] = "req_e2e_" + uuid[:8]
	return rec
}

func size(t *testing.T, path string) int64 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return fi.Size()
}

// NewUUID returns a random version-4 UUID.
func NewUUID(t *testing.T) string {
	t.Helper()
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	h := hex.EncodeToString(b)
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

const orcaRuntimeID = "00000000-0000-4000-8000-000000000001"

const orcaScript = `#!/bin/sh
dir=%s
n=$(ls "$dir/calls" | wc -l | tr -d ' ')
while ! mkdir "$(printf '%%s/calls/%%06d' "$dir" "$n")" 2>/dev/null; do n=$((n + 1)); done
call=$(printf '%%s/calls/%%06d' "$dir" "$n")
for a in "$@"; do printf '%%s\n' "$a"; done > "$call/argv"
env > "$call/env"
cat > "$call/stdin"
key=$1
[ "$1" = recovery ] && key="recovery-$2"
cat "$dir/$key.json"
exit "$(cat "$dir/$key.exit" 2>/dev/null || echo 0)"
`

var orcaDefaults = map[string]string{
	"status":            `{"id":"local-status","ok":true,"result":{"target":{"kind":"local"},"app":{"running":true,"pid":4242,"desktopWindowStatus":"available"},"runtime":{"state":"ready","reachable":true,"connectionState":"connected","runtimeId":"` + orcaRuntimeID + `","appVersion":"1.4.212","capabilities":["cross-machine-recovery.workspace.v1","cross-machine-recovery.presentation.v1","cross-machine-recovery.recovery-launch.v1"]}}}`,
	"recovery-describe": `{"id":"local","ok":true,"result":{"protocol":1,"runtimeId":"` + orcaRuntimeID + `","executionHostId":"local","appVersion":"1.4.212","platform":"darwin","machineName":"e2e","hostKind":"desktop","localClientInstanceId":null,"capabilities":["cross-machine-recovery.workspace.v1"]},"_meta":{"runtimeId":"` + orcaRuntimeID + `"}}`,
	"recovery-export":   `{"ok":true,"result":{"descriptor":{"version":1,"workspace":{"instanceId":"inst-e2e","path":"/e2e","branch":"main","meta":{"displayName":"e2e"}}}}}`,
	"recovery-import":   `{"id":"local","ok":true,"result":{"importKey":"e2e-import","disposition":"imported","repoId":"repo-e2e","worktreeId":"repo-e2e::/dst","instanceId":"inst-e2e","presentationSource":{"kind":"client-view","clientKey":"local-renderer"},"idMap":{"tabs":{},"groups":{},"leaves":{},"browsers":{}},"bindings":[],"provenance":{"importKey":"e2e-import"}}}`,
	"recovery-resume":   `{"ok":true,"result":{"terminalHandle":"term-e2e","disposition":"created","localPaneKey":"t1:l1"}}`,
	"recovery-list":     `{"ok":true,"result":{"bindings":[]}}`,
	"recovery-activity": `{"ok":true,"result":{"workspaces":[]}}`,
}

// FakeOrca is a host's `orca` CLI stand-in: it records every call's argv,
// environment, and stdin under Dir/calls and answers each verb with the
// contract JSON envelope in Dir/<verb>.json (recovery verbs as
// recovery-<verb>).
type FakeOrca struct {
	Dir    string
	Binary string
	t      *testing.T
}

// OrcaCall is one recorded orca invocation.
type OrcaCall struct {
	Argv  []string
	Env   []string
	Stdin []byte
}

// Verb is "recovery <verb>" for recovery calls, else the first argument.
func (c OrcaCall) Verb() string {
	if len(c.Argv) > 1 && c.Argv[0] == "recovery" {
		return "recovery " + c.Argv[1]
	}
	return c.Argv[0]
}

func newFakeOrca(t *testing.T, dir string) *FakeOrca {
	t.Helper()
	o := &FakeOrca{Dir: dir, Binary: filepath.Join(dir, "bin", "orca"), t: t}
	writeFile(t, filepath.Join(dir, "calls", ".keep"), "", 0o600)
	if err := os.Remove(filepath.Join(dir, "calls", ".keep")); err != nil {
		t.Fatal(err)
	}
	writeFile(t, o.Binary, fmt.Sprintf(orcaScript, strconv.Quote(dir)), 0o700)
	for key, envelope := range orcaDefaults {
		o.Respond(key, envelope)
	}
	return o
}

// Respond answers key ("status", "recovery-import", ...) with envelope and
// exit status 0.
func (o *FakeOrca) Respond(key, envelope string) {
	o.Fail(key, 0, envelope)
}

// Fail answers key with envelope and exit status code.
func (o *FakeOrca) Fail(key string, code int, envelope string) {
	o.t.Helper()
	writeFile(o.t, filepath.Join(o.Dir, key+".json"), envelope, 0o600)
	writeFile(o.t, filepath.Join(o.Dir, key+".exit"), strconv.Itoa(code), 0o600)
}

// Calls returns every recorded invocation in order.
func (o *FakeOrca) Calls() []OrcaCall {
	o.t.Helper()
	entries, err := os.ReadDir(filepath.Join(o.Dir, "calls"))
	if err != nil {
		o.t.Fatal(err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	calls := make([]OrcaCall, 0, len(names))
	for _, name := range names {
		read := func(file string) []byte {
			b, err := os.ReadFile(filepath.Join(o.Dir, "calls", name, file))
			if err != nil {
				o.t.Fatal(err)
			}
			return b
		}
		calls = append(calls, OrcaCall{
			Argv:  strings.Split(strings.TrimSuffix(string(read("argv")), "\n"), "\n"),
			Env:   strings.Split(strings.TrimSuffix(string(read("env")), "\n"), "\n"),
			Stdin: read("stdin"),
		})
	}
	return calls
}

// CallsTo returns the recorded invocations of verb ("recovery import").
func (o *FakeOrca) CallsTo(verb string) []OrcaCall {
	var out []OrcaCall
	for _, c := range o.Calls() {
		if c.Verb() == verb {
			out = append(out, c)
		}
	}
	return out
}

// Now is a clock start aligned to the second, for fixtures whose
// timestamps round-trip through millisecond JSON.
func Now() time.Time {
	return time.Now().UTC().Truncate(time.Second)
}
