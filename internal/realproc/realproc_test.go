//go:build realproc

// Package realproc_test drives the real synckitd and cc-sync binaries as
// foreground processes on isolated hosts: every HOME, DAEMONKIT_HOME, XDG,
// cc-sync, Claude, and TMPDIR path lives under one short temp root, launchd is
// never touched, and teardown kills exactly the processes the test started.
//
// Run with REALPROC_SYNCKIT naming the synckit source tree to build synckitd
// from, and optionally REALPROC_OLD_SYNCKIT naming an older synckit tree for
// the version-skew check:
//
//	REALPROC_SYNCKIT=~/src/synckit go test -tags realproc -count=1 -v ./internal/realproc/
package realproc_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/cc-sync/internal/resident"
	"github.com/yasyf/synckit/hostregistry"
)

const (
	maxDaemonkitHome = 37
	readyTimeout     = 90 * time.Second
	drainTimeout     = 40 * time.Second
	sessionID        = "f1c7c7e0-0000-4000-8000-00000000b001"
	codeword         = "ZEBRA-4127"
	wipLine          = "WIP-ZEBRA uncommitted edit"
	reposyncDecl     = "schema:{identity:string,version:uint64,fingerprint:string};host_registry:{self:string,hosts:array<string>,addrs:map<string,array<string>>};repo_sync:{default_location:string,repos:map<string,{added_at:int64,removed_at:int64,value:{relpath:string,trunk:string,local_only:bool,no_env_sync:bool}}>,local_repos:map<string,{added_at:int64,removed_at:int64,value:{relpath:string,trunk:string,local_only:bool,no_env_sync:bool}}>,settings:{idle_threshold:duration,repo_op_timeout:duration,push_after:duration}}"
	exitUnavailable  = 5
)

var (
	root    string
	bin     string
	oldBin  string
	buildOK bool
	procs   = &registry{}
)

func TestMain(m *testing.M) {
	code, err := run(m)
	if err != nil {
		fmt.Fprintln(os.Stderr, "realproc:", err)
		code = 1
	}
	os.Exit(code)
}

func run(m *testing.M) (int, error) {
	src := os.Getenv("REALPROC_SYNCKIT")
	if src == "" {
		fmt.Fprintln(os.Stderr, "realproc: REALPROC_SYNCKIT unset; every test skips")
		return m.Run(), nil
	}
	var err error
	root, err = os.MkdirTemp("/private/tmp", "ccs.")
	if err != nil {
		return 1, err
	}
	bin = filepath.Join(root, "bin")
	if err := build(src); err != nil {
		return 1, errors.Join(err, os.RemoveAll(root))
	}
	buildOK = true
	code := m.Run()
	if err := teardown(); err != nil {
		return 1, err
	}
	return code, nil
}

func build(src string) error {
	for _, dir := range []string{bin, filepath.Join(root, "oldbin")} {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return err
		}
	}
	module, err := filepath.Abs("../..")
	if err != nil {
		return err
	}
	steps := [][]string{
		{src, filepath.Join(bin, "synckitd.real"), "./cmd/synckitd"},
		{module, filepath.Join(bin, "cc-sync"), "./cmd/cc-sync"},
	}
	if old := os.Getenv("REALPROC_OLD_SYNCKIT"); old != "" {
		oldBin = filepath.Join(root, "oldbin")
		steps = append(steps, []string{old, filepath.Join(oldBin, "synckitd.real"), "./cmd/synckitd"})
	}
	for _, s := range steps {
		cmd := exec.Command("go", "build", "-o", s[1], s[2])
		cmd.Dir = s[0]
		if out, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("go build %s in %s: %w: %s", s[2], s[0], err, out)
		}
	}
	shims := map[string]string{
		filepath.Join(bin, "synckitd"): guardScript(filepath.Join(bin, "synckitd.real")),
		filepath.Join(bin, "claude"):   claudeScript,
		filepath.Join(bin, "orca"):     orcaScript,
	}
	if oldBin != "" {
		shims[filepath.Join(oldBin, "synckitd")] = guardScript(filepath.Join(oldBin, "synckitd.real"))
	}
	for path, body := range shims {
		if err := os.WriteFile(path, []byte(body), 0o700); err != nil { //nolint:gosec // G306: the shims must be executable.
			return err
		}
	}
	return nil
}

func guardScript(target string) string {
	return `#!/bin/sh
case "$1" in
  install|uninstall|host) echo "realproc: synckitd $1 is refused; it would converge the live launchd labels" >&2; exit 97;;
esac
exec ` + target + ` "$@"
`
}

const claudeScript = `#!/bin/sh
d="$FAKE_CLAUDE_DIR"; n=$(ls "$d" | wc -l | tr -d ' ')
for a in "$@"; do printf '%s\n' "$a"; done > "$d/$n.argv"; env > "$d/$n.env"; pwd > "$d/$n.cwd"
case "$1" in
  --version) echo "2.1.283 (Claude Code)";;
  --help) printf 'Usage: claude [options]\n  -r, --resume [value]  Resume a conversation\n  --append-system-prompt <prompt>  Append a system prompt\n';;
esac
exit 0
`

const orcaScript = `#!/bin/sh
d="$FAKE_ORCA_DIR"; n=$(ls "$d" | wc -l | tr -d ' ')
for a in "$@"; do printf '%s\n' "$a"; done > "$d/$n.argv"; env > "$d/$n.env"
key=$1; [ "$1" = recovery ] && key="recovery-$2"
case "$key" in
  status) echo '{"ok":true,"result":{"target":{"kind":"local"},"app":{"running":false}}}';;
  recovery-activity) echo '{"ok":true,"result":{"workspaces":[]}}';;
  recovery-list) echo '{"ok":true,"result":{"bindings":[]}}';;
  *) echo '{"ok":false,"error":{"code":"unsupported","message":"realproc fake orca"}}'; exit 1;;
esac
`

type registry struct {
	mu    sync.Mutex
	procs []*process
}

type process struct {
	name string
	cmd  *exec.Cmd
	done chan struct{}
}

func (r *registry) add(p *process) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.procs = append(r.procs, p)
}

func (r *registry) stopAll() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	errs := make([]error, 0, len(r.procs))
	for _, p := range r.procs {
		errs = append(errs, p.stop())
	}
	r.procs = nil
	return errors.Join(errs...)
}

func (p *process) stop() error {
	select {
	case <-p.done:
		return nil
	default:
	}
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return fmt.Errorf("SIGTERM %s: %w", p.name, err)
	}
	select {
	case <-p.done:
		return nil
	case <-time.After(drainTimeout):
	}
	if err := p.cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return fmt.Errorf("SIGKILL %s: %w", p.name, err)
	}
	<-p.done
	return fmt.Errorf("%s ignored SIGTERM for %s", p.name, drainTimeout)
}

func teardown() error {
	err := procs.stopAll()
	out, _ := exec.Command("pgrep", "-f", root).Output()
	if left := strings.TrimSpace(string(out)); left != "" {
		err = errors.Join(err, fmt.Errorf("processes still running under %s: %s", root, left))
	}
	scratch := filepath.Join("/private/tmp", "claude-"+strconv.Itoa(os.Getuid()))
	prefix := regexp.MustCompile(`[^0-9A-Za-z]`).ReplaceAllString(root, "-")
	entries, _ := os.ReadDir(scratch)
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), prefix) {
			err = errors.Join(err, os.RemoveAll(filepath.Join(scratch, e.Name())))
		}
	}
	return errors.Join(err, os.RemoveAll(root))
}

type host struct {
	t        *testing.T
	name     string
	self     string
	dir      string
	home     string
	dk       string
	bin      string
	claude   string
	orcaLog  string
	claudeLg string
	origin   string
	clone    string
	env      []string
}

func newHost(t *testing.T, name, binDir string) *host {
	t.Helper()
	if !buildOK {
		t.Skip("REALPROC_SYNCKIT unset")
	}
	dir := filepath.Join(root, name)
	h := &host{
		t:        t,
		name:     name,
		self:     "realproc@host-" + strings.ToLower(name),
		dir:      dir,
		home:     filepath.Join(dir, "home"),
		dk:       filepath.Join(dir, "dk"),
		bin:      binDir,
		orcaLog:  filepath.Join(dir, "orca-calls"),
		claudeLg: filepath.Join(dir, "claude-calls"),
		origin:   filepath.Join(dir, "origin", "proj.git"),
	}
	h.claude = filepath.Join(h.home, ".claude")
	h.clone = filepath.Join(h.home, "code", "proj")
	if len(h.dk) > maxDaemonkitHome {
		t.Fatalf("DAEMONKIT_HOME %s is %d bytes; sockets need <= %d", h.dk, len(h.dk), maxDaemonkitHome)
	}
	h.env = []string{
		"HOME=" + h.home,
		"USER=" + os.Getenv("USER"),
		"LOGNAME=" + os.Getenv("USER"),
		"XDG_CONFIG_HOME=" + filepath.Join(h.home, ".config"),
		"DAEMONKIT_HOME=" + h.dk,
		"CC_SYNC_CONFIG_DIR=" + filepath.Join(h.home, ".config", "cc-sync"),
		"CLAUDE_CONFIG_DIR=" + h.claude,
		"TMPDIR=" + filepath.Join(dir, "tmp") + "/",
		"PATH=" + binDir + ":" + bin + ":/usr/bin:/bin",
		"CC_SYNC_ORCA_BINARY=" + filepath.Join(bin, "orca"),
		"FAKE_ORCA_DIR=" + h.orcaLog,
		"FAKE_CLAUDE_DIR=" + h.claudeLg,
	}
	for _, kv := range h.env {
		key, value, _ := strings.Cut(kv, "=")
		switch key {
		case "USER", "LOGNAME", "PATH":
			continue
		}
		if !strings.HasPrefix(value, root+"/") {
			t.Fatalf("%s=%s escapes the isolation root %s", key, value, root)
		}
	}
	for _, d := range []string{h.dk, filepath.Join(dir, "tmp"), h.orcaLog, h.claudeLg, filepath.Join(h.home, ".config", "synckit"), filepath.Join(h.home, ".config", "reposync")} {
		if err := os.MkdirAll(d, 0o750); err != nil {
			t.Fatal(err)
		}
	}
	h.seedMesh()
	h.preflight()
	return h
}

func (h *host) seedMesh() {
	h.t.Helper()
	h.writeJSON(filepath.Join(h.home, ".config", "synckit", "state.json"), map[string]any{
		"schema":        map[string]any{"identity": hostregistry.Mesh.State.Identity, "version": 1, "fingerprint": hostregistry.Mesh.State.Fingerprint},
		"host_registry": map[string]any{"self": h.self, "hosts": []any{}},
		"synckit":       map[string]any{},
	})
}

func (h *host) preflight() {
	h.t.Helper()
	out, _ := h.run("synckitd", "status")
	sockets := regexp.MustCompile(`socket: (\S+)`).FindAllStringSubmatch(out, -1)
	if len(sockets) == 0 {
		h.t.Fatalf("synckitd status printed no socket:\n%s", out)
	}
	for _, s := range sockets {
		if !strings.HasPrefix(s[1], h.dk+"/") {
			h.t.Fatalf("synckitd socket %s is outside DAEMONKIT_HOME %s", s[1], h.dk)
		}
	}
}

func (h *host) writeJSON(path string, v any) {
	h.t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		h.t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		h.t.Fatal(err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o600); err != nil {
		h.t.Fatal(err)
	}
}

func (h *host) command(ctx context.Context, name string, args ...string) *exec.Cmd {
	path := filepath.Join(h.bin, name)
	if _, err := os.Stat(path); err != nil {
		path = filepath.Join(bin, name)
	}
	cmd := exec.CommandContext(ctx, path, args...)
	cmd.Env = h.env
	cmd.Dir = h.dir
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	return cmd
}

func (h *host) run(name string, args ...string) (string, int) {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	var stdout, stderr bytes.Buffer
	cmd := h.command(ctx, name, args...)
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	var exit *exec.ExitError
	switch {
	case errors.As(err, &exit):
	case err != nil:
		h.t.Fatalf("%s %v: %v", name, args, err)
	}
	h.t.Logf("[%s] %s %s → exit %d\nstdout: %s\nstderr: %s", h.name, name, strings.Join(args, " "), cmd.ProcessState.ExitCode(), strings.TrimSpace(stdout.String()), lastLines(stderr.String(), 6))
	return stdout.String() + stderr.String(), cmd.ProcessState.ExitCode()
}

func (h *host) json(want int, args ...string) map[string]any {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	var stdout, stderr bytes.Buffer
	cmd := h.command(ctx, "cc-sync", append(args, "--json")...)
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	var exit *exec.ExitError
	if err != nil && !errors.As(err, &exit) {
		h.t.Fatalf("cc-sync %v: %v", args, err)
	}
	h.t.Logf("[%s] cc-sync %s --json → exit %d\nstdout: %s\nstderr: %s", h.name, strings.Join(args, " "), cmd.ProcessState.ExitCode(), strings.TrimSpace(stdout.String()), lastLines(stderr.String(), 6))
	if got := cmd.ProcessState.ExitCode(); got != want {
		h.t.Fatalf("cc-sync %v exited %d, want %d", args, got, want)
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	if len(lines) != 1 {
		h.t.Fatalf("cc-sync %v printed %d stdout lines, want exactly one JSON document", args, len(lines))
	}
	var doc map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &doc); err != nil {
		h.t.Fatalf("cc-sync %v stdout is not JSON: %v", args, err)
	}
	if doc["version"] != float64(1) {
		h.t.Fatalf("cc-sync %v version = %v, want 1", args, doc["version"])
	}
	return doc
}

func (h *host) start(name string, args ...string) *process {
	h.t.Helper()
	log, err := os.Create(filepath.Join(h.dir, name+"-"+strings.Join(args, "-")+".log"))
	if err != nil {
		h.t.Fatal(err)
	}
	cmd := h.command(context.Background(), name, args...)
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil {
		h.t.Fatal(err)
	}
	p := &process{name: h.name + ":" + name + " " + strings.Join(args, " "), cmd: cmd, done: make(chan struct{})}
	go func() {
		_ = cmd.Wait()
		_ = log.Close()
		close(p.done)
	}()
	procs.add(p)
	h.t.Cleanup(func() {
		if err := p.stop(); err != nil {
			h.t.Error(err)
		}
	})
	return p
}

func (h *host) waitFor(what string, ok func() bool) time.Duration {
	h.t.Helper()
	start := time.Now()
	for time.Since(start) < readyTimeout {
		if ok() {
			h.t.Logf("[%s] %s after %s", h.name, what, time.Since(start).Round(time.Millisecond))
			return time.Since(start)
		}
		time.Sleep(250 * time.Millisecond)
	}
	h.t.Fatalf("[%s] %s: not within %s", h.name, what, readyTimeout)
	return 0
}

func (h *host) git(dir string, args ...string) {
	h.t.Helper()
	cmd := exec.Command("git", append([]string{"-c", "user.name=realproc", "-c", "user.email=realproc@example.invalid", "-c", "init.defaultBranch=main"}, args...)...)
	cmd.Dir = dir
	cmd.Env = append(slices.Clone(h.env), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null")
	if out, err := cmd.CombinedOutput(); err != nil {
		h.t.Fatalf("git %v: %v: %s", args, err, out)
	}
}

func (h *host) seedRepo() {
	h.t.Helper()
	if err := os.MkdirAll(filepath.Dir(h.origin), 0o750); err != nil {
		h.t.Fatal(err)
	}
	h.git(h.dir, "init", "-q", "--bare", h.origin)
	h.git(h.dir, "clone", "-q", h.origin, h.clone)
	h.writeFile(filepath.Join(h.clone, "a.txt"), "committed\n")
	h.git(h.clone, "add", "a.txt")
	h.git(h.clone, "commit", "-q", "-m", "a")
	h.git(h.clone, "push", "-q", "origin", "main")
	h.writeFile(filepath.Join(h.clone, "a.txt"), "committed\n"+wipLine+"\n")
	h.writeFile(filepath.Join(h.clone, "new.txt"), "untracked "+codeword+"\n")
	h.writeJSON(filepath.Join(h.home, ".config", "reposync", "state.json"), map[string]any{
		"schema":        map[string]any{"identity": "repo-sync-state-v1", "version": 1, "fingerprint": hostregistry.SchemaFingerprint("repo-sync-state-v1", reposyncDecl)},
		"host_registry": map[string]any{"self": h.self, "hosts": []any{}},
		"repo_sync": map[string]any{
			"default_location": filepath.Join(h.home, "code"),
			"repos": map[string]any{h.origin: map[string]any{
				"added_at": 1, "removed_at": 0,
				"value": map[string]any{"relpath": "proj", "trunk": "main", "local_only": false, "no_env_sync": false},
			}},
			"local_repos": map[string]any{},
			"settings":    map[string]any{"idle_threshold": "5m0s", "repo_op_timeout": "1m0s", "push_after": "1m0s"},
		},
	})
}

func (h *host) writeFile(path, body string) {
	h.t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		h.t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		h.t.Fatal(err)
	}
}

func projectDir(cwd string) string {
	return regexp.MustCompile(`[^0-9A-Za-z]`).ReplaceAllString(cwd, "-")
}

func (h *host) transcript() string {
	return filepath.Join(h.claude, "projects", projectDir(h.clone), sessionID+".jsonl")
}

func (h *host) seedSession() {
	h.t.Helper()
	now := time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
	common := map[string]any{"isSidechain": false, "userType": "external", "entrypoint": "cli", "cwd": h.clone, "sessionId": sessionID, "version": "2.1.283", "gitBranch": "main", "timestamp": now}
	rec := func(extra map[string]any) string {
		m := map[string]any{}
		for k, v := range common {
			m[k] = v
		}
		for k, v := range extra {
			m[k] = v
		}
		data, err := json.Marshal(m)
		if err != nil {
			h.t.Fatal(err)
		}
		return string(data) + "\n"
	}
	user := rec(map[string]any{
		"parentUuid": nil, "type": "user", "uuid": "f1c7c7e0-0004-4000-8000-0000000b0001", "permissionMode": "default", "promptSource": "user",
		"message": map[string]any{"role": "user", "content": "The codeword is " + codeword + ". Reply OK."},
	})
	assistant := rec(map[string]any{
		"parentUuid": "f1c7c7e0-0004-4000-8000-0000000b0001", "type": "assistant", "uuid": "f1c7c7e0-0004-4000-8000-0000000b0002", "requestId": "req_realproc_01",
		"message": map[string]any{"id": "msg_realproc_01", "type": "message", "role": "assistant", "model": "claude-haiku-4-5-20251001", "content": []any{map[string]any{"type": "text", "text": "OK"}}, "stop_reason": "end_turn", "stop_sequence": nil, "usage": map[string]any{"input_tokens": 12, "output_tokens": 2}},
	})
	h.writeFile(h.transcript(), user+assistant)
}

func (h *host) register() {
	h.t.Helper()
	data, err := json.MarshalIndent(resident.Manifest(), "", "  ")
	if err != nil {
		h.t.Fatal(err)
	}
	path := filepath.Join(h.dir, "tmp", "cc-sync-manifest.json")
	h.writeFile(path, string(data)+"\n")
	if out, code := h.run("synckitd", "register", path); code != 0 {
		h.t.Fatalf("synckitd register exited %d: %s", code, out)
	}
}

func (h *host) serveRunning() bool {
	out, _ := h.quiet("synckitd", "status")
	return strings.Contains(out, "daemon: running")
}

func (h *host) quiet(name string, args ...string) (string, int) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out, err := h.command(ctx, name, args...).CombinedOutput()
	var exit *exec.ExitError
	if err != nil && !errors.As(err, &exit) {
		h.t.Fatalf("%s %v: %v", name, args, err)
	}
	if exit != nil {
		return string(out), exit.ExitCode()
	}
	return string(out), 0
}

func (h *host) helperRunning() bool {
	out, code := h.quiet("cc-sync", "status", "--json")
	if code != 0 {
		return false
	}
	var doc struct {
		Helper struct {
			Running bool `json:"running"`
		} `json:"helper"`
	}
	return json.Unmarshal([]byte(strings.TrimSpace(lastLine(out))), &doc) == nil && doc.Helper.Running
}

func (h *host) calls(dir string) [][]string {
	h.t.Helper()
	entries, err := filepath.Glob(filepath.Join(dir, "*.argv"))
	if err != nil {
		h.t.Fatal(err)
	}
	out := make([][]string, 0, len(entries))
	for _, e := range entries {
		data, err := os.ReadFile(e)
		if err != nil {
			h.t.Fatal(err)
		}
		out = append(out, strings.Split(strings.TrimSuffix(string(data), "\n"), "\n"))
	}
	return out
}

func (h *host) envOf(dir string) []map[string]string {
	h.t.Helper()
	entries, err := filepath.Glob(filepath.Join(dir, "*.env"))
	if err != nil {
		h.t.Fatal(err)
	}
	out := make([]map[string]string, 0, len(entries))
	for _, e := range entries {
		data, err := os.ReadFile(e)
		if err != nil {
			h.t.Fatal(err)
		}
		env := map[string]string{}
		for line := range strings.SplitSeq(string(data), "\n") {
			if k, v, ok := strings.Cut(line, "="); ok {
				env[k] = v
			}
		}
		out = append(out, env)
	}
	return out
}

func lastLines(s string, n int) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	return strings.Join(lines[max(0, len(lines)-n):], "\n")
}

func lastLine(s string) string {
	return lastLines(s, 1)
}

func field(t *testing.T, doc any, path ...string) any {
	t.Helper()
	cur := doc
	for _, p := range path {
		switch c := cur.(type) {
		case map[string]any:
			v, ok := c[p]
			if !ok {
				t.Fatalf("JSON path %v: missing %q in %v", path, p, c)
			}
			cur = v
		case []any:
			i, err := strconv.Atoi(p)
			if err != nil || i >= len(c) {
				t.Fatalf("JSON path %v: bad index %q into %d items", path, p, len(c))
			}
			cur = c[i]
		default:
			t.Fatalf("JSON path %v: %q into %T", path, p, cur)
		}
	}
	return cur
}

func TestRealProcessHost(t *testing.T) {
	a := newHost(t, "A", bin)
	a.seedRepo()
	a.seedSession()

	install := a.json(0, "install", "--no-synckitd")
	if field(t, install, "ok") != true || field(t, install, "synckitd") != false {
		t.Fatalf("install --no-synckitd = %v", install)
	}
	a.register()

	unavailable := a.json(exitUnavailable, "status")
	if field(t, unavailable, "ok") != false || field(t, unavailable, "error", "code") != "unavailable" {
		t.Fatalf("status before serve = %v, want the unavailable error", unavailable)
	}

	a.start("cc-sync", "helper-serve")
	a.start("synckitd", "serve")
	a.waitFor("synckitd serve reports daemon: running", a.serveRunning)
	a.waitFor("cc-sync status reports the helper running", a.helperRunning)

	status := a.json(0, "status")
	if field(t, status, "helper", "running") != true || field(t, status, "helper", "build") == "" {
		t.Fatalf("status helper = %v", field(t, status, "helper"))
	}
	if got := field(t, status, "local", "host_id"); got != a.self {
		t.Fatalf("status local.host_id = %v, want %s", got, a.self)
	}
	if got := field(t, status, "local", "network", "status"); got == "" {
		t.Fatalf("status local.network.status is empty")
	}
	if peers := field(t, status, "peers").([]any); len(peers) != 0 {
		t.Fatalf("status peers = %v, want none on an isolated single-host mesh", peers)
	}

	synced := a.json(0, "sync")
	worktrees := field(t, synced, "worktrees").([]any)
	if len(worktrees) != 1 {
		t.Fatalf("sync worktrees = %v, want exactly the seeded clone", worktrees)
	}
	if got := field(t, worktrees[0], "sessions"); !slices.Contains(got.([]any), any(sessionID)) {
		t.Fatalf("sync sessions = %v, want %s", got, sessionID)
	}
	if field(t, worktrees[0], "checkpoint") == nil {
		t.Fatalf("sync produced no checkpoint: %v", worktrees[0])
	}

	for _, args := range [][]string{{"list"}, {"list", "--all"}} {
		if items := field(t, a.json(0, args...), "items").([]any); len(items) != 0 {
			t.Fatalf("%v items = %v, want none: list offers only peers' workspaces", args, items)
		}
	}

	if out, code := a.run("synckitd", "reconcile"); code != 0 {
		t.Fatalf("synckitd reconcile exited %d: %s", code, out)
	}

	orca := a.calls(a.orcaLog)
	if len(orca) == 0 {
		t.Fatal("the fake orca recorded no call; the helper may have reached the live Orca")
	}
	for i, env := range a.envOf(a.orcaLog) {
		if env["HOME"] != a.home {
			t.Fatalf("orca call %d ran with HOME=%s, want %s", i, env["HOME"], a.home)
		}
		for _, k := range []string{"ORCA_ENVIRONMENT", "ORCA_PAIRING_CODE", "ORCA_REMOTE_PAIRING"} {
			if _, ok := env[k]; ok {
				t.Fatalf("orca call %d carried remote-routing %s", i, k)
			}
		}
	}
	claude := a.calls(a.claudeLg)
	for i, env := range a.envOf(a.claudeLg) {
		if env["HOME"] != a.home || env["CLAUDE_CONFIG_DIR"] != a.claude {
			t.Fatalf("claude call %d ran with HOME=%s CLAUDE_CONFIG_DIR=%s", i, env["HOME"], env["CLAUDE_CONFIG_DIR"])
		}
	}
	t.Logf("orca calls: %v; claude calls: %v", orca, claude)
}

func TestCrossHostDelivery(t *testing.T) {
	t.Skip("gate G2 (also gates pickup: list and pickup offer only a peer's delivered workspace, never this host's own): synckit dials /usr/bin/ssh by absolute path with a sealed argv " +
		"(-F /dev/null, IdentityAgent=none, ProxyCommand=none; hostregistry/ssh_contract.go, hostregistry/dial.go), " +
		"so a PATH-injected ssh shim cannot route A→B, and Remote Login is off on this machine. " +
		"Opening it needs a peer with sshd whose synckitd_path points at an isolation wrapper, or a VM; " +
		"the in-process E2E harness covers A→B delivery and remote pickup until then.")
}

func TestInstallProbeRace(t *testing.T) {
	t.Skip("gate G1: synckit's launchd labels are fixed constants (internal/serviceidentity, daemon/service.go) " +
		"and daemonkit always targets gui/<uid>/<label> through /bin/launchctl, so an isolated `synckitd install` " +
		"would boot out and replace the live com.github.yasyf.synckit.serve; the rendered plist Env also drops " +
		"DAEMONKIT_HOME/HOME. No launchctl call is made. Opening it needs a label-prefix override honored under " +
		"DAEMONKIT_HOME plus isolation-env propagation into the plists, or a throwaway macOS user/VM.")
}

func TestVersionSkewOldSynckitd(t *testing.T) {
	if oldBin == "" {
		t.Skip("REALPROC_OLD_SYNCKIT unset")
	}
	c := newHost(t, "C", oldBin)
	c.json(0, "install", "--no-synckitd")
	c.register()
	c.start("synckitd", "serve")
	c.waitFor("old synckitd serve reports daemon: running", c.serveRunning)

	doc := c.json(exitUnavailable, "status")
	if field(t, doc, "ok") != false || field(t, doc, "error", "code") != "unavailable" {
		t.Fatalf("status against an old synckitd = %v, want the typed unavailable error", doc)
	}
	if msg, _ := field(t, doc, "error", "message").(string); !strings.Contains(msg, "synckitd too old") {
		t.Fatalf("status error message = %q, want it to name synckitd too old", msg)
	}
}
