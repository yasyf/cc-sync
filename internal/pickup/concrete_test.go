package pickup_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-sync/internal/capture"
	"github.com/yasyf/cc-sync/internal/catalog"
	"github.com/yasyf/cc-sync/internal/claudenative"
	"github.com/yasyf/cc-sync/internal/cli"
	"github.com/yasyf/cc-sync/internal/consumer"
	"github.com/yasyf/cc-sync/internal/inventory"
	"github.com/yasyf/cc-sync/internal/pickup"
	"github.com/yasyf/cc-sync/internal/scheduler"
	"github.com/yasyf/reposync/registry"
	"github.com/yasyf/reposync/worktree"
	"github.com/yasyf/synckit/artifact"
)

const (
	self   = "host-a"
	origin = "https://example.com/r.git"
	sid    = "3f2c9a4e-1b7d-4c8e-9f60-2a5b7c1d8e94"
)

type concrete struct {
	t        *testing.T
	root     string
	src      string
	dst      string
	artifact string
	catalog  *catalog.Store
	wt       worktree.Worktree
	pins     []string
}

func git(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...) //nolint:gosec // G204: a fixed git binary with test-built argv, never a shell string.
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %s: %v: %s", strings.Join(args, " "), err, out)
	}
}

func write(t *testing.T, path, data string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
}

func read(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path) //nolint:gosec // G304: a path under the test's temp dir.
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

type publisher struct{}

func (publisher) Publish(context.Context) error { return nil }

type orcaExport struct{ path string }

func (o orcaExport) Export(context.Context, string) ([]byte, error) {
	return []byte(`{"version":1,"workspace":{"worktreeId":"w1","instanceId":"inst-1","path":"` + o.path + `","branch":"feat","meta":{"displayName":"Feature"}},"omittedBindings":[]}`), nil
}

type targets struct {
	wt       worktree.Worktree
	sessions []claudenative.Session
}

func (s targets) Target(id string) (inventory.Target, bool) {
	return inventory.Target{Worktree: s.wt, Sessions: s.sessions}, id == s.wt.ID
}

type ready struct{}

func (ready) VerifyCode(context.Context, artifact.Ref, bool) (consumer.CodeVerdict, error) {
	return consumer.CodeVerdict{Ready: true}, nil
}

type noProcs struct{}

func (noProcs) Processes(context.Context) ([]claudenative.Process, error) { return nil, nil }

type pinner struct{ c *concrete }

func (p pinner) Pin(_ context.Context, owner string, roots []artifact.Ref, _ time.Duration) error {
	p.c.pins = append(p.c.pins, owner+":"+map[bool]string{true: "pin", false: "unpin"}[len(roots) > 0])
	return nil
}

func claudeProbe(_ context.Context, args ...string) ([]byte, error) {
	if args[0] == "--version" {
		return []byte("2.1.90 (Claude Code)\n"), nil
	}
	return []byte("  -r, --resume [value]  Resume a conversation\n  --append-system-prompt <prompt>\n"), nil
}

func newConcrete(t *testing.T) *concrete {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(root, "gitconfig"), "[user]\n\tname = cc-sync test\n\temail = test@example.com\n[init]\n\tdefaultBranch = main\n")
	t.Setenv("GIT_CONFIG_GLOBAL", filepath.Join(root, "gitconfig"))
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")
	c := &concrete{t: t, root: root, src: filepath.Join(root, "src", "r"), dst: filepath.Join(root, "dst", "r"), artifact: filepath.Join(root, "artifacts")}
	if err := os.MkdirAll(c.src, 0o750); err != nil {
		t.Fatal(err)
	}
	git(t, c.src, "init", "-q")
	write(t, filepath.Join(c.src, "a.txt"), "one\n")
	git(t, c.src, "add", "a.txt")
	git(t, c.src, "commit", "-q", "-m", "base")
	git(t, c.src, "remote", "add", "origin", origin)
	git(t, c.src, "update-ref", "refs/remotes/origin/main", "HEAD")
	git(t, root, "clone", "-q", c.src, c.dst)
	git(t, c.dst, "remote", "set-url", "origin", origin)
	git(t, c.src, "checkout", "-q", "-b", "feat")
	write(t, filepath.Join(c.src, "a.txt"), "two\n")
	write(t, filepath.Join(c.src, "b.txt"), "new\n")

	wts, _, err := worktree.Discover(t.Context(), registry.Registry{
		DefaultLocation: filepath.Join(root, "src"),
		Repos:           []registry.Repo{{Relpath: "r", Path: c.src, Origin: origin, Trunk: "main"}},
	})
	if err != nil || len(wts) != 1 {
		t.Fatalf("Discover = %+v, %v; want the source worktree", wts, err)
	}
	c.wt = wts[0]
	c.capture()
	return c
}

func (c *concrete) capture() {
	t := c.t
	t.Helper()
	config := filepath.Join(c.root, "src-claude")
	dir := claudenative.ProjectDirName(c.src)
	at := time.Now().Add(-time.Minute).UTC().Truncate(time.Second)
	transcript := filepath.Join(claudenative.ProjectsDir(config), dir, sid+".jsonl")
	write(t, transcript, `{"type":"user","uuid":"u-1","sessionId":"`+sid+`","version":"2.1.90","gitBranch":"feat","cwd":"`+c.src+`","origin":{"kind":"human"},"timestamp":"`+at.Format(time.RFC3339)+`","message":{"content":"edit a.txt"}}`+"\n")
	id, err := claudenative.ParseSessionID(sid)
	if err != nil {
		t.Fatal(err)
	}
	session := claudenative.Session{
		ID: id, ConfigDir: config, ProjectDirName: dir, TranscriptPath: transcript,
		Cwd: c.src, OriginalCwd: c.src, GitBranch: "feat", Version: "2.1.90", Title: "edit a.txt",
		LastHumanInput: at, LastActivity: at,
	}
	store, err := artifact.Open(c.artifact)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	code, err := worktree.OpenStore(filepath.Join(c.root, "src-reposync"))
	if err != nil {
		t.Fatal(err)
	}
	c.catalog = catalog.New(filepath.Join(c.root, "catalog.json"), self, time.Now)
	job := capture.New(capture.Config{
		Self:      self,
		Layout:    claudenative.Layout{ConfigDir: config, TmpRoot: filepath.Join(c.root, "src-tmp"), UID: os.Getuid()},
		Home:      filepath.Join(c.root, "src-home"),
		Store:     store,
		Code:      code,
		Stamper:   capture.StampFunc(worktree.Stamp),
		Orca:      orcaExport{path: c.src},
		Catalog:   c.catalog,
		Publisher: publisher{},
		Targets:   targets{wt: c.wt, sessions: []claudenative.Session{session}},
		CodeIndex: filepath.Join(c.root, "codesnap"),
		StateDir:  filepath.Join(c.root, "capture"),
		Now:       time.Now,
	})
	if _, err := job.Capture(t.Context(), scheduler.Unit{WorktreeID: c.wt.ID, RepoKey: c.wt.CommonDir, MetaStamp: "meta-1"}); err != nil {
		t.Fatalf("Capture: %v", err)
	}
}

func (c *concrete) config(orca pickup.Orca) pickup.Config {
	code, err := worktree.OpenStore(filepath.Join(c.root, "dst-reposync"))
	if err != nil {
		c.t.Fatal(err)
	}
	reg := registry.Registry{
		DefaultLocation: filepath.Join(c.root, "dst"),
		Repos:           []registry.Repo{{Relpath: "r", Path: c.dst, Origin: origin, Trunk: "main"}},
	}
	return pickup.Config{
		Catalog: c.catalog,
		Pinner:  pinner{c},
		OpenStore: func(context.Context) (pickup.Store, error) {
			return artifact.OpenReadOnly(c.artifact)
		},
		Verifier: ready{},
		Code:     pickup.Reposync{Worktrees: pickup.Worktrees{Store: code, Registry: func() (registry.Registry, error) { return reg, nil }}},
		Sessions: &pickup.Native{
			Run:           claudeProbe,
			Procs:         noProcs{},
			DisplacedRoot: filepath.Join(c.root, "displaced"),
			Now:           time.Now,
		},
		Orca:         orca,
		FetchAllowed: func() bool { return false },
		Layout:       claudenative.Layout{ConfigDir: filepath.Join(c.root, "dst-claude"), TmpRoot: filepath.Join(c.root, "dst-tmp"), UID: os.Getuid()},
		Home:         filepath.Join(c.root, "dst-home"),
		ReplicaRoot:  filepath.Join(c.root, "replicas"),
		CheckoutRoot: filepath.Join(c.root, "checkouts"),
		Now:          time.Now,
	}
}

func (c *concrete) checkRestored(res pickup.Result) {
	t := c.t
	t.Helper()
	co := res.Checkout
	if co.Path == "" || co.Reused || !co.Exact || len(co.Differences) > 0 || co.Sparse != nil {
		t.Fatalf("Checkout = %+v, want a fresh exact recovery checkout", co)
	}
	if !strings.HasPrefix(co.Path, filepath.Join(c.root, "checkouts")+string(filepath.Separator)) {
		t.Errorf("checkout %s is outside the checkout root", co.Path)
	}
	for name, want := range map[string]string{"a.txt": "two\n", "b.txt": "new\n"} {
		if got := read(t, filepath.Join(co.Path, name)); got != want {
			t.Errorf("%s = %q, want %q", name, got, want)
		}
	}
	installed := filepath.Join(claudenative.ProjectsDir(filepath.Join(c.root, "dst-claude")), claudenative.ProjectDirName(co.Path), sid+".jsonl")
	if got := read(t, installed); !strings.Contains(got, `"cwd":"`+co.Path+`"`) || strings.Contains(got, c.src) {
		t.Errorf("installed transcript = %s, want its cwd relocated to %s", got, co.Path)
	}
	if want := []string{"pin", "unpin"}; len(c.pins) != 2 || !strings.HasSuffix(c.pins[0], ":"+want[0]) || !strings.HasSuffix(c.pins[1], ":"+want[1]) {
		t.Errorf("pins = %v, want one pin then its release", c.pins)
	}
}

func TestPickupConcreteNoOrca(t *testing.T) {
	c := newConcrete(t)
	res, err := pickup.New(c.config(nil)).Run(t.Context(), pickup.Request{
		Target: cli.ItemRef{SourceHostID: self, WorkspaceID: c.wt.ID},
		NoOrca: true,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	c.checkRestored(res)
	if len(res.Sessions) != 1 || res.Orca != nil {
		t.Fatalf("Sessions = %+v, Orca = %+v; want one session and no Orca import", res.Sessions, res.Orca)
	}
	got := res.Sessions[0]
	if got.Launch == nil {
		t.Fatalf("session %+v has no launch", got)
	}
	launch := *got.Launch
	got.Launch = nil
	if want := (pickup.Session{SessionID: sid, Status: pickup.StatusRestored, Selected: true}); !reflect.DeepEqual(got, want) {
		t.Errorf("session = %+v, want %+v", got, want)
	}
	if len(launch.Argv) != 5 || launch.Argv[3] != "--append-system-prompt" ||
		!strings.Contains(launch.Argv[4], "from host "+self) || !strings.Contains(launch.Argv[4], "It now runs in "+res.Checkout.Path+" on this host.") {
		t.Errorf("launch argv = %q, want claude --resume with the recovery system prompt", launch.Argv)
	}
	launch.Argv = launch.Argv[:3]
	want := pickup.Launch{
		Argv:     []string{"claude", "--resume", sid},
		Dir:      res.Checkout.Path,
		EnvUnset: []string{"CLAUDECODE", "CLAUDE_CODE_ENTRYPOINT"},
		EnvSet:   map[string]string{"CLAUDE_CONFIG_DIR": filepath.Join(c.root, "dst-claude")},
	}
	if !reflect.DeepEqual(launch, want) {
		t.Errorf("launch = %#v, want %#v", launch, want)
	}
}
