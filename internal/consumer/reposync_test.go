package consumer

import (
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/yasyf/reposync/registry"
	"github.com/yasyf/reposync/worktree"

	"github.com/yasyf/cc-sync/internal/capture"
	"github.com/yasyf/cc-sync/internal/codesnap"
	"github.com/yasyf/synckit/artifact"
)

func runGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "git", append([]string{"-C", dir, "-c", "user.name=t", "-c", "user.email=t@example.com", "-c", "commit.gpgsign=false", "-c", "core.hooksPath=/dev/null"}, args...)...) //nolint:gosec // G204: fixed git subcommands against the test's own temp repos.
	cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

func commitFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, dir, "add", name)
	runGit(t, dir, "commit", "-q", "-m", "edit "+name)
	return runGit(t, dir, "rev-parse", "HEAD")
}

func repoRegistry(origin, path string) registry.Registry {
	return registry.Registry{Repos: []registry.Repo{{Relpath: "wt", Path: path, Origin: origin, Trunk: "main"}}}
}

type checkpointFixture struct {
	store    *artifact.Store
	root     artifact.Ref
	trunkTip string
	verifier Reposync
	receiver string
}

func newCheckpointFixture(t *testing.T) checkpointFixture {
	t.Helper()
	base := t.TempDir()
	origin := filepath.Join(base, "origin.git")
	runGit(t, base, "init", "-q", "--bare", "-b", "main", origin)
	src := filepath.Join(base, "src")
	runGit(t, base, "clone", "-q", origin, src)
	commitFile(t, src, "a.txt", "one\n")
	runGit(t, src, "push", "-q", "origin", "main")
	recv := filepath.Join(base, "recv")
	runGit(t, base, "clone", "-q", origin, recv)

	trunkTip := commitFile(t, src, "trunk.txt", "published after the receiver cloned\n")
	runGit(t, src, "push", "-q", "origin", "main")
	commitFile(t, src, "b.txt", "unpublished\n")
	if err := os.WriteFile(filepath.Join(src, "a.txt"), []byte("wip\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "untracked.txt"), []byte("new\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	store, err := artifact.Open(filepath.Join(base, "artifacts"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	wts, _, err := worktree.Discover(t.Context(), repoRegistry(origin, src))
	if err != nil || len(wts) != 1 {
		t.Fatalf("Discover = %+v, %v; want the one source worktree", wts, err)
	}
	srcCode, err := worktree.OpenStore(filepath.Join(base, "src-code"))
	if err != nil {
		t.Fatal(err)
	}
	sink, err := codesnap.NewSink(store, filepath.Join(base, "code-index"))
	if err != nil {
		t.Fatal(err)
	}
	snap, err := srcCode.Capture(t.Context(), wts[0], sink, worktree.CaptureOptions{Source: "host-a"})
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	code, err := sink.BuildCodeManifest(t.Context(), snap)
	if err != nil {
		t.Fatal(err)
	}
	root, err := store.PutGroup(t.Context(), capture.MediaCheckpoint, []artifact.Ref{code})
	if err != nil {
		t.Fatal(err)
	}
	recvCode, err := worktree.OpenStore(filepath.Join(base, "recv-code"))
	if err != nil {
		t.Fatal(err)
	}
	return checkpointFixture{
		store:    store,
		root:     root,
		trunkTip: trunkTip,
		receiver: recv,
		verifier: Reposync{
			Store:    store,
			Code:     recvCode,
			Registry: func() (registry.Registry, error) { return repoRegistry(origin, recv), nil },
		},
	}
}

func TestReposyncVerifyCode(t *testing.T) {
	f := newCheckpointFixture(t)

	deferred, err := f.verifier.VerifyCode(t.Context(), f.root, false)
	if err != nil {
		t.Fatalf("VerifyCode without fetch: %v", err)
	}
	if deferred.Ready || !slices.Contains(deferred.Missing, f.trunkTip) {
		t.Fatalf("VerifyCode without fetch = %+v, want not ready and missing trunk tip %s", deferred, f.trunkTip)
	}

	ready, err := f.verifier.VerifyCode(t.Context(), f.root, true)
	if err != nil {
		t.Fatalf("VerifyCode with fetch: %v", err)
	}
	if !ready.Ready || len(ready.Missing) != 0 {
		t.Fatalf("VerifyCode with fetch = %+v, want ready", ready)
	}
	if got := runGit(t, f.receiver, "cat-file", "-t", f.trunkTip); got != "commit" {
		t.Errorf("receiver object %s type = %q after the fetch, want commit", f.trunkTip, got)
	}

	again, err := f.verifier.VerifyCode(t.Context(), f.root, false)
	if err != nil || !again.Ready {
		t.Errorf("VerifyCode after fetch without fetching = %+v, %v; want ready", again, err)
	}
}

func TestReposyncVerifyCodeWithoutCodeGroup(t *testing.T) {
	store, err := artifact.Open(filepath.Join(t.TempDir(), "artifacts"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	blob, err := store.Put(t.Context(), strings.NewReader("session"), "cc-sync.session-manifest")
	if err != nil {
		t.Fatal(err)
	}
	session, err := store.PutGroup(t.Context(), "cc-sync.session", []artifact.Ref{blob})
	if err != nil {
		t.Fatal(err)
	}
	root, err := store.PutGroup(t.Context(), capture.MediaCheckpoint, []artifact.Ref{session})
	if err != nil {
		t.Fatal(err)
	}
	v := Reposync{Store: store, Registry: func() (registry.Registry, error) {
		t.Fatal("registry loaded for a checkpoint with no code")
		return registry.Registry{}, nil
	}}
	got, err := v.VerifyCode(t.Context(), root, true)
	if err != nil || got.Ready || !slices.Equal(got.Missing, []string{"code"}) {
		t.Errorf("VerifyCode = %+v, %v; want not ready, missing code", got, err)
	}
}
