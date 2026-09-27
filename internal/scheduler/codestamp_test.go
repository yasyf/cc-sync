package scheduler

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/yasyf/reposync/worktree"
)

type unitStamper func(context.Context, Unit) (string, error)

func (f unitStamper) CodeStamp(ctx context.Context, u Unit) (string, error) { return f(ctx, u) }

func gitRepo(t *testing.T) worktree.Worktree {
	t.Helper()
	dir := t.TempDir()
	git := func(args ...string) {
		t.Helper()
		cmd := exec.CommandContext(t.Context(), "git", append([]string{"-C", dir, "-c", "user.name=t", "-c", "user.email=t@example.com", "-c", "commit.gpgsign=false", "-c", "core.hooksPath=/dev/null"}, args...)...) //nolint:gosec // G204: fixed git subcommands against the test's own temp repo.
		cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}
	git("init", "-q", "-b", "main")
	if err := os.WriteFile(filepath.Join(dir, "tracked.txt"), []byte("one\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	git("add", "tracked.txt")
	git("commit", "-q", "-m", "init")
	gitDir := filepath.Join(dir, ".git")
	return worktree.Worktree{Root: dir, GitDir: gitDir, CommonDir: gitDir, Kind: worktree.KindGit}
}

func TestCodeOnlyEditCapturesAtNextTierInterval(t *testing.T) {
	wt := gitRepo(t)
	stamper := unitStamper(func(ctx context.Context, _ Unit) (string, error) { return worktree.Stamp(ctx, wt) })
	synctest.Test(t, func(t *testing.T) {
		const interval = 10 * time.Minute
		inv := newInventory(unit("wt", "r", Session{ID: "s", LastActivity: ago(2 * time.Hour)}))
		capt := newCapturer()
		_, stop := startWith(t, Config{Tiers: Tiers{IdleInterval: interval}}, inv, stamper, capt, &fakePublisher{})
		start := time.Now()

		time.Sleep(2*interval + time.Second)
		if calls := capt.snapshot(); len(calls) != 1 {
			t.Fatalf("captures over two unchanged intervals = %d, want only the first", len(calls))
		}

		if err := os.WriteFile(filepath.Join(wt.Root, "tracked.txt"), []byte("two\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(wt.Root, "new.txt"), []byte("new\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		time.Sleep(interval)
		stop()

		calls := capt.snapshot()
		if len(calls) != 2 {
			t.Fatalf("captures after a code-only edit = %d, want a second capture", len(calls))
		}
		if got := calls[1].start.Sub(start); got != 3*interval {
			t.Errorf("code-only capture at %v, want the next tier interval %v", got, 3*interval)
		}
	})
}
