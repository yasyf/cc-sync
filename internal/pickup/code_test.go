package pickup

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

func TestReposyncRemove(t *testing.T) {
	ctx := t.Context()
	repo := filepath.Join(t.TempDir(), "repo")
	dest := filepath.Join(t.TempDir(), "recovery")
	for _, args := range [][]string{
		{"init", "-q", "-b", "main", repo},
		{"-C", repo, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "--allow-empty", "-m", "init"},
		{"-C", repo, "worktree", "add", "-q", "-b", "recovery/x", dest},
	} {
		if _, err := git(ctx, args...); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dest, "wip.txt"), []byte("dirty"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := (Reposync{}).Remove(ctx, Restored{Path: dest, Branch: "recovery/x"}); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if _, err := os.Stat(dest); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("recovery worktree survived: %v", err)
	}
	if out, err := git(ctx, "-C", repo, "branch", "--list", "recovery/x"); err != nil || out != "" {
		t.Errorf("recovery branch = %q, %v; want deleted", out, err)
	}
}
