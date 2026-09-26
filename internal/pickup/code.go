package pickup

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"

	"github.com/yasyf/cc-sync/internal/codesnap"
	"github.com/yasyf/reposync/worktree"
	"github.com/yasyf/synckit/artifact"
)

// WorktreeRestorer is reposync's worktree.Store.Restore with the receiver's
// registry bound.
type WorktreeRestorer interface {
	Restore(ctx context.Context, snap worktree.Snapshot, src worktree.ArtifactSource, opts RestoreOptions) (Restored, error)
}

// Reposync is the CodeRestorer that reads a code group through codesnap and
// restores it with reposync.
type Reposync struct {
	Worktrees WorktreeRestorer
}

// Restore restores the code group code from store as a recovery checkout.
func (c Reposync) Restore(ctx context.Context, store codesnap.Reader, code artifact.Ref, opts RestoreOptions) (Restored, error) {
	src, err := codesnap.SourceFromManifest(ctx, store, code)
	if err != nil {
		return Restored{}, err
	}
	snap, err := src.Snapshot(ctx)
	if err != nil {
		return Restored{}, err
	}
	restored, err := c.Worktrees.Restore(ctx, snap, src, opts)
	if errors.Is(err, worktree.ErrDestinationExists) || errors.Is(err, worktree.ErrPathCollision) {
		return Restored{}, fmt.Errorf("%w: %w", ErrCheckoutConflict, err)
	}
	return restored, err
}

// Remove deletes the linked recovery worktree r and its recovery branch.
func (Reposync) Remove(ctx context.Context, r Restored) error {
	common, err := git(ctx, "-C", r.Path, "rev-parse", "--path-format=absolute", "--git-common-dir")
	if err != nil {
		return err
	}
	if _, err := git(ctx, "--git-dir", common, "worktree", "remove", "--force", r.Path); err != nil {
		return err
	}
	if r.Branch == "" {
		return nil
	}
	_, err = git(ctx, "--git-dir", common, "branch", "-D", r.Branch)
	return err
}

func git(ctx context.Context, args ...string) (string, error) {
	var stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, "git", args...) //nolint:gosec // G204: a fixed git binary with argv arrays, never a shell string.
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return strings.TrimSpace(string(out)), nil
}
