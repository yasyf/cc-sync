package consumer

import (
	"context"
	"fmt"

	"github.com/yasyf/reposync/registry"
	"github.com/yasyf/reposync/worktree"

	"github.com/yasyf/cc-sync/internal/codesnap"
	"github.com/yasyf/synckit/artifact"
)

// Reposync is the CodeVerifier over reposync: it reads a checkpoint root's
// code group through codesnap and verifies the snapshot against the
// checkout the receiver's registry names. A checkpoint with no code group is
// never ready.
type Reposync struct {
	Store    codesnap.Reader
	Code     *worktree.Store
	Registry func() (registry.Registry, error)
}

// VerifyCode verifies the code group under the checkpoint root.
func (r Reposync) VerifyCode(ctx context.Context, root artifact.Ref, fetchOrigin worktree.FetchGate) (CodeVerdict, error) {
	code, ok, err := codeGroup(ctx, r.Store, root)
	if err != nil {
		return CodeVerdict{}, err
	}
	if !ok {
		return CodeVerdict{Missing: []string{"code"}}, nil
	}
	src, err := codesnap.SourceFromManifest(ctx, r.Store, code)
	if err != nil {
		return CodeVerdict{}, err
	}
	snap, err := src.Snapshot(ctx)
	if err != nil {
		return CodeVerdict{}, err
	}
	reg, err := r.Registry()
	if err != nil {
		return CodeVerdict{}, fmt.Errorf("consumer: load reposync registry: %w", err)
	}
	v, err := r.Code.Verify(ctx, reg, snap, src, worktree.VerifyOptions{FetchOrigin: fetchOrigin})
	if err != nil {
		return CodeVerdict{}, fmt.Errorf("consumer: verify %s: %w", snap.Worktree.Root, err)
	}
	return CodeVerdict{Ready: v.Ready, Missing: v.Missing}, nil
}

func codeGroup(ctx context.Context, store codesnap.Reader, root artifact.Ref) (artifact.Ref, bool, error) {
	m, err := store.Manifest(ctx, root)
	if err != nil {
		return artifact.Ref{}, false, fmt.Errorf("consumer: read checkpoint root %s: %w", root.Digest, err)
	}
	for _, dep := range m.Deps {
		dm, err := store.Manifest(ctx, dep)
		if err != nil {
			return artifact.Ref{}, false, fmt.Errorf("consumer: read checkpoint dependency %s: %w", dep.Digest, err)
		}
		if dm.Media == codesnap.MediaCode {
			return dep, true, nil
		}
	}
	return artifact.Ref{}, false, nil
}
