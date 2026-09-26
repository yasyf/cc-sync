package codesnap

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"

	"github.com/yasyf/daemonkit/durable"
	"github.com/yasyf/reposync/worktree"
	"github.com/yasyf/synckit/artifact"
)

// Source is the worktree.ArtifactSource a receiver verifies and restores from:
// it resolves raw digests through a code manifest and checks every byte it
// streams against the raw ref.
type Source struct {
	store    Reader
	manifest codeManifest
}

// SourceFromManifest loads the code manifest under codeRoot, the ref
// Sink.BuildCodeManifest returned on the capturing host.
func SourceFromManifest(ctx context.Context, store Reader, codeRoot artifact.Ref) (*Source, error) {
	root, err := store.Manifest(ctx, codeRoot)
	if err != nil {
		return nil, fmt.Errorf("read code root %s: %w", codeRoot.Digest, err)
	}
	if root.Media != mediaCode || len(root.Deps) == 0 {
		return nil, fmt.Errorf("%s is a %q manifest with %d deps, not a code root", codeRoot.Digest, root.Media, len(root.Deps))
	}
	rc, err := store.Open(ctx, root.Deps[0])
	if err != nil {
		return nil, fmt.Errorf("open code manifest %s: %w", root.Deps[0].Digest, err)
	}
	data, err := readAll(rc)
	if err != nil {
		return nil, fmt.Errorf("read code manifest %s: %w", root.Deps[0].Digest, err)
	}
	manifest, err := durable.Unmarshal[codeManifest](data)
	if err != nil {
		return nil, fmt.Errorf("decode code manifest %s: %w", root.Deps[0].Digest, err)
	}
	return &Source{store: store, manifest: manifest}, nil
}

// Snapshot reads and decodes the worktree snapshot the code manifest names.
func (s *Source) Snapshot(ctx context.Context) (worktree.Snapshot, error) {
	rc, err := s.Open(ctx, s.manifest.Snapshot)
	if err != nil {
		return worktree.Snapshot{}, err
	}
	data, err := readAll(rc)
	if err != nil {
		return worktree.Snapshot{}, fmt.Errorf("read snapshot %s: %w", s.manifest.Snapshot.Digest, err)
	}
	snap, err := worktree.Decode(data)
	if err != nil {
		return worktree.Snapshot{}, fmt.Errorf("decode snapshot %s: %w", s.manifest.Snapshot.Digest, err)
	}
	return snap, nil
}

// Has reports each ref present when the code manifest maps it at the same size
// and the mapped ref's whole closure is complete in the store.
func (s *Source) Has(ctx context.Context, refs []worktree.ArtifactRef) ([]bool, error) {
	has := make([]bool, len(refs))
	for i, ref := range refs {
		stored, ok := s.manifest.Entries[ref.Digest]
		if !ok || stored.Size != ref.Size {
			continue
		}
		missing, err := s.store.Complete(ctx, []artifact.Ref{stored})
		if err != nil {
			return nil, fmt.Errorf("check %s: %w", ref.Digest, err)
		}
		has[i] = missing == 0
	}
	return has, nil
}

// Open streams ref's bytes from the store. The reader fails with ErrCorrupt
// once the bytes depart from ref's raw sha256 or size; a digest the code
// manifest does not map fails with an error wrapping fs.ErrNotExist.
func (s *Source) Open(ctx context.Context, ref worktree.ArtifactRef) (io.ReadCloser, error) {
	stored, ok := s.manifest.Entries[ref.Digest]
	if !ok {
		return nil, fmt.Errorf("open %s: %w", ref.Digest, fs.ErrNotExist)
	}
	if stored.Size != ref.Size {
		return nil, fmt.Errorf("open %s: stored %d bytes, ref names %d: %w", ref.Digest, stored.Size, ref.Size, ErrCorrupt)
	}
	rc, err := s.store.Open(ctx, stored)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", ref.Digest, err)
	}
	return &verifier{ReadCloser: rc, hash: sha256.New(), want: ref}, nil
}

func readAll(rc io.ReadCloser) ([]byte, error) {
	data, err := io.ReadAll(rc)
	return data, errors.Join(err, rc.Close())
}

type verifier struct {
	io.ReadCloser
	hash hash.Hash
	want worktree.ArtifactRef
	read int64
}

func (v *verifier) Read(p []byte) (int, error) {
	n, err := v.ReadCloser.Read(p)
	v.hash.Write(p[:n])
	v.read += int64(n)
	if v.read > v.want.Size {
		return n, fmt.Errorf("read %s: more than %d bytes: %w", v.want.Digest, v.want.Size, ErrCorrupt)
	}
	if !errors.Is(err, io.EOF) {
		return n, err
	}
	if got := digestPrefix + hex.EncodeToString(v.hash.Sum(nil)); got != v.want.Digest || v.read != v.want.Size {
		return n, fmt.Errorf("read %s: got %s over %d bytes: %w", v.want.Digest, got, v.read, ErrCorrupt)
	}
	return n, err
}
