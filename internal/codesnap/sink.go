package codesnap

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"

	"github.com/yasyf/daemonkit/durable"
	"github.com/yasyf/reposync/worktree"
	"github.com/yasyf/synckit/artifact"
)

// Sink is the worktree.ArtifactSink capture writes through: bytes stream into
// the synckit store and a durable index under dir remembers which synckit ref
// holds each raw digest.
type Sink struct {
	store Store
	dir   string
}

// NewSink returns a Sink storing into store and indexing under dir, creating
// dir when absent.
func NewSink(store Store, dir string) (*Sink, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create code index %s: %w", dir, err)
	}
	return &Sink{store: store, dir: dir}, nil
}

// Put streams r into the store under media while hashing the raw bytes, then
// indexes the stored ref under the raw digest it returns.
func (s *Sink) Put(ctx context.Context, media worktree.Media, r io.Reader) (worktree.ArtifactRef, error) {
	hash := sha256.New()
	var size byteCount
	stored, err := s.store.Put(ctx, io.TeeReader(r, io.MultiWriter(hash, &size)), string(media))
	if err != nil {
		return worktree.ArtifactRef{}, fmt.Errorf("store %s artifact: %w", media, err)
	}
	raw := worktree.ArtifactRef{Digest: digestPrefix + hex.EncodeToString(hash.Sum(nil)), Size: int64(size), Media: media}
	if err := s.record(raw.Digest, stored); err != nil {
		return worktree.ArtifactRef{}, err
	}
	return raw, nil
}

// Has reports each ref present when the index maps its raw digest at the same
// size and the store still holds the mapped ref.
func (s *Sink) Has(ctx context.Context, refs []worktree.ArtifactRef) ([]bool, error) {
	mapped := make([]artifact.Ref, len(refs))
	wanted := map[artifact.Digest]bool{}
	for i, ref := range refs {
		stored, ok, err := s.lookup(ref.Digest)
		if err != nil {
			return nil, err
		}
		if ok && stored.Size == ref.Size {
			mapped[i] = stored
			wanted[stored.Digest] = true
		}
	}
	absent := map[artifact.Digest]bool{}
	if len(wanted) > 0 {
		missing, err := s.store.Has(ctx, slices.Sorted(maps.Keys(wanted)))
		if err != nil {
			return nil, fmt.Errorf("check stored artifacts: %w", err)
		}
		for _, d := range missing {
			absent[d] = true
		}
	}
	has := make([]bool, len(refs))
	for i, stored := range mapped {
		has[i] = stored.Digest != "" && !absent[stored.Digest]
	}
	return has, nil
}

// BuildCodeManifest stores snap and a code manifest mapping every artifact snap
// references, snap itself included, to its synckit ref, then groups them under
// one code root whose closure is everything a receiver needs to restore snap.
// Every artifact snap references must already have gone through Put.
func (s *Sink) BuildCodeManifest(ctx context.Context, snap worktree.Snapshot) (artifact.Ref, error) {
	encoded, err := worktree.Encode(snap)
	if err != nil {
		return artifact.Ref{}, fmt.Errorf("encode snapshot: %w", err)
	}
	snapRef, err := s.Put(ctx, worktree.MediaSnapshot, bytes.NewReader(encoded))
	if err != nil {
		return artifact.Ref{}, err
	}
	manifest := codeManifest{Snapshot: snapRef, Entries: map[string]artifact.Ref{}}
	deps := map[artifact.Digest]artifact.Ref{}
	for _, raw := range append(snap.Artifacts(), snapRef) {
		stored, ok, err := s.lookup(raw.Digest)
		if err != nil {
			return artifact.Ref{}, err
		}
		if !ok {
			return artifact.Ref{}, fmt.Errorf("snapshot artifact %s was never stored", raw.Digest)
		}
		if stored.Size != raw.Size {
			return artifact.Ref{}, fmt.Errorf("snapshot artifact %s is %d bytes, indexed at %d", raw.Digest, raw.Size, stored.Size)
		}
		manifest.Entries[raw.Digest] = stored
		deps[stored.Digest] = stored
	}
	data, err := durable.Marshal(manifest)
	if err != nil {
		return artifact.Ref{}, fmt.Errorf("encode code manifest: %w", err)
	}
	manifestRef, err := s.store.Put(ctx, bytes.NewReader(data), mediaCodeManifest)
	if err != nil {
		return artifact.Ref{}, fmt.Errorf("store code manifest: %w", err)
	}
	refs := slices.SortedFunc(maps.Values(deps), func(a, b artifact.Ref) int { return cmp.Compare(a.Digest, b.Digest) })
	return s.group(ctx, manifestRef, refs)
}

func (s *Sink) group(ctx context.Context, manifest artifact.Ref, refs []artifact.Ref) (artifact.Ref, error) {
	for len(refs)+1 > artifact.MaxDeps {
		level := make([]artifact.Ref, 0, len(refs)/artifact.MaxDeps+1)
		for chunk := range slices.Chunk(refs, artifact.MaxDeps) {
			group, err := s.store.PutGroup(ctx, mediaCodeDeps, chunk)
			if err != nil {
				return artifact.Ref{}, fmt.Errorf("group code artifacts: %w", err)
			}
			level = append(level, group)
		}
		refs = level
	}
	root, err := s.store.PutGroup(ctx, MediaCode, append([]artifact.Ref{manifest}, refs...))
	if err != nil {
		return artifact.Ref{}, fmt.Errorf("group code root: %w", err)
	}
	return root, nil
}

func (s *Sink) record(digest string, stored artifact.Ref) error {
	current, ok, err := s.lookup(digest)
	if err != nil {
		return err
	}
	if ok && current == stored {
		return nil
	}
	path, err := s.entryPath(digest)
	if err != nil {
		return err
	}
	data, err := durable.Marshal(indexEntry{Ref: stored})
	if err != nil {
		return fmt.Errorf("encode index entry %s: %w", digest, err)
	}
	if err := durable.Mkdir(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create index shard for %s: %w", digest, err)
	}
	if err := durable.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("write index entry %s: %w", digest, err)
	}
	return nil
}

func (s *Sink) lookup(digest string) (artifact.Ref, bool, error) {
	path, err := s.entryPath(digest)
	if err != nil {
		return artifact.Ref{}, false, err
	}
	entry, err := durable.ReadFile[indexEntry](path)
	if errors.Is(err, fs.ErrNotExist) {
		return artifact.Ref{}, false, nil
	}
	if err != nil {
		return artifact.Ref{}, false, fmt.Errorf("read index entry %s: %w", digest, err)
	}
	return entry.Ref, true, nil
}

func (s *Sink) entryPath(digest string) (string, error) {
	hexDigest, err := rawHex(digest)
	if err != nil {
		return "", err
	}
	return filepath.Join(s.dir, hexDigest[:2], hexDigest+".json"), nil
}

type byteCount int64

func (c *byteCount) Write(p []byte) (int, error) {
	*c += byteCount(len(p))
	return len(p), nil
}
