// Package codesnap adapts reposync's raw-digest worktree artifacts onto the
// synckit artifact store. Capture writes through a Sink, whose durable index
// maps each raw sha256 digest to the synckit ref holding its bytes; a code
// manifest carries that mapping to the receiver, where a Source serves the
// bytes back under their raw digests and verifies every one it streams.
package codesnap

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/yasyf/reposync/worktree"
	"github.com/yasyf/synckit/artifact"
)

const (
	digestPrefix      = "sha256:"
	mediaCodeManifest = "cc-sync.code-manifest"
	mediaCode         = "cc-sync.code"
	mediaCodeDeps     = "cc-sync.code-deps"
)

// ErrCorrupt reports artifact content whose raw sha256 or length departs from
// the ref naming it.
var ErrCorrupt = errors.New("codesnap: artifact content does not match its ref")

// Store is the synckit artifact store surface capture writes through.
type Store interface {
	Put(ctx context.Context, r io.Reader, media string) (artifact.Ref, error)
	PutGroup(ctx context.Context, media string, deps []artifact.Ref) (artifact.Ref, error)
	Has(ctx context.Context, digests []artifact.Digest) (missing []artifact.Digest, err error)
}

// Reader is the synckit artifact store surface a receiver reads through; the
// owning store and a read-only view both satisfy it.
type Reader interface {
	Manifest(ctx context.Context, ref artifact.Ref) (artifact.Manifest, error)
	Open(ctx context.Context, ref artifact.Ref) (io.ReadCloser, error)
	Complete(ctx context.Context, roots []artifact.Ref) (missing int, err error)
}

type codeManifest struct {
	Snapshot worktree.ArtifactRef    `json:"snapshot"`
	Entries  map[string]artifact.Ref `json:"entries"`
}

func (m codeManifest) Validate() error {
	if m.Snapshot.Media != worktree.MediaSnapshot {
		return fmt.Errorf("snapshot media %q, want %q", m.Snapshot.Media, worktree.MediaSnapshot)
	}
	if stored, ok := m.Entries[m.Snapshot.Digest]; !ok || stored.Size != m.Snapshot.Size {
		return fmt.Errorf("snapshot %s has no %d-byte entry", m.Snapshot.Digest, m.Snapshot.Size)
	}
	for digest, stored := range m.Entries {
		if _, err := rawHex(digest); err != nil {
			return err
		}
		if err := checkStored(stored); err != nil {
			return fmt.Errorf("entry %s: %w", digest, err)
		}
	}
	return nil
}

type indexEntry struct {
	Ref artifact.Ref `json:"ref"`
}

func (e indexEntry) Validate() error {
	return checkStored(e.Ref)
}

func checkStored(ref artifact.Ref) error {
	if err := ref.Validate(); err != nil {
		return err
	}
	if ref.Kind != artifact.KindManifest {
		return fmt.Errorf("ref %s kind %q, want %q", ref.Digest, ref.Kind, artifact.KindManifest)
	}
	return nil
}

func rawHex(digest string) (string, error) {
	hexDigest, ok := strings.CutPrefix(digest, digestPrefix)
	if !ok || artifact.Digest(hexDigest).Validate() != nil {
		return "", fmt.Errorf("malformed raw digest %q", digest)
	}
	return hexDigest, nil
}
