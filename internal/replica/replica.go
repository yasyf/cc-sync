// Package replica materializes one archived Claude session from the artifact
// store into the replica directory sessionrestore installs from:
//
//	<ReplicaRoot>/<origin>/<session id>/<checkpoint>/
//	  meta.json  transcript.jsonl  session/  file-history/  tasks/<listId>/
//	  plans/  paste-cache/  scratchpad/
//
// Every file is written from store-verified bytes and every symlink is
// recreated with its recorded target verbatim. A replica lives under the
// cc-sync config dir and is never native Claude state.
package replica

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/yasyf/cc-sync/internal/sessionarchive"
	"github.com/yasyf/synckit/artifact"
)

// Replica file and directory names.
const (
	MetaFile       = "meta.json"
	TranscriptFile = "transcript.jsonl"
)

var rootDirs = map[sessionarchive.Root]string{
	sessionarchive.RootSession:     "session",
	sessionarchive.RootFileHistory: "file-history",
	sessionarchive.RootTasks:       "tasks",
	sessionarchive.RootPlans:       "plans",
	sessionarchive.RootPasteCache:  "paste-cache",
	sessionarchive.RootScratchpad:  "scratchpad",
}

// ErrInvalid reports a session manifest that cannot form a replica.
var ErrInvalid = errors.New("replica: invalid session manifest")

// Store writes an artifact's verified content to a path.
type Store interface {
	Materialize(ctx context.Context, ref artifact.Ref, path string, perm os.FileMode) error
}

// Meta is a replica's meta.json: the source layout the session was captured
// from and the checkpoint identity it was picked from.
type Meta struct {
	SessionID       string    `json:"session_id"`
	SourceHost      string    `json:"source_host"`
	SourceConfigDir string    `json:"source_config_dir"`
	SourceTmpRoot   string    `json:"source_tmp_root"`
	SourceUID       int       `json:"source_uid"`
	SourceCwd       string    `json:"source_cwd"`
	SourceHome      string    `json:"source_home"`
	CheckpointID    string    `json:"checkpoint_id"`
	CapturedAt      time.Time `json:"captured_at"`
	LeafUUID        string    `json:"leaf_uuid"`
	PrefixDigest    string    `json:"prefix_digest"`
	ClaudeVersion   string    `json:"claude_version"`
}

// Dir is the replica directory of session sid picked from checkpoint of origin.
func Dir(root, origin, sid, checkpoint string) string {
	return filepath.Join(root, origin, sid, checkpoint)
}

// Materialize writes the replica of m, picked from checkpoint, to dir. It
// builds the replica beside dir and swaps it in whole, replacing any earlier
// replica of the same checkpoint.
func Materialize(ctx context.Context, m sessionarchive.Manifest, checkpoint string, store Store, dir string) error {
	if err := os.MkdirAll(filepath.Dir(dir), 0o700); err != nil {
		return fmt.Errorf("create replica parent: %w", err)
	}
	stage := dir + ".stage-" + rand.Text()
	if err := build(ctx, m, checkpoint, store, stage); err != nil {
		return errors.Join(err, os.RemoveAll(stage))
	}
	if err := os.RemoveAll(dir); err != nil {
		return errors.Join(fmt.Errorf("remove stale replica: %w", err), os.RemoveAll(stage))
	}
	if err := os.Rename(stage, dir); err != nil {
		return errors.Join(fmt.Errorf("install replica: %w", err), os.RemoveAll(stage))
	}
	return nil
}

func build(ctx context.Context, m sessionarchive.Manifest, checkpoint string, store Store, dir string) error {
	if m.Format != sessionarchive.Format {
		return fmt.Errorf("%w: format %d", ErrInvalid, m.Format)
	}
	if err := os.Mkdir(dir, 0o700); err != nil {
		return fmt.Errorf("create replica: %w", err)
	}
	var links []link
	transcripts := 0
	for _, e := range m.Entries {
		rel, err := entryPath(e)
		if err != nil {
			return err
		}
		if e.Root == sessionarchive.RootTranscript {
			transcripts++
		}
		path := filepath.Join(dir, rel)
		switch {
		case e.Mode&fs.ModeSymlink != 0:
			links = append(links, link{path: path, target: e.Link})
		case e.Mode.IsDir():
			if err := os.MkdirAll(path, 0o700); err != nil {
				return fmt.Errorf("create %s: %w", e.Key(), err)
			}
		case e.Mode.IsRegular() && e.Ref.Digest != "":
			if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
				return fmt.Errorf("create parent of %s: %w", e.Key(), err)
			}
			if err := store.Materialize(ctx, e.Ref, path, e.Mode.Perm()); err != nil {
				return fmt.Errorf("materialize %s: %w", e.Key(), err)
			}
		default:
			return fmt.Errorf("%w: %s has mode %v and no content", ErrInvalid, e.Key(), e.Mode)
		}
	}
	if transcripts != 1 {
		return fmt.Errorf("%w: %d transcripts", ErrInvalid, transcripts)
	}
	for _, l := range links {
		if err := os.MkdirAll(filepath.Dir(l.path), 0o700); err != nil {
			return fmt.Errorf("create parent of %s: %w", l.path, err)
		}
		if err := os.Symlink(l.target, l.path); err != nil {
			return fmt.Errorf("link %s: %w", l.path, err)
		}
	}
	return writeMeta(m, checkpoint, dir)
}

type link struct {
	path   string
	target string
}

func entryPath(e sessionarchive.Entry) (string, error) {
	if e.Root == sessionarchive.RootTranscript {
		return TranscriptFile, nil
	}
	sub, ok := rootDirs[e.Root]
	if !ok {
		return "", fmt.Errorf("%w: unknown root %q", ErrInvalid, e.Root)
	}
	if !filepath.IsLocal(e.Path) {
		return "", fmt.Errorf("%w: %s escapes its root", ErrInvalid, e.Key())
	}
	return filepath.Join(sub, e.Path), nil
}

func writeMeta(m sessionarchive.Manifest, checkpoint, dir string) error {
	leaf, digest, err := scanTranscript(filepath.Join(dir, TranscriptFile))
	if err != nil {
		return err
	}
	b, err := json.MarshalIndent(Meta{
		SessionID:       m.SessionID,
		SourceHost:      m.SourceHost,
		SourceConfigDir: m.ConfigDir,
		SourceTmpRoot:   m.TmpRoot,
		SourceUID:       m.UID,
		SourceCwd:       m.Cwd,
		SourceHome:      m.Home,
		CheckpointID:    checkpoint,
		CapturedAt:      m.CapturedAt,
		LeafUUID:        leaf,
		PrefixDigest:    digest,
		ClaudeVersion:   m.ClaudeVersion,
	}, "", "  ")
	if err != nil {
		return fmt.Errorf("encode replica meta: %w", err)
	}
	if err := os.WriteFile(filepath.Join(dir, MetaFile), append(b, '\n'), 0o600); err != nil {
		return fmt.Errorf("write replica meta: %w", err)
	}
	return nil
}

func scanTranscript(path string) (leaf, digest string, err error) {
	f, err := os.Open(path) //nolint:gosec // G304: the transcript this package just materialized.
	if err != nil {
		return "", "", fmt.Errorf("open replica transcript: %w", err)
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	r := bufio.NewReader(io.TeeReader(f, h))
	for {
		line, err := r.ReadBytes('\n')
		var rec struct {
			UUID string `json:"uuid"`
		}
		if json.Unmarshal(line, &rec) == nil && rec.UUID != "" {
			leaf = rec.UUID
		}
		if errors.Is(err, io.EOF) {
			return leaf, "sha256:" + hex.EncodeToString(h.Sum(nil)), nil
		}
		if err != nil {
			return "", "", fmt.Errorf("read replica transcript: %w", err)
		}
	}
}
