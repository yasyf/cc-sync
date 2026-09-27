package replica

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/yasyf/cc-sync/internal/sessionarchive"
	"github.com/yasyf/synckit/artifact"
)

const sid = "11111111-2222-4333-8444-555555555555"

type memStore map[artifact.Digest][]byte

func (s memStore) put(b string) artifact.Ref {
	d := artifact.Sum([]byte(b))
	s[d] = []byte(b)
	return artifact.Ref{Digest: d, Kind: artifact.KindManifest, Size: int64(len(b))}
}

func (s memStore) Materialize(_ context.Context, ref artifact.Ref, path string, perm os.FileMode) error {
	return os.WriteFile(path, s[ref.Digest], perm)
}

func manifest(s memStore, transcript string, extra ...sessionarchive.Entry) sessionarchive.Manifest {
	return sessionarchive.Manifest{
		Format: sessionarchive.Format, SessionID: sid, SourceHost: "alice",
		CapturedAt: time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC), ClaudeVersion: "2.1.283",
		ConfigDir: "/Users/alice/.claude", Home: "/Users/alice", TmpRoot: "/private/tmp", UID: 501,
		Cwd: "/Users/alice/src/app",
		Entries: append([]sessionarchive.Entry{
			{Root: sessionarchive.RootTranscript, Path: sid + ".jsonl", Mode: 0o600, Ref: s.put(transcript)},
		}, extra...),
	}
}

func TestMaterialize(t *testing.T) {
	s := memStore{}
	transcript := `{"type":"user","uuid":"u1"}` + "\n" + `{"type":"assistant","uuid":"a2"}` + "\n"
	m := manifest(s, transcript,
		sessionarchive.Entry{Root: sessionarchive.RootSession, Path: "subagents/agent-a.jsonl", Mode: 0o600, Ref: s.put("sub\n")},
		sessionarchive.Entry{Root: sessionarchive.RootTasks, Path: "list-1/1.json", Mode: 0o644, Ref: s.put(`{"id":"1"}`)},
		sessionarchive.Entry{Root: sessionarchive.RootScratchpad, Path: "out/link", Mode: fs.ModeSymlink | 0o777, Link: "../../elsewhere"},
		sessionarchive.Entry{Root: sessionarchive.RootScratchpad, Path: "empty", Mode: fs.ModeDir | 0o700},
	)
	dir := Dir(t.TempDir(), "alice", sid, "cp-1")
	if err := os.MkdirAll(filepath.Join(dir, "stale"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := Materialize(t.Context(), m, "cp-1", s, dir); err != nil {
		t.Fatalf("Materialize: %v", err)
	}
	for rel, want := range map[string]string{
		TranscriptFile:                    transcript,
		"session/subagents/agent-a.jsonl": "sub\n",
		"tasks/list-1/1.json":             `{"id":"1"}`,
	} {
		got, err := os.ReadFile(filepath.Join(dir, rel)) //nolint:gosec // G304: a replica under t.TempDir.
		if err != nil || string(got) != want {
			t.Errorf("%s = %q, %v; want %q", rel, got, err, want)
		}
	}
	if link, err := os.Readlink(filepath.Join(dir, "scratchpad/out/link")); err != nil || link != "../../elsewhere" {
		t.Errorf("scratchpad link = %q, %v; want ../../elsewhere", link, err)
	}
	if info, err := os.Stat(filepath.Join(dir, "scratchpad/empty")); err != nil || !info.IsDir() {
		t.Errorf("scratchpad/empty = %v, %v; want a directory", info, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "stale")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("stale replica content survived: %v", err)
	}
	b, err := os.ReadFile(filepath.Join(dir, MetaFile)) //nolint:gosec // G304: a replica under t.TempDir.
	if err != nil {
		t.Fatal(err)
	}
	var got Meta
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(transcript))
	want := Meta{
		SessionID: sid, SourceHost: "alice", SourceConfigDir: "/Users/alice/.claude", SourceTmpRoot: "/private/tmp",
		SourceUID: 501, SourceCwd: "/Users/alice/src/app", SourceHome: "/Users/alice", CheckpointID: "cp-1",
		CapturedAt: m.CapturedAt, LeafUUID: "a2", PrefixDigest: "sha256:" + hex.EncodeToString(sum[:]), ClaudeVersion: "2.1.283",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("meta = %+v\nwant %+v", got, want)
	}
}

func TestMaterializeRefuses(t *testing.T) {
	tests := []struct {
		name  string
		entry func(memStore) sessionarchive.Entry
	}{
		{"escaping path", func(s memStore) sessionarchive.Entry {
			return sessionarchive.Entry{Root: sessionarchive.RootSession, Path: "../../x", Mode: 0o600, Ref: s.put("x")}
		}},
		{"unknown root", func(s memStore) sessionarchive.Entry {
			return sessionarchive.Entry{Root: "other", Path: "x", Mode: 0o600, Ref: s.put("x")}
		}},
		{"file without content", func(memStore) sessionarchive.Entry {
			return sessionarchive.Entry{Root: sessionarchive.RootPlans, Path: "p.md", Mode: 0o600}
		}},
		{"second transcript", func(s memStore) sessionarchive.Entry {
			return sessionarchive.Entry{Root: sessionarchive.RootTranscript, Path: "other.jsonl", Mode: 0o600, Ref: s.put("y\n")}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := memStore{}
			root := t.TempDir()
			dir := Dir(root, "alice", sid, "cp-1")
			err := Materialize(t.Context(), manifest(s, "{}\n", tt.entry(s)), "cp-1", s, dir)
			if !errors.Is(err, ErrInvalid) {
				t.Fatalf("Materialize = %v, want ErrInvalid", err)
			}
			left, _ := os.ReadDir(filepath.Dir(dir))
			if len(left) != 0 {
				t.Errorf("left %d entries beside the replica", len(left))
			}
		})
	}
}
