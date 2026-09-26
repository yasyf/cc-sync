package capture

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/cc-sync/internal/catalog"
	"github.com/yasyf/cc-sync/internal/claudenative"
	"github.com/yasyf/cc-sync/internal/inventory"
	"github.com/yasyf/cc-sync/internal/orcabridge"
	"github.com/yasyf/cc-sync/internal/scheduler"
	"github.com/yasyf/reposync/worktree"
	"github.com/yasyf/synckit/artifact"
)

const (
	self  = "host-a"
	sidA  = "11111111-1111-4111-8111-111111111111"
	sidB  = "22222222-2222-4222-8222-222222222222"
	wtID  = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	owner = PinOwnerPrefix + wtID
)

var t0 = time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)

type fakeStore struct {
	mu      sync.Mutex
	objects map[artifact.Digest][]byte
	puts    int
	pins    map[string][]artifact.Ref
}

func newFakeStore() *fakeStore {
	return &fakeStore{objects: map[artifact.Digest][]byte{}, pins: map[string][]artifact.Ref{}}
}

func (f *fakeStore) Put(_ context.Context, r io.Reader, media string) (artifact.Ref, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return artifact.Ref{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.puts++
	m := artifact.Manifest{Schema: artifact.ManifestSchema, Media: media, Size: int64(len(data))}
	for chunk := range slices.Chunk(data, artifact.ChunkSize) {
		m.Chunks = append(m.Chunks, artifact.ChunkRef{Digest: f.store(chunk), Size: int64(len(chunk))})
	}
	return f.storeManifest(m)
}

func (f *fakeStore) PutGroup(_ context.Context, media string, deps []artifact.Ref) (artifact.Ref, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.puts++
	for _, dep := range deps {
		if _, ok := f.objects[dep.Digest]; !ok {
			return artifact.Ref{}, &artifact.MissingError{Digest: dep.Digest}
		}
	}
	return f.storeManifest(artifact.Manifest{Schema: artifact.ManifestSchema, Media: media, Deps: deps})
}

func (f *fakeStore) Has(_ context.Context, digests []artifact.Digest) ([]artifact.Digest, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var missing []artifact.Digest
	for _, d := range digests {
		if _, ok := f.objects[d]; !ok {
			missing = append(missing, d)
		}
	}
	return missing, nil
}

func (f *fakeStore) Closure(_ context.Context, roots []artifact.Ref, bound artifact.ClosureBound) (artifact.Closure, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var c artifact.Closure
	seen := map[artifact.Digest]bool{}
	var walk func(d artifact.Digest) error
	walk = func(d artifact.Digest) error {
		if seen[d] {
			return nil
		}
		seen[d] = true
		m, err := f.manifest(d)
		if err != nil {
			return err
		}
		for _, dep := range m.Deps {
			if err := walk(dep.Digest); err != nil {
				return err
			}
		}
		c.Objects = append(c.Objects, artifact.ObjectEntry{Digest: d, Kind: artifact.KindManifest})
		if len(c.Objects) > bound.MaxObjects {
			return &artifact.ClosureError{Bound: artifact.BoundObjects, Limit: int64(bound.MaxObjects)}
		}
		return nil
	}
	for _, r := range roots {
		if err := walk(r.Digest); err != nil {
			return artifact.Closure{}, err
		}
	}
	return c, nil
}

func (f *fakeStore) SetPins(_ context.Context, owner string, roots []artifact.Ref) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(roots) == 0 {
		delete(f.pins, owner)
		return nil
	}
	f.pins[owner] = slices.Clone(roots)
	return nil
}

func (f *fakeStore) putCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.puts
}

func (f *fakeStore) deps(t *testing.T, ref artifact.Ref) []artifact.Ref {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	m, err := f.manifest(ref.Digest)
	if err != nil {
		t.Fatal(err)
	}
	return m.Deps
}

func (f *fakeStore) blob(t *testing.T, ref artifact.Ref) []byte {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	m, err := f.manifest(ref.Digest)
	if err != nil {
		t.Fatal(err)
	}
	var data []byte
	for _, c := range m.Chunks {
		data = append(data, f.objects[c.Digest]...)
	}
	return data
}

func (f *fakeStore) media(t *testing.T, ref artifact.Ref) string {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	m, err := f.manifest(ref.Digest)
	if err != nil {
		t.Fatal(err)
	}
	return m.Media
}

func (f *fakeStore) reaches(t *testing.T, root artifact.Ref, want artifact.Digest) bool {
	t.Helper()
	if root.Digest == want {
		return true
	}
	for _, d := range f.deps(t, root) {
		if f.reaches(t, d, want) {
			return true
		}
	}
	return false
}

func (f *fakeStore) store(data []byte) artifact.Digest {
	d := artifact.Sum(data)
	if _, ok := f.objects[d]; !ok {
		f.objects[d] = bytes.Clone(data)
	}
	return d
}

func (f *fakeStore) storeManifest(m artifact.Manifest) (artifact.Ref, error) {
	data, err := m.Encode()
	if err != nil {
		return artifact.Ref{}, err
	}
	return artifact.Ref{Digest: f.store(data), Kind: artifact.KindManifest, Size: m.Size}, nil
}

func (f *fakeStore) manifest(d artifact.Digest) (artifact.Manifest, error) {
	data, ok := f.objects[d]
	if !ok {
		return artifact.Manifest{}, &artifact.MissingError{Digest: d}
	}
	m, err := artifact.DecodeManifest(data)
	if err != nil {
		return artifact.Manifest{}, fmt.Errorf("decode manifest %s: %w", d, err)
	}
	return m, nil
}

type fakeCode struct {
	calls int
	files map[string]string
	err   error
}

func (f *fakeCode) Capture(ctx context.Context, wt worktree.Worktree, sink worktree.ArtifactSink, opts worktree.CaptureOptions) (worktree.Snapshot, error) {
	f.calls++
	snap := worktree.Snapshot{
		Schema:       worktree.SnapshotSchema,
		Source:       opts.Source,
		Worktree:     wt,
		ObjectFormat: "sha1",
		Head:         worktree.Head{Commit: oid('1'), Branch: "feat", TrunkTip: oid('1'), TrunkBase: oid('1')},
		Requires:     []string{oid('1')},
		Complete:     true,
	}
	for _, path := range slices.Sorted(maps.Keys(f.files)) {
		ref, err := sink.Put(ctx, worktree.MediaFile, strings.NewReader(f.files[path]))
		if err != nil {
			return worktree.Snapshot{}, err
		}
		snap.Files = append(snap.Files, worktree.FileEntry{Path: path, Kind: worktree.FileRegular, Content: &ref, Untracked: true})
	}
	if f.err != nil {
		return worktree.Snapshot{}, f.err
	}
	snap.CapturedAt = t0
	digest, err := snap.ContentDigest()
	if err != nil {
		return worktree.Snapshot{}, err
	}
	snap.Digest = digest
	return snap, nil
}

type fakeStamper struct{ stamp string }

func (f *fakeStamper) Stamp(context.Context, worktree.Worktree) (string, error) {
	return f.stamp, nil
}

type fakeOrca struct {
	calls      int
	descriptor string
	err        error
}

func (f *fakeOrca) Export(context.Context, string) ([]byte, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return []byte(f.descriptor), nil
}

type record struct {
	wt catalog.Worktree
	cp catalog.Checkpoint
}

type fakeCatalog struct{ records []record }

func (f *fakeCatalog) Record(_ context.Context, wt catalog.Worktree, cp catalog.Checkpoint) (catalog.Checkpoint, error) {
	cp.ID = catalog.CheckpointID(self, wt.ID, cp.Root)
	f.records = append(f.records, record{wt: wt, cp: cp})
	return cp, nil
}

func (f *fakeCatalog) last(t *testing.T) record {
	t.Helper()
	if len(f.records) == 0 {
		t.Fatal("no checkpoint recorded")
	}
	return f.records[len(f.records)-1]
}

type fakePublisher struct{ kicks int }

func (f *fakePublisher) Publish(context.Context) error {
	f.kicks++
	return nil
}

func oid(c byte) string {
	return strings.Repeat(string(c), 40)
}

func descriptor(instance string) string {
	return `{"version":1,"workspace":{"worktreeId":"w1","instanceId":"` + instance + `","path":"/src/r","branch":"feat","meta":{"displayName":"Feature"}}}`
}

func transcriptRecord(uuid string, at time.Time) string {
	return `{"type":"user","uuid":"` + uuid + `","cwd":"/src/r","origin":{"kind":"human"},"timestamp":"` + at.Format(time.RFC3339) + `","message":{"content":"hi"}}` + "\n"
}

func nativeSession(t *testing.T, config, sid string, at time.Time) claudenative.Session {
	t.Helper()
	id, err := claudenative.ParseSessionID(sid)
	if err != nil {
		t.Fatal(err)
	}
	dir := claudenative.ProjectDirName("/src/r")
	path := filepath.Join(claudenative.ProjectsDir(config), dir, sid+".jsonl")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(transcriptRecord("u-"+sid[:4], at)), 0o600); err != nil {
		t.Fatal(err)
	}
	return claudenative.Session{
		ID: id, ConfigDir: config, ProjectDirName: dir, TranscriptPath: path,
		Cwd: "/src/r", OriginalCwd: "/src/r", GitBranch: "feat", Version: "2.1.283", Title: "work " + sid[:4],
		LastHumanInput: at, LastActivity: at,
	}
}

func appendTranscript(t *testing.T, s claudenative.Session, line string) {
	t.Helper()
	f, err := os.OpenFile(s.TranscriptPath, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	if _, err := f.WriteString(line); err != nil {
		t.Fatal(err)
	}
}

var (
	_ scheduler.Capturer = (*Job)(nil)
	_ Code               = (*worktree.Store)(nil)
	_ Orca               = (*orcabridge.Client)(nil)
	_ Catalog            = (*catalog.Store)(nil)
	_ Publisher          = (*catalog.Publisher)(nil)
	_ Targets            = (*inventory.Inventory)(nil)
)
