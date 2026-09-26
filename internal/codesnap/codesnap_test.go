package codesnap

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/reposync/worktree"
	"github.com/yasyf/reposync/worktree/worktreetest"
	"github.com/yasyf/synckit/artifact"
)

type content struct {
	name  string
	media worktree.Media
	data  []byte
}

func contents() []content {
	big := make([]byte, 2*artifact.ChunkSize+17)
	for i := range big {
		big[i] = byte(i % 251)
	}
	return []content{
		{"bundle", worktree.MediaBundle, bytes.Repeat([]byte("bundle"), 50)},
		{"blob", worktree.MediaBlob, []byte("staged\n")},
		{"file", worktree.MediaFile, []byte("hello\n")},
		{"multi-chunk file", worktree.MediaFile, big},
		{"empty file", worktree.MediaFile, nil},
		{"lfs object", worktree.MediaLFSObject, []byte("large asset bytes")},
	}
}

type fixture struct {
	store *fakeStore
	sink  *Sink
	dir   string
}

func newFixture(t *testing.T) fixture {
	t.Helper()
	store := newFakeStore()
	dir := filepath.Join(t.TempDir(), "codesnap")
	sink, err := NewSink(store, dir)
	if err != nil {
		t.Fatalf("NewSink: %v", err)
	}
	return fixture{store: store, sink: sink, dir: dir}
}

func (f fixture) putAll(t *testing.T) map[string]worktree.ArtifactRef {
	t.Helper()
	refs := map[string]worktree.ArtifactRef{}
	for _, c := range contents() {
		ref, err := f.sink.Put(t.Context(), c.media, bytes.NewReader(c.data))
		if err != nil {
			t.Fatalf("Put %s: %v", c.name, err)
		}
		refs[c.name] = ref
	}
	return refs
}

func (f fixture) stored(t *testing.T, ref worktree.ArtifactRef) artifact.Ref {
	t.Helper()
	stored, ok, err := f.sink.lookup(ref.Digest)
	if err != nil || !ok {
		t.Fatalf("lookup %s = %v, %v", ref.Digest, ok, err)
	}
	return stored
}

func (f fixture) source(t *testing.T, snap worktree.Snapshot) *Source {
	t.Helper()
	root, err := f.sink.BuildCodeManifest(t.Context(), snap)
	if err != nil {
		t.Fatalf("BuildCodeManifest: %v", err)
	}
	src, err := SourceFromManifest(t.Context(), f.store, root)
	if err != nil {
		t.Fatalf("SourceFromManifest: %v", err)
	}
	return src
}

func oid(c byte) string {
	return strings.Repeat(string(c), 40)
}

func snapshotOf(t *testing.T, refs map[string]worktree.ArtifactRef) worktree.Snapshot {
	t.Helper()
	blob, file, big, empty, lfs := refs["blob"], refs["file"], refs["multi-chunk file"], refs["empty file"], refs["lfs object"]
	lfsOID := strings.TrimPrefix(lfs.Digest, digestPrefix)
	snap := worktree.Snapshot{
		Schema: worktree.SnapshotSchema,
		Source: "host-a",
		Worktree: worktree.Worktree{
			ID: strings.Repeat("e", 32), Origin: "https://example.com/r.git", Relpath: "r", Trunk: "main",
			Root: "/src/r", GitDir: "/src/r/.git", CommonDir: "/src/r/.git", Kind: worktree.KindGit,
			Branch: "feat", Head: oid('1'), Incarnation: 7,
		},
		CapturedAt:   time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC),
		ObjectFormat: "sha1",
		Head:         worktree.Head{Commit: oid('1'), Branch: "feat", TrunkTip: oid('2'), TrunkBase: oid('2'), Ahead: 1},
		History:      []worktree.Bundle{{Artifact: refs["bundle"], Tip: oid('1'), Prerequisites: []string{oid('2')}}},
		Requires:     []string{oid('2')},
		Index:        []worktree.IndexEntry{{Path: "a.txt", Mode: "100644", OID: oid('3'), Blob: &blob}},
		Files: []worktree.FileEntry{
			{Path: "a.txt", Kind: worktree.FileRegular, Content: &file},
			{Path: "big.bin", Kind: worktree.FileRegular, Content: &big, Untracked: true},
			{Path: "empty.txt", Kind: worktree.FileRegular, Content: &empty, Untracked: true},
		},
		LFSObjects: []worktree.LFSObject{{OID: lfsOID, Size: lfs.Size, Artifact: lfs}},
		LFS:        &worktree.LFSInfo{Objects: []worktree.LFSObjectRef{{Path: "assets/x.bin", OID: lfsOID, Size: lfs.Size}}, Remote: "origin"},
		Complete:   true,
	}
	digest, err := snap.ContentDigest()
	if err != nil {
		t.Fatalf("ContentDigest: %v", err)
	}
	snap.Digest = digest
	return snap
}

func TestRoundTrip(t *testing.T) {
	f := newFixture(t)
	refs := f.putAll(t)
	snap := snapshotOf(t, refs)
	src := f.source(t, snap)

	got, err := src.Snapshot(t.Context())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if !reflect.DeepEqual(got, snap) {
		t.Fatalf("Snapshot = %+v, want %+v", got, snap)
	}
	for _, c := range contents() {
		t.Run(c.name, func(t *testing.T) {
			ref := refs[c.name]
			want, err := worktreetest.New().Put(t.Context(), c.media, bytes.NewReader(c.data))
			if err != nil {
				t.Fatalf("worktreetest Put: %v", err)
			}
			if ref != want {
				t.Fatalf("Put = %+v, want raw ref %+v", ref, want)
			}
			has, err := src.Has(t.Context(), []worktree.ArtifactRef{ref})
			if err != nil || !reflect.DeepEqual(has, []bool{true}) {
				t.Fatalf("Has = %v, %v, want [true]", has, err)
			}
			rc, err := src.Open(t.Context(), ref)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			data, err := readAll(rc)
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			if !bytes.Equal(data, c.data) {
				t.Fatalf("read %d bytes, want %d identical bytes", len(data), len(c.data))
			}
		})
	}
}

func TestSourceOpenVerifiesRawDigest(t *testing.T) {
	other := worktree.ArtifactRef{Digest: digestPrefix + strings.Repeat("0", 64), Size: 6, Media: worktree.MediaFile}
	tests := []struct {
		name     string
		tamper   func([]byte) []byte
		ref      func(worktree.ArtifactRef) worktree.ArtifactRef
		wantOpen error
		wantRead error
	}{
		{name: "intact"},
		{name: "flipped byte", tamper: func(b []byte) []byte { b[len(b)/2] ^= 0xff; return b }, wantRead: ErrCorrupt},
		{name: "truncated", tamper: func(b []byte) []byte { return b[:len(b)-1] }, wantRead: ErrCorrupt},
		{name: "extended", tamper: func(b []byte) []byte { return append(b, 'x') }, wantRead: ErrCorrupt},
		{name: "size mismatch", ref: func(r worktree.ArtifactRef) worktree.ArtifactRef { r.Size++; return r }, wantOpen: ErrCorrupt},
		{name: "unmapped digest", ref: func(worktree.ArtifactRef) worktree.ArtifactRef { return other }, wantOpen: fs.ErrNotExist},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			refs := f.putAll(t)
			src := f.source(t, snapshotOf(t, refs))
			ref := refs["multi-chunk file"]
			if tt.tamper != nil {
				f.store.setTamper(f.stored(t, ref).Digest, tt.tamper)
			}
			if tt.ref != nil {
				ref = tt.ref(ref)
			}
			rc, err := src.Open(t.Context(), ref)
			if !errors.Is(err, tt.wantOpen) || (tt.wantOpen == nil) != (err == nil) {
				t.Fatalf("Open error = %v, want %v", err, tt.wantOpen)
			}
			if err != nil {
				return
			}
			_, err = readAll(rc)
			if !errors.Is(err, tt.wantRead) || (tt.wantRead == nil) != (err == nil) {
				t.Fatalf("read error = %v, want %v", err, tt.wantRead)
			}
		})
	}
}

func TestSinkPutReusesIndex(t *testing.T) {
	f := newFixture(t)
	data := []byte("same bytes\n")
	first, err := f.sink.Put(t.Context(), worktree.MediaFile, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	path, err := f.sink.entryPath(first.Digest)
	if err != nil {
		t.Fatalf("entryPath: %v", err)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat index entry: %v", err)
	}
	writes := f.store.writeCount()

	second, err := f.sink.Put(t.Context(), worktree.MediaFile, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("second Put: %v", err)
	}
	if second != first {
		t.Fatalf("second Put = %+v, want %+v", second, first)
	}
	if got := f.store.writeCount() - writes; got != 0 {
		t.Fatalf("second Put wrote %d store objects, want 0", got)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat index entry: %v", err)
	}
	if !os.SameFile(before, after) {
		t.Fatal("second Put rewrote the index entry")
	}
	shard, err := os.ReadDir(filepath.Dir(path))
	if err != nil || len(shard) != 1 || shard[0].Name() != filepath.Base(path) {
		t.Fatalf("index shard = %v, %v, want only %s", shard, err, filepath.Base(path))
	}

	reopened, err := NewSink(f.store, f.dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	has, err := reopened.Has(t.Context(), []worktree.ArtifactRef{first})
	if err != nil || !reflect.DeepEqual(has, []bool{true}) {
		t.Fatalf("reopened Has = %v, %v, want [true]", has, err)
	}
}

func TestSinkHas(t *testing.T) {
	f := newFixture(t)
	refs := f.putAll(t)
	lost := refs["blob"]
	f.store.remove(f.stored(t, lost).Digest)
	resized := refs["file"]
	resized.Size++
	never := worktree.ArtifactRef{Digest: digestPrefix + strings.Repeat("0", 64), Size: 6, Media: worktree.MediaFile}

	tests := []struct {
		name string
		ref  worktree.ArtifactRef
		want bool
	}{
		{"present", refs["file"], true},
		{"present lfs object", refs["lfs object"], true},
		{"empty", refs["empty file"], true},
		{"never put", never, false},
		{"size mismatch", resized, false},
		{"store lost object", lost, false},
	}
	queried := make([]worktree.ArtifactRef, len(tests))
	want := make([]bool, len(tests))
	for i, tt := range tests {
		queried[i], want[i] = tt.ref, tt.want
	}
	has, err := f.sink.Has(t.Context(), queried)
	if err != nil {
		t.Fatalf("Has: %v", err)
	}
	for i, tt := range tests {
		if has[i] != tt.want {
			t.Errorf("%s: Has = %v, want %v", tt.name, has[i], tt.want)
		}
	}

	if _, err := f.sink.Put(t.Context(), worktree.MediaBlob, strings.NewReader("staged\n")); err != nil {
		t.Fatalf("re-Put: %v", err)
	}
	if again, err := f.sink.Has(t.Context(), []worktree.ArtifactRef{lost}); err != nil || !reflect.DeepEqual(again, []bool{true}) {
		t.Fatalf("Has after re-Put = %v, %v, want [true]", again, err)
	}
}

func TestSourceHasRequiresCompleteObjects(t *testing.T) {
	tests := []struct {
		name   string
		remove func(t *testing.T, f fixture, stored artifact.Ref) artifact.Digest
	}{
		{"manifest", func(_ *testing.T, _ fixture, stored artifact.Ref) artifact.Digest { return stored.Digest }},
		{"chunk", func(t *testing.T, f fixture, stored artifact.Ref) artifact.Digest {
			m, err := f.store.Manifest(t.Context(), stored)
			if err != nil {
				t.Fatalf("Manifest: %v", err)
			}
			return m.Chunks[1].Digest
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			refs := f.putAll(t)
			src := f.source(t, snapshotOf(t, refs))
			target := refs["multi-chunk file"]
			f.store.remove(tt.remove(t, f, f.stored(t, target)))
			queried := []worktree.ArtifactRef{refs["file"], target, refs["lfs object"]}
			has, err := src.Has(t.Context(), queried)
			if err != nil {
				t.Fatalf("Has: %v", err)
			}
			if want := []bool{true, false, true}; !reflect.DeepEqual(has, want) {
				t.Fatalf("Has = %v, want %v", has, want)
			}
		})
	}
}

func TestCodeRootDeps(t *testing.T) {
	f := newFixture(t)
	refs := f.putAll(t)
	snap := snapshotOf(t, refs)
	root, err := f.sink.BuildCodeManifest(t.Context(), snap)
	if err != nil {
		t.Fatalf("BuildCodeManifest: %v", err)
	}
	src, err := SourceFromManifest(t.Context(), f.store, root)
	if err != nil {
		t.Fatalf("SourceFromManifest: %v", err)
	}

	rootManifest, err := f.store.Manifest(t.Context(), root)
	if err != nil {
		t.Fatalf("Manifest(root): %v", err)
	}
	raws := append(snap.Artifacts(), src.manifest.Snapshot)
	if rootManifest.Media != mediaCode || len(rootManifest.Deps) != 1+len(raws) {
		t.Fatalf("root = %q with %d deps, want %q with %d", rootManifest.Media, len(rootManifest.Deps), mediaCode, 1+len(raws))
	}
	first, err := f.store.Manifest(t.Context(), rootManifest.Deps[0])
	if err != nil || first.Media != mediaCodeManifest {
		t.Fatalf("first dep = %q, %v, want %q", first.Media, err, mediaCodeManifest)
	}
	closure := f.store.closure(root)
	for _, raw := range raws {
		if _, ok := closure[f.stored(t, raw).Digest]; !ok {
			t.Errorf("closure lacks %s %s", raw.Media, raw.Digest)
		}
	}
	if missing, err := f.store.Complete(t.Context(), []artifact.Ref{root}); err != nil || missing != 0 {
		t.Fatalf("Complete(root) = %d, %v, want 0", missing, err)
	}
	if len(src.manifest.Entries) != len(raws) {
		t.Fatalf("code manifest maps %d artifacts, want %d", len(src.manifest.Entries), len(raws))
	}
}

func TestBuildCodeManifestRefusesUnstoredArtifact(t *testing.T) {
	f := newFixture(t)
	refs := f.putAll(t)
	refs["file"] = worktree.ArtifactRef{Digest: digestPrefix + strings.Repeat("0", 64), Size: 6, Media: worktree.MediaFile}
	if _, err := f.sink.BuildCodeManifest(t.Context(), snapshotOf(t, refs)); err == nil {
		t.Fatal("BuildCodeManifest over an unstored artifact succeeded")
	}
}

func TestGroupFansOutPastMaxDeps(t *testing.T) {
	f := newFixture(t)
	refs := make([]artifact.Ref, artifact.MaxDeps+1)
	for i := range refs {
		ref, err := f.store.Put(t.Context(), strings.NewReader(fmt.Sprint(i)), string(worktree.MediaFile))
		if err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
		refs[i] = ref
	}
	manifest, err := f.store.Put(t.Context(), strings.NewReader("{}"), mediaCodeManifest)
	if err != nil {
		t.Fatalf("Put manifest: %v", err)
	}
	root, err := f.sink.group(t.Context(), manifest, refs)
	if err != nil {
		t.Fatalf("group: %v", err)
	}
	closure := f.store.closure(root)
	top := closure[root.Digest]
	if top.Media != mediaCode || len(top.Deps) != 3 || top.Deps[0] != manifest {
		t.Fatalf("root = %q deps %d first %v, want %q deps 3 first %v", top.Media, len(top.Deps), top.Deps[0], mediaCode, manifest)
	}
	for i, want := range []int{artifact.MaxDeps, 1} {
		if sub := closure[top.Deps[i+1].Digest]; sub.Media != mediaCodeDeps || len(sub.Deps) != want {
			t.Fatalf("subgroup %d = %q with %d deps, want %q with %d", i, sub.Media, len(sub.Deps), mediaCodeDeps, want)
		}
	}
	for i, ref := range refs {
		if _, ok := closure[ref.Digest]; !ok {
			t.Fatalf("closure lacks ref %d", i)
		}
	}
}

func TestSourceFromManifestRefusesNonCodeRoot(t *testing.T) {
	f := newFixture(t)
	plain, err := f.store.Put(t.Context(), strings.NewReader("not a root"), string(worktree.MediaFile))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := SourceFromManifest(t.Context(), f.store, plain); err == nil {
		t.Fatal("SourceFromManifest accepted a plain blob manifest")
	}
}
