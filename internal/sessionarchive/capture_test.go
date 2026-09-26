package sessionarchive_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-sync/internal/sessionarchive"
	"github.com/yasyf/synckit/artifact"
)

const sid = "0b9f3c1e-2d4a-4f6b-8c7d-9e0a1b2c3d4e"

var capturedAt = time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)

type fakeStore struct {
	chunks      map[artifact.Digest][]byte
	manifests   map[artifact.Digest]artifact.Manifest
	puts        map[string]int
	chunkWrites map[string]int
	groups      int
	onPut       func(media string, data []byte)
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		chunks:      map[artifact.Digest][]byte{},
		manifests:   map[artifact.Digest]artifact.Manifest{},
		puts:        map[string]int{},
		chunkWrites: map[string]int{},
	}
}

func (s *fakeStore) Put(_ context.Context, r io.Reader, media string) (artifact.Ref, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return artifact.Ref{}, err
	}
	s.puts[media]++
	m := artifact.Manifest{Schema: artifact.ManifestSchema, Media: media, Size: int64(len(data))}
	for chunk := range slices.Chunk(data, artifact.ChunkSize) {
		d := artifact.Sum(chunk)
		if _, ok := s.chunks[d]; !ok {
			s.chunks[d] = bytes.Clone(chunk)
			s.chunkWrites[media]++
		}
		m.Chunks = append(m.Chunks, artifact.ChunkRef{Digest: d, Size: int64(len(chunk))})
	}
	if s.onPut != nil {
		s.onPut(media, data)
	}
	return s.record(m)
}

func (s *fakeStore) PutGroup(_ context.Context, media string, deps []artifact.Ref) (artifact.Ref, error) {
	for _, dep := range deps {
		if _, ok := s.manifests[dep.Digest]; !ok {
			return artifact.Ref{}, &artifact.MissingError{Digest: dep.Digest}
		}
	}
	s.groups++
	return s.record(artifact.Manifest{Schema: artifact.ManifestSchema, Media: media, Deps: slices.Clone(deps)})
}

func (s *fakeStore) record(m artifact.Manifest) (artifact.Ref, error) {
	b, err := m.Encode()
	if err != nil {
		return artifact.Ref{}, err
	}
	d := artifact.Sum(b)
	s.manifests[d] = m
	return artifact.Ref{Digest: d, Kind: artifact.KindManifest, Size: m.Size}, nil
}

func (s *fakeStore) reset() {
	s.puts, s.chunkWrites, s.groups = map[string]int{}, map[string]int{}, 0
}

func (s *fakeStore) content(t *testing.T, ref artifact.Ref) []byte {
	t.Helper()
	m, ok := s.manifests[ref.Digest]
	if !ok {
		t.Fatalf("ref %s not stored", ref.Digest)
	}
	var out []byte
	for _, c := range m.Chunks {
		out = append(out, s.chunks[c.Digest]...)
	}
	return out
}

func (s *fakeStore) closure(ref artifact.Ref) map[artifact.Digest]bool {
	seen := map[artifact.Digest]bool{}
	var visit func(artifact.Ref)
	visit = func(r artifact.Ref) {
		if seen[r.Digest] {
			return
		}
		seen[r.Digest] = true
		for _, dep := range s.manifests[r.Digest].Deps {
			visit(dep)
		}
	}
	visit(ref)
	return seen
}

func (s *fakeStore) holds(needle []byte) bool {
	for _, c := range s.chunks {
		if bytes.Contains(c, needle) {
			return true
		}
	}
	return false
}

type native struct {
	config, project, tmp string
	src                  sessionarchive.Source
}

func newNative(t *testing.T, projectDirName string) native {
	t.Helper()
	root := t.TempDir()
	config := filepath.Join(root, "claude")
	project := filepath.Join(config, "projects", projectDirName)
	n := native{config: config, project: project, tmp: filepath.Join(root, "tmp")}
	n.src = sessionarchive.Source{
		SessionID:      sid,
		SourceHost:     "src-host",
		ConfigDir:      config,
		ProjectDirName: projectDirName,
		TranscriptPath: filepath.Join(project, sid+".jsonl"),
		SessionDir:     filepath.Join(project, sid),
		FileHistoryDir: filepath.Join(config, "file-history", sid),
		Cwd:            "/Users/me/src/repo",
		OriginalCwd:    "/Users/me/src/repo",
		GitBranch:      "main",
		Title:          "fix the thing",
		ClaudeVersion:  "2.1.283",
		LastHuman:      capturedAt.Add(-time.Minute),
		LastAutonomous: capturedAt.Add(-time.Second),
		CapturedAt:     capturedAt,
	}
	n.write(t, n.src.TranscriptPath, record("u1")+record("a1"))
	return n
}

func (n native) write(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func (n native) session(rel string) string {
	return filepath.Join(n.src.SessionDir, filepath.FromSlash(rel))
}

func appendFile(t *testing.T, path, content string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0) //nolint:gosec // G304: a temp path this test composed
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(content); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func record(uuid string) string {
	return fmt.Sprintf(`{"type":"user","uuid":%q,"message":{"content":"hi"}}`+"\n", uuid)
}

func capture(t *testing.T, src sessionarchive.Source, prev *sessionarchive.Archive, store *fakeStore) sessionarchive.Archive {
	t.Helper()
	a, err := sessionarchive.Capture(context.Background(), src, prev, store)
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	return a
}

func entry(t *testing.T, a sessionarchive.Archive, key string) sessionarchive.Entry {
	t.Helper()
	for _, e := range a.Manifest.Entries {
		if e.Key() == key {
			return e
		}
	}
	t.Fatalf("no entry %s in %v", key, keys(a))
	return sessionarchive.Entry{}
}

func keys(a sessionarchive.Archive) []string {
	out := make([]string, 0, len(a.Manifest.Entries))
	for _, e := range a.Manifest.Entries {
		out = append(out, e.Key())
	}
	return out
}

func TestCaptureFreezesJSONLAtLastNewline(t *testing.T) {
	complete := record("u1") + record("a1")
	tests := []struct {
		name        string
		content     string
		wantSize    int64
		wantDropped int64
	}{
		{"complete records", complete, int64(len(complete)), 0},
		{"partial tail", complete + `{"type":"assist`, int64(len(complete)), int64(len(`{"type":"assist`))},
		{"no newline at all", `{"type":"user"`, 0, int64(len(`{"type":"user"`))},
		{"empty", "", 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := newNative(t, "-Users-me-src-repo")
			n.write(t, n.src.TranscriptPath, tt.content)
			n.write(t, n.session("subagents/workflows/wf_1/agent-b.jsonl"), tt.content)
			n.write(t, n.session("tool-results/no-newline.txt"), "abc")
			store := newFakeStore()
			a := capture(t, n.src, nil, store)
			for _, key := range []string{"transcript/" + sid + ".jsonl", "session/subagents/workflows/wf_1/agent-b.jsonl"} {
				e := entry(t, a, key)
				if e.Size != tt.wantSize || e.DroppedTailBytes != tt.wantDropped {
					t.Errorf("%s: size %d dropped %d, want %d %d", key, e.Size, e.DroppedTailBytes, tt.wantSize, tt.wantDropped)
				}
				if got := string(store.content(t, e.Ref)); got != tt.content[:tt.wantSize] {
					t.Errorf("%s: stored %q, want %q", key, got, tt.content[:tt.wantSize])
				}
				if e.Stat.Size != int64(len(tt.content)) {
					t.Errorf("%s: stat size %d, want %d", key, e.Stat.Size, len(tt.content))
				}
			}
			e := entry(t, a, "session/tool-results/no-newline.txt")
			if got := string(store.content(t, e.Ref)); got != "abc" || e.DroppedTailBytes != 0 {
				t.Errorf("sidecar stored %q dropped %d, want whole file", got, e.DroppedTailBytes)
			}
			if store.puts[sessionarchive.MediaTranscript] != 2 || store.puts[sessionarchive.MediaFile] != 1 {
				t.Errorf("puts %v, want 2 transcripts and 1 file", store.puts)
			}
		})
	}
}

func TestCaptureSidecarsAndExclusions(t *testing.T) {
	n := newNative(t, "-Users-me-src-repo")
	for rel, content := range map[string]string{
		"subagents/agent-a.jsonl":                    record("s1"),
		"subagents/agent-a.meta.json":                `{"agentType":"Explore"}`,
		"subagents/workflows/wf_1/agent-b.jsonl":     record("w1"),
		"subagents/workflows/wf_1/agent-b.meta.json": `{}`,
		"tool-results/toolu_01.txt":                  "big output",
		"tool-results/pdf-9a/page-01.jpg":            "jpeg",
		"workflows/wf_1.json":                        `{"scriptPath":"scripts/s.js"}`,
		"workflows/scripts/s.js":                     "run()",
		"agent-notes/findings.md":                    "# notes",
		"agent-notes/x.lock":                         "lock",
		"agent-notes/secret.key":                     "key",
		"session-env/env.sh":                         "export A=1",
		"shell-snapshots/snap.sh":                    "snap",
		"statsig/cache":                              "s",
		"sessions/123.json":                          "{}",
		".credentials.json":                          "{}",
	} {
		n.write(t, n.session(rel), content)
	}
	n.write(t, filepath.Join(n.src.FileHistoryDir, "abc@v1"), "old")
	n.write(t, filepath.Join(n.config, "tasks", sid, "1.json"), `{"id":"1"}`)
	n.write(t, filepath.Join(n.config, "tasks", "team-x", "2.json"), `{"id":"2"}`)
	n.write(t, filepath.Join(n.config, "tasks", "unrelated", "3.json"), `{"id":"3"}`)
	n.write(t, filepath.Join(n.config, "plans", "brave-plan.md"), "# plan")
	n.write(t, filepath.Join(n.config, "paste-cache", "h1.txt"), "pasted")
	n.write(t, filepath.Join(n.config, "sessions", "123.key"), "secret")
	scratch := filepath.Join(n.tmp, "claude-502", "-Users-me-src-repo", sid)
	n.write(t, filepath.Join(scratch, "tasks", "b1.output"), "out")
	n.write(t, filepath.Join(scratch, "session-env", "x"), "x")
	if err := os.MkdirAll(filepath.Join(scratch, "scratchpad"), 0o750); err != nil {
		t.Fatal(err)
	}
	n.src.TaskListDirs = []string{filepath.Join(n.config, "tasks", "team-x"), filepath.Join(n.config, "tasks", sid)}
	n.src.PlanFiles = []string{filepath.Join(n.config, "plans", "brave-plan.md")}
	n.src.PasteFiles = []string{filepath.Join(n.config, "paste-cache", "h1.txt")}
	n.src.ScratchpadDir = scratch

	store := newFakeStore()
	a := capture(t, n.src, nil, store)

	want := []string{
		"file-history/abc@v1",
		"paste-cache/h1.txt",
		"plans/brave-plan.md",
		"scratchpad/scratchpad",
		"scratchpad/tasks",
		"scratchpad/tasks/b1.output",
		"session/agent-notes",
		"session/agent-notes/findings.md",
		"session/subagents",
		"session/subagents/agent-a.jsonl",
		"session/subagents/agent-a.meta.json",
		"session/subagents/workflows",
		"session/subagents/workflows/wf_1",
		"session/subagents/workflows/wf_1/agent-b.jsonl",
		"session/subagents/workflows/wf_1/agent-b.meta.json",
		"session/tool-results",
		"session/tool-results/pdf-9a",
		"session/tool-results/pdf-9a/page-01.jpg",
		"session/tool-results/toolu_01.txt",
		"session/workflows",
		"session/workflows/scripts",
		"session/workflows/scripts/s.js",
		"session/workflows/wf_1.json",
		"tasks/" + sid,
		"tasks/" + sid + "/1.json",
		"tasks/team-x",
		"tasks/team-x/2.json",
		"transcript/" + sid + ".jsonl",
	}
	if got := keys(a); !slices.Equal(got, want) {
		t.Errorf("entries\n got %q\nwant %q", got, want)
	}
	wantRoots := map[sessionarchive.Root]string{
		sessionarchive.RootTranscript:  n.project,
		sessionarchive.RootSession:     n.src.SessionDir,
		sessionarchive.RootFileHistory: n.src.FileHistoryDir,
		sessionarchive.RootScratchpad:  scratch,
		sessionarchive.RootTasks:       filepath.Join(n.config, "tasks"),
		sessionarchive.RootPlans:       filepath.Join(n.config, "plans"),
		sessionarchive.RootPasteCache:  filepath.Join(n.config, "paste-cache"),
	}
	if !reflect.DeepEqual(a.Manifest.Roots, wantRoots) {
		t.Errorf("roots %v, want %v", a.Manifest.Roots, wantRoots)
	}
	if !a.Manifest.Complete() {
		t.Errorf("completeness %+v, want complete", a.Manifest.Completeness)
	}
	for _, secret := range []string{"secret", "export A=1", "lock", "snap"} {
		if store.holds([]byte(secret)) {
			t.Errorf("store holds excluded content %q", secret)
		}
	}
	if e := entry(t, a, "session/subagents/agent-a.jsonl"); string(store.content(t, e.Ref)) != record("s1") {
		t.Errorf("subagent transcript stored %q", store.content(t, e.Ref))
	}
	if e := entry(t, a, "plans/brave-plan.md"); string(store.content(t, e.Ref)) != "# plan" || e.Mode != 0o600 {
		t.Errorf("plan stored %q mode %v", store.content(t, e.Ref), e.Mode)
	}
	if e := entry(t, a, "scratchpad/scratchpad"); !e.Mode.IsDir() || e.Ref.Digest != "" {
		t.Errorf("empty scratchpad dir entry %+v", e)
	}
	if a.Manifest.SessionID != sid || a.Manifest.Format != sessionarchive.Format || !a.Manifest.CapturedAt.Equal(capturedAt) {
		t.Errorf("manifest header %+v", a.Manifest)
	}
}

func TestCaptureReportsMissingReferences(t *testing.T) {
	n := newNative(t, "-Users-me-src-repo")
	toolResults := n.src.SessionDir + "/tool-results/"
	lines := []string{
		fmt.Sprintf(`{"type":"user","toolUseResult":"Full output saved to: %spresent.txt."}`, toolResults),
		fmt.Sprintf(`{"type":"user","message":{"content":"saved to: %sgone.txt\nand %spdf-9a/page-01.jpg"}}`, toolResults, toolResults),
		`{"type":"file-history-snapshot","snapshot":{"trackedFileBackups":{"/a.go":{"backupFileName":"abc@v1"},"/b.go":{"backupFileName":"def@v2"},"/c.go":{"backupFileName":null}}}}`,
	}
	n.write(t, n.src.TranscriptPath, strings.Join(lines, "\n")+"\n")
	n.write(t, n.session("subagents/agent-a.jsonl"), fmt.Sprintf(`{"toolUseResult":"%ssub-gone.json"}`+"\n", toolResults))
	n.write(t, n.session("tool-results/present.txt"), "here")
	n.write(t, n.session("tool-results/pdf-9a/page-01.jpg"), "jpeg")
	n.write(t, filepath.Join(n.src.FileHistoryDir, "abc@v1"), "old")
	n.write(t, filepath.Join(n.config, "plans", "kept.md"), "# kept")
	n.src.PlanFiles = []string{filepath.Join(n.config, "plans", "kept.md"), filepath.Join(n.config, "plans", "absent.md")}
	n.src.PasteFiles = []string{filepath.Join(n.config, "paste-cache", "gone.txt")}
	n.src.TaskListDirs = []string{filepath.Join(n.config, "tasks", "absent-list")}
	n.src.ScratchpadDir = filepath.Join(n.tmp, "claude-502", "reaped", sid)

	a := capture(t, n.src, nil, newFakeStore())

	want := []string{
		"file-history/def@v2",
		"paste-cache/gone.txt",
		"plans/absent.md",
		"scratchpad",
		"session/tool-results/gone.txt",
		"session/tool-results/sub-gone.json",
		"tasks/absent-list",
	}
	if got := a.Manifest.Completeness.Missing; !slices.Equal(got, want) {
		t.Errorf("missing\n got %q\nwant %q", got, want)
	}
	if a.Manifest.Complete() {
		t.Error("Complete() = true with missing references")
	}
	wantRefs := []string{
		"file-history/abc@v1",
		"file-history/def@v2",
		"session/tool-results/gone.txt",
		"session/tool-results/pdf-9a/page-01.jpg",
		"session/tool-results/present.txt",
	}
	if got := entry(t, a, "transcript/"+sid+".jsonl").References; !slices.Equal(got, wantRefs) {
		t.Errorf("transcript references\n got %q\nwant %q", got, wantRefs)
	}
}

func TestCaptureChunksLargeTranscriptWithPrefixReuse(t *testing.T) {
	n := newNative(t, "-Users-me-src-repo")
	line := `{"type":"assistant","pad":"` + strings.Repeat("x", 1000) + `"}` + "\n"
	const lines = 8700
	body := strings.Repeat(line, lines)
	n.write(t, n.src.TranscriptPath, body)
	n.write(t, n.session("tool-results/a.txt"), "sidecar")
	store := newFakeStore()

	first := capture(t, n.src, nil, store)
	wantChunks := (len(body) + artifact.ChunkSize - 1) / artifact.ChunkSize
	if len(body) <= 8<<20 || wantChunks != 9 {
		t.Fatalf("fixture is %d bytes in %d chunks, want > 8 MiB in 9", len(body), wantChunks)
	}
	if got := store.chunkWrites[sessionarchive.MediaTranscript]; got != wantChunks {
		t.Errorf("first capture wrote %d transcript chunks, want %d", got, wantChunks)
	}

	appendFile(t, n.src.TranscriptPath, line+`{"partial`)
	store.reset()
	second := capture(t, n.src, &first, store)

	wantPuts := map[string]int{sessionarchive.MediaTranscript: 1, sessionarchive.MediaManifest: 1}
	if !reflect.DeepEqual(store.puts, wantPuts) {
		t.Errorf("append puts %v, want %v", store.puts, wantPuts)
	}
	if got := store.chunkWrites[sessionarchive.MediaTranscript]; got != 1 {
		t.Errorf("append wrote %d transcript chunks, want only the last one", got)
	}
	e := entry(t, second, "transcript/"+sid+".jsonl")
	if e.Size != int64(len(body)+len(line)) || e.DroppedTailBytes != int64(len(`{"partial`)) {
		t.Errorf("appended transcript size %d dropped %d", e.Size, e.DroppedTailBytes)
	}
	if !bytes.Equal(store.content(t, e.Ref), []byte(body+line)) {
		t.Error("appended transcript content differs from the complete records")
	}
	if !reflect.DeepEqual(entry(t, second, "session/tool-results/a.txt"), entry(t, first, "session/tool-results/a.txt")) {
		t.Error("unchanged sidecar entry was not reused")
	}
}

func TestCaptureUnchangedSessionWritesNothing(t *testing.T) {
	n := newNative(t, "-Users-me-src-repo")
	n.write(t, n.session("subagents/agent-a.jsonl"), record("s1")+`{"tail`)
	n.write(t, n.session("tool-results/a.txt"), "sidecar")
	n.write(t, filepath.Join(n.src.FileHistoryDir, "abc@v1"), "old")
	store := newFakeStore()
	first := capture(t, n.src, nil, store)
	store.reset()

	later := n.src
	later.CapturedAt = capturedAt.Add(2 * time.Minute)
	second := capture(t, later, &first, store)

	if len(store.puts) != 0 || store.groups != 0 {
		t.Errorf("unchanged capture put %v and %d groups, want nothing", store.puts, store.groups)
	}
	if !reflect.DeepEqual(second, first) {
		t.Errorf("unchanged capture returned a new archive:\n got %+v\nwant %+v", second, first)
	}

	later.Title = "renamed"
	third := capture(t, later, &second, store)
	wantPuts := map[string]int{sessionarchive.MediaManifest: 1}
	if !reflect.DeepEqual(store.puts, wantPuts) || third.Ref == first.Ref || !third.Manifest.CapturedAt.Equal(later.CapturedAt) {
		t.Errorf("metadata-only change put %v ref %v captured %v", store.puts, third.Ref, third.Manifest.CapturedAt)
	}
}

func TestCaptureUnicodeAndPunctuationPaths(t *testing.T) {
	n := newNative(t, "-Users-me-Proj----na-ve--v2-----tests-----")
	files := map[string]string{
		"tool-results/résumé (1) & 'q' #2.txt": "unicode sidecar",
		"subagents/agent-ünï 🚀.jsonl":          record("ü1"),
		"agent notes/[draft]; $HOME?.md":       "punctuation",
	}
	for rel, content := range files {
		n.write(t, n.session(rel), content)
	}
	plan := filepath.Join(n.config, "plans", "plan: “smart” #1.md")
	n.write(t, plan, "# ✓")
	n.src.PlanFiles = []string{plan}
	n.src.Cwd = "/Users/me/Proj — naïve (v2) & 'tests' 🚀"
	store := newFakeStore()

	a := capture(t, n.src, nil, store)

	for rel, content := range files {
		if e := entry(t, a, "session/"+rel); string(store.content(t, e.Ref)) != content {
			t.Errorf("%s stored %q, want %q", rel, store.content(t, e.Ref), content)
		}
	}
	if e := entry(t, a, "plans/plan: “smart” #1.md"); string(store.content(t, e.Ref)) != "# ✓" {
		t.Errorf("plan stored %q", store.content(t, e.Ref))
	}
	var decoded sessionarchive.Manifest
	if err := json.Unmarshal(store.content(t, a.ManifestRef), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Cwd != n.src.Cwd || !slices.Equal(decodedKeys(decoded), keys(a)) {
		t.Errorf("manifest round trip cwd %q keys %q", decoded.Cwd, decodedKeys(decoded))
	}
}

func decodedKeys(m sessionarchive.Manifest) []string {
	return keys(sessionarchive.Archive{Manifest: m})
}

func TestCaptureRecordsSymlinksWithoutFollowing(t *testing.T) {
	n := newNative(t, "-Users-me-src-repo")
	outside := filepath.Join(filepath.Dir(n.config), "outside")
	n.write(t, filepath.Join(outside, "secret.txt"), "outside secret")
	n.write(t, n.session("subagents/agent-a.jsonl"), record("s1"))
	links := map[string]string{
		"tool-results/escape":          outside,
		"tool-results/secret-link.txt": filepath.Join(outside, "secret.txt"),
		"tool-results/rel":             "../subagents",
		"tool-results/dangling":        "does/not/exist",
	}
	if err := os.MkdirAll(n.session("tool-results"), 0o750); err != nil {
		t.Fatal(err)
	}
	for rel, target := range links {
		if err := os.Symlink(target, n.session(rel)); err != nil {
			t.Fatal(err)
		}
	}
	store := newFakeStore()

	a := capture(t, n.src, nil, store)

	for rel, target := range links {
		e := entry(t, a, "session/"+rel)
		if e.Link != target || e.Mode&fs.ModeSymlink == 0 || e.Ref.Digest != "" {
			t.Errorf("%s entry %+v, want verbatim link to %s", rel, e, target)
		}
	}
	for _, key := range keys(a) {
		if strings.HasPrefix(key, "session/tool-results/escape/") || strings.HasPrefix(key, "session/tool-results/rel/") {
			t.Errorf("walked through symlink: %s", key)
		}
	}
	if store.holds([]byte("outside secret")) {
		t.Error("store holds content reached through a symlink")
	}
}

func TestCaptureDefersTornFiles(t *testing.T) {
	tests := []struct {
		name     string
		withPrev bool
	}{
		{"no previous capture", false},
		{"previous capture stands in", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := newNative(t, "-Users-me-src-repo")
			live := n.session("tool-results/live.txt")
			n.write(t, live, "live v1")
			store := newFakeStore()
			var prev *sessionarchive.Archive
			if tt.withPrev {
				first := capture(t, n.src, nil, store)
				prev = &first
				n.write(t, live, "live v2 longer")
			}
			store.onPut = func(_ string, data []byte) {
				if bytes.HasPrefix(data, []byte("live")) {
					appendFile(t, live, "+")
				}
			}

			a := capture(t, n.src, prev, store)

			if got, want := a.Manifest.Completeness.Deferred, []string{"session/tool-results/live.txt"}; !slices.Equal(got, want) {
				t.Errorf("deferred %q, want %q", got, want)
			}
			if a.Manifest.Complete() {
				t.Error("Complete() = true with a deferred file")
			}
			got := slices.Contains(keys(a), "session/tool-results/live.txt")
			if got != tt.withPrev {
				t.Errorf("live entry present = %v, want %v", got, tt.withPrev)
			}
			if tt.withPrev && !reflect.DeepEqual(entry(t, a, "session/tool-results/live.txt"), entry(t, *prev, "session/tool-results/live.txt")) {
				t.Error("deferred file did not keep its previous entry")
			}
		})
	}
}

func TestCaptureToleratesTranscriptGrowthDuringRead(t *testing.T) {
	n := newNative(t, "-Users-me-src-repo")
	original := record("u1") + record("a1")
	store := newFakeStore()
	store.onPut = func(media string, _ []byte) {
		if media != sessionarchive.MediaTranscript {
			return
		}
		appendFile(t, n.src.TranscriptPath, record("late"))
	}

	a := capture(t, n.src, nil, store)

	e := entry(t, a, "transcript/"+sid+".jsonl")
	if len(a.Manifest.Completeness.Deferred) != 0 || string(store.content(t, e.Ref)) != original {
		t.Errorf("deferred %q stored %q, want the frozen prefix", a.Manifest.Completeness.Deferred, store.content(t, e.Ref))
	}
}

func TestArchiveClosureCoversEveryFile(t *testing.T) {
	tests := []struct {
		name  string
		files int
	}{
		{"single group", 3},
		{"fans out past MaxDeps", artifact.MaxDeps + 10},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := newNative(t, "-Users-me-src-repo")
			n.write(t, n.session("tool-results/dup-a.txt"), "same")
			n.write(t, n.session("tool-results/dup-b.txt"), "same")
			for i := range tt.files {
				n.write(t, n.session(fmt.Sprintf("tool-results/f%05d.txt", i)), fmt.Sprintf("file %d", i))
			}
			store := newFakeStore()

			a := capture(t, n.src, nil, store)

			refs := a.Manifest.Refs()
			if want := tt.files + 2; len(refs) != want {
				t.Errorf("Refs() has %d refs, want %d unique", len(refs), want)
			}
			closure := store.closure(a.Ref)
			for _, ref := range append(refs, a.ManifestRef) {
				if !closure[ref.Digest] {
					t.Errorf("closure lacks %s", ref.Digest)
				}
			}
			root := store.manifests[a.Ref.Digest]
			if root.Media != sessionarchive.MediaSession || root.Deps[0] != a.ManifestRef || len(root.Deps) > artifact.MaxDeps {
				t.Errorf("session group media %s deps %d first %v", root.Media, len(root.Deps), root.Deps[0])
			}
		})
	}
}
