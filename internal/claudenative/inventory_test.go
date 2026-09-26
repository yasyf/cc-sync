package claudenative

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	sidA = "aaaaaaaa-1111-4111-8111-111111111111"
	sidB = "bbbbbbbb-2222-4222-8222-222222222222"
)

func testLayout(t *testing.T) Layout {
	t.Helper()
	root := t.TempDir()
	return Layout{ConfigDir: filepath.Join(root, "claude"), TmpRoot: filepath.Join(root, "tmp"), UID: 501}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func appendFile(t *testing.T, path, content string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0) //nolint:gosec // G304: a fixture under t.TempDir.
	if err != nil {
		t.Fatalf("open for append: %v", err)
	}
	if _, err := f.WriteString(content); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func mkdirs(t *testing.T, dirs ...string) {
	t.Helper()
	for _, d := range dirs {
		if err := os.MkdirAll(d, 0o750); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}
}

func scan(t *testing.T, opts ScanOptions, prev Cursor) ([]Session, Cursor) {
	t.Helper()
	sessions, next, err := Scan(context.Background(), opts, prev)
	if err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	return sessions, next
}

func only(t *testing.T, sessions []Session) Session {
	t.Helper()
	if len(sessions) != 1 {
		t.Fatalf("Scan() returned %d sessions, want 1", len(sessions))
	}
	return sessions[0]
}

func inodeAt(t *testing.T, path string) uint64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	return inodeOf(info)
}

func rewriteInPlace(t *testing.T, path, content string) {
	t.Helper()
	before, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_TRUNC, 0) //nolint:gosec // G304: a fixture under t.TempDir.
	if err != nil {
		t.Fatalf("open for rewrite: %v", err)
	}
	if _, err := f.WriteString(content); err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := os.Chtimes(path, before.ModTime(), before.ModTime()); err != nil {
		t.Fatalf("restore mtime: %v", err)
	}
	if inodeAt(t, path) != inodeOf(before) {
		t.Fatalf("rewrite of %s changed the inode", path)
	}
}

func TestScanIncremental(t *testing.T) {
	l := testLayout(t)
	cwd := "/Users/me/proj"
	path := TranscriptPath(l.ConfigDir, cwd, sidA)
	complete := `{"type":"user","uuid":"u1","cwd":"/Users/me/proj","gitBranch":"main","version":"2.1.283","entrypoint":"cli","origin":{"kind":"human"},"timestamp":"2026-09-26T10:00:00Z","message":{"content":"hi"}}` + "\n" +
		`{"type":"assistant","uuid":"u2","cwd":"/Users/me/proj","timestamp":"2026-09-26T10:01:00Z","message":{"content":[]}}` + "\n" +
		`{"type":"ai-title","aiTitle":"Generated","sessionId":"` + sidA + `"}` + "\n"
	partial := `{"type":"custom-title","customTitle":"Named"`
	writeFile(t, path, complete+partial)
	opts := ScanOptions{Layout: l}

	first, cur1 := scan(t, opts, Cursor{})
	s := only(t, first)
	wantStat := TranscriptStat{
		Size: int64(len(complete + partial)), CompleteSize: int64(len(complete)), ModTime: s.Transcript.ModTime,
		Inode: inodeAt(t, path), PartialTail: true, Records: 3, LeafUUID: "u2",
	}
	if s.Transcript != wantStat {
		t.Errorf("first Transcript = %+v, want %+v", s.Transcript, wantStat)
	}
	if s.Cwd != cwd || s.OriginalCwd != cwd || s.Title != "Generated" || s.GitBranch != "main" || s.Version != "2.1.283" || s.Entrypoint != "cli" {
		t.Errorf("first session metadata = %+v", s)
	}
	if !s.LastHumanInput.Equal(time.Date(2026, 9, 26, 10, 0, 0, 0, time.UTC)) || !s.LastAutonomousActivity.Equal(time.Date(2026, 9, 26, 10, 1, 0, 0, time.UTC)) || !s.LastActivity.Equal(s.LastAutonomousActivity) {
		t.Errorf("first activity = %v / %v / %v", s.LastHumanInput, s.LastAutonomousActivity, s.LastActivity)
	}

	_, cur2 := scan(t, opts, cur1)
	if !reflect.DeepEqual(cur2.Files[path], cur1.Files[path]) {
		t.Errorf("unchanged rescan moved the cursor:\n%+v\nwant\n%+v", cur2.Files[path], cur1.Files[path])
	}

	appendFile(t, path, `,"sessionId":"`+sidA+`"}`+"\n"+
		`{"type":"user","uuid":"u5","origin":{"kind":"human"},"timestamp":"2026-09-26T11:00:00Z","message":{"content":"again"}}`+"\n")
	third, cur3 := scan(t, opts, cur2)
	s = only(t, third)
	if s.Transcript.Records != 5 || s.Title != "Named" || s.Transcript.LeafUUID != "u5" || s.OriginalCwd != cwd || s.Transcript.PartialTail {
		t.Errorf("append scan = %+v, want 5 records titled Named from the cursor's cwd", s)
	}
	if !s.LastHumanInput.Equal(time.Date(2026, 9, 26, 11, 0, 0, 0, time.UTC)) {
		t.Errorf("append LastHumanInput = %v", s.LastHumanInput)
	}

	shrunk := `{"type":"assistant","uuid":"s1","cwd":"/Users/me/proj","timestamp":"2026-09-26T12:00:00Z"}` + "\n"
	inode := inodeAt(t, path)
	writeFile(t, path, shrunk)
	if inodeAt(t, path) != inode {
		t.Fatalf("in-place rewrite changed the inode")
	}
	fourth, cur4 := scan(t, opts, cur3)
	s = only(t, fourth)
	if s.Transcript.Records != 1 || s.Transcript.LeafUUID != "s1" || s.Title != "" || s.Transcript.CompleteSize != int64(len(shrunk)) {
		t.Errorf("shrink scan = %+v, want a full reparse", s.Transcript)
	}

	replaced := `{"type":"user","uuid":"r1","cwd":"/Users/me/proj","origin":{"kind":"human"},"timestamp":"2026-09-26T13:00:00Z"}` + "\n" +
		`{"type":"assistant","uuid":"r2","cwd":"/Users/me/proj","timestamp":"2026-09-26T13:01:00Z","message":{"content":[]}}` + "\n"
	tmp := path + ".new"
	writeFile(t, tmp, replaced)
	if err := os.Rename(tmp, path); err != nil {
		t.Fatalf("rename: %v", err)
	}
	fifth, _ := scan(t, opts, cur4)
	s = only(t, fifth)
	if s.Transcript.Records != 2 || s.Transcript.LeafUUID != "r2" || s.Transcript.Inode == cur4.Files[path].Inode {
		t.Errorf("replaced scan = %+v, want a full reparse of the new inode", s.Transcript)
	}
}

func TestScanCompleteRecords(t *testing.T) {
	good := `{"type":"assistant","uuid":"g1","cwd":"/p","timestamp":"2026-09-26T10:00:00Z"}` + "\n"
	good2 := `{"type":"assistant","uuid":"g2","cwd":"/p","timestamp":"2026-09-26T10:01:00Z"}` + "\n"
	bad := `{"type":"assistant","uuid":` + "\n"
	tests := []struct {
		name         string
		content      string
		records      int
		completeSize int
		partial      bool
		leaf         string
	}{
		{name: "partial tail", content: good + `{"type":"assist`, records: 1, completeSize: len(good), partial: true, leaf: "g1"},
		{name: "final complete line unparseable", content: good + bad, records: 1, completeSize: len(good), partial: true, leaf: "g1"},
		{name: "middle line unparseable", content: good + bad + good2, records: 2, completeSize: len(good + bad + good2), leaf: "g2"},
		{name: "empty", content: "", records: 0, completeSize: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := testLayout(t)
			writeFile(t, TranscriptPath(l.ConfigDir, "/p", sidA), tt.content)
			sessions, _ := scan(t, ScanOptions{Layout: l}, Cursor{})
			got := only(t, sessions).Transcript
			if got.Records != tt.records || got.CompleteSize != int64(tt.completeSize) || got.PartialTail != tt.partial || got.LeafUUID != tt.leaf || got.Size != int64(len(tt.content)) {
				t.Errorf("Transcript = %+v, want records %d complete %d partial %v leaf %q", got, tt.records, tt.completeSize, tt.partial, tt.leaf)
			}
		})
	}
}

func TestScanDuplicates(t *testing.T) {
	cwd := "/Users/me/proj"
	line := `{"type":"assistant","uuid":"d1","cwd":"/Users/me/proj","timestamp":"2026-09-26T10:00:00Z"}` + "\n"
	old := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	fresh := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name     string
		copies   map[string]time.Time
		wantDir  string
		wantDups []string
	}{
		{
			name:     "home dir beats newer foreign copy",
			copies:   map[string]time.Time{ProjectDirName(cwd): old, "-Users-me-elsewhere": fresh},
			wantDir:  ProjectDirName(cwd),
			wantDups: []string{"-Users-me-elsewhere"},
		},
		{
			name:     "newest foreign copy without a home copy",
			copies:   map[string]time.Time{"-Users-me-a": old, "-Users-me-b": fresh, "-Users-me-c": old.Add(time.Hour)},
			wantDir:  "-Users-me-b",
			wantDups: []string{"-Users-me-a", "-Users-me-c"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := testLayout(t)
			projects := ProjectsDir(l.ConfigDir)
			for dir, mtime := range tt.copies {
				p := filepath.Join(projects, dir, sidA+".jsonl")
				writeFile(t, p, line)
				if err := os.Chtimes(p, mtime, mtime); err != nil {
					t.Fatalf("chtimes: %v", err)
				}
			}
			sessions, _ := scan(t, ScanOptions{Layout: l}, Cursor{})
			s := only(t, sessions)
			var wantDups []string
			for _, d := range tt.wantDups {
				wantDups = append(wantDups, filepath.Join(projects, d, sidA+".jsonl"))
			}
			if s.ProjectDirName != tt.wantDir || s.TranscriptPath != filepath.Join(projects, tt.wantDir, sidA+".jsonl") || !slices.Equal(s.Duplicates, wantDups) {
				t.Errorf("picked %s with duplicates %v, want %s with %v", s.ProjectDirName, s.Duplicates, tt.wantDir, wantDups)
			}
		})
	}
}

func TestScanSidecars(t *testing.T) {
	l := testLayout(t)
	original, moved := "/Users/me/proj", "/Users/me/moved"
	path := TranscriptPath(l.ConfigDir, original, sidA)
	writeFile(t, path, strings.Join([]string{
		`{"type":"user","uuid":"u1","slug":"brave-fox","cwd":"/Users/me/proj","origin":{"kind":"human"},"timestamp":"2026-09-26T10:00:00Z","message":{"content":"plan it"}}`,
		`{"type":"attachment","uuid":"a1","slug":"brave-fox","timestamp":"2026-09-26T10:00:01Z","attachment":{"type":"plan_mode","planFilePath":"/x/plans/p1.md","planExists":true}}`,
		`{"type":"attachment","uuid":"a2","slug":"lost-slug","timestamp":"2026-09-26T10:00:02Z","attachment":{"type":"plan_mode","planFilePath":"/x/plans/p2.md","planExists":false}}`,
		`{"type":"attachment","uuid":"a3","timestamp":"2026-09-26T10:00:03Z","attachment":{"type":"plan_file_reference","planFilePath":"/x/plans/p3.md","planContent":"..."}}`,
		`{"type":"assistant","uuid":"a4","timestamp":"2026-09-26T10:00:04Z","message":{"content":[{"type":"tool_use","id":"t1","name":"TeamCreate","input":{"team_name":"my-team"}},{"type":"tool_use","id":"t2","name":"TeamCreate","input":{"team_name":"../escape"}}]}}`,
		`{"type":"relocated","relocatedCwd":"/Users/me/moved","sessionId":"` + sidA + `"}`,
		`{"type":"custom-title","customTitle":"Plan work","sessionId":"` + sidA + `"}`,
	}, "\n")+"\n")
	sessionDir := filepath.Join(filepath.Dir(path), sidA)
	for _, rel := range []string{
		"subagents/agent-a1.jsonl", "subagents/agent-a1.meta.json", "subagents/workflows/wf_1/agent-b2.jsonl",
		"tool-results/r1.txt", "tool-results/pdf-1/page-01.jpg", "workflows/wf_1.json", "workflows/scripts/s.js",
	} {
		writeFile(t, filepath.Join(sessionDir, rel), "x")
	}
	mkdirs(t,
		l.fileHistoryDir(sidA),
		l.taskListDir(sidA), l.taskListDir("session-aaaaaaaa"), l.taskListDir("my-team"), l.taskListDir("unrelated"),
		ScratchpadDir(l.TmpRoot, l.UID, original, sidA),
	)
	writeFile(t, l.planPath("brave-fox"), "# plan")
	writeFile(t, l.historyPath(),
		`{"display":"secret","pastedContents":{"1":{"id":1,"type":"text","contentHash":"beef0001"},"2":{"id":2,"type":"text","content":"inline"}},"project":"/Users/me/proj","sessionId":"`+sidA+`","timestamp":1790500000000}`+"\n"+
			`{"display":"other","pastedContents":{},"project":"/Users/me/other","sessionId":"`+sidB+`","timestamp":1790000100000}`+"\n"+
			`{"display":"legacy","pastedContents":{},"project":"/Users/me/proj","timestamp":1790000200000}`+"\n")

	opts := ScanOptions{Layout: l}
	sessions, cur := scan(t, opts, Cursor{})
	s := only(t, sessions)
	want := SidecarSet{
		SessionDir: true, Subagents: 2, ToolResults: 2, Workflows: 1, FileHistory: true,
		TaskLists:   []string{sidA, "my-team", "session-aaaaaaaa"},
		Plans:       slices.Sorted(slices.Values([]string{"/x/plans/p1.md", "/x/plans/p3.md", l.planPath("brave-fox")})),
		PasteHashes: []string{"beef0001"},
		Scratchpad:  ScratchpadDir(l.TmpRoot, l.UID, original, sidA),
	}
	if !reflect.DeepEqual(s.Sidecars, want) {
		t.Errorf("Sidecars =\n%+v\nwant\n%+v", s.Sidecars, want)
	}
	if s.Cwd != moved || s.OriginalCwd != original || s.Title != "Plan work" {
		t.Errorf("tail metadata cwd %q original %q title %q", s.Cwd, s.OriginalCwd, s.Title)
	}
	if !s.LastHumanInput.Equal(time.UnixMilli(1790500000000)) {
		t.Errorf("LastHumanInput = %v, want the history timestamp", s.LastHumanInput)
	}
	if _, ok := cur.History[sidB]; ok {
		t.Errorf("history kept an entry for %s, which has no transcript", sidB)
	}

	appendFile(t, l.historyPath(), `{"display":"more","pastedContents":{"1":{"id":1,"type":"text","contentHash":"beef0002"}},"project":"/Users/me/proj","sessionId":"`+sidA+`","timestamp":1790600000000}`+"\n")
	sessions, cur = scan(t, opts, cur)
	s = only(t, sessions)
	info, err := os.Stat(l.historyPath())
	if err != nil {
		t.Fatalf("stat history: %v", err)
	}
	if !slices.Equal(s.Sidecars.PasteHashes, []string{"beef0001", "beef0002"}) || !s.LastHumanInput.Equal(time.UnixMilli(1790600000000)) || cur.HistoryOffset != info.Size() {
		t.Errorf("history tail = %v at %v offset %d, want both hashes at the appended time", s.Sidecars.PasteHashes, s.LastHumanInput, cur.HistoryOffset)
	}
}

type fakeRepos struct {
	bindings map[string]RepoBinding
	calls    int
}

func (f *fakeRepos) Resolve(_ context.Context, cwd string) (RepoBinding, bool, error) {
	f.calls++
	b, ok := f.bindings[cwd]
	return b, ok, nil
}

func TestScanOptions(t *testing.T) {
	l := testLayout(t)
	line := func(cwd string) string {
		return `{"type":"assistant","uuid":"x","cwd":"` + cwd + `","timestamp":"2026-09-26T10:00:00Z"}` + "\n"
	}
	pathA := TranscriptPath(l.ConfigDir, "/Users/me/repo", sidA)
	pathB := TranscriptPath(l.ConfigDir, "/Users/me/scratch", sidB)
	writeFile(t, pathA, line("/Users/me/repo"))
	writeFile(t, pathB, line("/Users/me/scratch"))
	writeFile(t, filepath.Join(filepath.Dir(pathA), "agent-deadbeef.jsonl"), line("/Users/me/repo"))
	writeFile(t, filepath.Join(filepath.Dir(pathA), "memory", "MEMORY.md"), "memo")
	binding := RepoBinding{Origin: "github.com/me/repo", Relpath: "repo", RegistryPath: "/Users/me/repo", CheckoutRoot: "/Users/me/repo"}
	repos := &fakeRepos{bindings: map[string]RepoBinding{"/Users/me/repo": binding}}
	live := LiveProcess{PID: 42, ProcStart: "Sat Sep 26 10:00:00 2026", SessionID: sidA}
	opts := ScanOptions{Layout: l, Repos: repos, Live: map[SessionID]LiveProcess{sidA: live}}

	all, cur := scan(t, opts, Cursor{})
	if len(all) != 2 || all[0].ID != sidA || all[1].ID != sidB {
		t.Fatalf("Scan() ids = %v, want [%s %s]", all, sidA, sidB)
	}
	if all[0].Repo == nil || *all[0].Repo != binding || all[1].Repo != nil {
		t.Errorf("repos = %v / %v, want %+v / nil", all[0].Repo, all[1].Repo, binding)
	}
	if all[0].Live == nil || *all[0].Live != live || all[1].Live != nil {
		t.Errorf("live = %v / %v, want %+v / nil", all[0].Live, all[1].Live, live)
	}

	appendFile(t, pathB, line("/Users/me/scratch"))
	opts.Only = []SessionID{sidA}
	filtered, next := scan(t, opts, cur)
	if len(filtered) != 1 || filtered[0].ID != sidA {
		t.Fatalf("Only scan = %v, want just %s", filtered, sidA)
	}
	if !reflect.DeepEqual(next.Files[pathB], cur.Files[pathB]) {
		t.Errorf("Only scan touched the cursor of an unselected session")
	}
	if repos.calls != 3 {
		t.Errorf("resolver calls = %d, want one per scanned cwd per scan", repos.calls)
	}
}

func TestScanRewrite(t *testing.T) {
	record := func(uuid, cwd, branch string) string {
		return `{"type":"assistant","uuid":"` + uuid + `","cwd":"` + cwd + `","gitBranch":"` + branch + `","timestamp":"2026-09-26T10:00:00Z","message":{"content":[]}}` + "\n"
	}
	padding := strings.Repeat(record("pad0001", "/p", ""), 200)
	tests := []struct {
		name       string
		before     string
		after      string
		wantCwd    string
		wantLeaf   string
		wantBranch string
		records    int
	}{
		{
			name:    "same-size rewrite of the only record",
			before:  record("old001", "/old1", "main"),
			after:   record("new001", "/new1", "main"),
			wantCwd: "/new1", wantLeaf: "new001", wantBranch: "main", records: 1,
		},
		{
			name:    "same-size rewrite of the last record behind a long prefix",
			before:  padding + record("old001", "/p", "main"),
			after:   padding + record("new001", "/p", "main"),
			wantCwd: "/p", wantLeaf: "new001", wantBranch: "main", records: 201,
		},
		{
			name:    "rewrite of the first record under a grown tail",
			before:  record("old001", "/old1", "main") + padding,
			after:   record("new001", "/new1", "main") + padding + record("tail01", "/p", ""),
			wantCwd: "/new1", wantLeaf: "tail01", wantBranch: "main", records: 202,
		},
		{
			name:    "append past an intact boundary folds only the appended bytes",
			before:  padding + record("mid001", "/p", "old-branch") + padding,
			after:   padding + record("mid001", "/p", "new-branch") + padding + record("tail01", "/p", ""),
			wantCwd: "/p", wantLeaf: "tail01", wantBranch: "old-branch", records: 402,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := testLayout(t)
			path := TranscriptPath(l.ConfigDir, "/p", sidA)
			writeFile(t, path, tt.before)
			opts := ScanOptions{Layout: l}
			_, cur := scan(t, opts, Cursor{})
			rewriteInPlace(t, path, tt.after)
			sessions, _ := scan(t, opts, cur)
			s := only(t, sessions)
			if s.Cwd != tt.wantCwd || s.Transcript.LeafUUID != tt.wantLeaf || s.GitBranch != tt.wantBranch || s.Transcript.Records != tt.records || s.Transcript.CompleteSize != int64(len(tt.after)) {
				t.Errorf("rescan cwd %q leaf %q branch %q records %d complete %d, want %q %q %q %d %d",
					s.Cwd, s.Transcript.LeafUUID, s.GitBranch, s.Transcript.Records, s.Transcript.CompleteSize,
					tt.wantCwd, tt.wantLeaf, tt.wantBranch, tt.records, len(tt.after))
			}
		})
	}
}

func TestScanHistoryRewrite(t *testing.T) {
	line := func(ms int64, hash string) string {
		return `{"display":"x","pastedContents":{"1":{"id":1,"type":"text","contentHash":"` + hash + `"}},"project":"/p","sessionId":"` + sidA + `","timestamp":` + strconv.FormatInt(ms, 10) + "}\n"
	}
	tests := []struct {
		name       string
		before     string
		after      string
		wantInput  time.Time
		wantHashes []string
	}{
		{
			name:       "same-size rewrite",
			before:     line(1790500000000, "oldhash1"),
			after:      line(1790600000000, "newhash1"),
			wantInput:  time.UnixMilli(1790600000000),
			wantHashes: []string{"newhash1"},
		},
		{
			name:       "rewrite of the first line under a grown tail",
			before:     line(1790500000000, "oldhash1"),
			after:      line(1790400000000, "newhash1") + line(1790450000000, "newhash2"),
			wantInput:  time.UnixMilli(1790450000000),
			wantHashes: []string{"newhash1", "newhash2"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := testLayout(t)
			writeFile(t, TranscriptPath(l.ConfigDir, "/p", sidA), `{"type":"assistant","uuid":"a1","cwd":"/p","timestamp":"2026-09-26T10:00:00Z"}`+"\n")
			writeFile(t, l.historyPath(), tt.before)
			opts := ScanOptions{Layout: l}
			_, cur := scan(t, opts, Cursor{})
			rewriteInPlace(t, l.historyPath(), tt.after)
			sessions, next := scan(t, opts, cur)
			s := only(t, sessions)
			if !s.LastHumanInput.Equal(tt.wantInput) || !slices.Equal(s.Sidecars.PasteHashes, tt.wantHashes) || next.HistoryOffset != int64(len(tt.after)) {
				t.Errorf("history = %v %v at offset %d, want %v %v at %d", s.LastHumanInput, s.Sidecars.PasteHashes, next.HistoryOffset, tt.wantInput, tt.wantHashes, len(tt.after))
			}
		})
	}
}

func TestScanSidecarOnlyChanges(t *testing.T) {
	l := testLayout(t)
	path := TranscriptPath(l.ConfigDir, "/p", sidA)
	writeFile(t, path, `{"type":"assistant","uuid":"a1","cwd":"/p","timestamp":"2026-09-26T10:00:00Z"}`+"\n")
	sessionDir := filepath.Join(filepath.Dir(path), sidA)
	opts := ScanOptions{Layout: l}
	sessions, cur := scan(t, opts, Cursor{})
	if got := only(t, sessions).Sidecars; !reflect.DeepEqual(got, SidecarSet{}) {
		t.Fatalf("initial Sidecars = %+v, want none", got)
	}
	transcript := cur.Files[path]
	steps := []struct {
		name   string
		mutate func(t *testing.T)
		want   SidecarSet
	}{
		{
			name:   "subagent created",
			mutate: func(t *testing.T) { writeFile(t, filepath.Join(sessionDir, "subagents", "agent-new.jsonl"), "{}\n") },
			want:   SidecarSet{SessionDir: true, Subagents: 1},
		},
		{
			name: "workflow agent created in a new workflow dir",
			mutate: func(t *testing.T) {
				writeFile(t, filepath.Join(sessionDir, "subagents", "workflows", "wf_1", "agent-w1.jsonl"), "{}\n")
			},
			want: SidecarSet{SessionDir: true, Subagents: 2},
		},
		{
			name: "workflow agent added to an existing workflow dir",
			mutate: func(t *testing.T) {
				writeFile(t, filepath.Join(sessionDir, "subagents", "workflows", "wf_1", "agent-w2.jsonl"), "{}\n")
			},
			want: SidecarSet{SessionDir: true, Subagents: 3},
		},
		{
			name:   "file history and task list created",
			mutate: func(t *testing.T) { mkdirs(t, l.fileHistoryDir(sidA), l.taskListDir("session-aaaaaaaa")) },
			want:   SidecarSet{SessionDir: true, Subagents: 3, FileHistory: true, TaskLists: []string{"session-aaaaaaaa"}},
		},
		{
			name: "subagent deleted",
			mutate: func(t *testing.T) {
				if err := os.Remove(filepath.Join(sessionDir, "subagents", "agent-new.jsonl")); err != nil {
					t.Fatalf("remove: %v", err)
				}
			},
			want: SidecarSet{SessionDir: true, Subagents: 2, FileHistory: true, TaskLists: []string{"session-aaaaaaaa"}},
		},
	}
	for _, step := range steps {
		step.mutate(t)
		sessions, cur = scan(t, opts, cur)
		if got := only(t, sessions).Sidecars; !reflect.DeepEqual(got, step.want) {
			t.Errorf("%s: Sidecars = %+v, want %+v", step.name, got, step.want)
		}
		fc := cur.Files[path]
		if fc.Inode != transcript.Inode || fc.Size != transcript.Size || !fc.ModTime.Equal(transcript.ModTime) || fc.ParsedOffset != transcript.ParsedOffset || fc.Boundary != transcript.Boundary || !reflect.DeepEqual(fc.Summary, transcript.Summary) {
			t.Errorf("%s: transcript cursor moved: %+v, want %+v", step.name, fc, transcript)
		}
	}
}

func TestScanActivity(t *testing.T) {
	ts := func(sec int) string { return time.Date(2026, 9, 26, 20, 4, sec, 0, time.UTC).Format(time.RFC3339Nano) }
	at := func(sec int) time.Time { return time.Date(2026, 9, 26, 20, 4, sec, 0, time.UTC) }
	queue := func(op, content string, sec int) string {
		return `{"type":"queue-operation","operation":"` + op + `","content":"` + content + `","sessionId":"` + sidA + `","timestamp":"` + ts(sec) + `"}`
	}
	tests := []struct {
		name           string
		lines          []string
		wantHuman      time.Time
		wantAutonomous time.Time
	}{
		{
			name: "enqueued task notification delivered as a user record",
			lines: []string{
				`{"type":"assistant","uuid":"a1","cwd":"/p","timestamp":"` + ts(1) + `","message":{"content":[]}}`,
				queue("enqueue", "<task-notification/>", 10),
				queue("dequeue", "", 13),
				`{"type":"user","uuid":"u1","isMeta":false,"origin":{"kind":"task-notification"},"timestamp":"` + ts(14) + `","message":{"content":"<task-notification/>"}}`,
			},
			wantAutonomous: at(14),
		},
		{
			name: "enqueued task notification delivered as a queued_command",
			lines: []string{
				queue("enqueue", "<task-notification/>", 10),
				queue("remove", "<task-notification/>", 12),
				`{"type":"attachment","uuid":"q1","timestamp":"` + ts(12) + `","attachment":{"type":"queued_command","prompt":"<task-notification/>","commandMode":"task-notification"}}`,
			},
			wantAutonomous: at(12),
		},
		{
			name: "enqueued human prompt delivered as a user record",
			lines: []string{
				`{"type":"assistant","uuid":"a1","cwd":"/p","timestamp":"` + ts(1) + `","message":{"content":[]}}`,
				queue("enqueue", "next step", 10),
				queue("dequeue", "", 13),
				`{"type":"user","uuid":"u1","origin":{"kind":"human"},"timestamp":"` + ts(14) + `","message":{"content":"next step"}}`,
			},
			wantHuman:      at(14),
			wantAutonomous: at(1),
		},
		{
			name: "enqueued human prompt delivered as a queued_command",
			lines: []string{
				`{"type":"assistant","uuid":"a1","cwd":"/p","timestamp":"` + ts(1) + `","message":{"content":[]}}`,
				queue("enqueue", "next step", 10),
				queue("remove", "next step", 12),
				`{"type":"attachment","uuid":"q1","timestamp":"` + ts(12) + `","attachment":{"type":"queued_command","prompt":"next step","commandMode":"prompt","origin":{"kind":"human"}}}`,
			},
			wantHuman:      at(12),
			wantAutonomous: at(1),
		},
		{
			name: "metadata after the final assistant record",
			lines: []string{
				`{"type":"user","uuid":"u1","cwd":"/p","origin":{"kind":"human"},"timestamp":"` + ts(1) + `","message":{"content":"go"}}`,
				`{"type":"attachment","uuid":"m1","timestamp":"` + ts(2) + `","attachment":{"type":"prompt_snapshot","systemPrompt":["s"]}}`,
				`{"type":"assistant","uuid":"a1","timestamp":"` + ts(3) + `","message":{"content":[]}}`,
				`{"type":"system","uuid":"s1","subtype":"stop_hook_summary","timestamp":"` + ts(4) + `"}`,
				`{"type":"system","uuid":"s2","subtype":"turn_duration","timestamp":"` + ts(5) + `"}`,
				`{"type":"attachment","uuid":"m2","timestamp":"` + ts(6) + `","attachment":{"type":"prompt_snapshot","tools":[],"cliPrefix":"c"}}`,
				`{"type":"system","uuid":"s3","subtype":"compact_boundary","timestamp":"` + ts(7) + `"}`,
				`{"type":"attachment","uuid":"m3","timestamp":"` + ts(8) + `","attachment":{"type":"environment","snapshot":{"workingDirectory":"/p"}}}`,
				`{"type":"last-prompt","lastPrompt":"go","leafUuid":"a1","sessionId":"` + sidA + `"}`,
			},
			wantHuman:      at(1),
			wantAutonomous: at(3),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := testLayout(t)
			writeFile(t, TranscriptPath(l.ConfigDir, "/p", sidA), strings.Join(tt.lines, "\n")+"\n")
			sessions, _ := scan(t, ScanOptions{Layout: l}, Cursor{})
			s := only(t, sessions)
			wantLast := later(tt.wantHuman, tt.wantAutonomous)
			if !s.LastHumanInput.Equal(tt.wantHuman) || !s.LastAutonomousActivity.Equal(tt.wantAutonomous) || !s.LastActivity.Equal(wantLast) {
				t.Errorf("activity human %v autonomous %v last %v, want %v %v %v",
					s.LastHumanInput, s.LastAutonomousActivity, s.LastActivity, tt.wantHuman, tt.wantAutonomous, wantLast)
			}
		})
	}
}
