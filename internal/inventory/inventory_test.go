package inventory

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/yasyf/cc-sync/internal/claudenative"
	"github.com/yasyf/cc-sync/internal/orcabridge"
	"github.com/yasyf/cc-sync/internal/scheduler"
	"github.com/yasyf/reposync/worktree"
)

const (
	sidA = "11111111-1111-4111-8111-111111111111"
	sidB = "22222222-2222-4222-8222-222222222222"
	sidC = "33333333-3333-4333-8333-333333333333"
	sidD = "44444444-4444-4444-8444-444444444444"
	idR1 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	idR2 = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

var t0 = time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)

type noProcesses struct{}

func (noProcesses) Processes(context.Context) ([]claudenative.Process, error) { return nil, nil }

type fakeActivity struct {
	calls    int
	activity []orcabridge.WorkspaceActivity
	err      error
}

func (f *fakeActivity) Activity(context.Context) ([]orcabridge.WorkspaceActivity, error) {
	f.calls++
	return f.activity, f.err
}

type fixture struct {
	root   string
	layout claudenative.Layout
	wts    []worktree.Worktree
	orca   *fakeActivity
	now    time.Time
	cfg    Config
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{
		root:   root,
		layout: claudenative.Layout{ConfigDir: filepath.Join(root, "claude"), TmpRoot: filepath.Join(root, "tmp"), UID: 501},
		orca:   &fakeActivity{},
		now:    t0,
	}
	mkdir(t, filepath.Join(f.layout.ConfigDir, "sessions"), filepath.Join(root, "src/r1/sub"), filepath.Join(root, "elsewhere"))
	for _, w := range []struct{ id, name string }{{idR1, "r1"}, {idR2, "r2"}} {
		dir := filepath.Join(root, "src", w.name)
		git := filepath.Join(dir, ".git")
		write(t, filepath.Join(git, "HEAD"), "ref: refs/heads/main\n")
		write(t, filepath.Join(git, "index"), "index")
		f.wts = append(f.wts, worktree.Worktree{
			ID: w.id, Origin: "https://example.com/" + w.name + ".git", Relpath: w.name, Trunk: "main",
			Root: dir, GitDir: git, CommonDir: git, Kind: worktree.KindGit, Branch: "main",
		})
	}
	for sid, cwd := range map[string]string{
		sidA: "src/r1", sidB: "src/r1/sub", sidC: "src/r2", sidD: "elsewhere",
	} {
		f.transcript(t, sid, filepath.Join(root, cwd), "u-"+sid[:4], t0.Add(-time.Minute))
	}
	f.cfg = Config{
		Layout:     f.layout,
		Processes:  noProcesses{},
		Worktrees:  func(context.Context) ([]worktree.Worktree, error) { return f.wts, nil },
		Orca:       f.orca,
		CursorPath: filepath.Join(root, "cursor.json"),
		Now:        func() time.Time { return f.now },
	}
	return f
}

func (f *fixture) transcript(t *testing.T, sid, cwd, uuid string, at time.Time) {
	t.Helper()
	path := filepath.Join(claudenative.ProjectsDir(f.layout.ConfigDir), claudenative.ProjectDirName(cwd), sid+".jsonl")
	line := `{"type":"user","uuid":"` + uuid + `","cwd":"` + cwd + `","origin":{"kind":"human"},"timestamp":"` + at.Format(time.RFC3339) + `","message":{"content":"hi"}}` + "\n"
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600) //nolint:gosec // G304: a fixture under t.TempDir.
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	if _, err := file.WriteString(line); err != nil {
		t.Fatal(err)
	}
}

func mkdir(t *testing.T, dirs ...string) {
	t.Helper()
	for _, d := range dirs {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
}

func write(t *testing.T, path, content string) {
	t.Helper()
	mkdir(t, filepath.Dir(path))
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func scan(t *testing.T, inv *Inventory) []scheduler.Unit {
	t.Helper()
	units, err := inv.Scan(context.Background())
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	return units
}

func stamps(units []scheduler.Unit) map[string]string {
	out := map[string]string{}
	for _, u := range units {
		out[u.WorktreeID] = u.MetaStamp
	}
	return out
}

func sessionIDs(u scheduler.Unit) []string {
	ids := make([]string, len(u.Sessions))
	for i, s := range u.Sessions {
		ids[i] = s.ID
	}
	return ids
}

func TestScanGroupsRegisteredSessions(t *testing.T) {
	f := newFixture(t)
	focus := t0.Add(-30 * time.Second)
	f.orca.activity = []orcabridge.WorkspaceActivity{{WorktreeID: "orca-2", Path: filepath.Join(f.root, "src/r2"), LastHumanFocusAt: focus}}
	inv, err := New(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	units := scan(t, inv)
	if len(units) != 2 || units[0].WorktreeID != idR1 || units[1].WorktreeID != idR2 {
		t.Fatalf("units = %+v, want r1 and r2", units)
	}
	if got := sessionIDs(units[0]); !slices.Equal(got, []string{sidA, sidB}) || units[0].RepoKey != f.wts[0].CommonDir {
		t.Fatalf("r1 unit sessions %v repo %s", got, units[0].RepoKey)
	}
	c := units[1].Sessions
	if len(c) != 1 || c[0].ID != sidC || !c[0].LastHumanFocus.Equal(focus) || !c[0].LastHumanInput.Equal(t0.Add(-time.Minute)) {
		t.Fatalf("r2 unit sessions = %+v, want %s focused at %s", c, sidC, focus)
	}
	if !units[0].Sessions[0].LastHumanFocus.IsZero() {
		t.Fatalf("r1 session focus = %s, want none", units[0].Sessions[0].LastHumanFocus)
	}
	target, ok := inv.Target(idR1)
	if !ok || len(target.Sessions) != 2 || target.Worktree.ID != idR1 {
		t.Fatalf("Target(r1) = %+v, %v", target, ok)
	}
	b := target.Sessions[1]
	wantRepo := claudenative.RepoBinding{
		Origin: "https://example.com/r1.git", Relpath: "r1", RegistryPath: f.wts[0].Root,
		CheckoutRoot: f.wts[0].Root, WorktreeRel: "sub",
	}
	if b.Repo == nil || *b.Repo != wantRepo {
		t.Fatalf("session B repo = %+v, want %+v", b.Repo, wantRepo)
	}
	for _, id := range []string{idR1, idR2} {
		tg, _ := inv.Target(id)
		for _, s := range tg.Sessions {
			if string(s.ID) == sidD {
				t.Fatalf("unprotected session %s grouped under %s", sidD, id)
			}
		}
	}
}

func TestScanMetaStamps(t *testing.T) {
	tests := []struct {
		name    string
		change  func(t *testing.T, f *fixture)
		changed []string
	}{
		{"unchanged", func(*testing.T, *fixture) {}, nil},
		{"transcript append", func(t *testing.T, f *fixture) {
			f.transcript(t, sidA, filepath.Join(f.root, "src/r1"), "u-more", t0)
		}, []string{idR1}},
		{"index rewrite", func(t *testing.T, f *fixture) {
			write(t, filepath.Join(f.wts[1].GitDir, "index"), "index v2")
		}, []string{idR2}},
		{"head move", func(t *testing.T, f *fixture) {
			write(t, filepath.Join(f.wts[0].GitDir, "HEAD"), "ref: refs/heads/feature\n")
		}, []string{idR1}},
		{"unregistered session", func(t *testing.T, f *fixture) {
			f.transcript(t, sidD, filepath.Join(f.root, "elsewhere"), "u-more", t0)
		}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			inv, err := New(f.cfg)
			if err != nil {
				t.Fatal(err)
			}
			before := stamps(scan(t, inv))
			tt.change(t, f)
			after := stamps(scan(t, inv))
			var changed []string
			for _, id := range []string{idR1, idR2} {
				if before[id] != after[id] {
					changed = append(changed, id)
				}
			}
			if !slices.Equal(changed, tt.changed) {
				t.Fatalf("changed stamps = %v, want %v", changed, tt.changed)
			}
		})
	}
}

func TestScanResumesFromPersistedCursor(t *testing.T) {
	f := newFixture(t)
	inv, err := New(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	first := stamps(scan(t, inv))
	resumed, err := New(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got := stamps(scan(t, resumed)); got[idR1] != first[idR1] || got[idR2] != first[idR2] {
		t.Fatalf("resumed stamps = %v, want %v", got, first)
	}
	target, ok := resumed.Target(idR1)
	if !ok || target.Sessions[0].Title != "" || !target.Sessions[0].LastHumanInput.Equal(t0.Add(-time.Minute)) {
		t.Fatalf("resumed target = %+v, %v", target, ok)
	}
}

func TestScanOrcaActivity(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		advance time.Duration
		calls   int
		wantErr bool
	}{
		{name: "cached within interval", advance: DefaultOrcaInterval - time.Second, calls: 1},
		{name: "refreshed after interval", advance: DefaultOrcaInterval, calls: 2},
		{name: "unavailable", err: &orcabridge.UnavailableError{Reason: orcabridge.ReasonNotInstalled}, advance: DefaultOrcaInterval, calls: 2},
		{name: "unexpected failure", err: errors.New("boom"), wantErr: true, calls: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			f.orca.err = tt.err
			inv, err := New(f.cfg)
			if err != nil {
				t.Fatal(err)
			}
			_, err = inv.Scan(context.Background())
			if tt.wantErr {
				if err == nil {
					t.Fatal("Scan succeeded, want the Orca failure")
				}
				if f.orca.calls != tt.calls {
					t.Fatalf("activity calls = %d, want %d", f.orca.calls, tt.calls)
				}
				return
			}
			if err != nil {
				t.Fatalf("Scan: %v", err)
			}
			f.now = f.now.Add(tt.advance)
			scan(t, inv)
			if f.orca.calls != tt.calls {
				t.Fatalf("activity calls = %d, want %d", f.orca.calls, tt.calls)
			}
		})
	}
}

var (
	_ scheduler.Inventory = (*Inventory)(nil)
	_ Activity            = (*orcabridge.Client)(nil)
)
