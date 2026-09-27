package pickup

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-sync/internal/catalog"
	"github.com/yasyf/cc-sync/internal/claudenative"
	"github.com/yasyf/cc-sync/internal/cli"
	"github.com/yasyf/cc-sync/internal/codesnap"
	"github.com/yasyf/cc-sync/internal/consumer"
	"github.com/yasyf/cc-sync/internal/orcabridge"
	"github.com/yasyf/cc-sync/internal/replica"
	"github.com/yasyf/cc-sync/internal/sessionarchive"
	"github.com/yasyf/reposync/worktree"
	"github.com/yasyf/synckit/artifact"
)

const (
	sidHuman = "aaaaaaaa-0000-4000-8000-000000000001"
	sidIdle  = "bbbbbbbb-0000-4000-8000-000000000002"
	sidFork  = "cccccccc-0000-4000-8000-000000000003"
	source   = "/Users/alice/src/app"
)

var (
	now        = time.Date(2026, 9, 26, 15, 0, 0, 0, time.UTC)
	capturedAt = time.Date(2026, 9, 26, 14, 5, 0, 0, time.UTC)
)

type object struct {
	manifest artifact.Manifest
	data     []byte
}

type fakeStore struct {
	objects map[artifact.Digest]object
	missing int
}

func (s *fakeStore) put(media string, data []byte, deps ...artifact.Ref) artifact.Ref {
	key := media + "\x00" + string(data)
	for _, d := range deps {
		key += "\x00" + string(d.Digest)
	}
	ref := artifact.Ref{Digest: artifact.Sum([]byte(key)), Kind: artifact.KindManifest, Size: int64(len(data))}
	s.objects[ref.Digest] = object{manifest: artifact.Manifest{Media: media, Size: ref.Size, Deps: deps}, data: data}
	return ref
}

func (s *fakeStore) Manifest(_ context.Context, ref artifact.Ref) (artifact.Manifest, error) {
	return s.objects[ref.Digest].manifest, nil
}

func (s *fakeStore) Open(_ context.Context, ref artifact.Ref) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(s.objects[ref.Digest].data)), nil
}

func (s *fakeStore) Complete(context.Context, []artifact.Ref) (int, error) { return s.missing, nil }

func (s *fakeStore) Materialize(_ context.Context, ref artifact.Ref, path string, perm os.FileMode) error {
	return os.WriteFile(path, s.objects[ref.Digest].data, perm)
}

type fakeCatalog struct{ snap catalog.Snapshot }

func (c fakeCatalog) Load() (catalog.Snapshot, error) { return c.snap, nil }

type pinCall struct {
	owner string
	roots []artifact.Ref
	ttl   time.Duration
}

type fakePinner struct{ calls []pinCall }

func (p *fakePinner) Pin(_ context.Context, owner string, roots []artifact.Ref, ttl time.Duration) error {
	p.calls = append(p.calls, pinCall{owner, roots, ttl})
	return nil
}

type fakeVerifier struct{ verdict consumer.CodeVerdict }

func (v fakeVerifier) VerifyCode(context.Context, artifact.Ref, worktree.FetchGate) (consumer.CodeVerdict, error) {
	return v.verdict, nil
}

type fakeCode struct {
	restored Restored
	opts     []RestoreOptions
	gated    []bool
	code     []artifact.Ref
	removed  []Restored
}

func (c *fakeCode) Restore(_ context.Context, _ codesnap.Reader, code artifact.Ref, opts RestoreOptions) (Restored, error) {
	c.gated = append(c.gated, opts.FetchLFS != nil)
	opts.FetchLFS = nil
	c.opts, c.code = append(c.opts, opts), append(c.code, code)
	r := c.restored
	if r.Path == "" {
		r.Path = opts.Dest
	}
	return r, nil
}

func (c *fakeCode) Remove(_ context.Context, r Restored) error {
	c.removed = append(c.removed, r)
	return nil
}

type prepareCall struct {
	meta   replica.Meta
	target SessionTarget
	mode   Divergence
}

type fakeSessions struct {
	errs    map[string]error
	forks   map[string]string
	calls   []prepareCall
	applied []string
	onApply func()
}

func (f *fakeSessions) Prepare(_ context.Context, dir string, t SessionTarget, d Divergence) (PreparedSession, error) {
	b, err := os.ReadFile(filepath.Join(dir, replica.MetaFile)) //nolint:gosec // G304: a replica under t.TempDir.
	if err != nil {
		return nil, err
	}
	var meta replica.Meta
	if err := json.Unmarshal(b, &meta); err != nil {
		return nil, err
	}
	f.calls = append(f.calls, prepareCall{meta, t, d})
	if err := f.errs[meta.SessionID]; err != nil {
		return nil, err
	}
	plan := SessionPlan{
		SessionID: claudenative.SessionID(meta.SessionID), SourceSessionID: claudenative.SessionID(meta.SessionID),
		Mode: ModeFresh, RecoveryContext: "recovered " + meta.SessionID,
		Launch: Launch{Argv: []string{"claude", "--resume", meta.SessionID}, Dir: t.Cwd},
	}
	if to, ok := f.forks[meta.SessionID]; ok {
		plan.SessionID, plan.Mode = claudenative.SessionID(to), ModeFork
		plan.Launch.Argv[2] = to
	}
	return &fakePrepared{f: f, plan: plan}, nil
}

type fakePrepared struct {
	f    *fakeSessions
	plan SessionPlan
}

func (p *fakePrepared) Plan() SessionPlan { return p.plan }

func (p *fakePrepared) Apply(ctx context.Context) error {
	if p.f.onApply != nil {
		p.f.onApply()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	p.f.applied = append(p.f.applied, string(p.plan.SessionID))
	return nil
}

type fakeOrca struct {
	result orcabridge.ImportResult
	err    error
	reqs   []orcabridge.ImportRequest
}

func (o *fakeOrca) Import(_ context.Context, req orcabridge.ImportRequest) (orcabridge.ImportResult, error) {
	o.reqs = append(o.reqs, req)
	return o.result, o.err
}

type world struct {
	t        *testing.T
	store    *fakeStore
	pinner   *fakePinner
	code     *fakeCode
	sessions *fakeSessions
	orca     *fakeOrca
	verifier fakeVerifier
	snap     catalog.Snapshot
	root     artifact.Ref
	codeRef  artifact.Ref
	cfg      Config
	phases   []string
}

func sessionManifest(s *fakeStore, sid, cwd string) sessionarchive.Manifest {
	return sessionarchive.Manifest{
		Format: sessionarchive.Format, SessionID: sid, SourceHost: "alice", CapturedAt: capturedAt,
		ConfigDir: "/Users/alice/.claude", Home: "/Users/alice", TmpRoot: "/private/tmp", UID: 501, Cwd: cwd,
		Entries: []sessionarchive.Entry{{
			Root: sessionarchive.RootTranscript, Path: sid + ".jsonl", Mode: 0o600,
			Ref: s.put(sessionarchive.MediaTranscript, []byte(`{"uuid":"leaf-`+sid[:4]+`"}`+"\n")),
		}},
	}
}

func newWorld(t *testing.T) *world {
	t.Helper()
	w := &world{
		t:        t,
		store:    &fakeStore{objects: map[artifact.Digest]object{}},
		pinner:   &fakePinner{},
		code:     &fakeCode{restored: Restored{Branch: "recovery/feature-login"}},
		sessions: &fakeSessions{},
		orca:     &fakeOrca{},
		verifier: fakeVerifier{verdict: consumer.CodeVerdict{Ready: true}},
	}
	w.codeRef = w.store.put(codesnap.MediaCode, nil, w.store.put("cc-sync.code-manifest", []byte("{}")))
	deps := make([]artifact.Ref, 0, 4)
	deps = append(deps, w.codeRef, w.store.put(orcabridge.MediaDescriptor, []byte(`{"version":1}`)))
	for _, m := range []sessionarchive.Manifest{
		sessionManifest(w.store, sidHuman, source+"/web"),
		sessionManifest(w.store, sidIdle, source),
	} {
		b, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		blob := w.store.put(sessionarchive.MediaManifest, b)
		deps = append(deps, w.store.put(sessionarchive.MediaSession, nil, append([]artifact.Ref{blob}, m.Entries[0].Ref)...))
	}
	w.root = w.store.put("cc-sync.checkpoint", nil, deps...)
	sessions := []catalog.Session{
		{ID: sidIdle, LastActivity: capturedAt, LastHumanActivity: capturedAt.Add(-3 * time.Hour)},
		{ID: sidHuman, LastActivity: capturedAt.Add(-time.Minute), LastHumanActivity: capturedAt.Add(-time.Minute)},
	}
	w.snap = catalog.Snapshot{
		Self: "bob",
		Origins: []catalog.Origin{{Origin: "alice", Worktrees: []catalog.Worktree{{
			ID:   "wt-1",
			Repo: catalog.Repo{Origin: "github.com/x/app", RelPath: "github.com/x/app", Branch: "feature/login", SourcePath: source},
			Checkpoints: []catalog.Checkpoint{
				{ID: "cp-mixed", Root: w.root, CapturedAt: capturedAt.Add(30 * time.Minute), Sessions: sessions, Deferred: "partial: big.bin", Classes: []catalog.Class{catalog.ClassLatest}, Completeness: catalog.Completeness{Complete: true}},
				{ID: "cp-good", Root: w.root, CapturedAt: capturedAt, Sessions: sessions, Classes: []catalog.Class{catalog.ClassHourly}, Completeness: catalog.Completeness{Complete: true}},
			},
		}}}},
		Readiness: map[string]catalog.Readiness{"cp-good": {Ready: true}, "cp-mixed": {Ready: true}},
	}
	home := t.TempDir()
	w.cfg = Config{
		Catalog: fakeCatalog{w.snap}, Pinner: w.pinner,
		OpenStore: func(context.Context) (Store, error) { return w.store, nil },
		Verifier:  w.verifier, Code: w.code, Sessions: w.sessions, Orca: w.orca,
		Network: newFakeNetwork(),
		Layout:  claudenative.Layout{ConfigDir: filepath.Join(home, ".claude"), TmpRoot: "/tmp", UID: 502},
		Home:    home, ReplicaRoot: filepath.Join(home, "replicas"), CheckoutRoot: "/co",
		PreferClient: "client-7", Now: func() time.Time { return now },
	}
	return w
}

func (w *world) run(req Request) (Result, error) {
	w.cfg.Catalog = fakeCatalog{w.snap}
	w.cfg.Verifier = w.verifier
	if req.Target == nil {
		req.Target = cli.ItemRef{SourceHostID: "alice", WorkspaceID: "wt-1"}
	}
	req.Progress = func(p cli.Progress) { w.phases = append(w.phases, string(p.Phase)) }
	return New(w.cfg).Run(w.t.Context(), req)
}

const dest = "/co/github.com/x/app/alice-feature-login-20260926-1405"

func launch(sid, dir string) *Launch {
	return &Launch{Argv: []string{"claude", "--resume", sid}, Dir: dir}
}

func (w *world) assertUnpinned() {
	w.t.Helper()
	calls := w.pinner.calls
	if len(calls) != 2 || !strings.HasPrefix(calls[0].owner, PinOwnerPrefix) || calls[1].owner != calls[0].owner ||
		!reflect.DeepEqual(calls[0].roots, []artifact.Ref{w.root}) || calls[0].ttl != PinTTL || calls[1].roots != nil {
		w.t.Errorf("pins = %+v, want pin of the root for %v then unpin", calls, PinTTL)
	}
}

func TestPickupWithOrca(t *testing.T) {
	w := newWorld(t)
	w.orca.result = orcabridge.ImportResult{WorktreeID: "orca-wt", Bindings: []orcabridge.ImportedBinding{
		{Binding: orcabridge.RecoveryBindingKey{Agent: orcabridge.AgentClaude, Key: "session_id", ID: sidHuman}, LocalPaneKey: "tab-9:leaf-1", Status: "resumed"},
		{Binding: orcabridge.RecoveryBindingKey{Agent: orcabridge.AgentClaude, Key: "session_id", ID: sidIdle}, LocalPaneKey: "tab-3:leaf-2", Status: "dormant"},
	}}
	res, err := w.run(Request{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	want := Result{
		Checkpoint: Checkpoint{ID: "cp-good", CapturedAt: capturedAt},
		Checkout:   Checkout{Path: dest, Branch: "recovery/feature-login"},
		Sessions: []Session{
			{SessionID: sidHuman, Status: StatusResumed, Selected: true},
			{SessionID: sidIdle, Status: StatusDormant, Launch: launch(sidIdle, dest)},
		},
		Orca: &OrcaResult{
			ExecutionHostID: "local", WorktreeID: "orca-wt",
			Resumed: []ResumedTab{{SessionID: sidHuman, TabID: "tab-9"}}, Dormant: []string{sidIdle},
		},
	}
	if !reflect.DeepEqual(res, want) {
		t.Errorf("result = %+v\nwant %+v", res, want)
	}
	if want := []RestoreOptions{{Dest: dest}}; !reflect.DeepEqual(w.code.opts, want) || !reflect.DeepEqual(w.code.gated, []bool{true}) || w.code.code[0] != w.codeRef {
		t.Errorf("restore = %+v (LFS fetch gated %v) of %v, want %+v gated by network policy of the code group", w.code.opts, w.code.gated, w.code.code, want)
	}
	if !reflect.DeepEqual(w.sessions.applied, []string{sidHuman, sidIdle}) {
		t.Errorf("applied = %v, want both sessions", w.sessions.applied)
	}
	c := w.sessions.calls[0]
	wantTarget := SessionTarget{Layout: w.cfg.Layout, Home: w.cfg.Home, Cwd: dest + "/web", Checkouts: PathMap{{From: source, To: dest}}}
	if !reflect.DeepEqual(c.target, wantTarget) || c.mode != DivergenceRefuse || c.meta.CheckpointID != "cp-good" ||
		c.meta.SourceTmpRoot != "/private/tmp" || c.meta.LeafUUID != "leaf-aaaa" {
		t.Errorf("prepare = %+v, want target %+v, refuse, and replica meta of cp-good", c, wantTarget)
	}
	dst := claudenative.ProjectsDir(w.cfg.Layout.ConfigDir)
	wantImport := orcabridge.ImportRequest{
		Descriptor: []byte(`{"version":1}`), Checkout: dest, CheckpointID: "cp-good",
		PathMap: []orcabridge.PathMapping{
			{From: "/Users/alice/.claude/projects/" + claudenative.ProjectDirName(source), To: filepath.Join(dst, claudenative.ProjectDirName(dest))},
			{From: "/Users/alice/.claude/projects/" + claudenative.ProjectDirName(source+"/web"), To: filepath.Join(dst, claudenative.ProjectDirName(dest+"/web"))},
			{From: source, To: dest},
			{From: "/private/tmp/claude-501", To: "/tmp/claude-502"},
		},
		Resume: []orcabridge.BindingSelector{sidHuman}, PreferClient: "client-7", RegisterRepo: true,
		RecoveryLaunch: map[string]orcabridge.RecoveryLaunch{
			sidHuman: {AppendSystemPrompt: "recovered " + sidHuman},
			sidIdle:  {AppendSystemPrompt: "recovered " + sidIdle},
		},
	}
	if len(w.orca.reqs) != 1 || !reflect.DeepEqual(w.orca.reqs[0], wantImport) {
		t.Errorf("import = %+v\nwant %+v", w.orca.reqs, wantImport)
	}
	wantPhases := []string{"select", "restore-code", "restore-sessions", "restore-sessions", "restore-sessions", "orca-import", "orca-resume"}
	if !reflect.DeepEqual(w.phases, wantPhases) {
		t.Errorf("phases = %v, want %v", w.phases, wantPhases)
	}
	w.assertUnpinned()
}

func TestPickupWithoutOrca(t *testing.T) {
	tests := []struct {
		name   string
		noOrca bool
		orca   Orca
		err    error
	}{
		{"no-orca flag", true, &fakeOrca{}, nil},
		{"orca absent", false, nil, nil},
		{"orca unavailable", false, &fakeOrca{}, &orcabridge.UnavailableError{Reason: orcabridge.ReasonNotRunning, Detail: "down"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := newWorld(t)
			w.cfg.Orca = tt.orca
			if o, ok := tt.orca.(*fakeOrca); ok {
				o.err = tt.err
			}
			res, err := w.run(Request{NoOrca: tt.noOrca})
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			want := []Session{
				{SessionID: sidHuman, Status: StatusRestored, Selected: true, Launch: launch(sidHuman, dest+"/web")},
				{SessionID: sidIdle, Status: StatusRestored, Launch: launch(sidIdle, dest)},
			}
			if res.Orca != nil || !reflect.DeepEqual(res.Sessions, want) {
				t.Errorf("result orca %+v sessions %+v, want no orca and %+v", res.Orca, res.Sessions, want)
			}
			if o, ok := tt.orca.(*fakeOrca); ok && tt.noOrca && len(o.reqs) != 0 {
				t.Errorf("--no-orca still imported: %+v", o.reqs)
			}
			w.assertUnpinned()
		})
	}
}

func TestDivergence(t *testing.T) {
	divergent := &DivergentLocalError{SessionID: sidHuman, LocalPath: "/x.jsonl"}
	tests := []struct {
		mode     Divergence
		errs     map[string]error
		forks    map[string]string
		wantErr  bool
		wantPass Divergence
	}{
		{mode: "", errs: map[string]error{sidHuman: divergent}, wantErr: true, wantPass: DivergenceRefuse},
		{mode: DivergenceKeepLocal, wantPass: DivergenceKeepLocal},
		{mode: DivergenceReplace, wantPass: DivergenceReplace},
		{mode: DivergenceFork, forks: map[string]string{sidHuman: sidFork}, wantPass: DivergenceFork},
	}
	for _, tt := range tests {
		t.Run(string(orRefuse(tt.mode)), func(t *testing.T) {
			w := newWorld(t)
			w.sessions.errs, w.sessions.forks = tt.errs, tt.forks
			w.orca.result = orcabridge.ImportResult{WorktreeID: "orca-wt"}
			res, err := w.run(Request{OnDivergence: tt.mode})
			for _, c := range w.sessions.calls {
				if c.mode != tt.wantPass {
					t.Errorf("prepare mode = %q, want %q", c.mode, tt.wantPass)
				}
			}
			if tt.wantErr {
				var got *DivergentLocalError
				if !errors.As(err, &got) || got != divergent {
					t.Fatalf("Run = %v, want the divergent-local error", err)
				}
				if len(w.sessions.applied) != 0 || len(w.orca.reqs) != 0 || len(w.code.removed) != 1 {
					t.Errorf("applied %v, imported %d, removed %d; want nothing applied and the fresh checkout removed",
						w.sessions.applied, len(w.orca.reqs), len(w.code.removed))
				}
				w.assertUnpinned()
				return
			}
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			if tt.forks == nil {
				return
			}
			if res.Sessions[0].SessionID != sidFork || res.Sessions[0].ForkedFrom != sidHuman {
				t.Errorf("fork session = %+v, want %s forked from %s", res.Sessions[0], sidFork, sidHuman)
			}
			req := w.orca.reqs[0]
			if !reflect.DeepEqual(req.Resume, []orcabridge.BindingSelector{sidFork}) ||
				!reflect.DeepEqual(req.SessionIDMap, []orcabridge.SessionMapping{{From: sidHuman, To: sidFork}}) {
				t.Errorf("import resume %v map %v, want the local fork id and its mapping", req.Resume, req.SessionIDMap)
			}
		})
	}
}

func TestLiveRefusal(t *testing.T) {
	live := errors.Join(ErrLiveLocal, errors.New("pid 42"))
	t.Run("selected", func(t *testing.T) {
		w := newWorld(t)
		w.sessions.errs = map[string]error{sidHuman: live}
		if _, err := w.run(Request{}); !errors.Is(err, ErrLiveLocal) {
			t.Fatalf("Run = %v, want ErrLiveLocal", err)
		}
		if len(w.code.removed) != 1 || len(w.sessions.applied) != 0 {
			t.Errorf("removed %d applied %v, want the fresh checkout removed and nothing applied", len(w.code.removed), w.sessions.applied)
		}
		w.assertUnpinned()
	})
	t.Run("not selected", func(t *testing.T) {
		w := newWorld(t)
		w.sessions.errs = map[string]error{sidIdle: live}
		w.orca.result = orcabridge.ImportResult{WorktreeID: "orca-wt", Bindings: []orcabridge.ImportedBinding{
			{Binding: orcabridge.RecoveryBindingKey{Agent: orcabridge.AgentClaude, Key: "session_id", ID: sidHuman}, LocalPaneKey: "tab-1:leaf-1", Status: "resumed"},
		}}
		res, err := w.run(Request{})
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		want := []Session{
			{SessionID: sidHuman, Status: StatusResumed, Selected: true},
			{SessionID: sidIdle, Status: StatusRefused, Reason: ReasonLiveLocal},
		}
		if !reflect.DeepEqual(res.Sessions, want) || !reflect.DeepEqual(w.sessions.applied, []string{sidHuman}) {
			t.Errorf("sessions %+v applied %v, want %+v with only the selected applied", res.Sessions, w.sessions.applied, want)
		}
	})
}

func TestNotReady(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*world)
		missing []string
		pinned  bool
	}{
		{"catalog readiness", func(w *world) {
			wt := &w.snap.Origins[0].Worktrees[0]
			wt.Checkpoints = wt.Checkpoints[1:]
			w.snap.Readiness["cp-good"] = catalog.Readiness{Missing: []string{"closure"}}
		}, []string{"closure"}, false},
		{"closure", func(w *world) { w.store.missing = 3 }, []string{"3 artifacts"}, true},
		{"verify", func(w *world) {
			w.verifier.verdict = consumer.CodeVerdict{Missing: []string{"prerequisites-missing"}}
		}, []string{"prerequisites-missing"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := newWorld(t)
			tt.mutate(w)
			_, err := w.run(Request{})
			var nr *NotReadyError
			if !errors.As(err, &nr) || nr.CheckpointID != "cp-good" || !reflect.DeepEqual(nr.Missing, tt.missing) {
				t.Fatalf("Run = %v, want not-ready cp-good missing %v", err, tt.missing)
			}
			if len(w.code.opts) != 0 {
				t.Errorf("restored code for a not-ready checkpoint")
			}
			if tt.pinned {
				w.assertUnpinned()
			} else if len(w.pinner.calls) != 0 {
				t.Errorf("pinned %+v before readiness", w.pinner.calls)
			}
		})
	}
}

func TestCancelCleanup(t *testing.T) {
	for _, reused := range []bool{false, true} {
		t.Run(map[bool]string{false: "fresh", true: "reused"}[reused], func(t *testing.T) {
			w := newWorld(t)
			w.code.restored.Reused = reused
			ctx, cancel := context.WithCancel(t.Context())
			w.sessions.onApply = cancel
			w.cfg.Catalog = fakeCatalog{w.snap}
			_, err := New(w.cfg).Run(ctx, Request{Target: cli.ItemRef{SourceHostID: "alice", WorkspaceID: "wt-1"}})
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Run = %v, want context.Canceled", err)
			}
			if wantRemoved := map[bool]int{false: 1, true: 0}[reused]; len(w.code.removed) != wantRemoved || len(w.orca.reqs) != 0 {
				t.Errorf("removed %d imported %d, want %d removed and no import", len(w.code.removed), len(w.orca.reqs), wantRemoved)
			}
			w.assertUnpinned()
		})
	}
}

func TestSiblingReuseReportsNewer(t *testing.T) {
	w := newWorld(t)
	w.code.restored = Restored{Path: "/co/sibling", Branch: "recovery/old", Reused: true, Newer: true, LFSPending: []string{"art/big.psd"}}
	res, err := w.run(Request{NoOrca: true})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	want := Checkout{Path: "/co/sibling", Branch: "recovery/old", Reused: true, Newer: true, LFSPending: []string{"art/big.psd"}}
	if !reflect.DeepEqual(res.Checkout, want) || res.Sessions[0].Launch.Dir != "/co/sibling/web" {
		t.Errorf("checkout %+v launch %+v, want %+v with sessions relocated into the sibling", res.Checkout, res.Sessions[0].Launch, want)
	}
}

func TestDeferredCode(t *testing.T) {
	t.Run("default skips the mixed checkpoint", func(t *testing.T) {
		w := newWorld(t)
		res, err := w.run(Request{NoOrca: true})
		if err != nil || res.Checkpoint != (Checkpoint{ID: "cp-good", CapturedAt: capturedAt}) {
			t.Fatalf("Run = %+v, %v; want the complete cp-good", res.Checkpoint, err)
		}
	})
	t.Run("default picks the retained complete checkpoint under a newer mixed one", func(t *testing.T) {
		w := newWorld(t)
		wt := &w.snap.Origins[0].Worktrees[0]
		for i := range wt.Checkpoints {
			wt.Checkpoints[i].Classes, wt.Checkpoints[i].ExpiresAt = nil, now.Add(24*time.Hour)
		}
		wt.Checkpoints = catalog.Retain(wt.Checkpoints, now)
		if len(wt.Checkpoints) != 2 {
			t.Fatalf("retained %d checkpoints, want the mixed one and the complete one", len(wt.Checkpoints))
		}
		res, err := w.run(Request{NoOrca: true})
		if err != nil || res.Checkpoint != (Checkpoint{ID: "cp-good", CapturedAt: capturedAt}) {
			t.Fatalf("Run = %+v, %v; want the complete cp-good", res.Checkpoint, err)
		}
	})
	t.Run("explicit mixed without allow-partial", func(t *testing.T) {
		w := newWorld(t)
		_, err := w.run(Request{Checkpoint: cli.CheckpointID{Prefix: "cp-mix"}})
		var nr *NotReadyError
		if !errors.As(err, &nr) || !reflect.DeepEqual(nr.Missing, []string{"code deferred: partial: big.bin", "incomplete"}) {
			t.Fatalf("Run = %v, want not-ready naming the deferred code", err)
		}
	})
	t.Run("explicit mixed with allow-partial", func(t *testing.T) {
		w := newWorld(t)
		res, err := w.run(Request{Checkpoint: cli.CheckpointID{Prefix: "cp-mix"}, AllowPartial: true, NoOrca: true})
		want := Checkpoint{ID: "cp-mixed", CapturedAt: capturedAt.Add(30 * time.Minute), Partial: true, CodeDeferred: "partial: big.bin"}
		if err != nil || res.Checkpoint != want {
			t.Fatalf("Run = %+v, %v; want %+v", res.Checkpoint, err, want)
		}
	})
}

func TestDefaultSkipsCheckpointsNotPickupReady(t *testing.T) {
	tests := []struct {
		name     string
		mutate   func(*world, *catalog.Checkpoint)
		explicit []string
		partial  *Checkpoint
	}{
		{
			name: "session-incomplete",
			mutate: func(_ *world, cp *catalog.Checkpoint) {
				cp.Completeness = catalog.Completeness{Missing: []string{"session:" + sidHuman}}
			},
			explicit: []string{"incomplete"},
			partial:  &Checkpoint{ID: "cp-newer", CapturedAt: capturedAt.Add(30 * time.Minute), Partial: true},
		},
		{
			name: "complete but unverified",
			mutate: func(w *world, _ *catalog.Checkpoint) {
				w.snap.Readiness["cp-newer"] = catalog.Readiness{Missing: []string{"prerequisites-missing"}}
			},
			explicit: []string{"prerequisites-missing"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := newWorld(t)
			newer := &w.snap.Origins[0].Worktrees[0].Checkpoints[0]
			newer.ID, newer.Deferred = "cp-newer", ""
			w.snap.Readiness["cp-newer"] = catalog.Readiness{Ready: true}
			tt.mutate(w, newer)
			res, err := w.run(Request{NoOrca: true})
			if err != nil || res.Checkpoint != (Checkpoint{ID: "cp-good", CapturedAt: capturedAt}) {
				t.Fatalf("default Run = %+v, %v; want the older complete cp-good", res.Checkpoint, err)
			}
			_, err = w.run(Request{Checkpoint: cli.CheckpointID{Prefix: "cp-newer"}, NoOrca: true})
			var nr *NotReadyError
			if !errors.As(err, &nr) || nr.CheckpointID != "cp-newer" || !reflect.DeepEqual(nr.Missing, tt.explicit) {
				t.Fatalf("explicit Run = %v, want not-ready cp-newer missing %v", err, tt.explicit)
			}
			res, err = w.run(Request{Checkpoint: cli.CheckpointID{Prefix: "cp-newer"}, AllowPartial: true, NoOrca: true})
			if tt.partial == nil {
				if !errors.As(err, &nr) || nr.CheckpointID != "cp-newer" || !reflect.DeepEqual(nr.Missing, tt.explicit) {
					t.Fatalf("allow-partial Run = %v, want not-ready cp-newer missing %v", err, tt.explicit)
				}
				return
			}
			if err != nil || res.Checkpoint != *tt.partial {
				t.Fatalf("allow-partial Run = %+v, %v; want %+v", res.Checkpoint, err, *tt.partial)
			}
		})
	}
}

func TestSelection(t *testing.T) {
	tests := []struct {
		name       string
		req        Request
		checkpoint string
		resume     []orcabridge.BindingSelector
		err        error
	}{
		{"session target resumes it", Request{Target: cli.SessionRef{ID: "bbbb"}}, "cp-good", []orcabridge.BindingSelector{sidIdle}, nil},
		{"source-qualified session", Request{Target: cli.SessionRef{Source: "alice", ID: sidHuman}}, "cp-good", []orcabridge.BindingSelector{sidHuman}, nil},
		{"explicit resume", Request{Resume: []string{"bbbb", "aaaa"}}, "cp-good", []orcabridge.BindingSelector{sidHuman, sidIdle}, nil},
		{"hourly", Request{Checkpoint: cli.CheckpointHourly{HoursAgo: 0}}, "cp-good", []orcabridge.BindingSelector{sidHuman}, nil},
		{"at before capture", Request{Checkpoint: cli.CheckpointAt{Time: capturedAt.Add(-time.Second)}}, "", nil, ErrNotFound},
		{"unknown item", Request{Target: cli.ItemRef{SourceHostID: "alice", WorkspaceID: "nope"}}, "", nil, ErrNotFound},
		{"unknown session", Request{Target: cli.SessionRef{ID: "ffff"}}, "", nil, ErrNotFound},
		{"ambiguous session", Request{Target: cli.SessionRef{ID: ""}}, "", nil, ErrAmbiguous},
		{"unknown resume", Request{Resume: []string{"ffff"}}, "", nil, ErrNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := newWorld(t)
			res, err := w.run(tt.req)
			if tt.err != nil {
				if !errors.Is(err, tt.err) {
					t.Fatalf("Run = %v, want %v", err, tt.err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			if res.Checkpoint.ID != tt.checkpoint || !reflect.DeepEqual(w.orca.reqs[0].Resume, tt.resume) {
				t.Errorf("picked %s resuming %v, want %s resuming %v", res.Checkpoint.ID, w.orca.reqs[0].Resume, tt.checkpoint, tt.resume)
			}
		})
	}
}

func TestDryRun(t *testing.T) {
	w := newWorld(t)
	res, err := w.run(Request{DryRun: true})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(w.code.opts) != 0 || len(w.sessions.applied) != 0 || len(w.orca.reqs) != 0 || len(w.sessions.calls) != 2 {
		t.Errorf("dry run restored %d, applied %v, imported %d, prepared %d; want only the two prepares",
			len(w.code.opts), w.sessions.applied, len(w.orca.reqs), len(w.sessions.calls))
	}
	if !reflect.DeepEqual(res.Checkout, Checkout{Path: dest, Branch: "feature/login"}) || res.Sessions[0].Status != StatusRestored {
		t.Errorf("dry run result %+v, want the planned checkout and restored sessions", res)
	}
	w.assertUnpinned()
}
