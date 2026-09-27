package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/yasyf/cc-sync/internal/catalog"
	"github.com/yasyf/cc-sync/internal/claudenative"
	"github.com/yasyf/cc-sync/internal/cli"
	"github.com/yasyf/cc-sync/internal/orcabridge"
	"github.com/yasyf/cc-sync/internal/scheduler"
	"github.com/yasyf/reposync/worktree"
	"github.com/yasyf/synckit/artifact"
	"github.com/yasyf/synckit/delivery"
	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/netpolicy"
	"github.com/yasyf/synckit/syncservice"
)

var update = flag.Bool("update", false, "rewrite golden files")

const (
	self    = "yasyf@studio.local"
	laptop  = "yasyf@laptop.local"
	mini    = "yasyf@mini.local"
	sLive   = "11111111-1111-4111-8111-111111111111"
	sBound  = "22222222-2222-4222-8222-222222222222"
	sHuman  = "33333333-3333-4333-8333-333333333333"
	sDocs   = "44444444-4444-4444-8444-444444444444"
	sOwn    = "55555555-5555-4555-8555-555555555555"
	sLib    = "66666666-6666-4666-8666-666666666666"
	orcaWT  = "orca-wt-1"
	checkIn = "/Users/me/.cc-sync/checkouts/app/laptop-app-20260926-1900"
)

var now = time.Date(2026, 9, 26, 20, 0, 0, 0, time.UTC)

func at(hour, minute int) time.Time {
	return time.Date(2026, 9, 26, hour, minute, 0, 0, time.UTC)
}

func cp(id string, class catalog.Class, captured time.Time, deferred string, sessions ...catalog.Session) catalog.Checkpoint {
	return catalog.Checkpoint{
		ID:               id,
		Root:             artifact.Ref{Digest: artifact.Digest(id), Kind: artifact.KindManifest, Size: 1},
		Classes:          []catalog.Class{class},
		CapturedAt:       captured,
		SourceActivityAt: captured.Add(-time.Minute),
		ExpiresAt:        captured.Add(-time.Minute).Add(catalog.ExpiryWindow),
		Sessions:         sessions,
		Code:             worktree.Summary{CapturedAt: captured.Add(-2 * time.Minute)},
		Deferred:         deferred,
		Completeness:     catalog.Completeness{Complete: true},
	}
}

func session(id, title, activity string, last, human time.Time) catalog.Session {
	return catalog.Session{ID: id, Title: title, LastActivity: last, LastHumanActivity: human, Activity: activity}
}

func fixture() catalog.Snapshot {
	mixed := cp("aaa300", catalog.ClassLatest, at(19, 50), "deferred:missing-lfs",
		session(sHuman, "Wire the service", "human", at(19, 49), at(19, 49)),
		session(sLive, "Tail logs", "autonomous", at(19, 45), time.Time{}))
	mixed.Code = worktree.Summary{CapturedAt: at(19, 10)}
	complete := cp("aaa200", catalog.ClassLatest, at(19, 30),
		"",
		session(sBound, "Refactor picker", "idle", at(18, 0), at(17, 0)),
		session(sHuman, "Wire the service", "human", at(19, 29), at(19, 29)),
		session(sLive, "Tail logs", "autonomous", at(19, 20), time.Time{}))
	complete.Omitted = []catalog.OmittedBinding{{Agent: "codex", Key: "session_id", ID: "019a2c4e-codex", Reason: "agent-not-supported-v1"}}
	hourly := cp("aaa100", catalog.ClassHourly, at(18, 30), "", session(sBound, "Refactor picker", "idle", at(18, 0), at(17, 0)))
	lib := cp("bbb100", catalog.ClassLatest, at(17, 0), "", session(sLib, "", "idle", at(16, 0), time.Time{}))
	lib.Completeness = catalog.Completeness{Missing: []string{"paste-cache/abc"}}
	docs := cp("ccc100", catalog.ClassDaily, at(9, 0), "", session(sDocs, "Docs pass", "human", at(8, 59), at(8, 59)))
	own := cp("ddd100", catalog.ClassLatest, at(19, 55), "", session(sOwn, "Own work", "human", at(19, 54), at(19, 54)))
	own.Code = worktree.Summary{}
	return catalog.Snapshot{
		Self: self,
		Origins: []catalog.Origin{
			{Origin: laptop, Revision: 7, Worktrees: []catalog.Worktree{
				{
					ID:          "wt-app",
					Repo:        catalog.Repo{Origin: "git@github.com:yasyf/app.git", RelPath: "app", Branch: "main", SourcePath: "/Users/yasyf/Code/app"},
					Orca:        &catalog.Orca{Kind: "worktree", Name: "app", InstanceID: "inst-1", Freshness: at(19, 30)},
					Checkpoints: []catalog.Checkpoint{mixed, complete, hourly},
				},
				{
					ID:          "wt-lib",
					Repo:        catalog.Repo{Origin: "https://github.com/yasyf/lib", RelPath: "lib", SourcePath: "/Users/yasyf/Code/lib"},
					Checkpoints: []catalog.Checkpoint{lib},
				},
			}},
			{Origin: mini, Revision: 3, Worktrees: []catalog.Worktree{{
				ID:          "wt-docs",
				Repo:        catalog.Repo{Origin: "github.com/yasyf/docs", RelPath: "docs", Branch: "draft", SourcePath: "/Users/yasyf/docs"},
				Checkpoints: []catalog.Checkpoint{docs},
			}}},
			{Origin: self, Revision: 9, Worktrees: []catalog.Worktree{{
				ID:          "wt-own",
				Repo:        catalog.Repo{Origin: "github.com/yasyf/own", RelPath: "own", SourcePath: "/Users/yasyf/own"},
				Checkpoints: []catalog.Checkpoint{own},
			}}},
		},
		Readiness: map[string]catalog.Readiness{
			"aaa300": {Ready: true},
			"aaa200": {Ready: true},
			"aaa100": {Missing: []string{catalog.MissingClosure}},
			"bbb100": {Missing: []string{catalog.MissingPrerequisites}, Deferred: "fetch-paused"},
			"ccc100": {Ready: true},
		},
	}
}

func cellular(observed time.Time) *netpolicy.State {
	return &netpolicy.State{Status: netpolicy.StatusConnected, Cellular: true, ObservedAt: observed}
}

func statuses() []delivery.PeerStatus {
	return []delivery.PeerStatus{
		{
			ServiceID: serviceID, Peer: laptop, Acked: syncservice.NewRevision(1_790_000_000_000_000), AckedAt: at(19, 0),
			Pending:     &delivery.Pending{ChangeID: "c1", SourceRevision: syncservice.NewRevision(1_790_000_500_000_000), StagedAt: at(19, 35), Roots: 4},
			State:       delivery.StatePaused,
			PauseReason: delivery.PausePeerCellular, PauseSince: at(19, 40),
			PeerNetwork: cellular(at(19, 58)),
		},
	}
}

type fakeCatalog struct{ snap catalog.Snapshot }

func (f fakeCatalog) Load() (catalog.Snapshot, error) { return f.snap, nil }

type fakeDeliveries struct {
	statuses []delivery.PeerStatus
	err      error
}

func (f fakeDeliveries) Status(_ context.Context, id string) ([]delivery.PeerStatus, error) {
	if id != serviceID {
		return nil, errors.New("wrong service " + id)
	}
	return f.statuses, f.err
}

type fakeMesh struct{}

func (fakeMesh) Load() (*hostregistry.Registry, error) {
	return &hostregistry.Registry{Self: self, Hosts: []string{mini, laptop}}, nil
}

type fakeOrca struct{ err error }

func (f fakeOrca) List(context.Context, orcabridge.Selector) ([]orcabridge.DormantBinding, error) {
	return []orcabridge.DormantBinding{{WorktreeID: orcaWT, ProviderSession: orcabridge.ProviderSession{Key: "session_id", ID: sBound}}}, f.err
}

func (f fakeOrca) Activity(context.Context) ([]orcabridge.WorkspaceActivity, error) {
	return []orcabridge.WorkspaceActivity{{WorktreeID: orcaWT, Path: checkIn}}, nil
}

type fakeCheckouts map[string]cli.LocalCheckout

func (f fakeCheckouts) Find(source string, w catalog.Worktree) (*cli.LocalCheckout, error) {
	c, ok := f[source+"/"+w.ID]
	if !ok {
		return nil, nil
	}
	return &c, nil
}

type fakeHelper struct {
	status   HelperStatus
	attempts []scheduler.Attempt
	err      error
	kicked   []string
}

func (f *fakeHelper) Status(context.Context) (HelperStatus, error) { return f.status, f.err }

func (f *fakeHelper) Kick(_ context.Context, ids []string) ([]scheduler.Attempt, error) {
	f.kicked = ids
	return f.attempts, f.err
}

type execCall struct {
	argv []string
	dir  string
	env  []string
}

func newConfig() Config {
	return Config{
		Catalog:    fakeCatalog{fixture()},
		Deliveries: fakeDeliveries{statuses: statuses()},
		Mesh:       fakeMesh{},
		Network: func(context.Context) (netpolicy.State, error) {
			return netpolicy.State{Status: netpolicy.StatusConnected, ObservedAt: at(19, 59)}, nil
		},
		Orca: fakeOrca{},
		Live: func(context.Context) (map[claudenative.SessionID]claudenative.LiveProcess, error) {
			return map[claudenative.SessionID]claudenative.LiveProcess{sLive: {PID: 42, SessionID: sLive, Cwd: "/Users/me/Code/app"}}, nil
		},
		Sessions: func(context.Context, claudenative.SessionID) ([]claudenative.Session, error) { return nil, nil },
		Checkouts: fakeCheckouts{
			mini + "/wt-docs": {Path: "/Users/me/.cc-sync/checkouts/docs/mini-docs-20260925-0900", Reusable: false},
		},
		Helper: &fakeHelper{status: HelperStatus{Build: "v0.1.0", Scheduler: scheduler.Status{
			QueuedByTier: scheduler.TierCounts{Human: 1, Idle: 2}, Workers: 1, LastRoundAt: at(19, 59),
		}}},
		Tiers: func() (cli.CaptureTiers, error) {
			return cli.CaptureTiers{HumanInterval: cli.Duration(2 * time.Minute), IdleInterval: cli.Duration(time.Hour)}, nil
		},
		Environ: []string{"HOME=/Users/me"},
		Now:     func() time.Time { return now },
	}
}

func run(t *testing.T, svc cli.Service, args ...string) (int, string, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := cli.New(svc, cli.Terminal{}).Run(context.Background(), args, &stdout, &stderr)
	return code, stdout.String(), stderr.String()
}

func golden(t *testing.T, name, got string) {
	t.Helper()
	path := filepath.Join("testdata", name)
	if *update {
		if err := os.WriteFile(path, []byte(got), 0o600); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := fs.ReadFile(os.DirFS("testdata"), name)
	if err != nil {
		t.Fatal(err)
	}
	if got != string(want) {
		t.Errorf("%s mismatch:\n--- got\n%s\n--- want\n%s", name, got, want)
	}
}

func TestJSONGolden(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{"list", []string{"list", "--json"}},
		{"list_all", []string{"list", "--all", "--json"}},
		{"list_own", []string{"list", "--all", "--source", "studio", "--json"}},
		{"list_repo", []string{"list", "--all", "--repo", "github.com/yasyf/docs", "--json"}},
		{"inspect", []string{"inspect", laptop + "/wt-app", "--json"}},
		{"inspect_session_hourly", []string{"inspect", "2222", "--checkpoint", "hourly:-1h", "--json"}},
		{"inspect_not_ready", []string{"inspect", laptop + "/wt-lib", "--json"}},
		{"status", []string{"status", "--json"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, stdout, stderr := run(t, New(newConfig()), tt.args...)
			if code != cli.ExitOK {
				t.Fatalf("exit %d, stderr %q, stdout %q", code, stderr, stdout)
			}
			golden(t, tt.name+".json", stdout)
		})
	}
}

func TestPauseReasons(t *testing.T) {
	since := at(19, 40)
	tests := []struct {
		reason   delivery.PauseReason
		want     cli.PauseReason
		endpoint cli.Endpoint
	}{
		{delivery.PauseLocalDisconnected, cli.PauseDisconnected, cli.EndpointLocal},
		{delivery.PauseLocalUnknown, cli.PauseUnknownNetwork, cli.EndpointLocal},
		{delivery.PauseLocalCellular, cli.PauseCellular, cli.EndpointLocal},
		{delivery.PauseLocalExpensive, cli.PauseExpensive, cli.EndpointLocal},
		{delivery.PauseLocalConstrained, cli.PauseConstrained, cli.EndpointLocal},
		{delivery.PauseLocalManualMetered, cli.PauseManualMetered, cli.EndpointLocal},
		{delivery.PausePeerDisconnected, cli.PauseDisconnected, cli.EndpointPeer},
		{delivery.PausePeerUnknown, cli.PauseUnknownNetwork, cli.EndpointPeer},
		{delivery.PausePeerCellular, cli.PauseCellular, cli.EndpointPeer},
		{delivery.PausePeerExpensive, cli.PauseExpensive, cli.EndpointPeer},
		{delivery.PausePeerConstrained, cli.PauseConstrained, cli.EndpointPeer},
		{delivery.PausePeerManualMetered, cli.PauseManualMetered, cli.EndpointPeer},
		{delivery.PausePeerUnreachable, cli.PausePeerOffline, cli.EndpointPeer},
		{delivery.PausePeerIncompatible, cli.PauseIncompatible, cli.EndpointPeer},
	}
	if len(tests) != len(pauseCauses) {
		t.Fatalf("table covers %d reasons, mapping has %d", len(tests), len(pauseCauses))
	}
	for _, tt := range tests {
		t.Run(string(tt.reason), func(t *testing.T) {
			cfg := newConfig()
			st := statuses()
			st[0].PauseReason = tt.reason
			if tt.reason == delivery.PausePeerUnreachable {
				st[0].PeerNetwork = nil
			}
			cfg.Deliveries = fakeDeliveries{statuses: st}
			res, err := New(cfg).Status(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			got := res.Peers[0]
			want := cli.Pause{Reason: tt.want, Endpoint: tt.endpoint, Since: cli.At(since)}
			if got.HostID != laptop || got.Pause == nil || *got.Pause != want {
				t.Fatalf("peer %s pause = %+v, want %+v", got.HostID, got.Pause, want)
			}
			if wantReachable := tt.reason != delivery.PausePeerUnreachable; got.Reachable != wantReachable {
				t.Errorf("reachable = %v, want %v", got.Reachable, wantReachable)
			}
			item, err := New(cfg).Inspect(context.Background(), cli.InspectRequest{Target: cli.ItemRef{SourceHostID: laptop, WorkspaceID: "wt-app"}, Checkpoint: cli.LatestCheckpoint{}})
			if err != nil {
				t.Fatal(err)
			}
			if item.Pause == nil || *item.Pause != want {
				t.Errorf("item pause = %+v, want %+v", item.Pause, want)
			}
		})
	}
	if _, err := pauseFor("peer-sleeping", since); err == nil {
		t.Error("unknown pause reason mapped without error")
	}
}

func TestItemPauseFromLocalNetwork(t *testing.T) {
	tests := []struct {
		name  string
		state netpolicy.State
		want  *cli.Pause
	}{
		{"unrestricted", netpolicy.State{Status: netpolicy.StatusConnected, ObservedAt: at(19, 59)}, nil},
		{"cellular", *cellular(at(19, 58)), &cli.Pause{Reason: cli.PauseCellular, Endpoint: cli.EndpointLocal, Since: cli.At(at(19, 58))}},
		{"metered", netpolicy.State{Status: netpolicy.StatusConnected, ManualMetered: true, ObservedAt: at(19, 57)}, &cli.Pause{Reason: cli.PauseManualMetered, Endpoint: cli.EndpointLocal, Since: cli.At(at(19, 57))}},
		{"unknown", netpolicy.State{}, &cli.Pause{Reason: cli.PauseUnknownNetwork, Endpoint: cli.EndpointLocal, Since: cli.At(time.Time{})}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := newConfig()
			cfg.Deliveries = fakeDeliveries{}
			cfg.Network = func(context.Context) (netpolicy.State, error) { return tt.state, nil }
			res, err := New(cfg).List(context.Background(), cli.ListRequest{Source: "mini"})
			if err != nil {
				t.Fatal(err)
			}
			if len(res.Items) != 1 {
				t.Fatalf("items = %d, want 1", len(res.Items))
			}
			got := res.Items[0]
			if got.Source.HostID != mini || got.Source.Reachable || got.Source.LastSeenAt != nil {
				t.Errorf("source = %+v, want unreachable relayed %s", got.Source, mini)
			}
			if (got.Pause == nil) != (tt.want == nil) || (got.Pause != nil && *got.Pause != *tt.want) {
				t.Errorf("pause = %+v, want %+v", got.Pause, tt.want)
			}
		})
	}
}

func TestDegradedDependencies(t *testing.T) {
	cfg := newConfig()
	cfg.Deliveries = fakeDeliveries{err: ErrUnavailable}
	cfg.Orca = fakeOrca{err: &orcabridge.UnavailableError{Reason: orcabridge.ReasonNotRunning}}
	res, err := New(cfg).List(context.Background(), cli.ListRequest{All: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range res.Items {
		if item.Source.Reachable || item.Pause != nil {
			t.Errorf("%s: source %+v pause %+v without delivery status", item.Ref(), item.Source, item.Pause)
		}
		for _, s := range item.Sessions {
			if s.BoundInOrca {
				t.Errorf("%s: session %s bound without Orca", item.Ref(), s.SessionID)
			}
		}
	}
	st, err := New(cfg).Status(context.Background())
	if err != nil {
		t.Fatalf("status without synckitd: %v", err)
	}
	if st.Delivery != (cli.DeliveryService{Reason: "unavailable"}) || !st.Helper.Running || st.Local.Network.Status != cli.NetworkConnected || st.Scheduler.Workers != 1 || st.Scheduler.Tiers.IdleInterval != cli.Duration(time.Hour) {
		t.Errorf("status without synckitd: delivery %+v helper %+v network %+v scheduler %+v", st.Delivery, st.Helper, st.Local.Network, st.Scheduler)
	}
	for _, p := range st.Peers {
		if p.Reachable || p.LastSeenAt != nil || p.AckedRevision != nil || p.Pause != nil {
			t.Errorf("peer %s carries delivery state without synckitd: %+v", p.HostID, p)
		}
	}
	cfg = newConfig()
	cfg.Helper = &fakeHelper{err: ErrUnavailable}
	st, err = New(cfg).Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if st.Helper.Running || st.Scheduler.Workers != 0 || st.Scheduler.LastRoundAt != nil || st.Scheduler.Tiers.IdleInterval != cli.Duration(time.Hour) {
		t.Errorf("helper down: %+v %+v", st.Helper, st.Scheduler)
	}
	cfg.Orca = fakeOrca{err: errors.New("orca crashed")}
	if _, err := New(cfg).List(context.Background(), cli.ListRequest{}); err == nil {
		t.Error("unexpected orca failure swallowed")
	}
}

func TestStatusDelivery(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want cli.DeliveryService
	}{
		{"available", nil, cli.DeliveryService{Available: true}},
		{"not running", fmt.Errorf("%w: synckitd is not running: dial refused", ErrUnavailable), cli.DeliveryService{Reason: "unavailable: synckitd is not running: dial refused"}},
		{"too old", ErrSynckitdTooOld, cli.DeliveryService{Reason: "synckitd too old; upgrade synckit"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := newConfig()
			cfg.Deliveries = fakeDeliveries{statuses: statuses(), err: tt.err}
			code, stdout, stderr := run(t, New(cfg), "status", "--json")
			if code != cli.ExitOK {
				t.Fatalf("exit %d, stderr %q, stdout %q", code, stderr, stdout)
			}
			var got struct {
				Delivery cli.DeliveryService `json:"delivery"`
				Helper   cli.Helper          `json:"helper"`
			}
			if err := json.Unmarshal([]byte(stdout), &got); err != nil {
				t.Fatal(err)
			}
			if got.Delivery != tt.want || !got.Helper.Running {
				t.Errorf("delivery %+v helper %+v, want %+v and a running helper", got.Delivery, got.Helper, tt.want)
			}
		})
	}
	cfg := newConfig()
	cfg.Deliveries = fakeDeliveries{err: errors.New("decode reply: bad frame")}
	if _, err := New(cfg).Status(context.Background()); err == nil || cli.Classify(err) != cli.CodeInternal {
		t.Errorf("status on a delivery failure = %v, want an internal error", err)
	}
}

type failingInstaller struct{ err error }

func (f failingInstaller) Install(context.Context, cli.InstallRequest) (cli.InstallResult, error) {
	return cli.InstallResult{}, f.err
}

func (f failingInstaller) Uninstall(context.Context, cli.UninstallRequest) (cli.UninstallResult, error) {
	return cli.UninstallResult{}, f.err
}

func TestSynckitdTooOldIsUnavailable(t *testing.T) {
	if !errors.Is(ErrSynckitdTooOld, ErrUnavailable) {
		t.Fatal("ErrSynckitdTooOld does not wrap ErrUnavailable")
	}
	cfg := newConfig()
	cfg.Installer = failingInstaller{err: ErrSynckitdTooOld}
	code, stdout, _ := run(t, New(cfg), "install", "--json")
	var got struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal([]byte(stdout), &got); err != nil {
		t.Fatal(err)
	}
	if code != cli.ExitUnavailable || got.Error.Code != "unavailable" || got.Error.Message != "synckitd too old; upgrade synckit" {
		t.Errorf("exit %d error %+v, want exit %d unavailable %q", code, got.Error, cli.ExitUnavailable, "synckitd too old; upgrade synckit")
	}
}

func TestSelectors(t *testing.T) {
	tests := []struct {
		name string
		sel  cli.CheckpointSelector
		want string
		code cli.Code
	}{
		{"latest skips mixed", cli.LatestCheckpoint{}, "aaa200", ""},
		{"id prefix", cli.CheckpointID{Prefix: "aaa3"}, "aaa300", ""},
		{"ambiguous prefix", cli.CheckpointID{Prefix: "aaa"}, "", cli.CodeUsage},
		{"unknown prefix", cli.CheckpointID{Prefix: "fff"}, "", cli.CodeNotFound},
		{"at", cli.CheckpointAt{Time: at(19, 40)}, "aaa200", ""},
		{"at before all", cli.CheckpointAt{Time: at(1, 0)}, "", cli.CodeNotFound},
		{"hourly", cli.CheckpointHourly{HoursAgo: 1}, "aaa100", ""},
		{"daily", cli.CheckpointDaily{Year: 2026, Month: time.September, Day: 26}, "aaa300", ""},
		{"daily miss", cli.CheckpointDaily{Year: 2026, Month: time.September, Day: 25}, "", cli.CodeNotFound},
	}
	w := fixture().Origins[0].Worktrees[0]
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := view{now: now, snap: fixture()}.pick(laptop, w, tt.sel)
			if tt.code != "" {
				if cli.Classify(err) != tt.code {
					t.Fatalf("code %q, want %q (err %v)", cli.Classify(err), tt.code, err)
				}
				return
			}
			if err != nil || got.ID != tt.want {
				t.Fatalf("pick = %s, %v; want %s", got.ID, err, tt.want)
			}
		})
	}
}

func TestTargets(t *testing.T) {
	tests := []struct {
		name   string
		target cli.Target
		want   string
		code   cli.Code
	}{
		{"item", cli.ItemRef{SourceHostID: mini, WorkspaceID: "wt-docs"}, mini + "/wt-docs", ""},
		{"unknown item", cli.ItemRef{SourceHostID: mini, WorkspaceID: "wt-app"}, "", cli.CodeNotFound},
		{"session prefix", cli.SessionRef{ID: "4444"}, mini + "/wt-docs", ""},
		{"session on named source", cli.SessionRef{Source: "laptop", ID: sHuman}, laptop + "/wt-app", ""},
		{"session on other source", cli.SessionRef{Source: "mini", ID: sHuman}, "", cli.CodeNotFound},
		{"ambiguous session", cli.SessionRef{ID: ""}, "", cli.CodeUsage},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := New(newConfig()).Inspect(context.Background(), cli.InspectRequest{Target: tt.target, Checkpoint: cli.LatestCheckpoint{}})
			if tt.code != "" {
				if cli.Classify(err) != tt.code {
					t.Fatalf("code %q, want %q (err %v)", cli.Classify(err), tt.code, err)
				}
				return
			}
			if err != nil || res.Ref().String() != tt.want {
				t.Fatalf("inspect = %s, %v; want %s", res.Ref(), err, tt.want)
			}
		})
	}
}

func TestSync(t *testing.T) {
	cfg := newConfig()
	helper := &fakeHelper{attempts: []scheduler.Attempt{
		{WorktreeID: "wt-own", At: at(19, 55), Result: scheduler.Result{Outcome: scheduler.OutcomeCaptured, Checkpoint: "ddd100"}},
		{WorktreeID: "wt-gone", At: at(19, 55), Result: scheduler.Result{Outcome: scheduler.OutcomeBusy, Reason: "index.lock held"}},
	}}
	cfg.Helper = helper
	res, err := New(cfg).Sync(context.Background(), cli.SyncRequest{Sessions: []string{sOwn}})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(helper.kicked, []string{sOwn}) {
		t.Errorf("kicked %v", helper.kicked)
	}
	want := []cli.SyncedWorktree{
		{WorkspaceID: "wt-own", Sessions: cli.Array[string]{sOwn}, Checkpoint: checkpoint(fixture().Origins[2].Worktrees[0].Checkpoints[0]), Deferred: cli.Array[string]{}},
		{WorkspaceID: "wt-gone", Sessions: cli.Array[string]{}, Deferred: cli.Array[string]{"busy: index.lock held"}},
	}
	if len(res.Worktrees) != len(want) {
		t.Fatalf("worktrees = %+v", res.Worktrees)
	}
	for i, w := range want {
		got := res.Worktrees[i]
		if got.WorkspaceID != w.WorkspaceID || !slices.Equal(got.Sessions, w.Sessions) || !slices.Equal(got.Deferred, w.Deferred) ||
			(got.Checkpoint == nil) != (w.Checkpoint == nil) || (got.Checkpoint != nil && got.Checkpoint.ID != w.Checkpoint.ID) {
			t.Errorf("worktree %d = %+v, want %+v", i, got, w)
		}
	}
	cfg.Helper = &fakeHelper{err: ErrUnavailable}
	if _, err := New(cfg).Sync(context.Background(), cli.SyncRequest{}); cli.Classify(err) != cli.CodeUnavailable {
		t.Errorf("sync without helper: code %q", cli.Classify(err))
	}
}

func TestResume(t *testing.T) {
	restored := claudenative.Session{ID: sHuman, Cwd: "/Users/me/.cc-sync/checkouts/app/laptop-app-20260926-1930", TranscriptPath: "/p/a.jsonl"}
	tests := []struct {
		name   string
		ref    cli.SessionRef
		copies []claudenative.Session
		code   cli.Code
	}{
		{"full id", cli.SessionRef{ID: sHuman}, []claudenative.Session{restored}, ""},
		{"catalog prefix", cli.SessionRef{Source: "laptop", ID: "3333"}, []claudenative.Session{restored}, ""},
		{"live collision", cli.SessionRef{ID: sLive}, []claudenative.Session{restored}, cli.CodeLiveLocalCollision},
		{"not restored", cli.SessionRef{ID: sHuman}, nil, cli.CodeNotFound},
		{"two copies", cli.SessionRef{ID: sHuman}, []claudenative.Session{restored, {ID: sHuman, TranscriptPath: "/p/b.jsonl"}}, cli.CodeCheckoutConflict},
		{"unknown prefix", cli.SessionRef{ID: "9999"}, nil, cli.CodeNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := newConfig()
			var calls []execCall
			cfg.Exec = func(argv []string, dir string, env []string) error {
				calls = append(calls, execCall{argv, dir, env})
				return nil
			}
			cfg.Sessions = func(_ context.Context, id claudenative.SessionID) ([]claudenative.Session, error) {
				if tt.code != cli.CodeLiveLocalCollision && id != sHuman {
					t.Errorf("looked up %s", id)
				}
				return tt.copies, nil
			}
			res, err := New(cfg).Resume(context.Background(), cli.ResumeRequest{Session: tt.ref})
			if tt.code != "" {
				if cli.Classify(err) != tt.code || len(calls) != 0 {
					t.Fatalf("code %q, want %q; exec calls %v (err %v)", cli.Classify(err), tt.code, calls, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			want := execCall{[]string{"claude", "--resume", sHuman}, restored.Cwd, []string{"HOME=/Users/me"}}
			if len(calls) != 1 || !slices.Equal(calls[0].argv, want.argv) || calls[0].dir != want.dir || !slices.Equal(calls[0].env, want.env) {
				t.Fatalf("exec calls %+v, want %+v", calls, want)
			}
			if res != (cli.ResumeResult{SessionID: sHuman, Cwd: restored.Cwd}) {
				t.Errorf("result %+v", res)
			}
		})
	}
}

func TestCheckoutDir(t *testing.T) {
	root := t.TempDir()
	for _, dir := range []string{
		"app/laptop-app-20260926-1800/.git",
		"app/laptop-app-20260926-1900",
		"app/mini-app-20260926-2000/.git",
		"app/laptop-application-20260926-2100/.git",
		"app/laptop-app-backup/.git",
	} {
		if err := os.MkdirAll(filepath.Join(root, dir), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	w := catalog.Worktree{ID: "wt-app", Repo: catalog.Repo{RelPath: "app", SourcePath: "/Users/yasyf/Code/app"}}
	tests := []struct {
		name   string
		source string
		w      catalog.Worktree
		want   *cli.LocalCheckout
	}{
		{"newest wins", laptop, w, &cli.LocalCheckout{Path: filepath.Join(root, "app/laptop-app-20260926-1900")}},
		{"other source", mini, w, &cli.LocalCheckout{Path: filepath.Join(root, "app/mini-app-20260926-2000"), Reusable: true}},
		{"none", "yasyf@ipad", w, nil},
		{"no repo dir", laptop, catalog.Worktree{Repo: catalog.Repo{RelPath: "gone", SourcePath: "/x/app"}}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CheckoutDir{Root: root}.Find(tt.source, tt.w)
			if err != nil {
				t.Fatal(err)
			}
			if (got == nil) != (tt.want == nil) || (got != nil && *got != *tt.want) {
				t.Fatalf("Find = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestAssurance(t *testing.T) {
	snap := fixture()
	snap.Carried = map[string]uint64{"ddd100": 5}
	own := snap.Origins[2].Worktrees[0].Checkpoints[0]
	incomplete := own
	incomplete.Completeness = catalog.Completeness{Missing: []string{"session:x"}}
	tests := []struct {
		name  string
		cp    catalog.Checkpoint
		acked syncservice.Revision
		at    time.Time
		want  cli.Assurance
	}{
		{"never acked", own, "", now, cli.AssuranceNone},
		{"acked before carried", own, "4", now, cli.AssuranceNone},
		{"complete and acked", own, "5", now, cli.AssuranceDurable},
		{"incomplete and acked", incomplete, "6", now, cli.AssuranceHeld},
		{"expired", own, "5", own.ExpiresAt, cli.AssuranceNone},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := view{now: tt.at, snap: snap}.assurance(delivery.PeerStatus{Peer: laptop, Acked: tt.acked}, tt.cp)
			if err != nil || got != tt.want {
				t.Fatalf("assurance = %q, %v; want %q", got, err, tt.want)
			}
		})
	}
}
