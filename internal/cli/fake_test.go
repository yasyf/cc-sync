package cli_test

import (
	"bytes"
	"context"
	"time"

	"github.com/yasyf/cc-sync/internal/cli"
)

type fakeService struct {
	install   cli.InstallResult
	uninstall cli.UninstallResult
	list      cli.ListResult
	inspect   cli.InspectResult
	status    cli.StatusResult
	sync      cli.SyncResult
	pickup    cli.PickupResult
	resume    cli.ResumeResult
	err       error
	phases    []cli.Progress
	block     chan struct{}
	got       any
}

func (f *fakeService) Install(_ context.Context, req cli.InstallRequest) (cli.InstallResult, error) {
	f.got = req
	return f.install, f.err
}

func (f *fakeService) Uninstall(_ context.Context, req cli.UninstallRequest) (cli.UninstallResult, error) {
	f.got = req
	return f.uninstall, f.err
}

func (f *fakeService) List(_ context.Context, req cli.ListRequest) (cli.ListResult, error) {
	f.got = req
	return f.list, f.err
}

func (f *fakeService) Inspect(_ context.Context, req cli.InspectRequest) (cli.InspectResult, error) {
	f.got = req
	return f.inspect, f.err
}

func (f *fakeService) Status(context.Context) (cli.StatusResult, error) {
	return f.status, f.err
}

func (f *fakeService) Sync(_ context.Context, req cli.SyncRequest) (cli.SyncResult, error) {
	f.got = req
	return f.sync, f.err
}

func (f *fakeService) Pickup(ctx context.Context, req cli.PickupRequest) (cli.PickupResult, error) {
	for _, p := range f.phases {
		req.Progress(p)
	}
	req.Progress = nil
	f.got = req
	if f.block != nil {
		close(f.block)
		<-ctx.Done()
		return cli.PickupResult{}, ctx.Err()
	}
	return f.pickup, f.err
}

func (f *fakeService) Resume(_ context.Context, req cli.ResumeRequest) (cli.ResumeResult, error) {
	f.got = req
	return f.resume, f.err
}

func (f *fakeService) HelperServe(context.Context) error {
	return f.err
}

type result struct {
	exit   int
	stdout string
	stderr string
}

func run(svc cli.Service, args ...string) result {
	return runOn(svc, cli.Terminal{}, args...)
}

func runOn(svc cli.Service, term cli.Terminal, args ...string) result {
	var stdout, stderr bytes.Buffer
	exit := cli.New(svc, term).Run(context.Background(), args, &stdout, &stderr)
	return result{exit: exit, stdout: stdout.String(), stderr: stderr.String()}
}

type execCall struct {
	argv []string
	dir  string
	env  []string
}

type fakeExec struct {
	calls []execCall
	err   error
}

func (f *fakeExec) terminal(interactive bool) cli.Terminal {
	return cli.Terminal{
		Interactive: interactive,
		Environ:     []string{"HOME=/Users/yasyf", "CLAUDECODE=1", "PATH=/usr/bin:/bin", "TERM=xterm-256color"},
		Exec: func(argv []string, dir string, env []string) error {
			f.calls = append(f.calls, execCall{argv: argv, dir: dir, env: env})
			return f.err
		},
	}
}

var pdt = time.FixedZone("PDT", -7*60*60)

func ts(hour, minute int) cli.Time {
	return cli.At(time.Date(2026, 9, 26, hour, minute, 0, 0, pdt))
}

func tsp(hour, minute int) *cli.Time {
	t := ts(hour, minute)
	return &t
}

func ptr[T any](v T) *T { return &v }

func fullItem() cli.Item {
	return cli.Item{
		Source: cli.Source{
			Host:       cli.Host{HostID: "host-mbp", HostName: "Yasyf's MacBook Pro"},
			LastSeenAt: tsp(12, 5),
			Reachable:  true,
		},
		Workspace: cli.Workspace{
			ID:         "wt-7f3a",
			RepoName:   "monorepo",
			RepoOrigin: ptr("git@github.com:yasyf/monorepo.git"),
			Branch:     ptr("feature/sync"),
			SourcePath: "/Users/yasyf/Code/monorepo",
			Orca:       &cli.OrcaWorkspace{Kind: cli.OrcaWorktree, Name: "sync", InstanceID: "inst-1"},
		},
		Sessions: []cli.Session{
			{
				SessionID:           "0f3c9a2e-5b1d-4c8e-9a7f-2d6b8e1c4a90",
				Title:               "Wire the picker",
				LastActivityAt:      tsp(12, 4),
				LastHumanActivityAt: tsp(12, 3),
				Activity:            cli.ActivityHuman,
				BoundInOrca:         true,
			},
			{
				SessionID:          "7a1e4c2b-9d3f-4b6a-8e5c-1f0a2b3c4d5e",
				Title:              "Background refactor",
				LastActivityAt:     tsp(11, 30),
				Activity:           cli.ActivityAutonomous,
				LiveLocalCollision: true,
			},
		},
		NotRestorable: []cli.NotRestorable{{Agent: "codex", Key: "session_id", ID: "019a2c4e-codex", Reason: "agent-not-supported-v1"}},
		Checkpoint: cli.Checkpoint{
			ID:               "c0ffee1234",
			Tier:             cli.TierLatest,
			CapturedAt:       cli.At(time.Date(2026, 9, 26, 12, 0, 0, 500_000_000, pdt)),
			SourceActivityAt: tsp(12, 4),
		},
		CheckpointCount: 3,
		Completeness: cli.Completeness{
			Ready:          true,
			Missing:        []string{},
			Transcript:     cli.TranscriptComplete,
			Code:           cli.CodeComplete,
			CodeCapturedAt: ptr(cli.At(time.Date(2026, 9, 26, 12, 0, 0, 500_000_000, pdt))),
			Layout:         cli.LayoutClientView,
		},
		LocalCheckout: &cli.LocalCheckout{Path: "/Users/yasyf/.cc-sync/checkouts/monorepo/host-mbp-sync-20260926-1200", Reusable: true},
	}
}

func sparseItem() cli.Item {
	return cli.Item{
		Source:    cli.Source{Host: cli.Host{HostID: "host-mini", HostName: "mini"}},
		Workspace: cli.Workspace{ID: "wt-01", RepoName: "scratch", SourcePath: "/Users/yasyf/scratch"},
		Checkpoint: cli.Checkpoint{
			ID:         "abc123",
			Tier:       cli.TierDaily,
			CapturedAt: ts(9, 0),
		},
		CheckpointCount: 1,
		Completeness: cli.Completeness{
			Missing:    []string{"lfs:assets/model.bin", "submodule:vendor/lib"},
			Transcript: cli.TranscriptPartial,
			Code:       cli.CodeMissing,
			Layout:     cli.LayoutNone,
		},
		Pause: &cli.Pause{Reason: cli.PauseCellular, Endpoint: cli.EndpointPeer, Since: ts(11, 0)},
	}
}

func deferredItem() cli.Item {
	return cli.Item{
		Source:    cli.Source{Host: cli.Host{HostID: "host-mbp", HostName: "mbp"}, LastSeenAt: tsp(12, 5), Reachable: true},
		Workspace: cli.Workspace{ID: "wt-9c1d", RepoName: "assets", Branch: ptr("main"), SourcePath: "/Users/yasyf/Code/assets"},
		Sessions: []cli.Session{{
			SessionID:           "3b8d1f6a-2e4c-4a9b-8d7e-6f5a4b3c2d1e",
			Title:               "Retarget textures",
			LastActivityAt:      tsp(12, 2),
			LastHumanActivityAt: tsp(12, 2),
			Activity:            cli.ActivityHuman,
		}},
		Checkpoint:      cli.Checkpoint{ID: "5eed4a11", Tier: cli.TierLatest, CapturedAt: ts(12, 5), SourceActivityAt: tsp(12, 2)},
		CheckpointCount: 2,
		Completeness: cli.Completeness{
			Ready:          true,
			Transcript:     cli.TranscriptComplete,
			Code:           cli.CodeDeferred,
			CodeCapturedAt: tsp(10, 30),
			Layout:         cli.LayoutHostOnly,
		},
	}
}

func claudeLaunch(dir, sessionID string) *cli.Launch {
	return &cli.Launch{
		Argv:     []string{"/opt/homebrew/bin/claude", "--resume", sessionID},
		Dir:      dir,
		EnvUnset: []string{"CLAUDECODE", "CLAUDE_CODE_ENTRYPOINT"},
	}
}

const (
	recoveredCheckout = "/Users/yasyf/.cc-sync/checkouts/monorepo/host-mbp-sync-20260926-1200"
	humanSession      = "0f3c9a2e-5b1d-4c8e-9a7f-2d6b8e1c4a90"
	backgroundSession = "7a1e4c2b-9d3f-4b6a-8e5c-1f0a2b3c4d5e"
	forkedSession     = "5d2b7e10-3c4a-4f8b-9e6d-0a1b2c3d4e5f"
)

func fullStatus() cli.StatusResult {
	return cli.StatusResult{
		Helper: cli.Helper{Running: true, Build: "v0.1.0"},
		Local: cli.LocalHost{
			Host:    cli.Host{HostID: "host-air", HostName: "air"},
			Network: cli.Network{Status: cli.NetworkConnected, Expensive: true, Cellular: true},
		},
		Peers: []cli.Peer{
			{
				Host:            cli.Host{HostID: "host-mbp", HostName: "mbp"},
				Reachable:       true,
				LastSeenAt:      tsp(12, 5),
				AckedRevision:   ptr(uint64(41)),
				PendingRevision: ptr(uint64(42)),
				PendingSince:    tsp(12, 1),
				Pause:           &cli.Pause{Reason: cli.PauseCellular, Endpoint: cli.EndpointLocal, Since: ts(11, 55)},
			},
			{Host: cli.Host{HostID: "host-mini", HostName: "mini"}},
		},
		Scheduler: cli.Scheduler{
			QueuedByTier: cli.QueuedByTier{Human: 1, Autonomous: 2, Recent: 3, Idle: 4},
			Workers:      2,
			LastRoundAt:  tsp(12, 6),
			Tiers: cli.CaptureTiers{
				HumanInterval:      cli.Duration(2 * time.Minute),
				AutonomousInterval: cli.Duration(5 * time.Minute),
				RecentInterval:     cli.Duration(15 * time.Minute),
				IdleInterval:       cli.Duration(time.Hour),
				HumanWindow:        cli.Duration(15 * time.Minute),
				AutonomousWindow:   cli.Duration(15 * time.Minute),
				RecentWindow:       cli.Duration(time.Hour),
			},
		},
	}
}

func fullInspect() cli.InspectResult {
	return cli.InspectResult{
		Item: fullItem(),
		Checkpoints: []cli.CheckpointDetail{
			{ID: "c0ffee1234", Tier: cli.TierLatest, CapturedAt: ts(12, 0), Ready: true},
			{ID: "beef5678", Tier: cli.TierHourly, CapturedAt: ts(11, 0), Missing: []string{"session:7a1e4c2b"}, Deferred: []string{"worktree busy"}},
		},
		Delivery: []cli.Delivery{
			{Peer: "host-air", State: cli.DeliveryIdle},
			{Peer: "host-mini", State: cli.DeliveryPaused, Pause: &cli.Pause{Reason: cli.PausePeerOffline, Endpoint: cli.EndpointPeer, Since: ts(10, 0)}},
		},
	}
}

var pickedCheckpoint = cli.PickupCheckpoint{ID: "c0ffee12", CapturedAt: cli.Time{Time: time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)}}

func partialSparsePickup() cli.PickupResult {
	return cli.PickupResult{
		Checkpoint: cli.PickupCheckpoint{ID: "d00d1e55", CapturedAt: cli.Time{Time: time.Date(2026, 9, 26, 13, 0, 0, 0, time.UTC)}, Partial: true, CodeDeferred: "missing-lfs"},
		Checkout: cli.PickupCheckout{
			Path: recoveredCheckout, Branch: ptr("feature/sync"),
			Differences: []string{"sparse checkout expanded to full: /web/"},
			Sparse:      &cli.SparseCheckout{Cone: true, Patterns: []string{"/web/"}, Expanded: true},
		},
		Sessions: []cli.PickedSession{{SessionID: humanSession, Status: cli.SessionRestored, Selected: true, Launch: claudeLaunch(recoveredCheckout, humanSession)}},
	}
}

func orcaPickup() cli.PickupResult {
	return cli.PickupResult{
		Checkpoint: pickedCheckpoint,
		Checkout:   cli.PickupCheckout{Path: recoveredCheckout, Branch: ptr("feature/sync")},
		Sessions: []cli.PickedSession{
			{SessionID: humanSession, Status: cli.SessionResumed, Selected: true},
			{SessionID: backgroundSession, Status: cli.SessionDormant, Launch: claudeLaunch(recoveredCheckout, backgroundSession)},
		},
		Orca: &cli.OrcaPickup{
			WorktreeID: "orca-wt-9",
			Resumed:    []cli.ResumedTab{{SessionID: humanSession, TabID: "tab-1"}},
			Dormant:    []string{backgroundSession},
		},
	}
}

func orcaDormantPickup() cli.PickupResult {
	return cli.PickupResult{
		Checkpoint: pickedCheckpoint,
		Checkout:   cli.PickupCheckout{Path: recoveredCheckout, Branch: ptr("feature/sync")},
		Sessions: []cli.PickedSession{
			{SessionID: humanSession, Status: cli.SessionDormant, Selected: true, Launch: claudeLaunch(recoveredCheckout, humanSession)},
			{SessionID: backgroundSession, Status: cli.SessionDormant, Launch: claudeLaunch(recoveredCheckout, backgroundSession)},
		},
		Orca: &cli.OrcaPickup{WorktreeID: "orca-wt-9", Dormant: []string{humanSession, backgroundSession}},
	}
}

func cliOnlyPickup() cli.PickupResult {
	return cli.PickupResult{
		Checkpoint: pickedCheckpoint,
		Checkout:   cli.PickupCheckout{Path: "/Users/yasyf/Code/monorepo-recovered", Reused: true},
		Sessions: []cli.PickedSession{{
			SessionID: humanSession,
			Status:    cli.SessionRestored,
			Selected:  true,
			Launch:    claudeLaunch("/Users/yasyf/Code/monorepo-recovered", humanSession),
		}},
	}
}

func forkRefusedPickup() cli.PickupResult {
	return cli.PickupResult{
		Checkpoint: pickedCheckpoint,
		Checkout:   cli.PickupCheckout{Path: recoveredCheckout, Branch: ptr("feature/sync")},
		Sessions: []cli.PickedSession{
			{SessionID: forkedSession, Status: cli.SessionRestored, Selected: true, Launch: claudeLaunch(recoveredCheckout, forkedSession), ForkedFrom: ptr(humanSession)},
			{SessionID: backgroundSession, Status: cli.SessionRefused, Reason: cli.CodeDivergentLocalCopy},
		},
	}
}
