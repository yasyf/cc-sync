// Command cc-sync: Recover Claude Code sessions, their uncommitted Git work, and Orca workspace layouts on another synckit peer.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/yasyf/cc-sync/internal/catalog"
	"github.com/yasyf/cc-sync/internal/claudenative"
	"github.com/yasyf/cc-sync/internal/cli"
	"github.com/yasyf/cc-sync/internal/config"
	"github.com/yasyf/cc-sync/internal/consumer"
	applog "github.com/yasyf/cc-sync/internal/log"
	"github.com/yasyf/cc-sync/internal/orcabridge"
	"github.com/yasyf/cc-sync/internal/pickup"
	"github.com/yasyf/cc-sync/internal/resident"
	"github.com/yasyf/cc-sync/internal/scheduler"
	"github.com/yasyf/cc-sync/internal/service"
	"github.com/yasyf/cc-sync/internal/sessionrestore"
	"github.com/yasyf/reposync/registry"
	"github.com/yasyf/reposync/worktree"
	"github.com/yasyf/synckit/artifact"
	"github.com/yasyf/synckit/delivery"
	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/netpolicy"
	"github.com/yasyf/synckit/rpc"
)

const networkSettle = time.Second

func main() {
	applog.Setup()
	layout, err := config.Resolve()
	if err != nil {
		fmt.Fprintln(os.Stderr, "cc-sync:", err)
		os.Exit(1)
	}
	os.Exit(cli.Execute(service.New(wiring(layout)), os.Args[1:], os.Stdout, os.Stderr))
}

func wiring(layout config.Layout) service.Config {
	term := cli.ProcessTerminal()
	return service.Config{
		Catalog:    meshCatalog{path: layout.CatalogPath},
		Deliveries: synckitDeliveries{},
		Mesh:       hostregistry.Mesh,
		Network:    currentNetwork,
		Orca:       localOrca{},
		Live:       liveSessions,
		Sessions:   localSessions,
		Checkouts:  service.CheckoutDir{Root: layout.CheckoutRoot},
		Helper:     helperClient{},
		Installer:  residentInstaller{layout: layout},
		Picker:     localPicker{layout: layout},
		Serve:      func(ctx context.Context) error { return serve(ctx, layout) },
		Tiers:      func() (cli.CaptureTiers, error) { return captureTiers(layout.ConfigPath) },
		Environ:    term.Environ,
		Exec:       term.Exec,
		Now:        time.Now,
	}
}

type meshCatalog struct {
	path string
}

func (c meshCatalog) Load() (catalog.Snapshot, error) {
	reg, err := hostregistry.Mesh.Load()
	if err != nil {
		return catalog.Snapshot{}, fmt.Errorf("load mesh: %w", err)
	}
	return catalog.New(c.path, reg.Self, time.Now).Load()
}

func currentNetwork(ctx context.Context) (st netpolicy.State, err error) {
	path, err := netpolicy.ManualPath()
	if err != nil {
		return netpolicy.State{}, fmt.Errorf("resolve manual network policy: %w", err)
	}
	m, err := netpolicy.NewMonitor(path)
	if err != nil {
		return netpolicy.State{}, fmt.Errorf("start network monitor: %w", err)
	}
	defer func() { err = errors.Join(err, m.Close()) }()
	st, changed := m.Current()
	if st.Status != netpolicy.StatusUnknown {
		return st, nil
	}
	timer := time.NewTimer(networkSettle)
	defer timer.Stop()
	select {
	case <-changed:
	case <-timer.C:
	case <-ctx.Done():
		return netpolicy.State{}, ctx.Err()
	}
	st, _ = m.Current()
	return st, nil
}

type localOrca struct{}

func (localOrca) List(ctx context.Context, worktree orcabridge.Selector) ([]orcabridge.DormantBinding, error) {
	c, err := orcabridge.New(orcabridge.Options{})
	if err != nil {
		return nil, err
	}
	return c.List(ctx, worktree)
}

func (localOrca) Activity(ctx context.Context) ([]orcabridge.WorkspaceActivity, error) {
	c, err := orcabridge.New(orcabridge.Options{})
	if err != nil {
		return nil, err
	}
	return c.Activity(ctx)
}

func liveSessions(ctx context.Context) (map[claudenative.SessionID]claudenative.LiveProcess, error) {
	layout, err := claudenative.DefaultLayout()
	if err != nil {
		return nil, err
	}
	return claudenative.LiveSessions(ctx, layout, claudenative.SystemProcesses())
}

func localSessions(ctx context.Context, id claudenative.SessionID) ([]claudenative.Session, error) {
	layout, err := claudenative.DefaultLayout()
	if err != nil {
		return nil, err
	}
	sessions, _, err := claudenative.Scan(ctx, claudenative.ScanOptions{Layout: layout, Only: []claudenative.SessionID{id}}, claudenative.Cursor{})
	return sessions, err
}

func captureTiers(path string) (cli.CaptureTiers, error) {
	cfg, err := config.Load(path)
	if err != nil {
		return cli.CaptureTiers{}, err
	}
	t := cfg.Capture
	return cli.CaptureTiers{
		HumanInterval:      cli.Duration(t.HumanInterval),
		AutonomousInterval: cli.Duration(t.AutonomousInterval),
		RecentInterval:     cli.Duration(t.RecentInterval),
		IdleInterval:       cli.Duration(t.IdleInterval),
		HumanWindow:        cli.Duration(t.HumanWindow),
		AutonomousWindow:   cli.Duration(t.AutonomousWindow),
		RecentWindow:       cli.Duration(t.RecentWindow),
	}, nil
}

func unwired(what string) error {
	return fmt.Errorf("%w: %s is not wired into this build", service.ErrUnavailable, what)
}

type synckitDeliveries struct{}

func (synckitDeliveries) Status(ctx context.Context, serviceID string) ([]delivery.PeerStatus, error) {
	statuses, err := delivery.Status(ctx, serviceID)
	var transport *rpc.TransportError
	if errors.As(err, &transport) && transport.Undispatched {
		return nil, fmt.Errorf("%w: synckitd is not running: %w", service.ErrUnavailable, err)
	}
	return statuses, err
}

func unavailable(err error) error {
	if errors.Is(err, resident.ErrNotRunning) {
		return fmt.Errorf("%w: %w", service.ErrUnavailable, err)
	}
	return err
}

type helperClient struct{}

func (helperClient) Status(ctx context.Context) (_ service.HelperStatus, err error) {
	c := resident.Dial()
	defer func() { err = errors.Join(err, c.Close()) }()
	reply, err := c.Status(ctx)
	if err != nil {
		return service.HelperStatus{}, unavailable(err)
	}
	return service.HelperStatus{Build: reply.Build, Scheduler: reply.Scheduler}, nil
}

func (helperClient) Kick(ctx context.Context, sessionIDs []string) (_ []scheduler.Attempt, err error) {
	c := resident.Dial()
	defer func() { err = errors.Join(err, c.Close()) }()
	attempts, err := c.Kick(ctx, sessionIDs)
	return attempts, unavailable(err)
}

func (helperClient) Pin(ctx context.Context, owner string, roots []artifact.Ref, ttl time.Duration) (err error) {
	c := resident.Dial()
	defer func() { err = errors.Join(err, c.Close()) }()
	return unavailable(c.Pin(ctx, owner, roots, ttl))
}

type residentInstaller struct {
	layout config.Layout
}

func (i residentInstaller) Install(ctx context.Context, req cli.InstallRequest) (cli.InstallResult, error) {
	self, err := resident.MeshSelf()
	if err != nil {
		return cli.InstallResult{}, err
	}
	if req.NoSynckitd {
		err = resident.Ensure(i.layout, self)
	} else {
		err = resident.Install(ctx, resident.ExecRunner, i.layout, self)
	}
	if err != nil {
		return cli.InstallResult{}, err
	}
	res := cli.InstallResult{ConfigDir: i.layout.Dir, Synckitd: !req.NoSynckitd}
	status, err := helperClient{}.Status(ctx)
	switch {
	case errors.Is(err, service.ErrUnavailable):
	case err != nil:
		return cli.InstallResult{}, fmt.Errorf("probe helper: %w", err)
	default:
		res.Helper = cli.Helper{Running: true, Build: status.Build}
	}
	return res, nil
}

func (i residentInstaller) Uninstall(ctx context.Context, req cli.UninstallRequest) (cli.UninstallResult, error) {
	var purge []string
	if req.Purge {
		root, err := resident.ArtifactRoot()
		if err != nil {
			return cli.UninstallResult{}, err
		}
		purge = []string{i.layout.Dir, root}
	}
	if err := resident.Uninstall(ctx, resident.ExecRunner, purge); err != nil {
		return cli.UninstallResult{}, err
	}
	return cli.UninstallResult{Purged: req.Purge}, nil
}

func serve(ctx context.Context, layout config.Layout) error {
	deps, err := resident.SystemDeps(layout)
	if err != nil {
		return err
	}
	return resident.Serve(ctx, deps)
}

type localPicker struct {
	layout config.Layout
}

func (p localPicker) Pickup(ctx context.Context, req cli.PickupRequest) (cli.PickupResult, error) {
	claude, err := claudenative.DefaultLayout()
	if err != nil {
		return cli.PickupResult{}, err
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return cli.PickupResult{}, fmt.Errorf("resolve home: %w", err)
	}
	root, err := resident.ArtifactRoot()
	if err != nil {
		return cli.PickupResult{}, err
	}
	store, err := artifact.OpenReadOnly(root)
	if err != nil {
		return cli.PickupResult{}, fmt.Errorf("open artifact store: %w", err)
	}
	code, err := worktree.OpenStore(p.layout.CodeStore)
	if err != nil {
		return cli.PickupResult{}, fmt.Errorf("open code store: %w", err)
	}
	cfg := pickup.Config{
		Catalog:   meshCatalog{path: p.layout.CatalogPath},
		Pinner:    helperClient{},
		OpenStore: func(context.Context) (pickup.Store, error) { return store, nil },
		Verifier:  consumer.Reposync{Store: store, Code: code, Registry: registry.Load},
		Code:      pickup.Reposync{Worktrees: unwiredWorktrees{}},
		Sessions: &pickup.Native{
			Run:           sessionrestore.SystemRunner(),
			Procs:         claudenative.SystemProcesses(),
			DisplacedRoot: filepath.Join(p.layout.Dir, "displaced"),
			Now:           time.Now,
		},
		FetchAllowed: func() bool { return false },
		Layout:       claude,
		Home:         home,
		ReplicaRoot:  p.layout.ReplicaRoot,
		CheckoutRoot: p.layout.CheckoutRoot,
		PreferClient: os.Getenv("CC_SYNC_ORCA_CLIENT_INSTANCE_ID"),
		Now:          time.Now,
	}
	if c, err := orcabridge.New(orcabridge.Options{}); err == nil {
		cfg.Orca = c
	} else {
		slog.Info("pickup: orca unavailable, sessions launch natively", "err", err)
	}
	return pickup.New(cfg).Pickup(ctx, req)
}

type unwiredWorktrees struct{}

func (unwiredWorktrees) Restore(context.Context, worktree.Snapshot, worktree.ArtifactSource, pickup.RestoreOptions) (pickup.Restored, error) {
	return pickup.Restored{}, unwired("the reposync Store.Restore adapter")
}
