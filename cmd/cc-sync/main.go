// Command cc-sync: Recover Claude Code sessions, their uncommitted Git work, and Orca workspace layouts on another synckit peer.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/yasyf/cc-sync/internal/catalog"
	"github.com/yasyf/cc-sync/internal/claudenative"
	"github.com/yasyf/cc-sync/internal/cli"
	"github.com/yasyf/cc-sync/internal/config"
	applog "github.com/yasyf/cc-sync/internal/log"
	"github.com/yasyf/cc-sync/internal/orcabridge"
	"github.com/yasyf/cc-sync/internal/scheduler"
	"github.com/yasyf/cc-sync/internal/service"
	"github.com/yasyf/synckit/delivery"
	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/netpolicy"
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
		Deliveries: unwiredDeliveries{},
		Mesh:       hostregistry.Mesh,
		Network:    currentNetwork,
		Orca:       localOrca{},
		Live:       liveSessions,
		Sessions:   localSessions,
		Checkouts:  service.CheckoutDir{Root: layout.CheckoutRoot},
		Helper:     unwiredHelper{},
		Installer:  unwiredInstaller{},
		Picker:     unwiredPicker{},
		Serve:      func(context.Context) error { return unwired("the resident helper (internal/resident)") },
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

type unwiredDeliveries struct{}

func (unwiredDeliveries) Status(context.Context, string) ([]delivery.PeerStatus, error) {
	return nil, unwired("synckit delivery status")
}

type unwiredHelper struct{}

func (unwiredHelper) Status(context.Context) (service.HelperStatus, error) {
	return service.HelperStatus{}, unwired("the resident helper client")
}

func (unwiredHelper) Kick(context.Context, []string) ([]scheduler.Attempt, error) {
	return nil, unwired("the resident helper client")
}

type unwiredInstaller struct{}

func (unwiredInstaller) Install(context.Context, cli.InstallRequest) (cli.InstallResult, error) {
	return cli.InstallResult{}, unwired("install (internal/resident)")
}

func (unwiredInstaller) Uninstall(context.Context, cli.UninstallRequest) (cli.UninstallResult, error) {
	return cli.UninstallResult{}, unwired("uninstall (internal/resident)")
}

type unwiredPicker struct{}

func (unwiredPicker) Pickup(context.Context, cli.PickupRequest) (cli.PickupResult, error) {
	return cli.PickupResult{}, unwired("pickup (internal/pickup)")
}
