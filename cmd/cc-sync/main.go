// Command cc-sync: Recover Claude Code sessions, their uncommitted Git work, and Orca workspace layouts on another synckit peer.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/yasyf/cc-sync/internal/catalog"
	"github.com/yasyf/cc-sync/internal/claudenative"
	"github.com/yasyf/cc-sync/internal/cli"
	applog "github.com/yasyf/cc-sync/internal/log"
	"github.com/yasyf/cc-sync/internal/orcabridge"
	"github.com/yasyf/cc-sync/internal/scheduler"
	"github.com/yasyf/cc-sync/internal/service"
	"github.com/yasyf/synckit/delivery"
	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/netpolicy"
)

const networkSettle = time.Second

var ccsync = hostregistry.Config{Name: "cc-sync", DirEnv: "CC_SYNC_CONFIG_DIR"}

func main() {
	applog.Setup()
	os.Exit(cli.Execute(service.New(wiring()), os.Args[1:], os.Stdout, os.Stderr))
}

func wiring() service.Config {
	term := cli.ProcessTerminal()
	return service.Config{
		Catalog:    meshCatalog{},
		Deliveries: unwiredDeliveries{},
		Mesh:       hostregistry.Mesh,
		Network:    currentNetwork,
		Orca:       localOrca{},
		Live:       liveSessions,
		Sessions:   localSessions,
		Checkouts:  homeCheckouts{},
		Helper:     unwiredHelper{},
		Installer:  unwiredInstaller{},
		Picker:     unwiredPicker{},
		Serve:      func(context.Context) error { return unwired("the resident helper (internal/resident)") },
		Tiers:      defaultTiers,
		Environ:    term.Environ,
		Exec:       term.Exec,
		Now:        time.Now,
	}
}

type meshCatalog struct{}

func (meshCatalog) Load() (catalog.Snapshot, error) {
	reg, err := hostregistry.Mesh.Load()
	if err != nil {
		return catalog.Snapshot{}, fmt.Errorf("load mesh: %w", err)
	}
	dir, err := ccsync.Dir()
	if err != nil {
		return catalog.Snapshot{}, fmt.Errorf("resolve config dir: %w", err)
	}
	return catalog.New(filepath.Join(dir, "catalog-v1.json"), reg.Self, time.Now).Load()
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

type homeCheckouts struct{}

func (homeCheckouts) Find(source string, w catalog.Worktree) (*cli.LocalCheckout, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("resolve home dir: %w", err)
	}
	return service.CheckoutDir{Root: filepath.Join(home, ".cc-sync", "checkouts")}.Find(source, w)
}

func defaultTiers() (cli.CaptureTiers, error) {
	return cli.CaptureTiers{
		HumanInterval:      cli.Duration(2 * time.Minute),
		AutonomousInterval: cli.Duration(5 * time.Minute),
		RecentInterval:     cli.Duration(15 * time.Minute),
		IdleInterval:       cli.Duration(time.Hour),
		HumanWindow:        cli.Duration(15 * time.Minute),
		AutonomousWindow:   cli.Duration(15 * time.Minute),
		RecentWindow:       cli.Duration(time.Hour),
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
