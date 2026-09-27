// Package service implements cc-sync's commands over the local checkpoint
// catalog, synckitd's delivery status, the local network policy, Orca, the
// resident helper, and the native Claude layout.
package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/yasyf/cc-sync/internal/catalog"
	"github.com/yasyf/cc-sync/internal/claudenative"
	"github.com/yasyf/cc-sync/internal/cli"
	"github.com/yasyf/cc-sync/internal/orcabridge"
	"github.com/yasyf/cc-sync/internal/scheduler"
	"github.com/yasyf/synckit/delivery"
	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/netpolicy"
)

// ErrUnavailable marks a dependency that is not running on this host or not
// wired into this build.
var ErrUnavailable = errors.New("unavailable")

// ErrSynckitdTooOld marks a synckitd that predates the delivery rpc cc-sync
// calls. It wraps ErrUnavailable, so commands degrade or fail as unavailable.
var ErrSynckitdTooOld error = &unavailableError{msg: "synckitd too old; upgrade synckit"}

type unavailableError struct{ msg string }

func (e *unavailableError) Error() string { return e.msg }

func (e *unavailableError) Unwrap() error { return ErrUnavailable }

// Catalog reads the local checkpoint catalog.
type Catalog interface {
	Load() (catalog.Snapshot, error)
}

// Deliveries reports synckitd's per-peer delivery of one service; it wraps
// ErrUnavailable when synckitd cannot be reached or is too old to answer.
type Deliveries interface {
	Status(ctx context.Context, serviceID string) ([]delivery.PeerStatus, error)
}

// Mesh reports this host's mesh identity and its registered peers.
type Mesh interface {
	Load() (*hostregistry.Registry, error)
}

// Orca reads the local Orca runtime's recovery state; it fails with an
// *orcabridge.UnavailableError when Orca is absent or not local.
type Orca interface {
	List(ctx context.Context, worktree orcabridge.Selector) ([]orcabridge.DormantBinding, error)
	Activity(ctx context.Context) ([]orcabridge.WorkspaceActivity, error)
}

// Checkouts finds an existing local checkout of a worktree captured on
// source, returning nil when there is none.
type Checkouts interface {
	Find(source string, w catalog.Worktree) (*cli.LocalCheckout, error)
}

// HelperStatus is the resident helper's ccsync.status.v1 reply.
type HelperStatus struct {
	Build     string
	Scheduler scheduler.Status
}

// Helper is the resident helper's rpc surface: ccsync.status.v1 and
// ccsync.kick.v1. Both wrap ErrUnavailable when the helper is not running.
type Helper interface {
	Status(ctx context.Context) (HelperStatus, error)
	Kick(ctx context.Context, sessionIDs []string) ([]scheduler.Attempt, error)
}

// Installer installs and removes the resident helper and its synckit
// registration.
type Installer interface {
	Install(ctx context.Context, req cli.InstallRequest) (cli.InstallResult, error)
	Uninstall(ctx context.Context, req cli.UninstallRequest) (cli.UninstallResult, error)
}

// Picker restores a checkpoint onto this host.
type Picker interface {
	Pickup(ctx context.Context, req cli.PickupRequest) (cli.PickupResult, error)
}

// Config wires a Service to its dependencies. Network reports this host's
// live network State; Live and Sessions read the native Claude layout; Exec
// replaces the process with argv run in dir under env, returning only on
// failure; Tiers is the effective capture cadence.
type Config struct {
	Catalog    Catalog
	Deliveries Deliveries
	Mesh       Mesh
	Network    func(ctx context.Context) (netpolicy.State, error)
	Orca       Orca
	Live       func(ctx context.Context) (map[claudenative.SessionID]claudenative.LiveProcess, error)
	Sessions   func(ctx context.Context, id claudenative.SessionID) ([]claudenative.Session, error)
	Checkouts  Checkouts
	Helper     Helper
	Installer  Installer
	Picker     Picker
	Serve      func(ctx context.Context) error
	Tiers      func() (cli.CaptureTiers, error)
	Environ    []string
	Exec       func(argv []string, dir string, env []string) error
	Now        func() time.Time
}

// Service is cc-sync's cli.Service.
type Service struct {
	cfg Config
}

var _ cli.Service = (*Service)(nil)

// New builds a Service over cfg.
func New(cfg Config) *Service {
	return &Service{cfg: cfg}
}

// Install delegates to the resident installer.
func (s *Service) Install(ctx context.Context, req cli.InstallRequest) (cli.InstallResult, error) {
	res, err := s.cfg.Installer.Install(ctx, req)
	return res, classify(err)
}

// Uninstall delegates to the resident installer.
func (s *Service) Uninstall(ctx context.Context, req cli.UninstallRequest) (cli.UninstallResult, error) {
	res, err := s.cfg.Installer.Uninstall(ctx, req)
	return res, classify(err)
}

// Pickup delegates to the picker.
func (s *Service) Pickup(ctx context.Context, req cli.PickupRequest) (cli.PickupResult, error) {
	res, err := s.cfg.Picker.Pickup(ctx, req)
	return res, classify(err)
}

// HelperServe runs the resident helper until ctx ends.
func (s *Service) HelperServe(ctx context.Context) error {
	return classify(s.cfg.Serve(ctx))
}

// Status reports the helper, the local network, per-peer delivery, and the
// capture scheduler. An unavailable synckitd degrades the report rather than
// failing it: delivery is marked unavailable with the reason and peers carry
// no delivery state.
func (s *Service) Status(ctx context.Context) (cli.StatusResult, error) {
	reg, err := s.cfg.Mesh.Load()
	if err != nil {
		return cli.StatusResult{}, fmt.Errorf("load mesh: %w", err)
	}
	local, err := s.cfg.Network(ctx)
	if err != nil {
		return cli.StatusResult{}, fmt.Errorf("read network state: %w", err)
	}
	statuses, err := s.cfg.Deliveries.Status(ctx, serviceID)
	availability := cli.DeliveryService{Available: true}
	switch {
	case errors.Is(err, ErrUnavailable):
		availability = cli.DeliveryService{Reason: err.Error()}
	case err != nil:
		return cli.StatusResult{}, fmt.Errorf("delivery status: %w", err)
	}
	tiers, err := s.cfg.Tiers()
	if err != nil {
		return cli.StatusResult{}, fmt.Errorf("capture tiers: %w", err)
	}
	res := cli.StatusResult{
		Local:     cli.LocalHost{Host: host(reg.Self), Network: network(local)},
		Delivery:  availability,
		Scheduler: cli.Scheduler{Tiers: tiers},
	}
	hs, err := s.cfg.Helper.Status(ctx)
	switch {
	case errors.Is(err, ErrUnavailable):
	case err != nil:
		return cli.StatusResult{}, fmt.Errorf("helper status: %w", err)
	default:
		res.Helper = cli.Helper{Running: true, Build: hs.Build}
		res.Scheduler.QueuedByTier = cli.QueuedByTier(hs.Scheduler.QueuedByTier)
		res.Scheduler.Workers = hs.Scheduler.Workers
		res.Scheduler.LastRoundAt = optionalTime(hs.Scheduler.LastRoundAt)
	}
	if res.Peers, err = peers(reg, statuses); err != nil {
		return cli.StatusResult{}, err
	}
	return res, nil
}

// Sync captures the local worktrees now, or only those holding sessionIDs,
// and reports each attempt against the catalog it produced.
func (s *Service) Sync(ctx context.Context, req cli.SyncRequest) (cli.SyncResult, error) {
	attempts, err := s.cfg.Helper.Kick(ctx, req.Sessions)
	if err != nil {
		return cli.SyncResult{}, classify(fmt.Errorf("kick helper: %w", err))
	}
	snap, err := s.cfg.Catalog.Load()
	if err != nil {
		return cli.SyncResult{}, fmt.Errorf("load catalog: %w", err)
	}
	var own catalog.Origin
	for _, o := range snap.Origins {
		if o.Origin == snap.Self {
			own = o
		}
	}
	res := cli.SyncResult{Worktrees: make(cli.Array[cli.SyncedWorktree], 0, len(attempts))}
	for _, a := range attempts {
		res.Worktrees = append(res.Worktrees, synced(own, a))
	}
	return res, nil
}

func synced(own catalog.Origin, a scheduler.Attempt) cli.SyncedWorktree {
	out := cli.SyncedWorktree{WorkspaceID: a.WorktreeID, Sessions: cli.Array[string]{}, Deferred: cli.Array[string]{}}
	switch a.Outcome {
	case scheduler.OutcomeCaptured, scheduler.OutcomeUnchanged:
	default:
		out.Deferred = append(out.Deferred, deferral(string(a.Outcome), a.Reason))
	}
	for _, w := range own.Worktrees {
		if w.ID != a.WorktreeID {
			continue
		}
		for _, cp := range w.Checkpoints {
			if a.Checkpoint != "" && cp.ID != a.Checkpoint {
				continue
			}
			out.Checkpoint = checkpoint(cp)
			for _, sess := range cp.Sessions {
				out.Sessions = append(out.Sessions, sess.ID)
			}
			return out
		}
	}
	return out
}

func deferral(what, reason string) string {
	if reason == "" {
		return what
	}
	return what + ": " + reason
}

func classify(err error) error {
	if errors.Is(err, ErrUnavailable) {
		return cli.Errorf(cli.CodeUnavailable, "%w", err)
	}
	return err
}
