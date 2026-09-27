// Package resident is cc-sync's synckit helper: one per-user process that
// owns the artifact store, the catalog, the capture scheduler, and the
// synckit artifact consumer, and serves the ccsync.* helper methods.
package resident

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/reposync/worktree"
	"golang.org/x/sync/errgroup"

	"github.com/yasyf/cc-sync/internal/catalog"
	"github.com/yasyf/cc-sync/internal/config"
	"github.com/yasyf/cc-sync/internal/consumer"
	"github.com/yasyf/cc-sync/internal/netgate"
	"github.com/yasyf/cc-sync/internal/scheduler"
	"github.com/yasyf/synckit/artifact"
	"github.com/yasyf/synckit/helperruntime"
	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/netpolicy"
	"github.com/yasyf/synckit/rpc"
	"github.com/yasyf/synckit/syncservice"
)

const (
	// DefaultVerifyInterval is how often the background verifier retries
	// deferred checkpoints; a pass the network policy paused retries as soon
	// as the policy allows an origin fetch again.
	DefaultVerifyInterval = 5 * time.Minute
	// DefaultExpireInterval is how often the capture pipeline's expired
	// partial-capture pins are dropped.
	DefaultExpireInterval = 15 * time.Minute
)

// Store is the slice of the synckit artifact store the resident itself
// drives; the capture pipeline receives the concrete store.
type Store interface {
	consumer.Artifacts
	Close() error
}

// Capture is what the capture pipeline builds on.
type Capture[S Store] struct {
	Self      string
	Layout    config.Layout
	Config    config.Config
	Artifacts S
	Code      *worktree.Store
	Catalog   *catalog.Store
	Publisher *catalog.Publisher
	Monitor   netpolicy.Monitor
}

// PinExpirer drops the capture pipeline's expired partial-capture pins.
type PinExpirer interface {
	ExpirePins(ctx context.Context) error
}

// Pipeline is the capture and verification surface the resident schedules.
type Pipeline struct {
	Inventory scheduler.Inventory
	Stamper   scheduler.Stamper
	Capturer  scheduler.Capturer
	Verifier  consumer.CodeVerifier
	Expirer   PinExpirer
}

// Deps injects everything Prepare does not construct itself. Register is
// syncservice.RegisterArtifactConsumer for the concrete store.
type Deps[S Store] struct {
	Layout         config.Layout
	Self           string
	Now            func() time.Time
	OpenArtifacts  func() (S, error)
	Monitor        func() (netpolicy.Monitor, error)
	Register       func(*rpc.Dispatcher, syncservice.ArtifactConsumer, S, netpolicy.Monitor)
	Pipeline       func(Capture[S]) (Pipeline, error)
	VerifyInterval time.Duration
	ExpireInterval time.Duration
}

// Resident is the prepared helper product.
type Resident struct {
	store     Store
	monitor   netpolicy.Monitor
	scheduler *scheduler.Scheduler
	cancel    context.CancelFunc
	done      chan struct{}
}

type service struct {
	*consumer.Consumer
	pins  *pins
	nudge func()
}

// Serve runs the cc-sync helper under synckit's helperruntime until ctx
// ends or the daemon drains.
func Serve[S Store](ctx context.Context, deps Deps[S]) error {
	program, err := daemonkit.Stable()
	if err != nil {
		return fmt.Errorf("resident: program: %w", err)
	}
	d := rpc.NewDispatcher()
	runtime, err := helperruntime.New(helperruntime.Config{
		App:        helperruntime.App{Name: consumer.ServiceID},
		Program:    program,
		Dispatcher: d,
		Prepare: func(c daemonkit.Ctx) (helperruntime.Product, error) {
			r, err := Prepare(c.Context, d, deps, c.Stop)
			if err != nil {
				return nil, err
			}
			return r, nil
		},
	})
	if err != nil {
		return err
	}
	return runtime.Run(ctx)
}

// MeshSelf is this host's synckit mesh identity, the origin its catalog
// block is published under.
func MeshSelf() (string, error) {
	reg, err := hostregistry.Mesh.Load()
	if err != nil {
		return "", fmt.Errorf("resident: load synckit mesh: %w", err)
	}
	if reg.Self == "" {
		return "", errors.New("resident: synckit mesh has no self identity; run synckitd init")
	}
	return reg.Self, nil
}

// SystemMonitor is the live netpolicy monitor with the synckit manual
// metered override.
func SystemMonitor() (netpolicy.Monitor, error) {
	path, err := netpolicy.ManualPath()
	if err != nil {
		return nil, fmt.Errorf("resident: netpolicy manual path: %w", err)
	}
	return netpolicy.NewMonitor(path)
}

// Prepare builds the helper: it ensures the layout and stamp, opens the
// stores, registers the consumer and the ccsync.* methods on d, and starts
// the scheduler, the stamp publisher, and the background verifier. stop
// begins a drain when one of them fails.
func Prepare[S Store](ctx context.Context, d *rpc.Dispatcher, deps Deps[S], stop func(error)) (_ *Resident, err error) {
	layout := deps.Layout
	if err := layout.Ensure(); err != nil {
		return nil, err
	}
	cfg, err := config.Load(layout.ConfigPath)
	if err != nil {
		return nil, err
	}
	cat := catalog.New(layout.CatalogPath, deps.Self, deps.Now)
	publisher := catalog.NewPublisher(cat, layout.StampDir)
	if err := publisher.Ensure(); err != nil {
		return nil, err
	}
	code, err := worktree.OpenStore(layout.CodeStore)
	if err != nil {
		return nil, fmt.Errorf("resident: open code store: %w", err)
	}
	monitor, err := deps.Monitor()
	if err != nil {
		return nil, fmt.Errorf("resident: network monitor: %w", err)
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, monitor.Close())
		}
	}()
	store, err := deps.OpenArtifacts()
	if err != nil {
		return nil, fmt.Errorf("resident: open artifact store: %w", err)
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, store.Close())
		}
	}()
	pipeline, err := deps.Pipeline(Capture[S]{
		Self: deps.Self, Layout: layout, Config: cfg, Artifacts: store, Code: code,
		Catalog: cat, Publisher: publisher, Monitor: monitor,
	})
	if err != nil {
		return nil, fmt.Errorf("resident: capture pipeline: %w", err)
	}

	sched, err := scheduler.New(scheduler.Config{Tiers: cfg.Capture.Scheduler()}, pipeline.Inventory, pipeline.Stamper, pipeline.Capturer, publisher)
	if err != nil {
		return nil, fmt.Errorf("resident: %w", err)
	}
	svc := &service{
		Consumer: consumer.New(consumer.Config{
			Catalog:   cat,
			Publisher: publisher,
			Artifacts: store,
			Verifier:  pipeline.Verifier,
			Network:   monitor,
			StampDir:  layout.StampDir,
		}),
		pins: &pins{path: layout.PinsPath, store: store, now: deps.Now},
	}
	nudges := make(chan struct{}, 1)
	svc.nudge = func() {
		select {
		case nudges <- struct{}{}:
		default:
		}
	}
	deps.Register(d, svc, store, monitor)
	register(d, methods{scheduler: sched, catalog: cat, config: cfg, monitor: monitor, pins: svc.pins})

	run, cancel := context.WithCancel(context.WithoutCancel(ctx))
	group, groupCtx := errgroup.WithContext(run)
	group.Go(func() error { return publisher.Run(groupCtx) })
	group.Go(func() error { return sched.Run(groupCtx) })
	group.Go(func() error {
		return verifyLoop(groupCtx, monitor, cmp.Or(deps.VerifyInterval, DefaultVerifyInterval), nudges, svc.VerifyDeferred)
	})
	group.Go(func() error {
		return expireLoop(groupCtx, cmp.Or(deps.ExpireInterval, DefaultExpireInterval), pipeline.Expirer.ExpirePins)
	})
	r := &Resident{store: store, monitor: monitor, scheduler: sched, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(r.done)
		if err := group.Wait(); err != nil && !errors.Is(err, context.Canceled) {
			stop(err)
		}
	}()
	if err := publisher.Publish(groupCtx); err != nil {
		return nil, errors.Join(err, r.Drain(ctx))
	}
	return r, nil
}

// Drain stops the scheduler at its captures' safe points, then the
// publisher and verifier, within ctx.
func (r *Resident) Drain(ctx context.Context) error {
	stopped := make(chan struct{})
	go func() {
		r.scheduler.Stop()
		r.cancel()
		close(stopped)
	}()
	for _, ch := range []chan struct{}{stopped, r.done} {
		select {
		case <-ch:
		case <-ctx.Done():
			return fmt.Errorf("resident: drain: %w", ctx.Err())
		}
	}
	return nil
}

// Close closes the artifact store and the network monitor.
func (r *Resident) Close(context.Context) error {
	var errs []error
	if err := r.store.Close(); err != nil {
		errs = append(errs, fmt.Errorf("resident: close artifact store: %w", err))
	}
	if err := r.monitor.Close(); err != nil {
		errs = append(errs, fmt.Errorf("resident: close network monitor: %w", err))
	}
	return errors.Join(errs...)
}

// Reconcile sweeps expired pickup pins before the consumer's retention,
// pinning, and artifact GC, so GC collects what the sweep released.
func (s *service) Reconcile(ctx context.Context, itemID string) (syncservice.ReconcileResult, error) {
	if err := s.pins.Sweep(ctx); err != nil {
		return syncservice.ReconcileResult{}, err
	}
	return s.Consumer.Reconcile(ctx, itemID)
}

// ApplyArtifacts wakes the background verifier after a partial apply so a
// missing prerequisite is fetched outside the exclusive apply handler.
func (s *service) ApplyArtifacts(ctx context.Context, change syncservice.ChangeEnvelope, ready []artifact.Ref) (syncservice.ApplyResult, error) {
	result, err := s.Consumer.ApplyArtifacts(ctx, change, ready)
	if err == nil && result.Partial {
		s.nudge()
	}
	return result, err
}

func verifyLoop(ctx context.Context, monitor netpolicy.Monitor, interval time.Duration, nudges <-chan struct{}, verify func(context.Context) error) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		err := verify(ctx)
		var paused *netpolicy.PausedError
		switch {
		case ctx.Err() != nil:
			return ctx.Err()
		case errors.As(err, &paused):
			slog.Info("deferred verification paused by network policy", "reason", paused.Reason)
		case err != nil:
			slog.Warn("deferred verification failed", "err", err)
		}
		if err := awaitPass(ctx, monitor, paused != nil, ticker.C, nudges); err != nil {
			return err
		}
	}
}

func awaitPass(ctx context.Context, monitor netpolicy.Monitor, paused bool, tick <-chan time.Time, nudges <-chan struct{}) error {
	for {
		var changed <-chan struct{}
		var recheck <-chan time.Time
		if paused {
			var state netpolicy.State
			state, changed = monitor.Current()
			if state.Unrestricted() {
				return nil
			}
			recheck = time.After(netgate.Poll)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick:
			return nil
		case <-nudges:
			return nil
		case <-changed:
		case <-recheck:
		}
	}
}

func expireLoop(ctx context.Context, interval time.Duration, expire func(context.Context) error) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := expire(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			slog.Warn("expiring partial-capture pins failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
