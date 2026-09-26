// Package scheduler decides when each worktree is captured. Activity tiers set
// each unit's cadence, a bounded worker pool runs at most one capture per
// repository, a unit whose session metadata and worktree code are both
// unchanged is never recaptured, and catalog stamp bumps are coalesced. It
// takes no network input, so local capture never pauses.
package scheduler

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"
)

var (
	// ErrUnknownSession reports a Kick naming a session that no capture unit holds.
	ErrUnknownSession = errors.New("unknown session")
	// ErrStopped reports a Kick against a scheduler that has stopped.
	ErrStopped = errors.New("scheduler stopped")
)

// Outcome names what became of one due unit. A Capturer reports
// OutcomeCaptured, OutcomeDeferred, OutcomeBusy, OutcomePartial or
// OutcomeMissingLFS; the scheduler itself reports the rest. Every outcome
// but OutcomeCaptured leaves the prior checkpoint current and the unit due
// again: OutcomePartial (resumable budget progress, never Ready) at once, the
// others at the unit's tier interval.
type Outcome string

// The outcomes.
const (
	OutcomeCaptured   Outcome = "captured"
	OutcomeUnchanged  Outcome = "unchanged"
	OutcomeDeferred   Outcome = "deferred"
	OutcomeBusy       Outcome = "busy"
	OutcomePartial    Outcome = "partial"
	OutcomeMissingLFS Outcome = "missing-lfs"
	OutcomeFailed     Outcome = "failed"
	OutcomeRemoved    Outcome = "removed"
)

// Result is a Capturer's report for one unit: the new Checkpoint for
// OutcomeCaptured, or the named Reason the prior checkpoint stays current.
type Result struct {
	Outcome    Outcome `json:"outcome"`
	Checkpoint string  `json:"checkpoint,omitempty"`
	Reason     string  `json:"reason,omitempty"`
}

// Attempt records what became of one due unit and when.
type Attempt struct {
	WorktreeID string    `json:"worktree_id"`
	At         time.Time `json:"at"`
	Result
}

// Inventory reports the current capture units from metadata alone.
type Inventory interface {
	Scan(ctx context.Context) ([]Unit, error)
}

// Stamper digests a unit's worktree code: HEAD, the index, and every dirty or
// untracked path. The scheduler calls it only for a unit that has come due,
// never on the metadata scan.
type Stamper interface {
	CodeStamp(ctx context.Context, u Unit) (string, error)
}

// Capturer captures one unit. It honors ctx cancellation at its safe points;
// an error means the attempt failed outright.
type Capturer interface {
	Capture(ctx context.Context, u Unit) (Result, error)
}

// Publisher announces catalog changes by bumping the synckit stamp. The
// scheduler calls it at most once per StampCoalesce window.
type Publisher interface {
	Publish(ctx context.Context) error
}

// Config tunes a Scheduler. Zero fields take the defaults: 2 capture workers,
// a 30s metadata scan, a 10s stamp coalescing window, and DefaultTiers for
// each zero Tiers field.
type Config struct {
	Workers       int
	ScanInterval  time.Duration
	StampCoalesce time.Duration
	Tiers         Tiers
}

// TierCounts counts units per tier.
type TierCounts struct {
	Human      int `json:"human"`
	Autonomous int `json:"autonomous"`
	Recent     int `json:"recent"`
	Idle       int `json:"idle"`
}

// UnitStatus is one unit's scheduling state.
type UnitStatus struct {
	WorktreeID string    `json:"worktree_id"`
	RepoKey    string    `json:"repo_key"`
	Tier       Tier      `json:"tier"`
	Due        time.Time `json:"due"`
	Running    bool      `json:"running"`
	Last       *Attempt  `json:"last,omitempty"`
}

// Status is a point-in-time snapshot of the scheduler. QueuedByTier counts
// due units waiting for a worker or their repository; Workers counts
// attempts in flight, each a code stamp and any capture it leads to;
// LastRoundAt is the last metadata scan.
type Status struct {
	QueuedByTier TierCounts   `json:"queued_by_tier"`
	Workers      int          `json:"workers"`
	LastRoundAt  time.Time    `json:"last_round_at,omitzero"`
	Units        []UnitStatus `json:"units"`
}

// Scheduler runs captures on the cadence each unit's activity earns.
type Scheduler struct {
	cfg     Config
	inv     Inventory
	stamper Stamper
	capt    Capturer
	pub     Publisher

	kicks   chan kickRequest
	results chan finished
	exited  chan struct{}

	life   sync.Mutex
	halted bool
	cancel context.CancelFunc

	mu        sync.Mutex
	units     map[string]*entry
	running   int
	lastRound time.Time

	dirty     bool
	published time.Time
}

type entry struct {
	unit     Unit
	tier     Tier
	interval time.Duration
	humanAt  time.Time
	checked  time.Time
	captured stamps
	urgent   bool
	running  bool
	removed  bool
	kickers  []*kick
	inflight []*kick
	last     *Attempt
}

type stamps struct {
	meta, code string
}

type finished struct {
	unit   Unit
	stamps stamps
	result Result
	err    error
}

type kickRequest struct {
	sessionIDs []string
	reply      chan kickReply
}

type kickReply struct {
	attempts []Attempt
	err      error
}

type kick struct {
	pending  int
	attempts []Attempt
	reply    chan kickReply
}

// New builds a Scheduler; Run drives it. It reports ErrInvalidTiers when
// cfg.Tiers, defaults applied, fails Tiers.Validate.
func New(cfg Config, inv Inventory, stamper Stamper, capt Capturer, pub Publisher) (*Scheduler, error) {
	cfg.Workers = cmp.Or(cfg.Workers, 2)
	cfg.ScanInterval = cmp.Or(cfg.ScanInterval, 30*time.Second)
	cfg.StampCoalesce = cmp.Or(cfg.StampCoalesce, 10*time.Second)
	cfg.Tiers = cfg.Tiers.orDefaults()
	if err := cfg.Tiers.Validate(); err != nil {
		return nil, fmt.Errorf("new scheduler: %w", err)
	}
	return &Scheduler{
		cfg:     cfg,
		inv:     inv,
		stamper: stamper,
		capt:    capt,
		pub:     pub,
		kicks:   make(chan kickRequest),
		results: make(chan finished, cfg.Workers),
		exited:  make(chan struct{}),
		units:   map[string]*entry{},
	}, nil
}

// Run scans and captures until ctx is canceled or Stop is called. Either one
// cancels an inventory scan and in-flight captures at once; Run then waits for
// the captures and flushes a pending publish. It returns nil after Stop,
// without scanning when Stop came first, and ctx's error after cancellation.
func (s *Scheduler) Run(ctx context.Context) error {
	defer close(s.exited)
	work, cancel := context.WithCancel(ctx)
	defer cancel()
	if !s.start(cancel) {
		return nil
	}
	var workers sync.WaitGroup
	s.rescan(work)
	nextScan := time.Now().Add(s.cfg.ScanInterval)
	timer := time.NewTimer(s.cfg.ScanInterval)
	defer timer.Stop()
	for work.Err() == nil {
		s.dispatch(work, &workers)
		s.publish(work)
		timer.Reset(time.Until(s.wake(nextScan)))
		select {
		case <-work.Done():
		case f := <-s.results:
			s.finish(f)
		case req := <-s.kicks:
			s.kick(work, req)
			nextScan = time.Now().Add(s.cfg.ScanInterval)
		case <-timer.C:
			if !time.Now().Before(nextScan) {
				s.rescan(work)
				nextScan = time.Now().Add(s.cfg.ScanInterval)
			}
		}
	}
	s.drain(ctx, &workers)
	return ctx.Err()
}

// Kick scans at once, makes the units holding sessionIDs (every unit when
// none are named) due now, and waits for each unit's attempt. A unit whose
// metadata and code stamps are unchanged reports OutcomeUnchanged without
// capturing.
func (s *Scheduler) Kick(ctx context.Context, sessionIDs ...string) ([]Attempt, error) {
	req := kickRequest{sessionIDs: sessionIDs, reply: make(chan kickReply, 1)}
	select {
	case s.kicks <- req:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.exited:
		return nil, ErrStopped
	}
	select {
	case r := <-req.reply:
		return r.attempts, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.exited:
		select {
		case r := <-req.reply:
			return r.attempts, r.err
		default:
			return nil, ErrStopped
		}
	}
}

// Stop cancels an inventory scan and in-flight captures at their next safe
// point and returns once Run has drained and returned. Before Run starts, it
// returns at once and makes Run return nil. Repeated calls are harmless.
func (s *Scheduler) Stop() {
	s.life.Lock()
	s.halted = true
	cancel := s.cancel
	s.life.Unlock()
	if cancel == nil {
		return
	}
	cancel()
	<-s.exited
}

// Status snapshots the scheduler, units in dispatch priority order.
func (s *Scheduler) Status() Status {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	entries := s.byPriority()
	st := Status{Workers: s.running, LastRoundAt: s.lastRound, Units: make([]UnitStatus, 0, len(entries))}
	for _, e := range entries {
		if e.queued(now) {
			st.QueuedByTier.add(e.tier)
		}
		st.Units = append(st.Units, UnitStatus{
			WorktreeID: e.unit.WorktreeID,
			RepoKey:    e.unit.RepoKey,
			Tier:       e.tier,
			Due:        later(e.due(), now),
			Running:    e.running,
			Last:       e.last,
		})
	}
	return st
}

func (s *Scheduler) start(cancel context.CancelFunc) bool {
	s.life.Lock()
	defer s.life.Unlock()
	s.cancel = cancel
	return !s.halted
}

func (s *Scheduler) rescan(ctx context.Context) {
	if err := s.scan(ctx); err != nil && ctx.Err() == nil {
		slog.Warn("scheduler scan failed", "err", err)
	}
}

func (s *Scheduler) scan(ctx context.Context) error {
	units, err := s.inv.Scan(ctx)
	if err != nil {
		return fmt.Errorf("scan inventory: %w", err)
	}
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastRound = now
	seen := make(map[string]bool, len(units))
	for _, u := range units {
		seen[u.WorktreeID] = true
		e, ok := s.units[u.WorktreeID]
		if !ok {
			e = &entry{}
			s.units[u.WorktreeID] = e
		}
		e.unit, e.removed = u, false
		e.tier, e.interval = u.Classify(now, s.cfg.Tiers)
		e.humanAt = u.humanAt()
	}
	for id, e := range s.units {
		if seen[id] {
			continue
		}
		e.removed = true
		if !e.running {
			s.forget(e, now)
		}
	}
	return nil
}

func (s *Scheduler) dispatch(ctx context.Context, workers *sync.WaitGroup) {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	busy := map[string]bool{}
	for _, e := range s.units {
		if e.running {
			busy[e.unit.RepoKey] = true
		}
	}
	for _, e := range s.byPriority() {
		if e.running || e.due().After(now) {
			continue
		}
		if s.running == s.cfg.Workers || busy[e.unit.RepoKey] {
			continue
		}
		busy[e.unit.RepoKey] = true
		s.running++
		e.running, e.checked, e.urgent = true, now, false
		e.inflight, e.kickers = e.kickers, nil
		u, last := e.unit, e.captured
		workers.Go(func() {
			st, res, err := s.attempt(ctx, u, last)
			s.results <- finished{unit: u, stamps: st, result: res, err: err}
		})
	}
}

func (s *Scheduler) attempt(ctx context.Context, u Unit, last stamps) (stamps, Result, error) {
	code, err := s.stamper.CodeStamp(ctx, u)
	if err != nil {
		return stamps{}, Result{}, fmt.Errorf("code stamp: %w", err)
	}
	st := stamps{meta: u.MetaStamp, code: code}
	if st == last {
		return st, Result{Outcome: OutcomeUnchanged}, nil
	}
	res, err := s.capt.Capture(ctx, u)
	return st, res, err
}

func (s *Scheduler) finish(f finished) {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.running--
	e := s.units[f.unit.WorktreeID]
	e.running = false
	a := Attempt{WorktreeID: f.unit.WorktreeID, At: now, Result: f.result}
	switch {
	case f.err != nil:
		a.Result = Result{Outcome: OutcomeFailed, Reason: f.err.Error()}
		slog.Warn("capture failed", "worktree", f.unit.WorktreeID, "repo", f.unit.RepoKey, "err", f.err)
	case f.result.Outcome == OutcomeCaptured:
		e.captured = f.stamps
		s.dirty = true
	case f.result.Outcome == OutcomeUnchanged:
	case f.result.Outcome == OutcomePartial:
		e.urgent = true
		s.dirty = true
	default:
		s.dirty = true
	}
	e.settle(a, e.inflight)
	e.inflight = nil
	if e.removed {
		s.forget(e, now)
	}
}

func (s *Scheduler) kick(ctx context.Context, req kickRequest) {
	if err := s.scan(ctx); err != nil {
		if ctx.Err() != nil {
			err = ErrStopped
		}
		req.reply <- kickReply{err: fmt.Errorf("kick: %w", err)}
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	targets, err := s.resolve(req.sessionIDs)
	if err != nil {
		req.reply <- kickReply{err: err}
		return
	}
	if len(targets) == 0 {
		req.reply <- kickReply{}
		return
	}
	k := &kick{pending: len(targets), reply: req.reply}
	for _, e := range targets {
		e.urgent = true
		e.kickers = append(e.kickers, k)
	}
}

func (s *Scheduler) resolve(sessionIDs []string) ([]*entry, error) {
	var live []*entry
	for _, e := range s.units {
		if !e.removed {
			live = append(live, e)
		}
	}
	if len(sessionIDs) == 0 {
		return live, nil
	}
	var targets []*entry
	var unknown []string
	for _, id := range sessionIDs {
		i := slices.IndexFunc(live, func(e *entry) bool { return e.unit.holds(id) })
		switch {
		case i < 0:
			unknown = append(unknown, id)
		case !slices.Contains(targets, live[i]):
			targets = append(targets, live[i])
		}
	}
	if len(unknown) > 0 {
		return nil, fmt.Errorf("kick %s: %w", strings.Join(unknown, ", "), ErrUnknownSession)
	}
	return targets, nil
}

func (s *Scheduler) forget(e *entry, now time.Time) {
	delete(s.units, e.unit.WorktreeID)
	e.settle(Attempt{WorktreeID: e.unit.WorktreeID, At: now, Result: Result{Outcome: OutcomeRemoved}}, e.kickers)
	e.kickers = nil
}

func (s *Scheduler) publish(ctx context.Context) {
	if !s.dirty || time.Now().Before(s.published.Add(s.cfg.StampCoalesce)) {
		return
	}
	s.publishNow(ctx)
}

func (s *Scheduler) publishNow(ctx context.Context) {
	s.published = time.Now()
	if err := s.pub.Publish(ctx); err != nil {
		slog.Warn("publish catalog stamp failed", "err", err)
		return
	}
	s.dirty = false
}

func (s *Scheduler) wake(nextScan time.Time) time.Time {
	now := time.Now()
	wake := nextScan
	if s.dirty {
		wake = earlier(wake, s.published.Add(s.cfg.StampCoalesce))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range s.units {
		if due := e.due(); !e.running && due.After(now) {
			wake = earlier(wake, due)
		}
	}
	return wake
}

func (s *Scheduler) drain(ctx context.Context, workers *sync.WaitGroup) {
	for s.running > 0 {
		s.finish(<-s.results)
	}
	workers.Wait()
	if s.dirty {
		s.publishNow(context.WithoutCancel(ctx))
	}
}

func (s *Scheduler) byPriority() []*entry {
	entries := make([]*entry, 0, len(s.units))
	for _, e := range s.units {
		entries = append(entries, e)
	}
	slices.SortFunc(entries, func(a, b *entry) int {
		return cmp.Or(
			cmp.Compare(a.tier, b.tier),
			b.humanAt.Compare(a.humanAt),
			cmp.Compare(a.unit.WorktreeID, b.unit.WorktreeID),
		)
	})
	return entries
}

func (e *entry) due() time.Time {
	if e.urgent {
		return time.Time{}
	}
	return e.checked.Add(e.interval)
}

func (e *entry) queued(now time.Time) bool {
	return !e.running && !e.due().After(now)
}

func (e *entry) settle(a Attempt, kicks []*kick) {
	e.last = &a
	for _, k := range kicks {
		k.record(a)
	}
}

func (k *kick) record(a Attempt) {
	k.attempts = append(k.attempts, a)
	k.pending--
	if k.pending > 0 {
		return
	}
	slices.SortFunc(k.attempts, func(a, b Attempt) int { return cmp.Compare(a.WorktreeID, b.WorktreeID) })
	k.reply <- kickReply{attempts: k.attempts}
}

func (c *TierCounts) add(t Tier) {
	switch t {
	case TierHuman:
		c.Human++
	case TierAutonomous:
		c.Autonomous++
	case TierRecent:
		c.Recent++
	case TierIdle:
		c.Idle++
	}
}

func earlier(a, b time.Time) time.Time {
	if b.Before(a) {
		return b
	}
	return a
}
