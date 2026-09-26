package catalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/yasyf/daemonkit/durable"
	"github.com/yasyf/synckit/artifact"
	"github.com/yasyf/synckit/syncservice"
)

const (
	stateIdentity = "cc-sync-catalog-state-v1"
	stateVersion  = 1
	lockTimeout   = 30 * time.Second
)

// Readiness reasons recorded for a relayed checkpoint that is not ready: its
// artifact closure is incomplete, its code was never verified, or its code
// lacks prerequisites only an origin fetch can supply.
const (
	MissingClosure       = "artifact-closure"
	MissingCode          = "code-unverified"
	MissingPrerequisites = "prerequisites-missing"
)

var (
	// ErrExpired reports a recorded checkpoint that expired before retention.
	ErrExpired = errors.New("catalog: checkpoint already expired")
	// ErrSuperseded reports a recorded checkpoint that claims no retention
	// class because newer checkpoints of its worktree already hold them.
	ErrSuperseded = errors.New("catalog: checkpoint superseded")
	// ErrOriginBlockConflict refuses a change carrying an origin block at a
	// revision this host already holds with different content.
	ErrOriginBlockConflict = errors.New("catalog: origin-block-conflict")
)

// Readiness is a relayed checkpoint's local pick-up state. A host's own
// checkpoints are always ready.
type Readiness struct {
	Ready    bool     `json:"ready"`
	Missing  []string `json:"missing,omitempty"`
	Deferred string   `json:"deferred,omitempty"`
}

// Evidence is what a receiver established before settling checkpoints: the
// roots whose artifact closures its store holds, and code verdicts by
// checkpoint ID.
type Evidence struct {
	Roots    map[artifact.Digest]bool
	Verified map[string]Readiness
}

// Snapshot is a read-only view of the catalog as of this host's clock.
// Carried maps each of this host's checkpoints to the export revision since
// which every export has carried its root.
type Snapshot struct {
	Self      string
	Origins   []Origin
	Readiness map[string]Readiness
	Carried   map[string]uint64
}

// Assurance is what a peer's acknowledgment establishes about one of this
// host's checkpoints.
type Assurance string

// The assurances a peer's acknowledgment can give: none, the checkpoint held
// but not a complete recovery point, or a complete recovery point durable on
// the peer.
const (
	AssuranceNone    Assurance = ""
	AssuranceHeld    Assurance = "held"
	AssuranceDurable Assurance = "durable"
)

// HeldReady reports whether this host holds checkpoint cp of origin's block
// ready: its root closure is in the local store and the code its root
// references, a mixed checkpoint's older code included, is verified. A host's
// own checkpoints are always held ready.
func (s Snapshot) HeldReady(origin string, cp Checkpoint) bool {
	return origin == s.Self || s.Readiness[cp.ID].Ready
}

// PickupReady reports whether cp is a complete recovery point held ready
// here, the only kind a default pick-up or list targets.
func (s Snapshot) PickupReady(origin string, cp Checkpoint) bool {
	return cp.Complete() && s.HeldReady(origin, cp)
}

// AssuranceOf reports what acked, the source revision a peer last
// acknowledged, establishes about this host's checkpoint cp at now. A peer
// acknowledges a change only once it holds ready every checkpoint the change
// carries that is unexpired by its clock, so acked at or past the revision
// since which exports have carried cp means the peer holds cp: durably when
// cp is Complete, as held but not a complete recovery point otherwise. An
// acknowledgment is never assurance past cp.ExpiresAt on this host's clock,
// and a peer whose clock runs ahead of this host's disposes of cp as expired,
// unheld, that much sooner.
func (s Snapshot) AssuranceOf(cp Checkpoint, acked uint64, now time.Time) Assurance {
	since, carried := s.Carried[cp.ID]
	switch {
	case !carried || acked < since || !now.Before(cp.ExpiresAt):
		return AssuranceNone
	case cp.Complete():
		return AssuranceDurable
	}
	return AssuranceHeld
}

// ReadinessOf reports checkpoint cp of origin's block on this host. A mixed
// checkpoint is never ready: it reports its deferred code beside whatever
// else is missing.
func (s Snapshot) ReadinessOf(origin string, cp Checkpoint) Readiness {
	r := Readiness{Ready: true}
	if origin != s.Self {
		r = s.Readiness[cp.ID]
	}
	if cp.Mixed() {
		r.Ready, r.Deferred = false, cp.Deferred
	}
	return r
}

// Exported is one export: the payload, its canonical bytes, and the source
// revision those bytes carry.
type Exported struct {
	Payload  Payload
	Data     []byte
	Revision uint64
}

// GCResult reports one retention and expiry pass: checkpoints removed,
// checkpoints pick-up ready here, and the roots of every retained checkpoint.
type GCResult struct {
	Removed int
	Ready   int
	Roots   []artifact.Ref
}

// Store is the durable catalog of one host, guarded by a cross-process lock.
type Store struct {
	path string
	self string
	now  func() time.Time
}

type fence struct {
	Origin   string `json:"origin"`
	Revision uint64 `json:"revision"`
	Digest   string `json:"digest"`
}

type ledger struct {
	Revision uint64    `json:"revision"`
	Digest   string    `json:"digest"`
	Roots    string    `json:"roots"`
	AsOf     time.Time `json:"as_of"`
}

type state struct {
	Identity  string                `json:"identity"`
	Version   uint64                `json:"version"`
	Self      string                `json:"self"`
	Origins   []Origin              `json:"origins"`
	Relays    []Origin              `json:"relays"`
	Fences    []fence               `json:"fences"`
	Readiness map[string]Readiness  `json:"readiness"`
	Receipts  []syncservice.Receipt `json:"receipts"`
	Export    ledger                `json:"export"`
	Carried   map[string]uint64     `json:"carried"`
}

// New returns the catalog persisted at path, locked through path+".lock",
// for host self, reading the time from now.
func New(path, self string, now func() time.Time) *Store {
	return &Store{path: path, self: self, now: now}
}

func (st state) Validate() error {
	if st.Identity != stateIdentity || st.Version != stateVersion || st.Self == "" {
		return fmt.Errorf("%w: state identity %q version %d self %q", ErrInvalid, st.Identity, st.Version, st.Self)
	}
	if err := validateOrigins(st.Origins); err != nil {
		return err
	}
	if err := validateOrigins(st.Relays); err != nil {
		return fmt.Errorf("relays: %w", err)
	}
	if err := strictlyIncreasing(st.Fences, func(f fence) string { return f.Origin }); err != nil {
		return fmt.Errorf("fences: %w", err)
	}
	for _, o := range st.Origins {
		if f, ok := findFence(st.Fences, o.Origin); o.Origin != st.Self && (!ok || f.Revision != o.Revision) {
			return fmt.Errorf("%w: held block %s revision %d is not fenced", ErrInvalid, o.Origin, o.Revision)
		}
	}
	for _, r := range st.Relays {
		if f, ok := findFence(st.Fences, r.Origin); r.Origin == st.Self || !ok || r.Revision > f.Revision {
			return fmt.Errorf("%w: relayed block %s revision %d is past its fence", ErrInvalid, r.Origin, r.Revision)
		}
	}
	relayed := st.relayedIDs()
	for id := range st.Readiness {
		if !relayed[id] {
			return fmt.Errorf("%w: readiness for unknown checkpoint %s", ErrInvalid, id)
		}
	}
	if err := strictlyIncreasing(st.Receipts, func(r syncservice.Receipt) string { return r.Origin }); err != nil {
		return fmt.Errorf("receipts: %w", err)
	}
	if e := st.Export; (e.Revision == 0) != (e.Digest == "") || (e.Revision == 0) != e.AsOf.IsZero() {
		return fmt.Errorf("%w: export ledger revision %d digest %q as of %s", ErrInvalid, e.Revision, e.Digest, e.AsOf)
	}
	return nil
}

// Load reads the catalog without locking; rename-atomic writes keep it
// consistent. Checkpoints and tombstones expired by this host's clock are
// hidden.
func (s *Store) Load() (Snapshot, error) {
	st, err := s.read()
	if err != nil {
		return Snapshot{}, err
	}
	now := s.now().UTC()
	origins := make([]Origin, 0, len(st.Origins))
	for _, o := range st.Origins {
		origins = append(origins, expired(o, now))
	}
	return Snapshot{Self: st.Self, Origins: origins, Readiness: st.Readiness, Carried: st.Carried}, nil
}

// Record adds or replaces cp in this host's block under worktree wt, applies
// retention to that worktree, and clears any tombstone for it, moving the
// block revision only when the block changes. It derives the checkpoint's
// ID, expiry, and classes and returns the checkpoint as retained. A record
// makes its checkpoint the newest of those captured at the same instant.
func (s *Store) Record(ctx context.Context, wt Worktree, cp Checkpoint) (Checkpoint, error) {
	cp = normalized(s.self, wt.ID, cp)
	var recorded Checkpoint
	err := s.update(ctx, func(st *state, now time.Time) (bool, error) {
		own := st.ensure(s.self)
		before, err := digestJSON(*own)
		if err != nil {
			return false, err
		}
		i, found := findWorktree(own.Worktrees, wt.ID)
		if !found {
			own.Worktrees = slices.Insert(own.Worktrees, i, Worktree{ID: wt.ID})
		}
		w := &own.Worktrees[i]
		w.Repo, w.Orca = wt.Repo, wt.Orca
		if w.Orca != nil {
			orca := *w.Orca
			orca.Freshness = orca.Freshness.UTC()
			w.Orca = &orca
		}
		cp.Revision = nextRevision(own.Revision, now)
		if k := slices.IndexFunc(w.Checkpoints, func(c Checkpoint) bool { return c.CapturedAt.Equal(cp.CapturedAt) }); k >= 0 && w.Checkpoints[k].ID == cp.ID {
			cp.Revision = w.Checkpoints[k].Revision
		}
		others := slices.DeleteFunc(w.Checkpoints, func(c Checkpoint) bool { return c.ID == cp.ID })
		w.Checkpoints = Retain(append(others, cp), now)
		j := slices.IndexFunc(w.Checkpoints, func(c Checkpoint) bool { return c.ID == cp.ID })
		switch {
		case j < 0 && !now.Before(cp.ExpiresAt):
			return false, fmt.Errorf("%w: %s expired at %s", ErrExpired, cp.ID, cp.ExpiresAt)
		case j < 0:
			return false, fmt.Errorf("%w: %s captured at %s holds no class beside newer checkpoints of %s", ErrSuperseded, cp.ID, cp.CapturedAt, wt.ID)
		}
		recorded = w.Checkpoints[j]
		own.Tombstones = slices.DeleteFunc(own.Tombstones, func(t Tombstone) bool { return t.ID == wt.ID })
		after, err := digestJSON(*own)
		if err != nil || after == before {
			return false, err
		}
		own.Revision = nextRevision(own.Revision, now)
		return true, nil
	})
	return recorded, err
}

// Remove drops worktree id from this host's block and tombstones it.
func (s *Store) Remove(ctx context.Context, id string) error {
	return s.update(ctx, func(st *state, now time.Time) (bool, error) {
		own, ok := st.block(s.self)
		if !ok {
			return false, nil
		}
		i, found := findWorktree(own.Worktrees, id)
		if !found {
			return false, nil
		}
		own.Revision = nextRevision(own.Revision, now)
		own.tombstone(own.Worktrees[i], now)
		own.Worktrees = slices.Delete(own.Worktrees, i, i+1)
		return true, nil
	})
}

// GC applies retention to this host's block, moving its revision and
// tombstoning emptied worktrees when anything changes. Relayed blocks are
// never edited: a held or relayed block is forgotten once every checkpoint
// and tombstone in it has expired here, and its fence is kept.
func (s *Store) GC(ctx context.Context) (GCResult, error) {
	var res GCResult
	err := s.update(ctx, func(st *state, now time.Time) (bool, error) {
		before, err := durable.Marshal(*st)
		if err != nil {
			return false, err
		}
		total := st.count()
		st.Origins = slices.DeleteFunc(st.Origins, func(o Origin) bool { return o.Origin != st.Self && lapsed(o, now) })
		if own, ok := st.block(st.Self); ok {
			*own = retainOwn(*own, now)
		}
		st.Relays = slices.DeleteFunc(st.Relays, func(o Origin) bool { return lapsed(o, now) })
		st.pruneReadiness()
		res = GCResult{Removed: total - st.count(), Ready: st.ready(now), Roots: st.roots(now)}
		after, err := durable.Marshal(*st)
		if err != nil {
			return false, err
		}
		return !bytes.Equal(before, after), nil
	})
	return res, err
}

// Export returns this host's block and every relayed block, verbatim. The
// source revision moves to max(previous+1, now in Unix microseconds), and
// AsOf to now, exactly when the blocks or the roots they derive change.
func (s *Store) Export(ctx context.Context) (Exported, error) {
	var out Exported
	err := s.update(ctx, func(st *state, now time.Time) (bool, error) {
		p := st.exportable()
		body, roots, refs, err := exportDigests(p, now)
		if err != nil {
			return false, err
		}
		changed := body != st.Export.Digest || roots != st.Export.Roots
		if changed {
			st.Export = ledger{Revision: nextRevision(st.Export.Revision, now), Digest: body, Roots: roots, AsOf: now}
			st.Carried = st.carriedSince(refs, now)
		}
		p.AsOf = st.Export.AsOf
		data, err := Encode(p)
		if err != nil {
			return false, err
		}
		out = Exported{Payload: p, Data: data, Revision: st.Export.Revision}
		return changed, nil
	})
	return out, err
}

// Digest returns the digest of the blocks and roots Export would carry now.
func (s *Store) Digest() (string, error) {
	st, err := s.read()
	if err != nil {
		return "", err
	}
	body, roots, _, err := exportDigests(st.exportable(), s.now().UTC())
	if err != nil {
		return "", err
	}
	return digestOf([]byte(body + roots)), nil
}

// Fence decides change against the receipt held for its origin, without
// locking.
func (s *Store) Fence(change syncservice.ChangeEnvelope) (syncservice.FenceDecision, syncservice.ApplyResult, error) {
	st, err := s.read()
	if err != nil {
		return 0, syncservice.ApplyResult{}, err
	}
	return syncservice.Fence(st.receipt(change.Origin), change)
}

// Unverified returns the unexpired relayed checkpoints p carries that
// applying p leaves held here and not yet ready, whether their block is
// admitted or older than its origin's fence. It refuses p with
// ErrOriginBlockConflict exactly when Apply would.
func (s *Store) Unverified(p Payload) ([]Checkpoint, error) {
	st, err := s.read()
	if err != nil {
		return nil, err
	}
	admitted, err := st.admit(p)
	if err != nil {
		return nil, err
	}
	now := s.now().UTC()
	for _, in := range admitted {
		if err := st.hold(in, now); err != nil {
			return nil, err
		}
	}
	return slices.DeleteFunc(st.carriedHeld(p, now), func(cp Checkpoint) bool { return st.Readiness[cp.ID].Ready }), nil
}

// Pending returns every unexpired held relayed checkpoint not yet ready
// here: what background verification still has to settle.
func (s *Store) Pending() ([]Checkpoint, error) {
	st, err := s.read()
	if err != nil {
		return nil, err
	}
	held := slices.DeleteFunc(slices.Clone(st.Origins), func(o Origin) bool { return o.Origin == st.Self })
	return st.unready(held, s.now().UTC()), nil
}

// Apply merges p, the decoded payload of change, under the change's fence.
// Per relayed origin, a block older than its fence never replaces the held
// block, a newer one replaces it verbatim, and an equal one must match the
// fenced digest or the change is refused with ErrOriginBlockConflict and
// nothing is recorded. A newer block whose checkpoints and tombstones have
// all expired by this host's clock is disposed of: fenced but never held, so
// none of it is recorded, ready, or relayed. A held checkpoint is ready only
// when its root closure is in ev.Roots and its code verdict, a mixed
// checkpoint's included, is ready; a held block becomes the relayed one once
// every unexpired root in it is closure-complete, verified or not. The change
// is acknowledged as processed, and its receipt persisted, only when every
// carried root of an unexpired checkpoint is closure-complete and every
// unexpired relayed checkpoint it carries that its origin's held block still
// carries, a stale block's included, is ready here, even when it held
// nothing; otherwise the merge is kept and the result is Partial with the
// prior receipt.
func (s *Store) Apply(ctx context.Context, change syncservice.ChangeEnvelope, p Payload, ev Evidence) (syncservice.ApplyResult, error) {
	var res syncservice.ApplyResult
	err := s.update(ctx, func(st *state, now time.Time) (bool, error) {
		held := st.receipt(change.Origin)
		decision, fenced, err := syncservice.Fence(held, change)
		if err != nil {
			return false, err
		}
		if decision != syncservice.FenceApply {
			res = fenced
			return false, nil
		}
		admitted, err := st.admit(p)
		if err != nil {
			return false, err
		}
		unexpiredRoots := make(map[artifact.Digest]bool)
		for _, o := range p.Origins {
			for _, cp := range live(o, now) {
				unexpiredRoots[cp.Root.Digest] = true
			}
		}
		carried := make(map[artifact.Digest]bool, len(change.Artifacts))
		complete := true
		for _, root := range change.Artifacts {
			carried[root.Digest] = true
			complete = complete && (ev.Roots[root.Digest] || !unexpiredRoots[root.Digest])
		}
		for _, in := range admitted {
			if err := st.hold(in, now); err != nil {
				return false, err
			}
		}
		holding := st.carriedHeld(p, now)
		for _, cp := range holding {
			r := st.Readiness[cp.ID]
			switch v, verified := ev.Verified[cp.ID]; {
			case r.Ready:
			case !ev.Roots[cp.Root.Digest]:
				r = Readiness{Missing: []string{MissingClosure}}
			case verified:
				r = v
			default:
				r = Readiness{Missing: []string{MissingCode}}
			}
			st.Readiness[cp.ID] = r
		}
		for _, in := range p.Origins {
			if in.Origin != st.Self {
				st.promote(in.Origin, ev.Roots, now)
			}
		}
		st.pruneReadiness()
		for _, cp := range holding {
			complete = complete && (st.Readiness[cp.ID].Ready || !carried[cp.Root.Digest])
		}
		if !complete {
			res = syncservice.ApplyResult{AckedRevision: syncservice.NewRevision(0), Partial: true}
			if held != nil {
				res.AckedRevision = held.Revision
			}
			return true, nil
		}
		st.setReceipt(change.Receipt())
		res = syncservice.ApplyResult{AckedRevision: change.SourceRevision}
		return true, nil
	})
	return res, err
}

// Settle records background verdicts for held relayed checkpoints not yet
// ready and relays every held block whose unexpired roots are now all
// closure-complete.
func (s *Store) Settle(ctx context.Context, ev Evidence) error {
	return s.update(ctx, func(st *state, now time.Time) (bool, error) {
		before, err := durable.Marshal(*st)
		if err != nil {
			return false, err
		}
		for _, o := range st.Origins {
			if o.Origin == st.Self {
				continue
			}
			for _, cp := range live(o, now) {
				if v, ok := ev.Verified[cp.ID]; ok && !st.Readiness[cp.ID].Ready {
					st.Readiness[cp.ID] = v
				}
			}
			st.promote(o.Origin, ev.Roots, now)
		}
		after, err := durable.Marshal(*st)
		if err != nil {
			return false, err
		}
		return !bytes.Equal(before, after), nil
	})
}

func (s *Store) update(ctx context.Context, fn func(st *state, now time.Time) (bool, error)) (err error) {
	ctx, cancel := context.WithTimeout(ctx, lockTimeout)
	defer cancel()
	lock, err := durable.AcquireLock(ctx, s.path+".lock")
	if err != nil {
		return fmt.Errorf("catalog: lock: %w", err)
	}
	defer func() { err = errors.Join(err, lock.Close()) }()
	st, err := s.read()
	if err != nil {
		return err
	}
	changed, err := fn(&st, s.now().UTC())
	if err != nil || !changed {
		return err
	}
	data, err := durable.Marshal(st)
	if err != nil {
		return fmt.Errorf("catalog: encode: %w", err)
	}
	return durable.WriteFile(s.path, data, 0o600)
}

func (s *Store) read() (state, error) {
	st, err := durable.ReadFile[state](s.path)
	if errors.Is(err, os.ErrNotExist) {
		return state{Identity: stateIdentity, Version: stateVersion, Self: s.self, Readiness: map[string]Readiness{}}, nil
	}
	if err != nil {
		return state{}, fmt.Errorf("catalog: read: %w", err)
	}
	if st.Self != s.self {
		return state{}, fmt.Errorf("%w: catalog belongs to %q, not %q", ErrInvalid, st.Self, s.self)
	}
	return st, nil
}

func normalized(origin, worktreeID string, cp Checkpoint) Checkpoint {
	cp.ID = CheckpointID(origin, worktreeID, cp.Root)
	cp.CapturedAt = cp.CapturedAt.UTC()
	cp.SourceActivityAt = cp.SourceActivityAt.UTC()
	cp.ExpiresAt = cp.SourceActivityAt.Add(ExpiryWindow)
	cp.Code.CapturedAt = cp.Code.CapturedAt.UTC()
	cp.Sessions = slices.Clone(cp.Sessions)
	for i := range cp.Sessions {
		cp.Sessions[i].LastActivity = cp.Sessions[i].LastActivity.UTC()
		cp.Sessions[i].LastHumanActivity = cp.Sessions[i].LastHumanActivity.UTC()
	}
	slices.SortFunc(cp.Sessions, func(a, b Session) int { return strings.Compare(a.ID, b.ID) })
	return cp
}

func nextRevision(prev uint64, now time.Time) uint64 {
	return max(prev+1, uint64(now.UnixMicro()))
}

func searchOrigin(origins []Origin, name string) (int, bool) {
	return slices.BinarySearchFunc(origins, name, func(o Origin, name string) int { return strings.Compare(o.Origin, name) })
}

func findFence(fences []fence, origin string) (fence, bool) {
	i, found := slices.BinarySearchFunc(fences, origin, func(f fence, origin string) int { return strings.Compare(f.Origin, origin) })
	if !found {
		return fence{}, false
	}
	return fences[i], true
}

func (st *state) block(origin string) (*Origin, bool) {
	i, found := searchOrigin(st.Origins, origin)
	if !found {
		return nil, false
	}
	return &st.Origins[i], true
}

func (st *state) relay(origin string) (Origin, bool) {
	i, found := searchOrigin(st.Relays, origin)
	if !found {
		return Origin{}, false
	}
	return st.Relays[i], true
}

func (st *state) ensure(origin string) *Origin {
	i, found := searchOrigin(st.Origins, origin)
	if !found {
		st.Origins = slices.Insert(st.Origins, i, Origin{Origin: origin})
	}
	return &st.Origins[i]
}

func (o *Origin) tombstone(w Worktree, now time.Time) {
	last := now
	for _, cp := range w.Checkpoints {
		if cp.ExpiresAt.After(last) {
			last = cp.ExpiresAt
		}
	}
	i, _ := slices.BinarySearchFunc(o.Tombstones, w.ID, func(t Tombstone, id string) int { return strings.Compare(t.ID, id) })
	o.Tombstones = slices.Insert(o.Tombstones, i, Tombstone{ID: w.ID, Revision: o.Revision, DeletedAt: now, ExpiresAt: last.Add(ExpiryWindow)})
}

func retainOwn(o Origin, now time.Time) Origin {
	changed := false
	var worktrees, emptied []Worktree
	for _, w := range o.Worktrees {
		kept := Retain(w.Checkpoints, now)
		changed = changed || !slices.EqualFunc(kept, w.Checkpoints, func(a, b Checkpoint) bool {
			return a.ID == b.ID && slices.Equal(a.Classes, b.Classes)
		})
		if len(kept) == 0 {
			emptied = append(emptied, w)
			continue
		}
		w.Checkpoints = kept
		worktrees = append(worktrees, w)
	}
	tombstones := liveTombstones(o.Tombstones, now)
	if !changed && len(tombstones) == len(o.Tombstones) {
		return o
	}
	o.Revision = nextRevision(o.Revision, now)
	o.Worktrees, o.Tombstones = worktrees, tombstones
	for _, w := range emptied {
		o.tombstone(w, now)
	}
	return o
}

func expired(o Origin, now time.Time) Origin {
	var worktrees []Worktree
	for _, w := range o.Worktrees {
		if w.Checkpoints = unexpired(w.Checkpoints, now); len(w.Checkpoints) > 0 {
			worktrees = append(worktrees, w)
		}
	}
	o.Worktrees, o.Tombstones = worktrees, liveTombstones(o.Tombstones, now)
	return o
}

func lapsed(o Origin, now time.Time) bool {
	o = expired(o, now)
	return len(o.Worktrees) == 0 && len(o.Tombstones) == 0
}

func live(o Origin, now time.Time) []Checkpoint {
	cps := make([]Checkpoint, 0, len(o.Worktrees))
	for _, w := range o.Worktrees {
		cps = append(cps, unexpired(w.Checkpoints, now)...)
	}
	return cps
}

func liveTombstones(tombstones []Tombstone, now time.Time) []Tombstone {
	return slices.DeleteFunc(slices.Clone(tombstones), func(t Tombstone) bool { return !now.Before(t.ExpiresAt) })
}

func (st *state) admit(p Payload) ([]Origin, error) {
	var admitted []Origin
	for _, in := range p.Origins {
		if in.Origin == st.Self {
			continue
		}
		digest, err := digestJSON(in)
		if err != nil {
			return nil, err
		}
		f, fenced := findFence(st.Fences, in.Origin)
		_, held := st.block(in.Origin)
		relayed, hasRelay := st.relay(in.Origin)
		switch {
		case !fenced || in.Revision > f.Revision:
			admitted = append(admitted, in)
		case in.Revision == f.Revision && digest != f.Digest:
			return nil, fmt.Errorf("%w: origin %s revision %d", ErrOriginBlockConflict, in.Origin, in.Revision)
		case in.Revision == f.Revision && held:
			admitted = append(admitted, in)
		case hasRelay && in.Revision == relayed.Revision:
			relayDigest, err := digestJSON(relayed)
			if err != nil {
				return nil, err
			}
			if relayDigest != digest {
				return nil, fmt.Errorf("%w: origin %s revision %d", ErrOriginBlockConflict, in.Origin, in.Revision)
			}
		}
	}
	return admitted, nil
}

func (st *state) hold(in Origin, now time.Time) error {
	digest, err := digestJSON(in)
	if err != nil {
		return err
	}
	i, found := searchOrigin(st.Origins, in.Origin)
	switch disposed := lapsed(in, now); {
	case disposed && found:
		st.Origins = slices.Delete(st.Origins, i, i+1)
	case disposed:
	case found:
		st.Origins[i] = in
	default:
		st.Origins = slices.Insert(st.Origins, i, in)
	}
	f := fence{Origin: in.Origin, Revision: in.Revision, Digest: digest}
	if i, found := slices.BinarySearchFunc(st.Fences, in.Origin, func(f fence, origin string) int { return strings.Compare(f.Origin, origin) }); found {
		st.Fences[i] = f
	} else {
		st.Fences = slices.Insert(st.Fences, i, f)
	}
	return nil
}

func (st *state) promote(origin string, closure map[artifact.Digest]bool, now time.Time) {
	held, ok := st.block(origin)
	if !ok {
		return
	}
	if r, ok := st.relay(origin); ok && r.Revision == held.Revision {
		return
	}
	for _, cp := range live(*held, now) {
		if !closure[cp.Root.Digest] && !st.Readiness[cp.ID].Ready {
			return
		}
	}
	if i, found := searchOrigin(st.Relays, origin); found {
		st.Relays[i] = *held
	} else {
		st.Relays = slices.Insert(st.Relays, i, *held)
	}
}

func (st *state) carriedHeld(p Payload, now time.Time) []Checkpoint {
	held := st.relayedIDs()
	var out []Checkpoint
	for _, in := range p.Origins {
		if in.Origin == st.Self {
			continue
		}
		for _, cp := range live(in, now) {
			if held[cp.ID] {
				out = append(out, cp)
			}
		}
	}
	return out
}

func (st *state) unready(blocks []Origin, now time.Time) []Checkpoint {
	var out []Checkpoint
	for _, o := range blocks {
		for _, cp := range live(o, now) {
			if !st.Readiness[cp.ID].Ready {
				out = append(out, cp)
			}
		}
	}
	return out
}

func (st *state) exportable() Payload {
	origins := slices.Clone(st.Relays)
	if own, ok := st.block(st.Self); ok {
		origins = append(origins, *own)
	}
	slices.SortFunc(origins, func(a, b Origin) int { return strings.Compare(a.Origin, b.Origin) })
	return Payload{Identity: Identity, Version: Version, Exporter: st.Self, Origins: origins}
}

func exportDigests(p Payload, now time.Time) (body, roots string, refs []artifact.Ref, err error) {
	if body, err = digestJSON(p.Origins); err != nil {
		return "", "", nil, err
	}
	if refs, err = rootsAt(p.Origins, now); err != nil {
		return "", "", nil, err
	}
	roots, err = digestJSON(refs)
	return body, roots, refs, err
}

func (st *state) carriedSince(refs []artifact.Ref, now time.Time) map[string]uint64 {
	carried := make(map[string]uint64)
	own, ok := st.block(st.Self)
	if !ok {
		return carried
	}
	roots := make(map[artifact.Digest]bool, len(refs))
	for _, r := range refs {
		roots[r.Digest] = true
	}
	for _, cp := range live(*own, now) {
		if !roots[cp.Root.Digest] {
			continue
		}
		since, ok := st.Carried[cp.ID]
		if !ok {
			since = st.Export.Revision
		}
		carried[cp.ID] = since
	}
	return carried
}

func (st *state) relayedIDs() map[string]bool {
	ids := make(map[string]bool)
	for _, o := range st.Origins {
		if o.Origin == st.Self {
			continue
		}
		for _, w := range o.Worktrees {
			for _, cp := range w.Checkpoints {
				ids[cp.ID] = true
			}
		}
	}
	return ids
}

func (st *state) pruneReadiness() {
	relayed := st.relayedIDs()
	for id := range st.Readiness {
		if !relayed[id] {
			delete(st.Readiness, id)
		}
	}
}

func (st *state) count() int {
	n := 0
	for _, o := range st.Origins {
		for _, w := range o.Worktrees {
			n += len(w.Checkpoints)
		}
	}
	return n
}

func (st *state) ready(now time.Time) int {
	n := 0
	for _, o := range st.Origins {
		for _, cp := range live(o, now) {
			if cp.Complete() && (o.Origin == st.Self || st.Readiness[cp.ID].Ready) {
				n++
			}
		}
	}
	return n
}

func (st *state) roots(now time.Time) []artifact.Ref {
	var roots []artifact.Ref
	for _, o := range slices.Concat(st.Origins, st.Relays) {
		for _, cp := range live(o, now) {
			roots = append(roots, cp.Root)
		}
	}
	slices.SortFunc(roots, func(a, b artifact.Ref) int { return strings.Compare(string(a.Digest), string(b.Digest)) })
	return slices.CompactFunc(roots, func(a, b artifact.Ref) bool { return a.Digest == b.Digest })
}

func (st *state) receipt(origin string) *syncservice.Receipt {
	i := slices.IndexFunc(st.Receipts, func(r syncservice.Receipt) bool { return r.Origin == origin })
	if i < 0 {
		return nil
	}
	r := st.Receipts[i]
	return &r
}

func (st *state) setReceipt(r syncservice.Receipt) {
	i, found := slices.BinarySearchFunc(st.Receipts, r.Origin, func(h syncservice.Receipt, origin string) int { return strings.Compare(h.Origin, origin) })
	if found {
		st.Receipts[i] = r
		return
	}
	st.Receipts = slices.Insert(st.Receipts, i, r)
}

func digestJSON(v any) (string, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return "", fmt.Errorf("catalog: digest: %w", err)
	}
	return digestOf(data), nil
}

func digestOf(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
