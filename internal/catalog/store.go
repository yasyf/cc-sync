package catalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
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

// Readiness reasons recorded for a relayed checkpoint that is not ready.
const (
	MissingClosure = "artifact-closure"
	MissingCode    = "code-unverified"
)

// ErrExpired reports a recorded checkpoint that expired before retention.
var ErrExpired = errors.New("catalog: checkpoint already expired")

// Readiness is a relayed checkpoint's local pick-up state. A host's own
// checkpoints are always ready.
type Readiness struct {
	Ready    bool     `json:"ready"`
	Missing  []string `json:"missing,omitempty"`
	Deferred string   `json:"deferred,omitempty"`
}

// Evidence is what a receiver established before applying a change: the
// roots whose closures its store holds, and code verdicts by checkpoint ID.
type Evidence struct {
	Roots    map[artifact.Digest]bool
	Verified map[string]Readiness
}

// Snapshot is a read-only view of the catalog.
type Snapshot struct {
	Self      string
	Origins   []Origin
	Readiness map[string]Readiness
}

// ReadinessOf reports checkpoint id of origin's block on this host.
func (s Snapshot) ReadinessOf(origin, id string) Readiness {
	if origin == s.Self {
		return Readiness{Ready: true}
	}
	return s.Readiness[id]
}

// Exported is one export: the ready payload, its canonical bytes, and the
// source revision those bytes carry.
type Exported struct {
	Payload  Payload
	Data     []byte
	Revision uint64
}

// GCResult reports one retention and expiry pass: checkpoints removed,
// checkpoints ready here, and the roots of every retained checkpoint.
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

type ledger struct {
	Revision uint64 `json:"revision"`
	Digest   string `json:"digest"`
}

type state struct {
	Identity  string                `json:"identity"`
	Version   uint64                `json:"version"`
	Self      string                `json:"self"`
	Origins   []Origin              `json:"origins"`
	Readiness map[string]Readiness  `json:"readiness"`
	Receipts  []syncservice.Receipt `json:"receipts"`
	Export    ledger                `json:"export"`
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
	relayed := st.relayedIDs()
	for id := range st.Readiness {
		if !relayed[id] {
			return fmt.Errorf("%w: readiness for unknown checkpoint %s", ErrInvalid, id)
		}
	}
	if err := strictlyIncreasing(st.Receipts, func(r syncservice.Receipt) string { return r.Origin }); err != nil {
		return fmt.Errorf("receipts: %w", err)
	}
	if (st.Export.Revision == 0) != (st.Export.Digest == "") {
		return fmt.Errorf("%w: export ledger revision %d digest %q", ErrInvalid, st.Export.Revision, st.Export.Digest)
	}
	return nil
}

// Load reads the catalog without locking; rename-atomic writes keep it
// consistent.
func (s *Store) Load() (Snapshot, error) {
	st, err := s.read()
	if err != nil {
		return Snapshot{}, err
	}
	return Snapshot{Self: st.Self, Origins: st.Origins, Readiness: st.Readiness}, nil
}

// Record adds or replaces cp in this host's block under worktree wt, applies
// retention to that worktree, clears any tombstone for it, and bumps the
// block revision. It derives the checkpoint's ID, expiry, and classes and
// returns the checkpoint as retained.
func (s *Store) Record(ctx context.Context, wt Worktree, cp Checkpoint) (Checkpoint, error) {
	cp = normalized(s.self, wt.ID, cp)
	var recorded Checkpoint
	err := s.update(ctx, func(st *state, now time.Time) (bool, error) {
		own := st.ensure(s.self)
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
		others := slices.DeleteFunc(w.Checkpoints, func(c Checkpoint) bool { return c.ID == cp.ID })
		w.Checkpoints = Retain(append(others, cp), now)
		j := slices.IndexFunc(w.Checkpoints, func(c Checkpoint) bool { return c.ID == cp.ID })
		if j < 0 {
			return false, fmt.Errorf("%w: %s expired at %s", ErrExpired, cp.ID, cp.ExpiresAt)
		}
		recorded = w.Checkpoints[j]
		own.Tombstones = slices.DeleteFunc(own.Tombstones, func(t Tombstone) bool { return t.ID == wt.ID })
		own.Revision++
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
		own.Revision++
		own.tombstone(own.Worktrees[i], now)
		own.Worktrees = slices.Delete(own.Worktrees, i, i+1)
		return true, nil
	})
}

// GC applies retention to this host's block, bumping its revision and
// tombstoning emptied worktrees when anything changes; drops expired
// checkpoints and tombstones from relayed blocks without touching their
// revisions; and forgets relayed blocks left with nothing to fence.
func (s *Store) GC(ctx context.Context) (GCResult, error) {
	var res GCResult
	err := s.update(ctx, func(st *state, now time.Time) (bool, error) {
		before, err := durable.Marshal(*st)
		if err != nil {
			return false, err
		}
		total := st.count()
		kept := st.Origins[:0]
		for _, o := range st.Origins {
			if o.Origin == s.self {
				o = retainOwn(o, now)
			} else if o = expired(o, now); len(o.Worktrees) == 0 && len(o.Tombstones) == 0 {
				continue
			}
			kept = append(kept, o)
		}
		st.Origins = kept
		st.pruneReadiness()
		res = GCResult{Removed: total - st.count(), Ready: st.ready(), Roots: st.roots()}
		after, err := durable.Marshal(*st)
		if err != nil {
			return false, err
		}
		return !bytes.Equal(before, after), nil
	})
	return res, err
}

// Export returns the payload of every ready, unexpired checkpoint, bumping
// the source revision exactly when the payload's digest changes.
func (s *Store) Export(ctx context.Context) (Exported, error) {
	var out Exported
	err := s.update(ctx, func(st *state, now time.Time) (bool, error) {
		p := st.exportable(now)
		data, err := Encode(p)
		if err != nil {
			return false, err
		}
		digest := digestOf(data)
		changed := digest != st.Export.Digest
		if changed {
			st.Export = ledger{Revision: st.Export.Revision + 1, Digest: digest}
		}
		out = Exported{Payload: p, Data: data, Revision: st.Export.Revision}
		return changed, nil
	})
	return out, err
}

// Digest returns the digest of the payload Export would produce now.
func (s *Store) Digest() (string, error) {
	st, err := s.read()
	if err != nil {
		return "", err
	}
	data, err := Encode(st.exportable(s.now().UTC()))
	if err != nil {
		return "", err
	}
	return digestOf(data), nil
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

// Unverified returns the relayed checkpoints of p that applying it now would
// record and that are not yet ready here.
func (s *Store) Unverified(p Payload) ([]Checkpoint, error) {
	st, err := s.read()
	if err != nil {
		return nil, err
	}
	return slices.DeleteFunc(st.merge(p, s.now().UTC()), func(cp Checkpoint) bool { return st.Readiness[cp.ID].Ready }), nil
}

// Apply merges p, the decoded payload of change, under the change's fence:
// per origin, an older revision is ignored, a newer one replaces the block,
// and an equal one unions it. Each recorded checkpoint is ready only when
// its root is in ev.Roots and its code verdict is ready. The change is
// acknowledged, and its receipt persisted, only when every root it carries
// is present and every recorded checkpoint is ready; otherwise the merge is
// kept and the result is Partial with the prior receipt.
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
		complete := true
		for _, o := range p.Origins {
			for _, w := range o.Worktrees {
				for _, cp := range w.Checkpoints {
					complete = complete && ev.Roots[cp.Root.Digest]
				}
			}
		}
		for _, cp := range st.merge(p, now) {
			r := st.Readiness[cp.ID]
			switch v, verified := ev.Verified[cp.ID]; {
			case !ev.Roots[cp.Root.Digest]:
				r = Readiness{Missing: []string{MissingClosure}}
			case r.Ready:
			case verified:
				r = v
			default:
				r = Readiness{Missing: []string{MissingCode}}
			}
			st.Readiness[cp.ID] = r
			complete = complete && r.Ready
		}
		st.pruneReadiness()
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

func searchOrigin(origins []Origin, name string) (int, bool) {
	return slices.BinarySearchFunc(origins, name, func(o Origin, name string) int { return strings.Compare(o.Origin, name) })
}

func (st *state) block(origin string) (*Origin, bool) {
	i, found := searchOrigin(st.Origins, origin)
	if !found {
		return nil, false
	}
	return &st.Origins[i], true
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
	o.Revision++
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

func liveTombstones(tombstones []Tombstone, now time.Time) []Tombstone {
	return slices.DeleteFunc(slices.Clone(tombstones), func(t Tombstone) bool { return !now.Before(t.ExpiresAt) })
}

func (st *state) merge(p Payload, now time.Time) []Checkpoint {
	var recorded []Checkpoint
	for _, in := range p.Origins {
		if in.Origin == st.Self {
			continue
		}
		in = expired(in, now)
		i, found := searchOrigin(st.Origins, in.Origin)
		switch {
		case !found:
			st.Origins = slices.Insert(st.Origins, i, in)
		case in.Revision < st.Origins[i].Revision:
			continue
		case in.Revision > st.Origins[i].Revision:
			st.Origins[i] = in
		default:
			st.Origins[i] = union(st.Origins[i], in)
		}
		stored := st.Origins[i]
		for _, w := range in.Worktrees {
			j, ok := findWorktree(stored.Worktrees, w.ID)
			if !ok {
				continue
			}
			for _, cp := range w.Checkpoints {
				if slices.ContainsFunc(stored.Worktrees[j].Checkpoints, func(c Checkpoint) bool { return c.ID == cp.ID }) {
					recorded = append(recorded, cp)
				}
			}
		}
	}
	return recorded
}

func union(held, in Origin) Origin {
	tombstones := slices.Clone(held.Tombstones)
	for _, t := range in.Tombstones {
		i, found := slices.BinarySearchFunc(tombstones, t.ID, func(t Tombstone, id string) int { return strings.Compare(t.ID, id) })
		if !found {
			tombstones = slices.Insert(tombstones, i, t)
		}
	}
	worktrees := slices.Clone(held.Worktrees)
	for _, w := range in.Worktrees {
		i, found := findWorktree(worktrees, w.ID)
		if !found {
			worktrees = slices.Insert(worktrees, i, w)
			continue
		}
		cps := slices.Clone(worktrees[i].Checkpoints)
		for _, cp := range w.Checkpoints {
			if !slices.ContainsFunc(cps, func(c Checkpoint) bool { return c.ID == cp.ID }) {
				cps = append(cps, cp)
			}
		}
		slices.SortFunc(cps, compareCheckpoints)
		worktrees[i].Checkpoints = cps
	}
	worktrees = slices.DeleteFunc(worktrees, func(w Worktree) bool {
		_, dead := slices.BinarySearchFunc(tombstones, w.ID, func(t Tombstone, id string) int { return strings.Compare(t.ID, id) })
		return dead
	})
	held.Worktrees, held.Tombstones = worktrees, tombstones
	return held
}

func (st *state) exportable(now time.Time) Payload {
	p := Payload{Identity: Identity, Version: Version, Exporter: st.Self, Origins: make([]Origin, 0, len(st.Origins))}
	for _, o := range st.Origins {
		out := Origin{Origin: o.Origin, Revision: o.Revision, Worktrees: []Worktree{}, Tombstones: liveTombstones(o.Tombstones, now)}
		for _, w := range o.Worktrees {
			w.Checkpoints = slices.DeleteFunc(unexpired(w.Checkpoints, now), func(cp Checkpoint) bool {
				return o.Origin != st.Self && !st.Readiness[cp.ID].Ready
			})
			if len(w.Checkpoints) > 0 {
				out.Worktrees = append(out.Worktrees, w)
			}
		}
		p.Origins = append(p.Origins, out)
	}
	return p
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

func (st *state) ready() int {
	n := 0
	for _, o := range st.Origins {
		for _, w := range o.Worktrees {
			for _, cp := range w.Checkpoints {
				if o.Origin == st.Self || st.Readiness[cp.ID].Ready {
					n++
				}
			}
		}
	}
	return n
}

func (st *state) roots() []artifact.Ref {
	var roots []artifact.Ref
	for _, o := range st.Origins {
		for _, w := range o.Worktrees {
			for _, cp := range w.Checkpoints {
				roots = append(roots, cp.Root)
			}
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

func digestOf(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
