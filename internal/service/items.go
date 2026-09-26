package service

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/yasyf/cc-sync/internal/catalog"
	"github.com/yasyf/cc-sync/internal/claudenative"
	"github.com/yasyf/cc-sync/internal/cli"
	"github.com/yasyf/cc-sync/internal/consumer"
	"github.com/yasyf/cc-sync/internal/orcabridge"
	"github.com/yasyf/synckit/delivery"
	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/netpolicy"
)

const serviceID = consumer.ServiceID

type view struct {
	now       time.Time
	snap      catalog.Snapshot
	self      string
	statuses  []delivery.PeerStatus
	peers     map[string]delivery.PeerStatus
	local     netpolicy.State
	live      map[claudenative.SessionID]claudenative.LiveProcess
	bound     map[string]string
	checkouts Checkouts
}

type located struct {
	origin   string
	worktree catalog.Worktree
}

func (s *Service) gather(ctx context.Context) (view, error) {
	v := view{now: s.cfg.Now().UTC(), checkouts: s.cfg.Checkouts, peers: map[string]delivery.PeerStatus{}, bound: map[string]string{}}
	var err error
	if v.snap, err = s.cfg.Catalog.Load(); err != nil {
		return view{}, fmt.Errorf("load catalog: %w", err)
	}
	v.self = v.snap.Self
	v.statuses, err = s.cfg.Deliveries.Status(ctx, serviceID)
	switch {
	case errors.Is(err, ErrUnavailable):
		slog.Debug("delivery status unavailable", "err", err)
	case err != nil:
		return view{}, fmt.Errorf("delivery status: %w", err)
	}
	for _, ps := range v.statuses {
		v.peers[ps.Peer] = ps
	}
	if v.local, err = s.cfg.Network(ctx); err != nil {
		return view{}, fmt.Errorf("read network state: %w", err)
	}
	if v.live, err = s.cfg.Live(ctx); err != nil {
		return view{}, fmt.Errorf("live sessions: %w", err)
	}
	if v.bound, err = boundSessions(ctx, s.cfg.Orca); err != nil {
		return view{}, err
	}
	return v, nil
}

func boundSessions(ctx context.Context, orca Orca) (map[string]string, error) {
	bound := map[string]string{}
	var unavailable *orcabridge.UnavailableError
	bindings, err := orca.List(ctx, "")
	switch {
	case errors.As(err, &unavailable):
		slog.Debug("orca unavailable", "err", err)
		return bound, nil
	case err != nil:
		return nil, fmt.Errorf("orca dormant bindings: %w", err)
	}
	activity, err := orca.Activity(ctx)
	if err != nil {
		return nil, fmt.Errorf("orca activity: %w", err)
	}
	paths := make(map[string]string, len(activity))
	for _, a := range activity {
		paths[a.WorktreeID] = a.Path
	}
	for _, b := range bindings {
		bound[b.ProviderSession.ID] = paths[b.WorktreeID]
	}
	return bound, nil
}

// List reports every recoverable worktree captured on another host, each at
// its newest pick-up ready checkpoint.
func (s *Service) List(ctx context.Context, req cli.ListRequest) (cli.ListResult, error) {
	v, err := s.gather(ctx)
	if err != nil {
		return cli.ListResult{}, err
	}
	var items []cli.Item
	for _, o := range v.snap.Origins {
		if !v.listsSource(o.Origin, req.Source) {
			continue
		}
		for _, w := range o.Worktrees {
			if !listsRepo(w, req.Repo) {
				continue
			}
			item, err := v.item(o.Origin, w, v.latest(o.Origin, w))
			if err != nil {
				return cli.ListResult{}, err
			}
			if item.Completeness.Ready || req.All {
				items = append(items, item)
			}
		}
	}
	slices.SortFunc(items, func(a, b cli.Item) int {
		return cmp.Or(activityOf(b).Compare(activityOf(a)), strings.Compare(a.Ref().String(), b.Ref().String()))
	})
	return cli.ListResult{GeneratedAt: cli.At(v.now), Local: host(v.self), Items: items}, nil
}

// Inspect reports one worktree at the selected checkpoint with every retained
// checkpoint and this host's delivery toward each peer.
func (s *Service) Inspect(ctx context.Context, req cli.InspectRequest) (cli.InspectResult, error) {
	v, err := s.gather(ctx)
	if err != nil {
		return cli.InspectResult{}, err
	}
	loc, err := v.locate(req.Target)
	if err != nil {
		return cli.InspectResult{}, err
	}
	cp, err := v.pick(loc.origin, loc.worktree, req.Checkpoint)
	if err != nil {
		return cli.InspectResult{}, err
	}
	item, err := v.item(loc.origin, loc.worktree, cp)
	if err != nil {
		return cli.InspectResult{}, err
	}
	res := cli.InspectResult{Item: item, Checkpoints: cli.Array[cli.CheckpointDetail]{}, Delivery: cli.Array[cli.Delivery]{}}
	for _, cp := range loc.worktree.Checkpoints {
		res.Checkpoints = append(res.Checkpoints, v.detail(loc.origin, cp))
	}
	for _, ps := range slices.SortedFunc(slices.Values(v.statuses), func(a, b delivery.PeerStatus) int { return strings.Compare(a.Peer, b.Peer) }) {
		d := cli.Delivery{Peer: ps.Peer, State: cli.DeliveryState(ps.State)}
		if d.Pause, err = peerPause(ps); err != nil {
			return cli.InspectResult{}, err
		}
		if loc.origin == v.self {
			if d.Assurance, err = v.assurance(ps, cp); err != nil {
				return cli.InspectResult{}, fmt.Errorf("peer %s acked: %w", ps.Peer, err)
			}
		}
		res.Delivery = append(res.Delivery, d)
	}
	return res, nil
}

func activityOf(i cli.Item) time.Time {
	if i.Checkpoint.SourceActivityAt == nil {
		return i.Checkpoint.CapturedAt.Time
	}
	return i.Checkpoint.SourceActivityAt.Time
}

func (v view) listsSource(origin, source string) bool {
	if source == "" {
		return origin != v.self
	}
	return origin == source || hostregistry.HostNode(origin) == source
}

func listsRepo(w catalog.Worktree, repo string) bool {
	return repo == "" || repo == w.Repo.Origin || repo == w.Repo.SourcePath || repo == w.Repo.RelPath
}

func (v view) item(origin string, w catalog.Worktree, cp catalog.Checkpoint) (cli.Item, error) {
	pause, err := v.pause(origin)
	if err != nil {
		return cli.Item{}, err
	}
	item := cli.Item{
		Source:          v.source(origin),
		Workspace:       workspace(w),
		Sessions:        v.sessions(cp),
		NotRestorable:   notRestorable(cp),
		Checkpoint:      *checkpoint(cp),
		CheckpointCount: len(w.Checkpoints),
		Completeness:    v.completeness(origin, w, cp),
		NewerPartial:    newerPartial(w, cp),
		Pause:           pause,
	}
	if origin == v.self {
		return item, nil
	}
	if item.LocalCheckout, err = v.localCheckout(origin, w, cp); err != nil {
		return cli.Item{}, err
	}
	return item, nil
}

func (v view) source(origin string) cli.Source {
	src := cli.Source{Host: host(origin)}
	if origin == v.self {
		src.LastSeenAt, src.Reachable = cli.AtPtr(v.now), true
		return src
	}
	if ps, ok := v.peers[origin]; ok {
		src.LastSeenAt, src.Reachable = lastSeen(ps), reachable(ps)
	}
	return src
}

func workspace(w catalog.Worktree) cli.Workspace {
	ws := cli.Workspace{
		ID:         w.ID,
		RepoName:   repoName(w.Repo),
		RepoOrigin: &w.Repo.Origin,
		Branch:     optionalString(w.Repo.Branch),
		SourcePath: w.Repo.SourcePath,
	}
	if w.Orca != nil {
		ws.Orca = &cli.OrcaWorkspace{Kind: cli.OrcaKind(w.Orca.Kind), Name: w.Orca.Name, InstanceID: w.Orca.InstanceID}
	}
	return ws
}

func repoName(r catalog.Repo) string {
	origin := strings.TrimSuffix(strings.TrimSuffix(r.Origin, "/"), ".git")
	return path.Base(strings.ReplaceAll(origin, ":", "/"))
}

func (v view) sessions(cp catalog.Checkpoint) cli.Array[cli.Session] {
	out := make(cli.Array[cli.Session], 0, len(cp.Sessions))
	for _, s := range cp.Sessions {
		_, bound := v.bound[s.ID]
		_, live := v.live[claudenative.SessionID(s.ID)]
		out = append(out, cli.Session{
			SessionID:           s.ID,
			Title:               s.Title,
			LastActivityAt:      optionalTime(s.LastActivity),
			LastHumanActivityAt: optionalTime(s.LastHumanActivity),
			Activity:            cli.Activity(s.Activity),
			BoundInOrca:         bound,
			LiveLocalCollision:  live,
		})
	}
	slices.SortStableFunc(out, func(a, b cli.Session) int {
		return cmp.Or(timeOf(b.LastActivityAt).Compare(timeOf(a.LastActivityAt)), strings.Compare(a.SessionID, b.SessionID))
	})
	return out
}

func notRestorable(cp catalog.Checkpoint) cli.Array[cli.NotRestorable] {
	out := make(cli.Array[cli.NotRestorable], len(cp.Omitted))
	for i, o := range cp.Omitted {
		out[i] = cli.NotRestorable(o)
	}
	return out
}

func checkpoint(cp catalog.Checkpoint) *cli.Checkpoint {
	return &cli.Checkpoint{
		ID:               cp.ID,
		Tier:             cli.Tier(cp.Classes[0]),
		CapturedAt:       cli.At(cp.CapturedAt),
		SourceActivityAt: optionalTime(cp.SourceActivityAt),
	}
}

func (v view) completeness(origin string, w catalog.Worktree, cp catalog.Checkpoint) cli.Completeness {
	r := v.snap.ReadinessOf(origin, cp)
	c := cli.Completeness{
		Ready:          v.snap.PickupReady(origin, cp),
		Missing:        cli.Array[string](slices.Clone(r.Missing)),
		Transcript:     cli.TranscriptComplete,
		Code:           cli.CodeComplete,
		CodeCapturedAt: optionalTime(cp.Code.CapturedAt),
		Layout:         cli.LayoutNone,
	}
	switch {
	case len(cp.Sessions) == 0:
		c.Transcript = cli.TranscriptMissing
	case !cp.Completeness.Complete:
		c.Transcript = cli.TranscriptPartial
	}
	switch {
	case cp.Deferred != "":
		c.Code = cli.CodeDeferred
	case cp.Code.CapturedAt.IsZero():
		c.Code = cli.CodeNone
	case slices.ContainsFunc(r.Missing, func(m string) bool {
		return m == catalog.MissingClosure || m == catalog.MissingCode || m == catalog.MissingPrerequisites
	}):
		c.Code = cli.CodeMissing
	}
	if w.Orca != nil {
		c.Layout = cli.LayoutHostOnly
	}
	return c
}

func (v view) detail(origin string, cp catalog.Checkpoint) cli.CheckpointDetail {
	r := v.snap.ReadinessOf(origin, cp)
	d := cli.CheckpointDetail{
		ID:         cp.ID,
		Tier:       cli.Tier(cp.Classes[0]),
		CapturedAt: cli.At(cp.CapturedAt),
		Ready:      v.snap.PickupReady(origin, cp),
		Missing:    cli.Array[string](slices.Clone(r.Missing)),
		Deferred:   cli.Array[string]{},
	}
	if r.Deferred != "" {
		d.Deferred = append(d.Deferred, r.Deferred)
	}
	return d
}

func (v view) pause(origin string) (*cli.Pause, error) {
	if origin == v.self {
		return nil, nil
	}
	if ps, ok := v.peers[origin]; ok && ps.State == delivery.StatePaused {
		return peerPause(ps)
	}
	verdict := netpolicy.Evaluate(v.local, netpolicy.State{Status: netpolicy.StatusConnected})
	if verdict.Allowed {
		return nil, nil
	}
	return pauseFor(delivery.ReasonForVerdict(verdict), v.local.ObservedAt)
}

func (v view) localCheckout(origin string, w catalog.Worktree, cp catalog.Checkpoint) (*cli.LocalCheckout, error) {
	for _, s := range cp.Sessions {
		if p := v.bound[s.ID]; p != "" {
			return &cli.LocalCheckout{Path: p, Reusable: true}, nil
		}
	}
	found, err := v.checkouts.Find(origin, w)
	if err != nil {
		return nil, fmt.Errorf("find local checkout of %s/%s: %w", origin, w.ID, err)
	}
	return found, nil
}

func (v view) locate(t cli.Target) (located, error) {
	switch t := t.(type) {
	case cli.ItemRef:
		for _, o := range v.snap.Origins {
			if o.Origin != t.SourceHostID {
				continue
			}
			for _, w := range o.Worktrees {
				if w.ID == t.WorkspaceID {
					return located{origin: o.Origin, worktree: w}, nil
				}
			}
		}
		return located{}, cli.Errorf(cli.CodeNotFound, "no item %s", t)
	case cli.SessionRef:
		return v.locateSession(t)
	}
	panic(fmt.Sprintf("service: unknown target %T", t))
}

func (v view) locateSession(ref cli.SessionRef) (located, error) {
	var found located
	var newest time.Time
	ids := map[string]bool{}
	for _, o := range v.snap.Origins {
		if ref.Source != "" && ref.Source != o.Origin && ref.Source != hostregistry.HostNode(o.Origin) {
			continue
		}
		for _, w := range o.Worktrees {
			for _, cp := range w.Checkpoints {
				for _, s := range cp.Sessions {
					if !strings.HasPrefix(s.ID, ref.ID) {
						continue
					}
					ids[s.ID] = true
					if cp.CapturedAt.After(newest) {
						found, newest = located{origin: o.Origin, worktree: w}, cp.CapturedAt
					}
				}
			}
		}
	}
	switch len(ids) {
	case 0:
		return located{}, cli.Errorf(cli.CodeNotFound, "no captured session %s", ref)
	case 1:
		return found, nil
	}
	return located{}, cli.Errorf(cli.CodeUsage, "session %s is ambiguous: %s", ref, strings.Join(slices.Sorted(maps.Keys(ids)), ", "))
}

func newerPartial(w catalog.Worktree, target catalog.Checkpoint) *cli.PartialCheckpoint {
	for _, cp := range w.Checkpoints {
		if !cp.Mixed() || !cp.CapturedAt.After(target.CapturedAt) {
			continue
		}
		return &cli.PartialCheckpoint{
			ID:                cp.ID,
			CapturedAt:        cli.At(cp.CapturedAt),
			SessionActivityAt: optionalTime(cp.SourceActivityAt),
			CodeCapturedAt:    optionalTime(cp.Code.CapturedAt),
		}
	}
	return nil
}

func (v view) latest(origin string, w catalog.Worktree) catalog.Checkpoint {
	if i := slices.IndexFunc(w.Checkpoints, func(cp catalog.Checkpoint) bool { return v.snap.PickupReady(origin, cp) }); i >= 0 {
		return w.Checkpoints[i]
	}
	if i := slices.IndexFunc(w.Checkpoints, catalog.Checkpoint.Complete); i >= 0 {
		return w.Checkpoints[i]
	}
	return w.Checkpoints[0]
}

func (v view) assurance(ps delivery.PeerStatus, cp catalog.Checkpoint) (cli.Assurance, error) {
	acked, err := revision(ps.Acked)
	if err != nil || acked == nil {
		return cli.AssuranceNone, err
	}
	return assurances[v.snap.AssuranceOf(cp, *acked, v.now)], nil
}

var assurances = map[catalog.Assurance]cli.Assurance{
	catalog.AssuranceNone:    cli.AssuranceNone,
	catalog.AssuranceHeld:    cli.AssuranceHeld,
	catalog.AssuranceDurable: cli.AssuranceDurable,
}

func (v view) pick(origin string, w catalog.Worktree, sel cli.CheckpointSelector) (catalog.Checkpoint, error) {
	var match func(catalog.Checkpoint) bool
	switch sel := sel.(type) {
	case cli.LatestCheckpoint:
		return v.latest(origin, w), nil
	case cli.CheckpointID:
		return byPrefix(w, sel.Prefix)
	case cli.CheckpointAt:
		match = func(cp catalog.Checkpoint) bool { return !cp.CapturedAt.After(sel.Time) }
	case cli.CheckpointHourly:
		cutoff := v.now.Add(-time.Duration(sel.HoursAgo) * time.Hour)
		match = func(cp catalog.Checkpoint) bool { return !cp.CapturedAt.After(cutoff) }
	case cli.CheckpointDaily:
		start := time.Date(sel.Year, sel.Month, sel.Day, 0, 0, 0, 0, time.UTC)
		match = func(cp catalog.Checkpoint) bool {
			return !cp.CapturedAt.Before(start) && cp.CapturedAt.Before(start.AddDate(0, 0, 1))
		}
	default:
		panic(fmt.Sprintf("service: unknown checkpoint selector %T", sel))
	}
	for _, cp := range w.Checkpoints {
		if match(cp) {
			return cp, nil
		}
	}
	return catalog.Checkpoint{}, cli.Errorf(cli.CodeNotFound, "no checkpoint of %s matches %s", w.ID, sel)
}

func byPrefix(w catalog.Worktree, prefix string) (catalog.Checkpoint, error) {
	var found []catalog.Checkpoint
	for _, cp := range w.Checkpoints {
		if strings.HasPrefix(cp.ID, prefix) {
			found = append(found, cp)
		}
	}
	switch len(found) {
	case 0:
		return catalog.Checkpoint{}, cli.Errorf(cli.CodeNotFound, "no checkpoint of %s has id prefix %s", w.ID, prefix)
	case 1:
		return found[0], nil
	}
	return catalog.Checkpoint{}, cli.Errorf(cli.CodeUsage, "checkpoint prefix %s matches %d checkpoints of %s", prefix, len(found), w.ID)
}

func optionalTime(t time.Time) *cli.Time {
	if t.IsZero() {
		return nil
	}
	return cli.AtPtr(t)
}

func optionalString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func timeOf(t *cli.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return t.Time
}
