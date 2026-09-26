//go:build e2e

package e2e

import (
	"cmp"
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/yasyf/reposync/worktree"

	"github.com/yasyf/cc-sync/internal/catalog"
	"github.com/yasyf/cc-sync/internal/consumer"
	"github.com/yasyf/cc-sync/internal/scheduler"
	"github.com/yasyf/synckit/artifact"
	"github.com/yasyf/synckit/delivery"
	"github.com/yasyf/synckit/syncservice"
)

var fleetRepos = []string{"acme/r0", "acme/r1", "acme/r2", "acme/r3", "acme/r4"}

var fleetLayout = []struct {
	repo     int
	linked   string
	tier     scheduler.Tier
	sessions int
}{
	{0, "", scheduler.TierHuman, 4},
	{0, "pair", scheduler.TierHuman, 4},
	{1, "", scheduler.TierHuman, 4},
	{2, "", scheduler.TierAutonomous, 3},
	{3, "", scheduler.TierAutonomous, 3},
	{1, "auto", scheduler.TierAutonomous, 3},
	{1, "recent", scheduler.TierRecent, 3},
	{2, "recent", scheduler.TierRecent, 3},
	{3, "recent", scheduler.TierRecent, 3},
	{0, "idle", scheduler.TierIdle, 3},
	{4, "", scheduler.TierIdle, 3},
	{4, "idle", scheduler.TierIdle, 4},
}

type fleetUnit struct {
	Relpath  string
	Root     string
	Branch   string
	Tier     scheduler.Tier
	HumanAt  time.Time
	ID       string
	RepoKey  string
	Sessions []*Session
}

func fleetOrigins(t *testing.T) []*Origin {
	t.Helper()
	origins := make([]*Origin, len(fleetRepos))
	for i, rel := range fleetRepos {
		origins[i] = NewOrigin(t, rel, map[string]string{"README.md": rel + "\n", "main.go": "package main\n"})
	}
	return origins
}

func buildFleet(t *testing.T, h *Host) []*fleetUnit {
	t.Helper()
	units := make([]*fleetUnit, len(fleetLayout))
	for i, l := range fleetLayout {
		rel := fleetRepos[l.repo]
		u := &fleetUnit{Relpath: rel, Root: h.Checkout(rel), Branch: "main", Tier: l.tier}
		if l.linked != "" {
			u.Root, u.Branch = filepath.Join(h.Root, "linked", rel, l.linked), l.linked
			h.Git(h.Checkout(rel), "worktree", "add", "-q", "-b", l.linked, u.Root)
		}
		units[i] = u
	}
	write := func(i int, human bool) {
		u := units[i]
		for n := range fleetLayout[i].sessions {
			turns := []Turn{{Text: fmt.Sprintf("autonomous step %d", n)}, {Text: "continuing"}}
			if human {
				turns = []Turn{{Human: true, Text: fmt.Sprintf("please do task %d", n)}, {Text: "done"}}
				u.HumanAt = h.Clock.Now()
			}
			u.Sessions = append(u.Sessions, h.WriteSession(SessionSpec{Cwd: u.Root, Branch: u.Branch, Turns: turns}))
		}
	}
	now := Now()
	for _, phase := range []struct {
		at    time.Time
		tier  scheduler.Tier
		human bool
		only  int
	}{
		{time.Time{}, scheduler.TierIdle, true, -1},
		{now.Add(-30 * time.Minute), scheduler.TierRecent, true, -1},
		{now.Add(-4 * time.Minute), scheduler.TierAutonomous, false, -1},
		{now.Add(-3 * time.Minute), scheduler.TierHuman, true, 2},
		{now.Add(-2 * time.Minute), scheduler.TierHuman, true, 1},
		{now.Add(-1 * time.Minute), scheduler.TierHuman, true, 0},
	} {
		if !phase.at.IsZero() {
			if phase.at.Before(h.Clock.Now()) {
				t.Fatalf("clock %s already past fleet phase %s", h.Clock.Now(), phase.at)
			}
			h.Clock.Advance(phase.at.Sub(h.Clock.Now()))
		}
		for i, l := range fleetLayout {
			if l.tier == phase.tier && (phase.only < 0 || phase.only == i) {
				write(i, phase.human)
			}
		}
	}
	wts, err := h.worktrees(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(wts) != len(units) {
		t.Fatalf("discovered %d worktrees, want %d", len(wts), len(units))
	}
	for _, u := range units {
		i := slices.IndexFunc(wts, func(w worktree.Worktree) bool { return w.Root == u.Root })
		if i < 0 {
			t.Fatalf("worktree %s not discovered", u.Root)
		}
		u.ID, u.RepoKey = wts[i].ID, wts[i].CommonDir
	}
	return units
}

func byID(units []*fleetUnit) map[string]*fleetUnit {
	m := make(map[string]*fleetUnit, len(units))
	for _, u := range units {
		m[u.ID] = u
	}
	return m
}

func priority(units []*fleetUnit) []*fleetUnit {
	order := slices.Clone(units)
	slices.SortFunc(order, func(a, b *fleetUnit) int {
		return cmp.Or(cmp.Compare(a.Tier, b.Tier), b.HumanAt.Compare(a.HumanAt), cmp.Compare(a.ID, b.ID))
	})
	return order
}

func admit(order []*fleetUnit, running, done map[string]bool, workers int) {
	for len(running) < workers {
		busy := map[string]bool{}
		for _, u := range order {
			if running[u.ID] {
				busy[u.RepoKey] = true
			}
		}
		i := slices.IndexFunc(order, func(u *fleetUnit) bool { return !running[u.ID] && !done[u.ID] && !busy[u.RepoKey] })
		if i < 0 {
			return
		}
		running[order[i].ID] = true
	}
}

func checkpointsByWorktree(snap catalog.Snapshot) map[string][]catalog.Checkpoint {
	out := map[string][]catalog.Checkpoint{}
	for _, o := range snap.Origins {
		for _, w := range o.Worktrees {
			out[w.ID] = append(out[w.ID], w.Checkpoints...)
		}
	}
	return out
}

func checkpointIDs(snap catalog.Snapshot) map[string]bool {
	ids := map[string]bool{}
	for _, cps := range checkpointsByWorktree(snap) {
		for _, cp := range cps {
			ids[cp.ID] = true
		}
	}
	return ids
}

func unitStatus(t *testing.T, st scheduler.Status, id string) scheduler.UnitStatus {
	t.Helper()
	i := slices.IndexFunc(st.Units, func(u scheduler.UnitStatus) bool { return u.WorktreeID == id })
	if i < 0 {
		t.Fatalf("scheduler status has no unit %s", id)
	}
	return st.Units[i]
}

func TestScaleSchedulerPriorityConcurrencyCadence(t *testing.T) {
	a := NewHost(t, "host-a", NewClock(Now().Add(-3*time.Hour)), fleetOrigins(t)...)
	a.Offline()
	a.WriteFile(a.Layout.ConfigPath, `{"capture":{"recent_interval":"7m0s"}}`, 0o600)
	units := buildFleet(t, a)
	ids := byID(units)
	a.Captures.Gate()
	a.Online()

	order := priority(units)
	running, done := map[string]bool{}, map[string]bool{}
	var admitted []string
	for len(done) < len(units) {
		before := maps.Clone(running)
		admit(order, running, done, 2)
		for _, u := range order {
			if running[u.ID] && !before[u.ID] {
				admitted = append(admitted, u.ID)
			}
		}
		want := slices.Sorted(maps.Keys(running))
		if got := a.Captures.WaitParked(t, len(want)); !slices.Equal(got, want) {
			t.Fatalf("after %d finished, captures in flight = %v, want %v (priority order %v)", len(done), got, want, admitted)
		}
		oldest := admitted[slices.IndexFunc(admitted, func(id string) bool { return running[id] })]
		a.Captures.Release(oldest)
		delete(running, oldest)
		done[oldest] = true
	}

	records := a.Captures.WaitRecords(t, len(units))
	if len(records) != len(units) {
		t.Fatalf("captures = %d, want %d", len(records), len(units))
	}
	for _, r := range records {
		if r.Err != nil || r.Result.Outcome != scheduler.OutcomeCaptured {
			t.Fatalf("capture of %s = %+v, %v; want captured", r.WorktreeID, r.Result, r.Err)
		}
	}
	if all, perRepo := a.Captures.Peak(); all != 2 || perRepo != 1 {
		t.Fatalf("peak captures in flight = %d overall, %d per repo; want exactly 2 and 1", all, perRepo)
	}
	first := func(tier scheduler.Tier) int {
		return slices.IndexFunc(admitted, func(id string) bool { return ids[id].Tier == tier })
	}
	last := func(tier scheduler.Tier) int {
		i := -1
		for j, id := range admitted {
			if ids[id].Tier == tier {
				i = j
			}
		}
		return i
	}
	if last(scheduler.TierHuman) != 2 || first(scheduler.TierAutonomous) != 3 || last(scheduler.TierAutonomous) != 5 {
		t.Fatalf("admission order %v: want the 3 human worktrees first, then the 3 autonomous", admitted)
	}
	if want := []string{units[0].ID, units[2].ID, units[1].ID}; !slices.Equal(admitted[:3], want) {
		t.Fatalf("human admissions = %v, want %v: the second-freshest waits for its repo", admitted[:3], want)
	}

	dispatched := map[string]time.Time{}
	for _, d := range a.Captures.Dispatches() {
		if _, ok := dispatched[d.WorktreeID]; !ok {
			dispatched[d.WorktreeID] = d.At
		}
	}
	st := a.Status()
	intervals := map[scheduler.Tier]time.Duration{
		scheduler.TierHuman:      time.Duration(st.Capture.HumanInterval),
		scheduler.TierAutonomous: time.Duration(st.Capture.AutonomousInterval),
		scheduler.TierRecent:     time.Duration(st.Capture.RecentInterval),
		scheduler.TierIdle:       time.Duration(st.Capture.IdleInterval),
	}
	wantIntervals := map[scheduler.Tier]time.Duration{
		scheduler.TierHuman: 2 * time.Minute, scheduler.TierAutonomous: 5 * time.Minute,
		scheduler.TierRecent: 7 * time.Minute, scheduler.TierIdle: time.Hour,
	}
	if !maps.Equal(intervals, wantIntervals) {
		t.Fatalf("configured intervals = %v, want %v (recent_interval overridden to 7m)", intervals, wantIntervals)
	}
	if len(st.Scheduler.Units) != len(units) {
		t.Fatalf("scheduler tracks %d units, want %d", len(st.Scheduler.Units), len(units))
	}
	for _, u := range units {
		us := unitStatus(t, st.Scheduler, u.ID)
		if us.Tier != u.Tier {
			t.Fatalf("%s tier = %s, want %s", u.Root, us.Tier, u.Tier)
		}
		at, ok := dispatched[u.ID]
		if !ok {
			t.Fatalf("%s never dispatched", u.Root)
		}
		if lag := at.Sub(us.Due.Add(-wantIntervals[u.Tier])); lag < 0 || lag > 2*time.Second {
			t.Fatalf("%s (%s) due %s after dispatch %s: next capture is %s off its %s interval",
				u.Root, u.Tier, us.Due, at, lag, wantIntervals[u.Tier])
		}
	}

	snap := a.Catalog()
	latest := checkpointsByWorktree(snap)
	var sessions []string
	for _, u := range units {
		cps := latest[u.ID]
		if len(cps) != 1 {
			t.Fatalf("%s has %d checkpoints, want 1", u.Root, len(cps))
		}
		var got []string
		for _, s := range cps[0].Sessions {
			got = append(got, s.ID)
		}
		var want []string
		for _, s := range u.Sessions {
			want = append(want, s.ID)
		}
		slices.Sort(got)
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Fatalf("%s checkpoint sessions = %v, want %v", u.Root, got, want)
		}
		sessions = append(sessions, got...)
	}
	if len(sessions) != 40 {
		t.Fatalf("captured %d sessions, want 40", len(sessions))
	}

	a.Captures.Ungate()
	attempts := a.Kick()
	if len(attempts) != len(units) {
		t.Fatalf("kick attempted %d worktrees, want %d", len(attempts), len(units))
	}
	for _, at := range attempts {
		if at.Outcome != scheduler.OutcomeUnchanged {
			t.Fatalf("unchanged %s re-kicked = %+v, want unchanged", at.WorktreeID, at.Result)
		}
	}
	if got := len(a.Captures.Records()); got != len(units) {
		t.Fatalf("unchanged kick ran %d captures", got-len(units))
	}
}

func TestScaleCodeOnlyEditCapturesAtNextTier(t *testing.T) {
	const interval = 4 * time.Second
	origin := NewOrigin(t, "acme/solo", map[string]string{"README.md": "solo\n", "main.go": "package main\n"})
	a := NewHost(t, "host-a", NewClock(Now()), origin)
	a.Offline()
	a.WriteFile(a.Layout.ConfigPath, fmt.Sprintf(`{"capture":{"human_interval":%q}}`, interval), 0o600)
	a.Online()
	if got := time.Duration(a.Status().Capture.HumanInterval); got != interval {
		t.Fatalf("human interval = %s, want %s", got, interval)
	}

	src := a.Checkout("acme/solo")
	sess := a.WriteSession(SessionSpec{Cwd: src, Branch: "main", Turns: []Turn{{Human: true, Text: "tidy main"}, {Text: "tidied"}}})
	kicked := a.Kick(sess.ID)
	if len(kicked) != 1 || (kicked[0].Outcome != scheduler.OutcomeCaptured && kicked[0].Outcome != scheduler.OutcomeUnchanged) {
		t.Fatalf("kick = %+v, want one captured or already-captured attempt", kicked)
	}
	id := kicked[0].WorktreeID
	initial := checkpointsByWorktree(a.Catalog())[id]
	if len(initial) != 1 {
		t.Fatalf("catalog holds %d checkpoints after the kick, want 1", len(initial))
	}
	c1 := initial[0].ID
	us := unitStatus(t, a.Status().Scheduler, id)
	if us.Tier != scheduler.TierHuman {
		t.Fatalf("tier = %s, want human", us.Tier)
	}
	due := us.Due
	transcript := size(t, sess.Transcript)

	a.WriteFile(filepath.Join(src, "main.go"), "package main\n\nfunc main() {}\n", 0o644)
	a.WriteFile(filepath.Join(src, "scratch.txt"), "new untracked file\n", 0o644)
	a.Clock.Advance(time.Second)
	if edited := time.Now(); !edited.Before(due.Add(-time.Second)) {
		t.Fatalf("edit at %s is too close to the next due %s to observe the interval", edited, due)
	}
	baseline := len(a.Captures.Records())
	time.Sleep(time.Until(due.Add(-500 * time.Millisecond)))
	if got := len(a.Captures.Records()); got != baseline {
		t.Fatalf("%d captures ran before the tier interval elapsed", got-baseline)
	}
	if cps := checkpointsByWorktree(a.Catalog())[id]; len(cps) != 1 {
		t.Fatalf("catalog holds %d checkpoints before the interval, want 1", len(cps))
	}

	next := a.Captures.WaitRecords(t, baseline+1)[baseline]
	if next.WorktreeID != id || next.Err != nil || next.Result.Outcome != scheduler.OutcomeCaptured {
		t.Fatalf("code-only capture = %+v, want a captured %s", next, id)
	}
	if next.Start.Before(due) {
		t.Fatalf("code-only capture started %s, before the next due %s", next.Start, due)
	}
	if next.Result.Checkpoint == c1 {
		t.Fatalf("code-only capture reused checkpoint %s", c1)
	}
	if got := size(t, sess.Transcript); got != transcript {
		t.Fatalf("transcript grew %d → %d bytes; the edit must be code-only", transcript, got)
	}
	cps := checkpointsByWorktree(a.Catalog())[id]
	i := slices.IndexFunc(cps, func(cp catalog.Checkpoint) bool { return cp.ID == next.Result.Checkpoint })
	if i < 0 {
		t.Fatalf("catalog lacks code-only checkpoint %s: %+v", next.Result.Checkpoint, cps)
	}
	if len(cps[i].Sessions) != 1 || cps[i].Sessions[0].ID != sess.ID {
		t.Fatalf("code-only checkpoint sessions = %+v, want only %s", cps[i].Sessions, sess.ID)
	}
}

func TestScaleCoalescedDeliveryAndRedeliveryDedup(t *testing.T) {
	const maxWait = 10 * time.Second
	origins := fleetOrigins(t)
	mesh := NewMesh(t, NewClock(Now().Add(-3*time.Hour)))
	a := mesh.Add("host-a", origins...)
	peers := []*Host{mesh.Add("host-b", origins...), mesh.Add("host-c", origins...)}
	units := buildFleet(t, a)
	for _, at := range a.Kick() {
		if at.Outcome != scheduler.OutcomeCaptured && at.Outcome != scheduler.OutcomeUnchanged {
			t.Fatalf("initial capture of %s = %+v", at.WorktreeID, at.Result)
		}
	}
	initial := checkpointsByWorktree(a.Catalog())
	for _, u := range units {
		if len(initial[u.ID]) != 1 {
			t.Fatalf("%s has %d checkpoints after the initial kick, want 1", u.Root, len(initial[u.ID]))
		}
	}

	lanes := mesh.Lanes(maxWait, time.Second)
	settle := func() map[string]delivery.PeerStatus {
		t.Helper()
		out := map[string]delivery.PeerStatus{}
		for _, p := range peers {
			st, err := lanes.WaitIdle(t.Context(), "host-a", consumer.ServiceID, p.Name)
			if err != nil {
				t.Fatal(err)
			}
			if st.State != delivery.StateIdle || st.Pending != nil || st.LastError != "" {
				t.Fatalf("lane host-a→%s = %+v, want idle and acknowledged", p.Name, st)
			}
			out[p.Name] = st
		}
		return out
	}
	calls := func(method string) map[string]int {
		out := map[string]int{}
		for _, p := range peers {
			out[p.Name] = mesh.Link("host-a", p.Name).Calls(method)
		}
		return out
	}
	settle()
	for _, p := range peers {
		if n := mesh.Link("host-a", p.Name).Calls(syncservice.MethodApplyV2); n != 1 {
			t.Fatalf("start-up delivery to %s applied %d times, want 1", p.Name, n)
		}
		have := checkpointIDs(p.Catalog())
		for id := range checkpointIDs(a.Catalog()) {
			if !have[id] {
				t.Fatalf("%s lacks checkpoint %s after start-up delivery", p.Name, id)
			}
		}
	}

	applies, puts := calls(syncservice.MethodApplyV2), calls(artifact.MethodBatchPut)
	var fresh []string
	var firstKick time.Time
	for i, u := range units[:8] {
		s := u.Sessions[0]
		a.Clock.Advance(time.Second)
		a.AppendTurns(s, Turn{Human: true, Text: "one more change"}, Turn{Text: "changed"})
		got := a.Kick(s.ID)
		if len(got) != 1 || got[0].Outcome != scheduler.OutcomeCaptured {
			t.Fatalf("kick %s = %+v, want one capture", s.ID, got)
		}
		fresh = append(fresh, got[0].Checkpoint)
		if i == 0 {
			firstKick = time.Now()
		}
		if err := lanes.Kick(consumer.ServiceID, "host-a", ""); err != nil {
			t.Fatal(err)
		}
	}
	if spread := time.Since(firstKick); spread >= maxWait-time.Second {
		t.Fatalf("8 kicks spread over %s, too long to fall in one %s window", spread, maxWait)
	}
	time.Sleep(time.Until(firstKick.Add(maxWait - time.Second)))
	if got := calls(syncservice.MethodApplyV2); !maps.Equal(got, applies) {
		t.Fatalf("deliveries ran inside the coalescing window: applies %v → %v", applies, got)
	}
	coalesced := settle()
	for _, p := range peers {
		if got := calls(syncservice.MethodApplyV2)[p.Name] - applies[p.Name]; got != 1 {
			t.Fatalf("8 kicks in one window ran %d deliveries to %s, want 1", got, p.Name)
		}
		if calls(artifact.MethodBatchPut)[p.Name] == puts[p.Name] {
			t.Fatalf("coalesced delivery to %s uploaded no parts", p.Name)
		}
		if at := coalesced[p.Name].LastAttemptAt; at.Before(firstKick.Add(maxWait)) {
			t.Fatalf("delivery to %s ran at %s, before the window closed at %s", p.Name, at, firstKick.Add(maxWait))
		}
		have := checkpointIDs(p.Catalog())
		for _, id := range fresh {
			if !have[id] {
				t.Fatalf("%s lacks coalesced checkpoint %s", p.Name, id)
			}
		}
	}

	captures := len(a.Captures.Records())
	for _, at := range a.Kick() {
		if at.Outcome != scheduler.OutcomeUnchanged {
			t.Fatalf("unchanged %s re-kicked = %+v, want unchanged", at.WorktreeID, at.Result)
		}
	}
	if got := len(a.Captures.Records()); got != captures {
		t.Fatalf("unchanged kick ran %d captures", got-captures)
	}
	methods := []string{syncservice.MethodApplyV2, artifact.MethodBatchBegin, artifact.MethodBatchPut}
	before := map[string]map[string]int{}
	for _, m := range methods {
		before[m] = calls(m)
	}
	redeliver := time.Now()
	if err := lanes.Kick(consumer.ServiceID, "host-a", ""); err != nil {
		t.Fatal(err)
	}
	idle := settle()
	for _, p := range peers {
		if at := idle[p.Name].LastAttemptAt; at.Before(redeliver.Add(maxWait)) {
			t.Fatalf("redelivery lane to %s last ran %s; it never ran after the kick at %s", p.Name, at, redeliver)
		}
		if idle[p.Name].Acked != coalesced[p.Name].Acked {
			t.Fatalf("redelivery to %s moved the ack %v → %v", p.Name, coalesced[p.Name].Acked, idle[p.Name].Acked)
		}
	}
	for _, m := range methods {
		if got := calls(m); !maps.Equal(got, before[m]) {
			t.Fatalf("unchanged redelivery called %s: %v → %v", m, before[m], got)
		}
	}

	d, err := mesh.Deliver(t.Context(), "host-a", "host-b")
	if err != nil {
		t.Fatal(err)
	}
	if d.Objects != 0 || d.Bytes != 0 || d.Result.Partial || d.Result.AckedRevision != d.Change.SourceRevision {
		t.Fatalf("full replay = %d objects, %d bytes, %+v; want nothing uploaded and a full ACK", d.Objects, d.Bytes, d.Result)
	}
	if got := mesh.Link("host-a", "host-b").Calls(artifact.MethodBatchPut); got != before[artifact.MethodBatchPut]["host-b"] {
		t.Fatalf("full replay uploaded %d parts", got-before[artifact.MethodBatchPut]["host-b"])
	}
}
