package catalog

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/yasyf/synckit/artifact"
	"github.com/yasyf/synckit/syncservice"
)

func evidence(roots []Checkpoint, verified []Checkpoint) Evidence {
	ev := Evidence{Roots: map[artifact.Digest]bool{}, Verified: map[string]Readiness{}}
	for _, cp := range roots {
		ev.Roots[cp.Root.Digest] = true
	}
	for _, cp := range verified {
		ev.Verified[cp.ID] = Readiness{Ready: true}
	}
	return ev
}

func applyWith(t *testing.T, s *Store, ch syncservice.ChangeEnvelope, p Payload, ev Evidence) syncservice.ApplyResult {
	t.Helper()
	res, err := s.Apply(t.Context(), ch, p, ev)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	return res
}

func TestApplyAcksMixedOnlyOnceItsCodeIsVerified(t *testing.T) {
	a, b := newStore(t, "a", &clock{t: t0}), newStore(t, "b", &clock{t: t0})
	mixed := point("rm", t0, t0)
	mixed.Deferred = "deferred:partial-max-new-bytes"
	cp := record(t, a, "w", mixed)
	out := export(t, a)
	ch := change(t, "a", out.Revision, out.Payload)
	if res := applyWith(t, b, ch, out.Payload, evidence([]Checkpoint{cp}, nil)); !res.Partial || res.AckedRevision != syncservice.NewRevision(0) {
		t.Fatalf("mixed apply without a code verdict = %+v, want Partial at revision 0", res)
	}
	if res := applyWith(t, b, ch, out.Payload, evidence([]Checkpoint{cp}, []Checkpoint{cp})); res.Partial || res.AckedRevision != syncservice.NewRevision(out.Revision) {
		t.Fatalf("mixed apply with verified code = %+v, want a full ack of %d", res, out.Revision)
	}
	assertHeldNotPickupReady(t, a, b, cp, out.Revision)
}

func assertHeldNotPickupReady(t *testing.T, source, peer *Store, cp Checkpoint, acked uint64) {
	t.Helper()
	snap, err := peer.Load()
	if err != nil {
		t.Fatal(err)
	}
	if !snap.HeldReady(source.self, cp) || snap.PickupReady(source.self, cp) {
		t.Fatalf("peer HeldReady=%t PickupReady=%t, want held ready but not pick-up ready", snap.HeldReady(source.self, cp), snap.PickupReady(source.self, cp))
	}
	own, err := source.Load()
	if err != nil {
		t.Fatal(err)
	}
	if got := own.AssuranceOf(cp, acked, t0); got != AssuranceHeld {
		t.Fatalf("source assurance = %q, want %q", got, AssuranceHeld)
	}
}

func TestApplyAcksHeldIncompleteCheckpoint(t *testing.T) {
	a, b := newStore(t, "a", &clock{t: t0}), newStore(t, "b", &clock{t: t0})
	incomplete := point("ri", t0, t0)
	incomplete.Completeness = Completeness{Missing: []string{"session:s-ri:tool-results/x.txt"}}
	cp := record(t, a, "w", incomplete)
	out := export(t, a)
	ch := change(t, "a", out.Revision, out.Payload)
	if res := applyWith(t, b, ch, out.Payload, evidence([]Checkpoint{cp}, []Checkpoint{cp})); res.Partial || res.AckedRevision != syncservice.NewRevision(out.Revision) {
		t.Fatalf("held incomplete apply = %+v, want a full ack of %d", res, out.Revision)
	}
	assertHeldNotPickupReady(t, a, b, cp, out.Revision)
}

func TestRetainNeverLetsIncompleteEvictComplete(t *testing.T) {
	cp := func(id string, age time.Duration, mutate func(*Checkpoint)) Checkpoint {
		c := Checkpoint{ID: id, CapturedAt: t0.Add(-age), ExpiresAt: t0.Add(24 * time.Hour), Completeness: Completeness{Complete: true}}
		if mutate != nil {
			mutate(&c)
		}
		return c
	}
	incomplete := func(c *Checkpoint) { c.Completeness = Completeness{Missing: []string{"session:x"}} }
	mixed := func(c *Checkpoint) { c.Deferred = "deferred:rebase" }
	omitted := func(c *Checkpoint) {
		c.Omitted = []OmittedBinding{{Agent: "codex", Key: "k", ID: "i", Reason: "unsupported"}}
	}
	tests := []struct {
		name string
		cps  []Checkpoint
		want []string
	}{
		{
			name: "session-incomplete newer than complete",
			cps:  []Checkpoint{cp("complete", 30*time.Minute, nil), cp("incomplete", 10*time.Minute, incomplete)},
			want: []string{"incomplete:[latest]", "complete:[latest hourly daily]"},
		},
		{
			name: "omission is complete",
			cps:  []Checkpoint{cp("complete", 30*time.Minute, nil), cp("omitted", 10*time.Minute, omitted)},
			want: []string{"omitted:[latest hourly daily]"},
		},
		{
			name: "newest mixed and newest incomplete",
			cps: []Checkpoint{
				cp("complete", 50*time.Minute, nil),
				cp("incomplete-old", 40*time.Minute, incomplete),
				cp("mixed-old", 30*time.Minute, mixed),
				cp("incomplete", 20*time.Minute, incomplete),
				cp("mixed", 10*time.Minute, mixed),
			},
			want: []string{"mixed:[latest]", "incomplete:[latest]", "complete:[latest hourly daily]"},
		},
		{
			name: "incomplete older than complete",
			cps:  []Checkpoint{cp("incomplete", 40*time.Minute, incomplete), cp("complete", 30*time.Minute, nil)},
			want: []string{"complete:[latest hourly daily]"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got []string
			for _, k := range Retain(tt.cps, t0) {
				got = append(got, k.ID+":"+fmt.Sprint(k.Classes))
			}
			if !slices.Equal(got, tt.want) {
				t.Fatalf("Retain = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestApplyDisposesPendingCheckpointOnceExpired(t *testing.T) {
	ca, cb := &clock{t: t0}, &clock{t: t0}
	a, b := newStore(t, "a", ca), newStore(t, "b", cb)
	cp := record(t, a, "w", point("r", t0, t0.Add(-ExpiryWindow+time.Hour)))
	out := export(t, a)
	ch := change(t, "a", out.Revision, out.Payload)
	nothing := evidence(nil, nil)
	if res := applyWith(t, b, ch, out.Payload, nothing); !res.Partial || res.AckedRevision != syncservice.NewRevision(0) {
		t.Fatalf("pending apply = %+v, want Partial at revision 0", res)
	}
	cb.t = cp.ExpiresAt
	if _, err := b.GC(t.Context()); err != nil {
		t.Fatal(err)
	}
	if res := applyWith(t, b, ch, out.Payload, nothing); res.Partial || res.AckedRevision != syncservice.NewRevision(out.Revision) {
		t.Fatalf("replay after expiry = %+v, want a processed ack of %d", res, out.Revision)
	}
	stale := out.Payload
	stale.Exporter = "c"
	if res := apply(t, b, "c", 1, stale); res.Partial || res.AckedRevision != "1" {
		t.Fatalf("stale relay after expiry = %+v, want a processed ack", res)
	}
	snap, err := b.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Origins) != 0 || len(snap.Readiness) != 0 {
		t.Fatalf("catalog after expiry = %+v, want nothing held", snap)
	}
	if pending, err := b.Pending(); err != nil || len(pending) != 0 {
		t.Fatalf("Pending = %v, %v; want none", pending, err)
	}
	if relayed := export(t, b).Payload.Origins; len(relayed) != 0 {
		t.Fatalf("export after expiry relays %+v, want nothing", relayed)
	}
	own, err := a.Load()
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name  string
		acked uint64
		at    time.Time
		want  Assurance
	}{
		{"acked before expiry", out.Revision, t0, AssuranceDurable},
		{"acked an earlier export", out.Revision - 1, t0, AssuranceNone},
		{"acked at expiry", out.Revision, cp.ExpiresAt, AssuranceNone},
	} {
		if got := own.AssuranceOf(cp, tt.acked, tt.at); got != tt.want {
			t.Errorf("%s: source assurance = %q, want %q", tt.name, got, tt.want)
		}
	}
}

func TestApplyDisposesCheckpointExpiredOnArrival(t *testing.T) {
	a := newStore(t, "a", &clock{t: t0})
	cp := record(t, a, "w", point("r", t0, t0.Add(-ExpiryWindow+time.Hour)))
	out := export(t, a)
	b := newStore(t, "b", &clock{t: cp.ExpiresAt})
	if res := applyWith(t, b, change(t, "a", out.Revision, out.Payload), out.Payload, evidence(nil, nil)); res.Partial || res.AckedRevision != syncservice.NewRevision(out.Revision) {
		t.Fatalf("apply of an expired checkpoint = %+v, want a processed ack of %d", res, out.Revision)
	}
	st, err := b.read()
	if err != nil {
		t.Fatal(err)
	}
	if f, ok := findFence(st.Fences, "a"); len(st.Origins) != 0 || len(st.Readiness) != 0 || !ok || f.Revision != out.Payload.Origins[0].Revision {
		t.Fatalf("state = %+v, want only a fence at revision %d", st, out.Payload.Origins[0].Revision)
	}
}

func TestApplyAcksStaleOriginBlockOnlyOnceItsCheckpointsAreHeldReady(t *testing.T) {
	a, b := newStore(t, "a", &clock{t: t0}), newStore(t, "b", &clock{t: t0})
	x := record(t, a, "wx", point("rx", t0, t0))
	first := export(t, a)
	y := record(t, a, "wy", point("ry", t0.Add(time.Minute), t0.Add(time.Minute)))
	relayed := export(t, a).Payload
	relayed.Exporter = "c"
	if res := applyWith(t, b, change(t, "c", 1, relayed), relayed, evidence([]Checkpoint{x, y}, []Checkpoint{y})); !res.Partial {
		t.Fatalf("relayed apply without x's code verdict = %+v, want Partial", res)
	}
	stale := change(t, "a", first.Revision, first.Payload)
	if res := applyWith(t, b, stale, first.Payload, evidence([]Checkpoint{x}, nil)); !res.Partial || res.AckedRevision != syncservice.NewRevision(0) {
		t.Fatalf("stale apply without x's code verdict = %+v, want Partial at revision 0", res)
	}
	unverified, err := b.Unverified(first.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(unverified) != 1 || unverified[0].ID != x.ID {
		t.Fatalf("Unverified(stale) = %+v, want only %s", unverified, x.ID)
	}
	if res := applyWith(t, b, stale, first.Payload, evidence([]Checkpoint{x}, []Checkpoint{x})); res.Partial || res.AckedRevision != syncservice.NewRevision(first.Revision) {
		t.Fatalf("stale apply with x verified = %+v, want a full ack of %d", res, first.Revision)
	}
	snap, err := b.Load()
	if err != nil {
		t.Fatal(err)
	}
	if !snap.PickupReady("a", x) {
		t.Fatalf("x PickupReady = false after its verified stale apply")
	}
	if got := block(t, b, "a").Revision; got != relayed.Origins[0].Revision {
		t.Fatalf("held block revision = %d, want the newer %d", got, relayed.Origins[0].Revision)
	}
}
