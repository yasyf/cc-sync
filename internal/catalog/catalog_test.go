package catalog

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/yasyf/reposync/worktree"
	"github.com/yasyf/synckit/artifact"
	"github.com/yasyf/synckit/syncservice"
)

var t0 = time.Date(2026, 9, 26, 12, 30, 0, 0, time.UTC)

const testFingerprint = "0000000000000000000000000000000000000000000000000000000000000001"

type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

func ref(name string) artifact.Ref {
	return artifact.Ref{Digest: artifact.Sum([]byte(name)), Kind: artifact.KindManifest, Size: 100}
}

func tree(id string) Worktree {
	return Worktree{ID: id, Repo: Repo{Origin: "git@github.com:yasyf/" + id + ".git", RelPath: "yasyf/" + id, Branch: "main", SourcePath: "/src/" + id}}
}

func point(root string, captured, activity time.Time) Checkpoint {
	return Checkpoint{
		Root: ref(root), CapturedAt: captured, SourceActivityAt: activity,
		Sessions:     []Session{{ID: "s-" + root, LastActivity: activity, LastHumanActivity: activity, Activity: "human"}},
		Code:         worktree.Summary{WorktreeID: "wt", Head: "abc123", Digest: "d", CapturedAt: captured, Complete: true},
		Completeness: Completeness{Complete: true},
	}
}

func micros(at time.Time) uint64 {
	return uint64(at.UnixMicro())
}

func newStore(t *testing.T, self string, c *clock) *Store {
	t.Helper()
	return New(filepath.Join(t.TempDir(), "catalog-v1.json"), self, c.now)
}

func record(t *testing.T, s *Store, wt string, cp Checkpoint) Checkpoint {
	t.Helper()
	got, err := s.Record(t.Context(), tree(wt), cp)
	if err != nil {
		t.Fatalf("Record(%s): %v", wt, err)
	}
	return got
}

func change(t *testing.T, from string, revision uint64, p Payload) syncservice.ChangeEnvelope {
	t.Helper()
	data, err := Encode(p)
	if err != nil {
		t.Fatal(err)
	}
	roots, err := Roots(p)
	if err != nil {
		t.Fatal(err)
	}
	c, err := syncservice.NewExportedArtifactChange("cc-sync", testFingerprint, syncservice.ChangeSnapshot,
		syncservice.NewRevision(0), syncservice.NewRevision(revision), data, roots)
	if err != nil {
		t.Fatal(err)
	}
	if c, err = syncservice.BindDelivery(c, from); err != nil {
		t.Fatal(err)
	}
	return c
}

func allReady(p Payload) Evidence {
	ev := Evidence{Roots: map[artifact.Digest]bool{}, Verified: map[string]Readiness{}}
	for _, o := range p.Origins {
		for _, w := range o.Worktrees {
			for _, cp := range w.Checkpoints {
				ev.Roots[cp.Root.Digest] = true
				ev.Verified[cp.ID] = Readiness{Ready: true}
			}
		}
	}
	return ev
}

func apply(t *testing.T, s *Store, from string, revision uint64, p Payload) syncservice.ApplyResult {
	t.Helper()
	res, err := s.Apply(t.Context(), change(t, from, revision, p), p, allReady(p))
	if err != nil {
		t.Fatalf("Apply from %s: %v", from, err)
	}
	return res
}

func export(t *testing.T, s *Store) Exported {
	t.Helper()
	e, err := s.Export(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func block(t *testing.T, s *Store, origin string) Origin {
	t.Helper()
	snap, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	i, found := searchOrigin(snap.Origins, origin)
	if !found {
		t.Fatalf("no block for %s in %+v", origin, snap.Origins)
	}
	return snap.Origins[i]
}

func TestRetain(t *testing.T) {
	mk := func(id string, captured, activity time.Time) Checkpoint {
		return Checkpoint{ID: id, CapturedAt: captured, SourceActivityAt: activity, ExpiresAt: activity.Add(ExpiryWindow), Completeness: Completeness{Complete: true}}
	}
	tests := []struct {
		name string
		cps  []Checkpoint
		want map[string][]Class
	}{
		{
			name: "hourly just inside 24h",
			cps:  []Checkpoint{mk("a", t0.Add(-time.Minute), t0), mk("b", t0.Add(-HourlyWindow+time.Nanosecond), t0)},
			want: map[string][]Class{"a": {ClassLatest, ClassHourly, ClassDaily}, "b": {ClassHourly, ClassDaily}},
		},
		{
			name: "hourly ends at exactly 24h",
			cps:  []Checkpoint{mk("a", t0.Add(-time.Minute), t0), mk("c", t0.Add(-HourlyWindow), t0)},
			want: map[string][]Class{"a": {ClassLatest, ClassHourly, ClassDaily}, "c": {ClassDaily}},
		},
		{
			name: "newest per hour",
			cps: []Checkpoint{
				mk("a", t0.Add(-time.Minute), t0),
				mk("b", t0.Add(-20*time.Minute), t0),
				mk("c", t0.Add(-40*time.Minute), t0),
			},
			want: map[string][]Class{"a": {ClassLatest, ClassHourly, ClassDaily}, "c": {ClassHourly}},
		},
		{
			name: "daily just inside 7d",
			cps:  []Checkpoint{mk("a", t0, t0), mk("b", t0.Add(-DailyWindow+time.Nanosecond), t0)},
			want: map[string][]Class{"a": {ClassLatest, ClassHourly, ClassDaily}, "b": {ClassDaily}},
		},
		{
			name: "daily ends at exactly 7d",
			cps:  []Checkpoint{mk("a", t0, t0), mk("c", t0.Add(-DailyWindow), t0)},
			want: map[string][]Class{"a": {ClassLatest, ClassHourly, ClassDaily}},
		},
		{
			name: "expiry exactly 7d after source activity",
			cps: []Checkpoint{
				mk("a", t0.Add(-time.Minute), t0.Add(-ExpiryWindow)),
				mk("b", t0.Add(-2*time.Minute), t0.Add(-ExpiryWindow+time.Nanosecond)),
			},
			want: map[string][]Class{"b": {ClassLatest, ClassHourly, ClassDaily}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := map[string][]Class{}
			for _, cp := range Retain(tt.cps, t0) {
				got[cp.ID] = cp.Classes
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("Retain = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRecordDerivesExpiryAndBumpsRevision(t *testing.T) {
	c := &clock{t: t0}
	s := newStore(t, "a", c)
	activity := t0.Add(-time.Hour)
	got := record(t, s, "w1", point("r1", t0, activity))
	if !got.ExpiresAt.Equal(activity.Add(7 * 24 * time.Hour)) {
		t.Fatalf("ExpiresAt = %s, want %s", got.ExpiresAt, activity.Add(7*24*time.Hour))
	}
	if want := CheckpointID("a", "w1", ref("r1")); got.ID != want {
		t.Fatalf("ID = %s, want %s", got.ID, want)
	}
	if o := block(t, s, "a"); o.Revision != micros(t0) {
		t.Fatalf("first revision = %d, want now in micros %d", o.Revision, micros(t0))
	}
	record(t, s, "w1", point("r2", t0.Add(time.Minute), t0))
	if o := block(t, s, "a"); o.Revision != micros(t0)+1 || len(o.Worktrees[0].Checkpoints) != 1 {
		t.Fatalf("block = revision %d with %d checkpoints, want revision %d with 1 (same hour)", o.Revision, len(o.Worktrees[0].Checkpoints), micros(t0)+1)
	}
	record(t, s, "w1", point("r2", t0.Add(time.Minute), t0))
	if o := block(t, s, "a"); o.Revision != micros(t0)+1 {
		t.Fatalf("unchanged re-record moved revision to %d", o.Revision)
	}
	_, err := s.Record(t.Context(), tree("w1"), point("r3", t0, t0.Add(-ExpiryWindow)))
	if !errors.Is(err, ErrExpired) {
		t.Fatalf("Record expired = %v, want ErrExpired", err)
	}
	if o := block(t, s, "a"); o.Revision != micros(t0)+1 {
		t.Fatalf("expired record changed revision to %d", o.Revision)
	}
	c.t = t0.Add(-time.Hour)
	record(t, s, "w1", point("r4", t0.Add(2*time.Minute), t0))
	if o := block(t, s, "a"); o.Revision != micros(t0)+2 {
		t.Fatalf("revision after the clock stepped back = %d, want previous+1 %d", o.Revision, micros(t0)+2)
	}
	c.t = t0.Add(time.Hour)
	record(t, s, "w1", point("r5", t0.Add(time.Hour), t0.Add(time.Hour)))
	if o := block(t, s, "a"); o.Revision != micros(c.t) {
		t.Fatalf("revision = %d, want now in micros %d", o.Revision, micros(c.t))
	}
}

func TestGCTombstonesExpiredWorktrees(t *testing.T) {
	c := &clock{t: t0}
	s := newStore(t, "a", c)
	record(t, s, "w1", point("r1", t0, t0))
	c.t = t0.Add(ExpiryWindow)
	res, err := s.GC(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if res.Removed != 1 || len(res.Roots) != 0 {
		t.Fatalf("GC = %+v, want 1 removed and no roots", res)
	}
	want := Origin{Origin: "a", Revision: micros(c.t), Tombstones: []Tombstone{{ID: "w1", Revision: micros(c.t), DeletedAt: c.t, ExpiresAt: c.t.Add(ExpiryWindow)}}}
	if got := block(t, s, "a"); !reflect.DeepEqual(got, want) {
		t.Fatalf("block = %+v, want %+v", got, want)
	}
	c.t = c.t.Add(ExpiryWindow)
	if _, err := s.GC(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := block(t, s, "a"); got.Revision != micros(c.t) || len(got.Tombstones) != 0 {
		t.Fatalf("block = %+v, want revision %d without tombstones", got, micros(c.t))
	}
}

func TestRelayCopiesExpiryVerbatim(t *testing.T) {
	ca, cb, cc := &clock{t: t0}, &clock{t: t0.Add(time.Hour)}, &clock{t: t0.Add(2 * time.Hour)}
	a, b, c := newStore(t, "a", ca), newStore(t, "b", cb), newStore(t, "c", cc)
	recorded := record(t, a, "w1", point("r1", t0, t0.Add(-time.Hour)))
	fromA := export(t, a)
	if res := apply(t, b, "a", fromA.Revision, fromA.Payload); res.AckedRevision != syncservice.NewRevision(fromA.Revision) || res.Partial {
		t.Fatalf("b apply = %+v", res)
	}
	cb.t = t0.Add(24 * time.Hour)
	if _, err := b.GC(t.Context()); err != nil {
		t.Fatal(err)
	}
	fromB := export(t, b)
	apply(t, c, "b", fromB.Revision, fromB.Payload)
	got := block(t, c, "a")
	if got.Revision != micros(t0) || !got.Worktrees[0].Checkpoints[0].ExpiresAt.Equal(recorded.ExpiresAt) {
		t.Fatalf("relayed block = %+v, want revision %d expiring %s", got, micros(t0), recorded.ExpiresAt)
	}
	if !reflect.DeepEqual(got, block(t, a, "a")) {
		t.Fatalf("relayed block differs from origin:\n got %+v\nwant %+v", got, block(t, a, "a"))
	}
	cc.t = recorded.ExpiresAt
	res, err := c.GC(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if res.Removed != 1 || len(res.Roots) != 0 {
		t.Fatalf("GC at origin expiry = %+v, want the relayed checkpoint removed", res)
	}
}

func TestTombstoneBlocksStaleRelayedResurrection(t *testing.T) {
	ca, cc := &clock{t: t0}, &clock{t: t0}
	a, c := newStore(t, "a", ca), newStore(t, "c", cc)
	record(t, a, "w", point("rw", t0, t0))
	record(t, a, "x", point("rx", t0.Add(-6*24*time.Hour), t0.Add(-6*24*time.Hour)))
	stale := export(t, a).Payload
	if err := a.Remove(t.Context(), "w"); err != nil {
		t.Fatal(err)
	}
	fresh := export(t, a)
	apply(t, c, "a", fresh.Revision, fresh.Payload)
	cc.t = t0.Add(36 * time.Hour)
	if _, err := c.GC(t.Context()); err != nil {
		t.Fatal(err)
	}
	kept := block(t, c, "a")
	if kept.Revision != micros(t0)+2 || len(kept.Worktrees) != 0 || len(kept.Tombstones) != 1 {
		t.Fatalf("after x expired = %+v, want revision %d holding only the w tombstone", kept, micros(t0)+2)
	}
	stale.Exporter = "b"
	res := apply(t, c, "b", 1, stale)
	if res.AckedRevision != "1" || res.Partial {
		t.Fatalf("stale relay apply = %+v, want a full ack", res)
	}
	if got := block(t, c, "a"); !reflect.DeepEqual(got, kept) {
		t.Fatalf("stale relayed block resurrected: got %+v, want %+v", got, kept)
	}
}

func TestApplyFencesPerOriginRevision(t *testing.T) {
	ca, cc := &clock{t: t0}, &clock{t: t0}
	a, c := newStore(t, "a", ca), newStore(t, "c", cc)
	record(t, a, "w", point("r1", t0.Add(-2*time.Hour), t0))
	older := export(t, a).Payload
	record(t, a, "w", point("r2", t0, t0))
	newer := export(t, a).Payload
	apply(t, c, "a", 2, newer)
	older.Exporter = "b"
	apply(t, c, "b", 1, older)
	if got, want := block(t, c, "a"), newer.Origins[0]; !reflect.DeepEqual(got, want) {
		t.Fatalf("block = %+v, want the newer revision %+v", got, want)
	}
	subset := newer
	subset.Origins = []Origin{newer.Origins[0]}
	subset.Origins[0].Worktrees = []Worktree{newer.Origins[0].Worktrees[0]}
	subset.Origins[0].Worktrees[0].Checkpoints = newer.Origins[0].Worktrees[0].Checkpoints[:1]
	subset.Exporter = "b"
	for _, tt := range []struct {
		name  string
		first Payload
	}{
		{"nothing held", Payload{}},
		{"full block held", newer},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := newStore(t, "c", cc)
			if tt.first.Identity != "" {
				apply(t, s, "a", 1, tt.first)
			}
			before, err := s.Load()
			if err != nil {
				t.Fatal(err)
			}
			if tt.first.Identity == "" {
				apply(t, s, "a", 1, newer)
				if before, err = s.Load(); err != nil {
					t.Fatal(err)
				}
			}
			_, err = s.Apply(t.Context(), change(t, "b", 1, subset), subset, allReady(subset))
			if !errors.Is(err, ErrOriginBlockConflict) {
				t.Fatalf("equal revision with a different block = %v, want ErrOriginBlockConflict", err)
			}
			if _, err := s.Unverified(subset); !errors.Is(err, ErrOriginBlockConflict) {
				t.Fatalf("Unverified = %v, want ErrOriginBlockConflict", err)
			}
			after, err := s.Load()
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(after, before) {
				t.Fatalf("conflicting change was recorded: %+v, want %+v", after, before)
			}
			if decision, _, err := s.Fence(change(t, "b", 1, subset)); err != nil || decision != syncservice.FenceApply {
				t.Fatalf("conflicting change left a receipt: %v %v", decision, err)
			}
		})
	}
}

func TestFencesOutliveForgottenBlocks(t *testing.T) {
	older, newer, cc := &clock{t: t0}, &clock{t: t0.Add(time.Hour)}, &clock{t: t0}
	aOld, aNew, c := newStore(t, "a", older), newStore(t, "a", newer), newStore(t, "c", cc)
	record(t, aOld, "w", point("long", t0, t0))
	record(t, aNew, "w", point("short", t0.Add(-6*24*time.Hour), t0.Add(-6*24*time.Hour)))
	stale, fresh := export(t, aOld), export(t, aNew)
	apply(t, c, "a", fresh.Revision, fresh.Payload)
	cc.t = t0.Add(2 * 24 * time.Hour)
	res, err := c.GC(t.Context())
	if err != nil || res.Removed != 1 {
		t.Fatalf("GC = %+v, %v; want the lapsed block forgotten", res, err)
	}
	stale.Payload.Exporter = "b"
	apply(t, c, "b", 1, stale.Payload)
	if snap, err := c.Load(); err != nil || len(snap.Origins) != 0 {
		t.Fatalf("older revision resurrected past a forgotten block's fence: %+v, %v", snap.Origins, err)
	}
}

func TestRelayExportsLastArtifactCompleteRevision(t *testing.T) {
	ca, cb := &clock{t: t0}, &clock{t: t0}
	a, b := newStore(t, "a", ca), newStore(t, "b", cb)
	record(t, a, "w1", point("r1", t0, t0.Add(-6*24*time.Hour)))
	record(t, a, "w2", point("r2", t0, t0))
	first := export(t, a)
	closure := func(roots ...string) Evidence {
		ev := Evidence{Roots: map[artifact.Digest]bool{}, Verified: map[string]Readiness{}}
		for _, r := range roots {
			ev.Roots[ref(r).Digest] = true
		}
		return ev
	}
	relayed := func() (Exported, []Origin) {
		t.Helper()
		e := export(t, b)
		return e, slices.DeleteFunc(slices.Clone(e.Payload.Origins), func(o Origin) bool { return o.Origin == "b" })
	}
	res, err := b.Apply(t.Context(), change(t, "a", first.Revision, first.Payload), first.Payload, closure("r1"))
	if err != nil || !res.Partial {
		t.Fatalf("apply without r2's closure = %+v, %v; want Partial", res, err)
	}
	if _, origins := relayed(); len(origins) != 0 {
		t.Fatalf("relayed %+v before any revision was artifact-complete", origins)
	}
	res, err = b.Apply(t.Context(), change(t, "a", first.Revision, first.Payload), first.Payload, closure("r1", "r2"))
	if err != nil || !res.Partial {
		t.Fatalf("apply with closures but unverified code = %+v, %v; want Partial", res, err)
	}
	if got := block(t, b, "a"); len(got.Worktrees) != 2 {
		t.Fatalf("held block = %+v", got)
	}
	e1, origins := relayed()
	if len(origins) != 1 || !reflect.DeepEqual(origins[0], first.Payload.Origins[0]) {
		t.Fatalf("relayed %+v, want the artifact-complete block verbatim %+v", origins, first.Payload.Origins[0])
	}
	ca.t = t0.Add(time.Minute)
	record(t, a, "w3", point("r3", ca.t, ca.t))
	second := export(t, a)
	if _, err := b.Apply(t.Context(), change(t, "a", second.Revision, second.Payload), second.Payload, closure("r1", "r2")); err != nil {
		t.Fatal(err)
	}
	if got := block(t, b, "a"); got.Revision != second.Payload.Origins[0].Revision {
		t.Fatalf("held revision = %d, want %d", got.Revision, second.Payload.Origins[0].Revision)
	}
	if e, origins := relayed(); len(origins) != 1 || !reflect.DeepEqual(origins[0], first.Payload.Origins[0]) || e.Revision != e1.Revision {
		t.Fatalf("relayed %+v at %d, want the previous complete revision unchanged at %d", origins, e.Revision, e1.Revision)
	}
	if err := b.Settle(t.Context(), closure("r1", "r2", "r3")); err != nil {
		t.Fatal(err)
	}
	e2, origins := relayed()
	if len(origins) != 1 || !reflect.DeepEqual(origins[0], second.Payload.Origins[0]) || e2.Revision <= e1.Revision {
		t.Fatalf("relayed %+v at %d, want the newly complete block verbatim past %d", origins, e2.Revision, e1.Revision)
	}
	cb.t = t0.Add(2 * 24 * time.Hour)
	e3, origins := relayed()
	roots, err := Roots(e3.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(origins[0], second.Payload.Origins[0]) || slices.Contains(roots, ref("r1")) || len(roots) != 2 || e3.Revision <= e2.Revision {
		t.Fatalf("after r1 expired: relayed %+v roots %v at %d; want the block verbatim, r1 dropped from roots, revision past %d", origins, roots, e3.Revision, e2.Revision)
	}
	snap, err := b.Load()
	if err != nil {
		t.Fatal(err)
	}
	if i, _ := searchOrigin(snap.Origins, "a"); len(snap.Origins[i].Worktrees) != 2 {
		t.Fatalf("local view of a = %+v, want r1's worktree hidden once expired", snap.Origins[i])
	}
}

func TestRootsOverflowKeepsLatestThenHourlyThenDaily(t *testing.T) {
	n := artifact.MaxRoots/3 + 100
	o := Origin{Origin: "a", Revision: 1}
	var latest, hourly, daily []artifact.Ref
	for i := range n {
		w := tree(fmt.Sprintf("w%05d", i))
		for _, c := range []struct {
			kind    string
			age     time.Duration
			classes []Class
		}{
			{"latest", 0, []Class{ClassLatest, ClassHourly, ClassDaily}},
			{"hourly", 2 * time.Hour, []Class{ClassHourly}},
			{"daily", 30 * time.Hour, []Class{ClassDaily}},
		} {
			at := t0.Add(-c.age - time.Duration(i)*time.Second)
			cp := point(fmt.Sprintf("%s-%05d", c.kind, i), at, at)
			cp.ExpiresAt, cp.Classes = at.Add(ExpiryWindow), c.classes
			w.Checkpoints = append(w.Checkpoints, cp)
			switch c.kind {
			case "latest":
				latest = append(latest, cp.Root)
			case "hourly":
				hourly = append(hourly, cp.Root)
			default:
				daily = append(daily, cp.Root)
			}
		}
		o.Worktrees = append(o.Worktrees, w)
	}
	got, err := Roots(Payload{AsOf: t0, Origins: []Origin{o}})
	if err != nil {
		t.Fatal(err)
	}
	want := slices.Concat(latest, hourly, daily)[:artifact.MaxRoots]
	if !slices.Equal(got, want) {
		t.Fatalf("Roots = %d roots, want the first %d of latest, hourly, then daily by recency", len(got), artifact.MaxRoots)
	}
}

func TestDecodeIsStrict(t *testing.T) {
	c := &clock{t: t0}
	s := newStore(t, "a", c)
	record(t, s, "w", point("r1", t0, t0))
	good := export(t, s).Data
	var indented bytes.Buffer
	indented.Write(bytes.Replace(good, []byte(`{"identity"`), []byte(`{ "identity"`), 1))
	tests := []struct {
		name string
		data []byte
		ok   bool
	}{
		{"canonical", good, true},
		{"unknown field", bytes.Replace(good, []byte(`{"identity"`), []byte(`{"extra":1,"identity"`), 1), false},
		{"non-canonical whitespace", indented.Bytes(), false},
		{"forged checkpoint id", bytes.Replace(good, []byte(`"id":"`+CheckpointID("a", "w", ref("r1"))), []byte(`"id":"`+CheckpointID("a", "w", ref("r2"))), 1), false},
		{"renewed expiry", bytes.Replace(good, []byte(`"expires_at":"2026-10-03`), []byte(`"expires_at":"2026-10-04`), 1), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Decode(tt.data); (err == nil) != tt.ok {
				t.Fatalf("Decode err = %v, want ok=%v", err, tt.ok)
			}
		})
	}
}

func TestRootsPriorityIsDeterministic(t *testing.T) {
	c := &clock{t: t0}
	s := newStore(t, "a", c)
	record(t, s, "w1", point("old1", t0.Add(-3*time.Hour), t0.Add(-3*time.Hour)))
	record(t, s, "w1", point("new1", t0.Add(-time.Hour), t0.Add(-time.Hour)))
	record(t, s, "w2", point("old2", t0.Add(-45*time.Minute), t0.Add(-45*time.Minute)))
	record(t, s, "w2", point("new2", t0.Add(-5*time.Minute), t0.Add(-5*time.Minute)))
	p := export(t, s).Payload
	want := []artifact.Ref{ref("new2"), ref("new1"), ref("old2"), ref("old1")}
	got, err := Roots(p)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("Roots = %v, want %v", got, want)
	}
	slices.Reverse(p.Origins[0].Worktrees)
	if again, _ := Roots(p); !slices.Equal(again, want) {
		t.Fatalf("Roots after reordering = %v, want %v", again, want)
	}
	first, second := export(t, s), export(t, s)
	if !bytes.Equal(first.Data, second.Data) || first.Revision != second.Revision {
		t.Fatalf("unchanged catalog exported revisions %d and %d", first.Revision, second.Revision)
	}
}

func readStamp(t *testing.T, dir string) (uint64, string) {
	t.Helper()
	generation, digest, err := (&Publisher{dir: dir}).read()
	if err != nil {
		t.Fatal(err)
	}
	return generation, digest
}

func TestPublisherCoalescesBumps(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		dir := t.TempDir()
		s := New(filepath.Join(dir, "catalog-v1.json"), "a", time.Now)
		stamp := filepath.Join(dir, "stamp")
		p := NewPublisher(s, stamp)
		if err := p.Ensure(); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- p.Run(ctx) }()
		assertGeneration := func(want uint64) {
			t.Helper()
			synctest.Wait()
			if got, _ := readStamp(t, stamp); got != want {
				t.Fatalf("generation = %d, want %d", got, want)
			}
		}
		_ = p.Publish(ctx)
		assertGeneration(1)
		for i := range 40 {
			if _, err := s.Record(ctx, tree("w"), point(fmt.Sprint("r", i), time.Now(), time.Now())); err != nil {
				t.Fatal(err)
			}
			_ = p.Publish(ctx)
			time.Sleep(100 * time.Millisecond)
		}
		assertGeneration(2)
		time.Sleep(StampWindow)
		assertGeneration(3)
		digest, err := s.Digest()
		if err != nil {
			t.Fatal(err)
		}
		if _, announced := readStamp(t, stamp); announced != digest {
			t.Fatalf("stamp digest = %s, want %s", announced, digest)
		}
		_ = p.Publish(ctx)
		time.Sleep(3 * StampWindow)
		assertGeneration(3)
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("Run = %v, want context.Canceled", err)
		}
	})
}

func TestEchoedOwnBlockNeverOverridesOwnState(t *testing.T) {
	ca, cb := &clock{t: t0}, &clock{t: t0}
	previous, b := newStore(t, "a", ca), newStore(t, "b", cb)
	record(t, previous, "w1", point("r1", t0.Add(-2*time.Hour), t0))
	record(t, previous, "w1", point("r2", t0, t0))
	e := export(t, previous)
	apply(t, b, "a", e.Revision, e.Payload)
	reset := newStore(t, "a", ca)
	record(t, reset, "w2", point("r3", t0, t0))
	own := block(t, reset, "a")
	echo := export(t, b)
	if res := apply(t, reset, "b", echo.Revision, echo.Payload); res.Partial {
		t.Fatalf("echo apply = %+v, want a full ack", res)
	}
	if got := block(t, reset, "a"); !reflect.DeepEqual(got, own) {
		t.Fatalf("echoed revision %d overrode own block: got %+v, want %+v", e.Payload.Origins[0].Revision, got, own)
	}
}

func TestRetainKeepsLastCompleteUnderMixed(t *testing.T) {
	at := func(d time.Duration) time.Time { return t0.Add(-d) }
	cp := func(id string, age time.Duration, deferred string) Checkpoint {
		return Checkpoint{ID: id, CapturedAt: at(age), ExpiresAt: t0.Add(24 * time.Hour), Deferred: deferred, Completeness: Completeness{Complete: true}}
	}
	kept := Retain([]Checkpoint{
		cp("complete", 30*time.Minute, ""),
		cp("mixed-1", 20*time.Minute, "deferred:rebase"),
		cp("mixed-2", 10*time.Minute, "deferred:partial-max-new-bytes"),
	}, t0)
	got := make([]string, 0, len(kept))
	for _, k := range kept {
		got = append(got, k.ID+":"+fmt.Sprint(k.Classes))
	}
	want := []string{"mixed-2:[latest]", "complete:[latest hourly daily]"}
	if !slices.Equal(got, want) {
		t.Fatalf("Retain = %v, want %v", got, want)
	}
}

func TestReadinessOfMixed(t *testing.T) {
	snap := Snapshot{Self: "me", Readiness: map[string]Readiness{"relayed": {Ready: true}, "gap": {Missing: []string{MissingClosure}}}}
	tests := []struct {
		name   string
		origin string
		cp     Checkpoint
		want   Readiness
	}{
		{"own complete", "me", Checkpoint{ID: "own"}, Readiness{Ready: true}},
		{"own mixed", "me", Checkpoint{ID: "own", Deferred: "deferred:rebase"}, Readiness{Deferred: "deferred:rebase"}},
		{"relayed verified mixed", "peer", Checkpoint{ID: "relayed", Deferred: "deferred:missing-lfs"}, Readiness{Deferred: "deferred:missing-lfs"}},
		{"relayed missing mixed", "peer", Checkpoint{ID: "gap", Deferred: "deferred:rebase"}, Readiness{Missing: []string{MissingClosure}, Deferred: "deferred:rebase"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := snap.ReadinessOf(tt.origin, tt.cp); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ReadinessOf = %+v, want %+v", got, tt.want)
			}
		})
	}
}
