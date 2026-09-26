package consumer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-sync/internal/catalog"
	"github.com/yasyf/reposync/worktree"
	"github.com/yasyf/synckit/artifact"
	"github.com/yasyf/synckit/syncservice"
)

var t0 = time.Date(2026, 9, 26, 12, 30, 0, 0, time.UTC)

type fakeArtifacts struct {
	owner    string
	roots    []artifact.Ref
	gcs      int
	complete map[artifact.Digest]bool
}

func (f *fakeArtifacts) Complete(_ context.Context, roots []artifact.Ref) (int, error) {
	missing := 0
	for _, r := range roots {
		if !f.complete[r.Digest] {
			missing++
		}
	}
	return missing, nil
}

func (f *fakeArtifacts) SetPins(_ context.Context, owner string, roots []artifact.Ref) error {
	f.owner, f.roots = owner, roots
	return nil
}

func (f *fakeArtifacts) GC(context.Context) (artifact.GCReport, error) {
	f.gcs++
	return artifact.GCReport{}, nil
}

type fakeVerifier struct {
	verdicts map[artifact.Digest]CodeVerdict
	calls    []artifact.Digest
	fetches  []bool
}

func (f *fakeVerifier) VerifyCode(_ context.Context, root artifact.Ref, fetchOrigin bool) (CodeVerdict, error) {
	f.calls = append(f.calls, root.Digest)
	f.fetches = append(f.fetches, fetchOrigin)
	if v, ok := f.verdicts[root.Digest]; ok {
		return v, nil
	}
	return CodeVerdict{Ready: true}, nil
}

type countingPublisher struct{ n int }

func (p *countingPublisher) Publish(context.Context) error {
	p.n++
	return nil
}

type host struct {
	name      string
	clock     *time.Time
	catalog   *catalog.Store
	artifacts *fakeArtifacts
	verifier  *fakeVerifier
	publisher *countingPublisher
	fetch     bool
	stampDir  string
	consumer  *Consumer
}

func newHost(t *testing.T, name string) *host {
	t.Helper()
	dir := t.TempDir()
	now := t0
	h := &host{
		name: name, clock: &now, artifacts: &fakeArtifacts{complete: map[artifact.Digest]bool{}}, verifier: &fakeVerifier{verdicts: map[artifact.Digest]CodeVerdict{}},
		publisher: &countingPublisher{}, stampDir: filepath.Join(dir, "stamp"),
	}
	h.catalog = catalog.New(filepath.Join(dir, "catalog-v1.json"), name, func() time.Time { return *h.clock })
	h.consumer = New(Config{
		Catalog: h.catalog, Publisher: h.publisher, Artifacts: h.artifacts, Verifier: h.verifier,
		FetchAllowed: func() bool { return h.fetch }, StampDir: h.stampDir,
	})
	return h
}

func ref(name string) artifact.Ref {
	return artifact.Ref{Digest: artifact.Sum([]byte(name)), Kind: artifact.KindManifest, Size: 4096}
}

func tree(id string) catalog.Worktree {
	return catalog.Worktree{ID: id, Repo: catalog.Repo{Origin: "git@github.com:yasyf/" + id + ".git", RelPath: "yasyf/" + id, Branch: "main", SourcePath: "/Users/yasyf/Code/" + id}}
}

func point(root string, at time.Time, sessions int) catalog.Checkpoint {
	cp := catalog.Checkpoint{
		Root: ref(root), CapturedAt: at, SourceActivityAt: at,
		Code:         worktree.Summary{WorktreeID: "wt-" + root, Branch: "main", Head: strings.Repeat("a", 40), Digest: strings.Repeat("b", 64), CapturedAt: at, Ahead: 2, Staged: 3, Unstaged: 4, Untracked: 1, Bytes: 1 << 20, Complete: true},
		Completeness: catalog.Completeness{Complete: true},
	}
	for i := range sessions {
		cp.Sessions = append(cp.Sessions, catalog.Session{
			ID: fmt.Sprintf("%08d-0000-4000-8000-%012d", i, i), Title: strings.Repeat("refactor the scheduler tiers ", 3),
			LastActivity: at, LastHumanActivity: at.Add(-time.Duration(i) * time.Minute), Activity: "human", ClaudeVersion: "2.3.14",
		})
	}
	return cp
}

func (h *host) record(t *testing.T, wt, root string, at time.Time) catalog.Checkpoint {
	t.Helper()
	cp, err := h.catalog.Record(t.Context(), tree(wt), point(root, at, 2))
	if err != nil {
		t.Fatal(err)
	}
	return cp
}

func micros(at time.Time) syncservice.Revision {
	return syncservice.NewRevision(uint64(at.UnixMicro()))
}

func (h *host) export(t *testing.T) syncservice.ChangeEnvelope {
	t.Helper()
	change, err := h.consumer.ExportArtifacts(t.Context(), syncservice.ExportRequest{ServiceID: ServiceID, SchemaFingerprint: Fingerprint, SinceRevision: "0"})
	if err != nil {
		t.Fatal(err)
	}
	if change, err = syncservice.BindDelivery(change, h.name); err != nil {
		t.Fatal(err)
	}
	return change
}

func TestFingerprintIsLowercaseSHA256(t *testing.T) {
	if err := syncservice.ValidateServiceSchema(ServiceID, Fingerprint); err != nil {
		t.Fatal(err)
	}
}

func TestListReportsOnlyTheStamp(t *testing.T) {
	h := newHost(t, "a")
	if err := catalog.NewPublisher(h.catalog, h.stampDir).Ensure(); err != nil {
		t.Fatal(err)
	}
	stamp, err := os.ReadFile(filepath.Join(h.stampDir, catalog.StampFile))
	if err != nil {
		t.Fatal(err)
	}
	got, err := h.consumer.List(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	want := []syncservice.WatchItem{{ID: "checkpoints", WatchDirs: []string{h.stampDir}, Fingerprint: string(stamp)}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("List = %+v, want %+v", got, want)
	}
	caps, err := h.consumer.Capabilities(t.Context())
	if err != nil || !reflect.DeepEqual(caps, syncservice.ArtifactCapabilities("cc-sync")) {
		t.Fatalf("Capabilities = %+v, %v", caps, err)
	}
}

func TestExportArtifacts(t *testing.T) {
	h := newHost(t, "a")
	h.record(t, "w1", "r1", t0)
	first := h.export(t)
	payload, err := catalog.Decode(first.Payload)
	if err != nil {
		t.Fatal(err)
	}
	roots, err := catalog.Roots(payload)
	if err != nil {
		t.Fatal(err)
	}
	if first.Kind != syncservice.ChangeSnapshot || first.BaseRevision != "0" || first.SourceRevision != micros(t0) || !slices.Equal(first.Artifacts, roots) {
		t.Fatalf("export = kind %s base %s source %s artifacts %v", first.Kind, first.BaseRevision, first.SourceRevision, first.Artifacts)
	}
	if again := h.export(t); again.ChangeID != first.ChangeID || again.SourceRevision != first.SourceRevision {
		t.Fatalf("unchanged export = %s@%s, want %s@%s", again.ChangeID, again.SourceRevision, first.ChangeID, first.SourceRevision)
	}
	h.record(t, "w1", "r2", t0.Add(time.Minute))
	if next, want := h.export(t), syncservice.NewRevision(uint64(t0.UnixMicro())+1); next.SourceRevision != want {
		t.Fatalf("changed export source = %s, want previous+1 %s", next.SourceRevision, want)
	}
	*h.clock = t0.Add(time.Hour)
	h.record(t, "w1", "r3", t0.Add(time.Hour))
	if next := h.export(t); next.SourceRevision != micros(*h.clock) {
		t.Fatalf("changed export source = %s, want now in micros %s", next.SourceRevision, micros(*h.clock))
	}
	for _, req := range []syncservice.ExportRequest{
		{ServiceID: "other", SchemaFingerprint: Fingerprint, SinceRevision: "0"},
		{ServiceID: ServiceID, SchemaFingerprint: strings.Repeat("0", 64), SinceRevision: "0"},
	} {
		if _, err := h.consumer.ExportArtifacts(t.Context(), req); !errors.Is(err, ErrSchema) {
			t.Fatalf("ExportArtifacts(%s) = %v, want ErrSchema", req.ServiceID, err)
		}
	}
	if _, err := h.consumer.Export(t.Context(), syncservice.ExportRequest{}); !errors.Is(err, ErrV1) {
		t.Fatalf("Export = %v, want ErrV1", err)
	}
	if _, err := h.consumer.Apply(t.Context(), first); !errors.Is(err, ErrV1) {
		t.Fatalf("Apply = %v, want ErrV1", err)
	}
}

func TestApplyArtifactsReadiness(t *testing.T) {
	tests := []struct {
		name        string
		readyRoots  []string
		verdicts    map[string]CodeVerdict
		fetch       bool
		wantPartial bool
		want        map[string]catalog.Readiness
	}{
		{
			name:       "every checkpoint ready",
			readyRoots: []string{"r1", "r2"},
			want:       map[string]catalog.Readiness{"r1": {Ready: true}, "r2": {Ready: true}},
		},
		{
			name:        "root closure missing",
			readyRoots:  []string{"r1"},
			wantPartial: true,
			want:        map[string]catalog.Readiness{"r1": {Ready: true}, "r2": {Missing: []string{catalog.MissingClosure}}},
		},
		{
			name:        "prerequisites missing",
			readyRoots:  []string{"r1", "r2"},
			verdicts:    map[string]CodeVerdict{"r2": {Missing: []string{"trunk base 1234"}}},
			wantPartial: true,
			want:        map[string]catalog.Readiness{"r1": {Ready: true}, "r2": {Missing: []string{"trunk base 1234"}, Deferred: catalog.MissingPrerequisites}},
		},
		{
			name:        "prerequisites missing with fetch allowed",
			readyRoots:  []string{"r1", "r2"},
			verdicts:    map[string]CodeVerdict{"r2": {Missing: []string{"trunk base 1234"}}},
			fetch:       true,
			wantPartial: true,
			want:        map[string]catalog.Readiness{"r1": {Ready: true}, "r2": {Missing: []string{"trunk base 1234"}, Deferred: catalog.MissingPrerequisites}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, b := newHost(t, "a"), newHost(t, "b")
			ids := map[string]string{"r1": a.record(t, "w1", "r1", t0).ID, "r2": a.record(t, "w2", "r2", t0.Add(-time.Minute)).ID}
			change := a.export(t)
			var ready []artifact.Ref
			for _, r := range tt.readyRoots {
				ready = append(ready, ref(r))
			}
			for r, v := range tt.verdicts {
				b.verifier.verdicts[ref(r).Digest] = v
			}
			b.fetch = tt.fetch
			res, err := b.consumer.ApplyArtifacts(t.Context(), change, ready)
			if err != nil {
				t.Fatal(err)
			}
			want := syncservice.ApplyResult{AckedRevision: change.SourceRevision}
			if tt.wantPartial {
				want = syncservice.ApplyResult{AckedRevision: "0", Partial: true}
			}
			if res != want {
				t.Fatalf("ApplyArtifacts = %+v, want %+v", res, want)
			}
			snap, err := b.catalog.Load()
			if err != nil {
				t.Fatal(err)
			}
			for r, w := range tt.want {
				if got := snap.ReadinessOf("a", ids[r]); !reflect.DeepEqual(got, w) {
					t.Errorf("readiness of %s = %+v, want %+v", r, got, w)
				}
			}
			if slices.Contains(b.verifier.fetches, true) {
				t.Fatalf("verifier fetch flags = %v, want no origin fetch inside apply", b.verifier.fetches)
			}
			if b.publisher.n != 1 {
				t.Fatalf("publishes = %d, want 1", b.publisher.n)
			}
		})
	}
}

func TestApplyArtifactsAcksOnlyCompleteApplies(t *testing.T) {
	a, b := newHost(t, "a"), newHost(t, "b")
	a.record(t, "w1", "r1", t0)
	a.record(t, "w2", "r2", t0.Add(-time.Minute))
	change := a.export(t)
	partial, err := b.consumer.ApplyArtifacts(t.Context(), change, []artifact.Ref{ref("r1")})
	if err != nil || !partial.Partial {
		t.Fatalf("first apply = %+v, %v; want Partial", partial, err)
	}
	full, err := b.consumer.ApplyArtifacts(t.Context(), change, []artifact.Ref{ref("r1"), ref("r2")})
	if err != nil || full != (syncservice.ApplyResult{AckedRevision: change.SourceRevision}) {
		t.Fatalf("second apply = %+v, %v; want a full ack of %s", full, err, change.SourceRevision)
	}
	if want := []artifact.Digest{ref("r1").Digest, ref("r2").Digest}; !slices.Equal(b.verifier.calls, want) {
		t.Fatalf("verified %v, want %v (r1 once, r2 only once its root arrived)", b.verifier.calls, want)
	}
	replay, err := b.consumer.ApplyArtifacts(t.Context(), change, nil)
	if err != nil || replay != (syncservice.ApplyResult{AckedRevision: change.SourceRevision}) || len(b.verifier.calls) != 2 {
		t.Fatalf("replay = %+v, %v after %d verifications; want the same ack with no new verification", replay, err, len(b.verifier.calls))
	}
	a.record(t, "w1", "r3", t0.Add(time.Minute))
	newer := a.export(t)
	if _, err := b.consumer.ApplyArtifacts(t.Context(), newer, []artifact.Ref{ref("r1"), ref("r2"), ref("r3")}); err != nil {
		t.Fatal(err)
	}
	stale, err := b.consumer.ApplyArtifacts(t.Context(), change, []artifact.Ref{ref("r1"), ref("r2")})
	want := syncservice.ApplyResult{AckedRevision: newer.SourceRevision, Stale: true, HeldDigest: newer.PayloadDigest}
	if err != nil || stale != want {
		t.Fatalf("stale apply = %+v, %v; want %+v", stale, err, want)
	}
}

func TestApplyArtifactsRefusesRootMismatch(t *testing.T) {
	a, b := newHost(t, "a"), newHost(t, "b")
	a.record(t, "w1", "r1", t0)
	a.record(t, "w2", "r2", t0.Add(-time.Minute))
	change := a.export(t)
	tests := []struct {
		name  string
		roots []artifact.Ref
	}{
		{"dropped root", change.Artifacts[:1]},
		{"reordered roots", []artifact.Ref{change.Artifacts[1], change.Artifacts[0]}},
		{"foreign root", []artifact.Ref{change.Artifacts[0], ref("elsewhere")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			forged := change
			forged.Artifacts = tt.roots
			if _, err := b.consumer.ApplyArtifacts(t.Context(), forged, tt.roots); !errors.Is(err, ErrRootsMismatch) {
				t.Fatalf("ApplyArtifacts = %v, want ErrRootsMismatch", err)
			}
			if snap, err := b.catalog.Load(); err != nil || len(snap.Origins) != 0 {
				t.Fatalf("refused change was recorded: %+v, %v", snap.Origins, err)
			}
		})
	}
}

func relayedBlocks(t *testing.T, change syncservice.ChangeEnvelope) []catalog.Origin {
	t.Helper()
	p, err := catalog.Decode(change.Payload)
	if err != nil {
		t.Fatal(err)
	}
	return slices.DeleteFunc(p.Origins, func(o catalog.Origin) bool { return o.Origin == change.Origin })
}

func TestRelayExportsArtifactCompleteBlocksVerbatim(t *testing.T) {
	a, b, c := newHost(t, "a"), newHost(t, "b"), newHost(t, "c")
	a.record(t, "w1", "r1", t0)
	a.record(t, "w2", "r2", t0.Add(-time.Minute))
	fromA := a.export(t)
	original, err := catalog.Decode(fromA.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.consumer.ApplyArtifacts(t.Context(), fromA, []artifact.Ref{ref("r1")}); err != nil {
		t.Fatal(err)
	}
	if got := relayedBlocks(t, b.export(t)); len(got) != 0 {
		t.Fatalf("relayed %+v while r2's closure is missing", got)
	}
	b.verifier.verdicts[ref("r2").Digest] = CodeVerdict{Missing: []string{"trunk base 1234"}}
	res, err := b.consumer.ApplyArtifacts(t.Context(), fromA, []artifact.Ref{ref("r1"), ref("r2")})
	if err != nil || !res.Partial {
		t.Fatalf("apply with unverified code = %+v, %v; want Partial", res, err)
	}
	fromB := b.export(t)
	if got := relayedBlocks(t, fromB); len(got) != 1 || !reflect.DeepEqual(got[0], original.Origins[0]) {
		t.Fatalf("relayed %+v, want the artifact-complete block verbatim %+v", got, original.Origins[0])
	}
	res, err = c.consumer.ApplyArtifacts(t.Context(), fromB, fromB.Artifacts)
	if err != nil || res != (syncservice.ApplyResult{AckedRevision: fromB.SourceRevision}) {
		t.Fatalf("c apply = %+v, %v", res, err)
	}
	snap, err := c.catalog.Load()
	if err != nil || !reflect.DeepEqual(snap.Origins, relayedBlocks(t, fromB)) {
		t.Fatalf("c holds %+v, want %+v (%v)", snap.Origins, relayedBlocks(t, fromB), err)
	}
	forged, err := catalog.Decode(fromB.Payload)
	if err != nil {
		t.Fatal(err)
	}
	forged.Origins[0].Worktrees = forged.Origins[0].Worktrees[:1]
	data, err := catalog.Encode(forged)
	if err != nil {
		t.Fatal(err)
	}
	roots, err := catalog.Roots(forged)
	if err != nil {
		t.Fatal(err)
	}
	conflict, err := syncservice.NewExportedArtifactChange(ServiceID, Fingerprint, syncservice.ChangeSnapshot, "0", fromB.SourceRevision, data, roots)
	if err != nil {
		t.Fatal(err)
	}
	if conflict, err = syncservice.BindDelivery(conflict, "d"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.consumer.ApplyArtifacts(t.Context(), conflict, roots); !errors.Is(err, catalog.ErrOriginBlockConflict) {
		t.Fatalf("equal revision with a different block = %v, want ErrOriginBlockConflict", err)
	}
	if again, err := c.catalog.Load(); err != nil || !reflect.DeepEqual(again, snap) {
		t.Fatalf("conflicting change was recorded: %+v, %v", again.Origins, err)
	}
}

func TestVerifyDeferredFetchesOnlyWhenAllowed(t *testing.T) {
	a, b := newHost(t, "a"), newHost(t, "b")
	id := a.record(t, "w1", "r1", t0).ID
	change := a.export(t)
	b.verifier.verdicts[ref("r1").Digest] = CodeVerdict{Missing: []string{"trunk base 1234"}}
	res, err := b.consumer.ApplyArtifacts(t.Context(), change, []artifact.Ref{ref("r1")})
	if err != nil || !res.Partial {
		t.Fatalf("apply = %+v, %v; want Partial", res, err)
	}
	b.artifacts.complete[ref("r1").Digest] = true
	if err := b.consumer.VerifyDeferred(t.Context()); err != nil {
		t.Fatal(err)
	}
	snap, err := b.catalog.Load()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := snap.ReadinessOf("a", id), (catalog.Readiness{Missing: []string{"trunk base 1234"}, Deferred: catalog.MissingPrerequisites}); !reflect.DeepEqual(got, want) || len(b.verifier.calls) != 1 {
		t.Fatalf("paused network: readiness %+v after %d verifications, want %+v after 1", got, len(b.verifier.calls), want)
	}
	b.fetch = true
	delete(b.verifier.verdicts, ref("r1").Digest)
	published := b.publisher.n
	if err := b.consumer.VerifyDeferred(t.Context()); err != nil {
		t.Fatal(err)
	}
	if want := []bool{false, true}; !slices.Equal(b.verifier.fetches, want) || b.publisher.n != published+1 {
		t.Fatalf("fetch flags %v, publishes %d; want %v and one more publish", b.verifier.fetches, b.publisher.n-published, want)
	}
	if snap, err = b.catalog.Load(); err != nil || !snap.ReadinessOf("a", id).Ready {
		t.Fatalf("readiness after background fetch = %+v, %v; want ready", snap.ReadinessOf("a", id), err)
	}
	res, err = b.consumer.ApplyArtifacts(t.Context(), change, []artifact.Ref{ref("r1")})
	if err != nil || res != (syncservice.ApplyResult{AckedRevision: change.SourceRevision}) || len(b.verifier.calls) != 2 {
		t.Fatalf("redelivery = %+v, %v after %d verifications; want a full ack without re-verifying", res, err, len(b.verifier.calls))
	}
}

func TestReconcilePinsRetainedRoots(t *testing.T) {
	h := newHost(t, "a")
	h.record(t, "w1", "r1", t0.Add(-2*time.Hour))
	h.record(t, "w1", "r2", t0)
	h.record(t, "w2", "r3", t0.Add(-7*24*time.Hour+30*time.Minute))
	*h.clock = t0.Add(time.Hour)
	res, err := h.consumer.Reconcile(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	want := []artifact.Ref{ref("r1"), ref("r2")}
	slices.SortFunc(want, func(x, y artifact.Ref) int { return strings.Compare(string(x.Digest), string(y.Digest)) })
	if res.Converged != 2 || h.artifacts.owner != PinOwner || !slices.Equal(h.artifacts.roots, want) || h.artifacts.gcs != 1 || h.publisher.n != 1 {
		t.Fatalf("Reconcile = %+v, pins %s %v, gcs %d, publishes %d", res, h.artifacts.owner, h.artifacts.roots, h.artifacts.gcs, h.publisher.n)
	}
}

func TestPayloadAtScale(t *testing.T) {
	const worktrees, checkpoints = 40, 33
	p := catalog.Payload{Identity: catalog.Identity, Version: catalog.Version, Exporter: "a", AsOf: t0}
	block := catalog.Origin{Origin: "a", Revision: 99999}
	for w := range worktrees {
		wt := tree(fmt.Sprintf("worktree-%02d", w))
		for i := range checkpoints {
			cp := point(fmt.Sprintf("root-%02d-%02d", w, i), t0.Add(-time.Duration(i)*time.Hour), 3)
			cp.ID = catalog.CheckpointID("a", wt.ID, cp.Root)
			cp.ExpiresAt = cp.SourceActivityAt.Add(catalog.ExpiryWindow)
			cp.Classes = []catalog.Class{catalog.ClassHourly, catalog.ClassDaily}
			if i == 0 {
				cp.Classes = []catalog.Class{catalog.ClassLatest, catalog.ClassHourly, catalog.ClassDaily}
			}
			wt.Checkpoints = append(wt.Checkpoints, cp)
		}
		wt.Orca = &catalog.Orca{Kind: "worktree", Name: wt.ID, InstanceID: "orca-instance-0001", Freshness: t0}
		block.Worktrees = append(block.Worktrees, wt)
	}
	p.Origins = []catalog.Origin{block}
	data, err := catalog.Encode(p)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) > syncservice.MaxTransferPayload {
		t.Fatalf("payload is %d bytes, over %d", len(data), syncservice.MaxTransferPayload)
	}
	roots, err := catalog.Roots(p)
	if err != nil {
		t.Fatal(err)
	}
	if len(roots) != worktrees*checkpoints {
		t.Fatalf("roots = %d, want %d", len(roots), worktrees*checkpoints)
	}
	if _, err := syncservice.NewExportedArtifactChange(ServiceID, Fingerprint, syncservice.ChangeSnapshot, "0", "1", data, roots); err != nil {
		t.Fatal(err)
	}
	decoded, err := catalog.Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	again, err := catalog.Roots(decoded)
	if err != nil || !slices.Equal(again, roots) {
		t.Fatalf("decoded payload derives different roots (%v)", err)
	}
	t.Logf("payload %d bytes for %d checkpoints", len(data), worktrees*checkpoints)
}
