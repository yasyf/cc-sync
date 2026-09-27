package resident

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/cc-sync/internal/catalog"
	"github.com/yasyf/cc-sync/internal/config"
	"github.com/yasyf/cc-sync/internal/consumer"
	"github.com/yasyf/cc-sync/internal/netgate"
	"github.com/yasyf/cc-sync/internal/scheduler"
	"github.com/yasyf/cc-sync/internal/version"
	"github.com/yasyf/reposync/worktree"
	"github.com/yasyf/synckit/artifact"
	"github.com/yasyf/synckit/netpolicy"
	"github.com/yasyf/synckit/syncservice"
)

func TestPrepareWiring(t *testing.T) {
	h := newHarness(t)

	for _, dir := range []string{h.layout.Dir, h.layout.StampDir, h.layout.CodeStore, h.layout.CodeIndex, h.layout.ReplicaRoot, h.layout.JournalDir} {
		if !exists(t, dir) {
			t.Errorf("layout dir %s missing", dir)
		}
	}
	if !exists(t, filepath.Join(h.layout.StampDir, catalog.StampFile)) {
		t.Errorf("stamp %s missing", catalog.StampFile)
	}
	if h.capture.Artifacts != h.store || h.capture.Monitor != h.monitor || h.capture.Code == nil || h.capture.Catalog == nil || h.capture.Publisher == nil {
		t.Errorf("pipeline capture = %+v, want the prepared store, monitor, code store, catalog, and publisher", h.capture)
	}
	if h.capture.Config.Capture != config.DefaultTiers || h.capture.Layout != h.layout {
		t.Errorf("pipeline config/layout = %+v/%+v, want defaults and the deps layout", h.capture.Config, h.capture.Layout)
	}
	caps, err := h.registered.Capabilities(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if want := syncservice.ArtifactCapabilities(consumer.ServiceID); !reflect.DeepEqual(caps, want) {
		t.Errorf("registered Capabilities = %+v, want %+v", caps, want)
	}
	items, err := h.registered.List(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].ID != consumer.WatchItemID || !reflect.DeepEqual(items[0].WatchDirs, []string{h.layout.StampDir}) {
		t.Errorf("registered List = %+v, want the one stamp item", items)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := h.resident.Drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if _, errText := h.call(t, MethodKick, KickRequest{}); !strings.Contains(errText, scheduler.ErrStopped.Error()) {
		t.Errorf("kick after Drain error = %q, want %q", errText, scheduler.ErrStopped)
	}
	if err := h.resident.Close(ctx); err != nil || !h.store.closed || !h.monitor.closed {
		t.Errorf("Close = %v, store closed = %v, monitor closed = %v; want nil, true, true", err, h.store.closed, h.monitor.closed)
	}
	select {
	case err := <-h.stops:
		t.Errorf("stop(%v) called on a clean drain", err)
	default:
	}
}

func TestStatusMethod(t *testing.T) {
	h := newHarness(t)
	if _, errText := h.call(t, MethodKick, KickRequest{}); errText != "" {
		t.Fatalf("kick: %s", errText)
	}
	raw, errText := h.call(t, MethodStatus, map[string]any{})
	if errText != "" {
		t.Fatalf("status: %s", errText)
	}
	var got StatusReply
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	want := StatusReply{
		Build:     version.String(),
		Scheduler: got.Scheduler,
		Catalog:   CatalogStatus{Self: "me@host", Origins: []OriginStatus{}},
		Capture:   config.DefaultTiers,
		Network:   unrestricted,
		Pins:      []Pin{},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("status = %+v, want %+v", got, want)
	}
	units := got.Scheduler.Units
	if len(units) != 1 || units[0].WorktreeID != "wt1" || units[0].Last == nil || units[0].Last.Checkpoint != "cp-wt1" {
		t.Errorf("status scheduler units = %+v, want wt1 last captured as cp-wt1", units)
	}
}

func TestStatusCountsPickupReadyCheckpoints(t *testing.T) {
	h := newHarness(t)
	complete := catalog.Completeness{Complete: true}
	checkpoints := map[string]catalog.Checkpoint{
		"wt-complete": {Completeness: complete},
		"wt-omitted":  {Completeness: complete, Omitted: []catalog.OmittedBinding{{Agent: "codex", Key: "session_id", ID: "c-1", Reason: "agent-not-supported-v1"}}},
		"wt-mixed":    {Completeness: complete, Deferred: "deferred:missing-lfs"},
		"wt-partial":  {Completeness: catalog.Completeness{Missing: []string{"session:s1"}}},
	}
	for id, cp := range checkpoints {
		cp.Root = artifact.Ref{Digest: artifact.Sum([]byte(id)), Kind: artifact.KindManifest, Size: 100}
		cp.CapturedAt, cp.SourceActivityAt = h.clock.Now(), h.clock.Now()
		wt := catalog.Worktree{ID: id, Repo: catalog.Repo{Origin: "git@github.com:me/" + id + ".git", SourcePath: "/src/" + id}}
		if _, err := h.capture.Catalog.Record(t.Context(), wt, cp); err != nil {
			t.Fatalf("Record %s: %v", id, err)
		}
	}
	raw, errText := h.call(t, MethodStatus, map[string]any{})
	if errText != "" {
		t.Fatalf("status: %s", errText)
	}
	var got StatusReply
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	snapshot, err := h.capture.Catalog.Load()
	if err != nil {
		t.Fatal(err)
	}
	want := []OriginStatus{{Origin: "me@host", Revision: snapshot.Origins[0].Revision, Worktrees: 4, Checkpoints: 4, Ready: 2}}
	if !reflect.DeepEqual(got.Catalog.Origins, want) {
		t.Errorf("status catalog origins = %+v, want %+v", got.Catalog.Origins, want)
	}
}

func TestKickMethod(t *testing.T) {
	tests := []struct {
		name    string
		req     KickRequest
		want    []string
		wantErr string
	}{
		{name: "named session", req: KickRequest{SessionIDs: []string{"s1"}}, want: []string{"wt1:captured:cp-wt1"}},
		{name: "every unit", req: KickRequest{}, want: []string{"wt1:captured:cp-wt1"}},
		{name: "unknown session", req: KickRequest{SessionIDs: []string{"nope"}}, wantErr: scheduler.ErrUnknownSession.Error()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t)
			raw, errText := h.call(t, MethodKick, tt.req)
			if tt.wantErr != "" {
				if !strings.Contains(errText, tt.wantErr) {
					t.Fatalf("kick error = %q, want %q", errText, tt.wantErr)
				}
				return
			}
			if errText != "" {
				t.Fatalf("kick: %s", errText)
			}
			var reply KickReply
			if err := json.Unmarshal(raw, &reply); err != nil {
				t.Fatal(err)
			}
			var got []string
			for _, a := range reply.Attempts {
				got = append(got, a.WorktreeID+":"+string(a.Outcome)+":"+a.Checkpoint)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("kick attempts = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestKickRejectsUnknownParams(t *testing.T) {
	h := newHarness(t)
	if _, errText := h.call(t, MethodKick, map[string]any{"sessions": []string{"s1"}}); !strings.Contains(errText, "unknown field") {
		t.Errorf("kick error = %q, want an unknown field refusal", errText)
	}
}

func TestVerifyLoop(t *testing.T) {
	monitor := newFakeMonitor(netpolicy.State{Status: netpolicy.StatusConnected, ManualMetered: true})
	nudges := make(chan struct{}, 1)
	calls := make(chan struct{}, 8)
	var failures atomic.Int32
	failures.Store(1)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- verifyLoop(ctx, monitor, time.Hour, nudges, func(context.Context) error {
			calls <- struct{}{}
			if failures.Add(-1) >= 0 {
				return errors.New("closure check failed")
			}
			return nil
		})
	}()

	for range 3 {
		select {
		case <-calls:
		case <-time.After(5 * time.Second):
			t.Fatal("local verification did not run while the network was restricted")
		}
		nudges <- struct{}{}
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Errorf("verifyLoop = %v, want context.Canceled", err)
	}
}

func TestVerifyLoopRestrictionCancelsOnlyTheFetch(t *testing.T) {
	tests := []struct {
		name     string
		restrict func(*fakeMonitor)
		within   time.Duration
		reason   string
	}{
		{
			name:     "cellular",
			restrict: func(m *fakeMonitor) { m.set(netpolicy.State{Status: netpolicy.StatusConnected, Cellular: true}) },
			within:   time.Second,
			reason:   "local: cellular",
		},
		{
			name:     "disconnected",
			restrict: func(m *fakeMonitor) { m.set(netpolicy.State{Status: netpolicy.StatusDisconnected}) },
			within:   time.Second,
			reason:   "local: disconnected",
		},
		{
			name:     "unknown",
			restrict: func(m *fakeMonitor) { m.set(netpolicy.State{Status: netpolicy.StatusUnknown}) },
			within:   time.Second,
			reason:   "local: unknown",
		},
		{
			name:     "manual metered without a path change",
			restrict: func(m *fakeMonitor) { m.meter(true) },
			within:   netgate.Poll + time.Second,
			reason:   "local: manual metered",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			monitor := newFakeMonitor(unrestricted)
			ctx, cancel := context.WithCancel(t.Context())
			fetching := make(chan struct{})
			cancelled := make(chan error, 1)
			passLive := make(chan bool, 1)
			done := make(chan error, 1)
			go func() {
				done <- verifyLoop(ctx, monitor, time.Hour, make(chan struct{}), func(passCtx context.Context) error {
					err := netgate.Run(passCtx, monitor, func(fetchCtx context.Context) error {
						close(fetching)
						<-fetchCtx.Done()
						cancelled <- context.Cause(fetchCtx)
						return fetchCtx.Err()
					})
					passLive <- passCtx.Err() == nil
					return err
				})
			}()
			<-fetching
			monitor.awaitReads(2)
			tt.restrict(monitor)
			select {
			case cause := <-cancelled:
				var paused *netpolicy.PausedError
				if !errors.As(cause, &paused) || paused.Reason != tt.reason {
					t.Errorf("in-flight fetch cancelled with cause %v, want a pause for %q", cause, tt.reason)
				}
			case <-time.After(tt.within):
				t.Fatalf("in-flight prerequisite fetch was not cancelled within %v of the restriction", tt.within)
			}
			if !<-passLive {
				t.Error("the restriction cancelled the verification pass, want only its fetch cancelled")
			}
			cancel()
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Errorf("verifyLoop = %v, want context.Canceled", err)
			}
		})
	}
}

func TestVerifyLoopResumesWhenUnrestricted(t *testing.T) {
	monitor := newFakeMonitor(netpolicy.State{Status: netpolicy.StatusConnected, Cellular: true})
	ctx, cancel := context.WithCancel(t.Context())
	calls := make(chan int, 8)
	var n atomic.Int32
	done := make(chan error, 1)
	go func() {
		done <- verifyLoop(ctx, monitor, time.Hour, make(chan struct{}), func(context.Context) error {
			call := int(n.Add(1))
			calls <- call
			if call > 1 {
				return nil
			}
			return &netpolicy.PausedError{Reason: "local: cellular"}
		})
	}()
	if call := <-calls; call != 1 {
		t.Fatalf("first verification = call %d, want 1", call)
	}
	select {
	case call := <-calls:
		t.Fatalf("verification %d retried while the network was cellular", call)
	case <-time.After(100 * time.Millisecond):
	}
	monitor.set(unrestricted)
	select {
	case call := <-calls:
		if call != 2 {
			t.Fatalf("resumed verification = call %d, want 2", call)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("paused verification did not resume after the network became unrestricted")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Errorf("verifyLoop = %v, want context.Canceled", err)
	}
}

func TestVerifyLoopResumesPromptlyWhenManualMeterClears(t *testing.T) {
	tests := []struct {
		name     string
		inFlight bool
	}{
		{"metered before verification", false},
		{"metered during verification", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			monitor := newFakeMonitor(unrestricted)
			if !tt.inFlight {
				monitor.meter(true)
			}
			ctx, cancel := context.WithCancel(t.Context())
			calls := make(chan int, 8)
			fetching := make(chan struct{})
			var n, admissions atomic.Int32
			done := make(chan error, 1)
			go func() {
				done <- verifyLoop(ctx, monitor, time.Hour, make(chan struct{}), func(passCtx context.Context) error {
					calls <- int(n.Add(1))
					return netgate.Run(passCtx, monitor, func(fetchCtx context.Context) error {
						if admissions.Add(1) > 1 || !tt.inFlight {
							return nil
						}
						close(fetching)
						<-fetchCtx.Done()
						return fetchCtx.Err()
					})
				})
			}()
			<-calls
			if tt.inFlight {
				<-fetching
				monitor.meter(true)
			}
			select {
			case call := <-calls:
				t.Fatalf("verification %d retried while the network was manually metered", call)
			case <-time.After(netgate.Poll + 200*time.Millisecond):
			}
			monitor.meter(false)
			select {
			case call := <-calls:
				if call != 2 {
					t.Fatalf("resumed verification = call %d, want 2", call)
				}
			case <-time.After(netgate.Poll + time.Second):
				t.Fatalf("paused verification did not resume within %v of manual metering clearing", netgate.Poll+time.Second)
			}
			cancel()
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Errorf("verifyLoop = %v, want context.Canceled", err)
			}
			want := int32(1)
			if tt.inFlight {
				want = 2
			}
			if got := admissions.Load(); got != want {
				t.Errorf("fetch admissions = %d, want %d", got, want)
			}
		})
	}
}

type hookedArtifacts struct {
	*fakeStore
	complete func()
}

func (a hookedArtifacts) Complete(context.Context, []artifact.Ref) (int, error) {
	a.complete()
	return 0, nil
}

type hookedPublisher func()

func (p hookedPublisher) Publish(context.Context) error {
	p()
	return nil
}

type closureArtifacts struct {
	*fakeStore
	missing atomic.Int32
}

func (a *closureArtifacts) Complete(context.Context, []artifact.Ref) (int, error) {
	return int(a.missing.Load()), nil
}

type blockingVerifier struct{ fetching chan<- struct{} }

func (v blockingVerifier) VerifyCode(ctx context.Context, _ artifact.Ref, fetchOrigin worktree.FetchGate) (consumer.CodeVerdict, error) {
	if fetchOrigin == nil {
		return consumer.CodeVerdict{Missing: []string{"trunk"}}, nil
	}
	err := fetchOrigin(ctx, func(fetchCtx context.Context) error {
		v.fetching <- struct{}{}
		<-fetchCtx.Done()
		return fetchCtx.Err()
	})
	if errors.Is(err, worktree.ErrFetchDeferred) {
		return consumer.CodeVerdict{Missing: []string{"trunk"}}, nil
	}
	return consumer.CodeVerdict{}, err
}

type fetchingVerifier struct{ admissions *atomic.Int32 }

func (v fetchingVerifier) VerifyCode(ctx context.Context, _ artifact.Ref, fetchOrigin worktree.FetchGate) (consumer.CodeVerdict, error) {
	if fetchOrigin == nil {
		return consumer.CodeVerdict{Missing: []string{"trunk"}}, nil
	}
	err := fetchOrigin(ctx, func(context.Context) error {
		v.admissions.Add(1)
		return nil
	})
	switch {
	case errors.Is(err, worktree.ErrFetchDeferred):
		return consumer.CodeVerdict{Missing: []string{"trunk"}}, nil
	case err != nil:
		return consumer.CodeVerdict{}, err
	}
	return consumer.CodeVerdict{Ready: true}, nil
}

func relayPending(t *testing.T, dir string, now func() time.Time, to *consumer.Consumer, closureReady bool) {
	t.Helper()
	peerCatalog := catalog.New(filepath.Join(dir, "peer-catalog.json"), "peer@host", now)
	cp := catalog.Checkpoint{
		Root:       artifact.Ref{Digest: artifact.Sum([]byte("r1")), Kind: artifact.KindManifest, Size: 100},
		CapturedAt: now(), SourceActivityAt: now(), Completeness: catalog.Completeness{Complete: true},
	}
	wt := catalog.Worktree{ID: "wt1", Repo: catalog.Repo{Origin: "git@github.com:me/wt1.git", SourcePath: "/src/wt1"}}
	if _, err := peerCatalog.Record(t.Context(), wt, cp); err != nil {
		t.Fatal(err)
	}
	peer := consumer.New(consumer.Config{Catalog: peerCatalog})
	change, err := peer.ExportArtifacts(t.Context(), syncservice.ExportRequest{ServiceID: consumer.ServiceID, SchemaFingerprint: consumer.Fingerprint, SinceRevision: "0"})
	if err != nil {
		t.Fatal(err)
	}
	if change, err = syncservice.BindDelivery(change, "peer@host"); err != nil {
		t.Fatal(err)
	}
	var ready []artifact.Ref
	if closureReady {
		ready = change.Artifacts
	}
	if res, err := to.ApplyArtifacts(t.Context(), change, ready); err != nil || !res.Partial {
		t.Fatalf("apply = %+v, %v; want Partial", res, err)
	}
}

func TestVerifyLoopResumesAfterConsumerDefersFetch(t *testing.T) {
	tests := []struct {
		name string
		held bool
	}{
		{"restriction cleared before the pass ends", false},
		{"restriction held past the poll", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			at := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
			now := func() time.Time { return at }
			monitor := newFakeMonitor(unrestricted)
			var metered atomic.Bool
			passed := make(chan struct{})
			var pass sync.Once
			var admissions atomic.Int32
			me := consumer.New(consumer.Config{
				Catalog: catalog.New(filepath.Join(dir, "catalog.json"), "me@host", now),
				Publisher: hookedPublisher(func() {
					if !metered.Load() {
						return
					}
					if !tt.held {
						monitor.meter(false)
					}
					pass.Do(func() { close(passed) })
				}),
				Artifacts: hookedArtifacts{fakeStore: newFakeStore(), complete: func() {
					if metered.CompareAndSwap(false, true) {
						monitor.meter(true)
					}
				}},
				Verifier: fetchingVerifier{admissions: &admissions},
				Network:  monitor,
				StampDir: dir,
			})
			relayPending(t, dir, now, me, true)
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan error, 1)
			go func() { done <- verifyLoop(ctx, monitor, time.Hour, make(chan struct{}), me.VerifyDeferred) }()

			<-passed
			if n := admissions.Load(); n != 0 {
				t.Fatalf("admissions = %d after the restricted pass, want 0", n)
			}
			if tt.held {
				time.Sleep(netgate.Poll + 200*time.Millisecond)
				monitor.meter(false)
			}
			deadline := time.After(netgate.Poll + 2*time.Second)
			for admissions.Load() == 0 {
				select {
				case <-deadline:
					t.Fatalf("admissions=%d: deferred fetch did not resume within %v of the restriction clearing", admissions.Load(), netgate.Poll+2*time.Second)
				case <-time.After(10 * time.Millisecond):
				}
			}
			cancel()
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Errorf("verifyLoop = %v, want context.Canceled", err)
			}
			if n := admissions.Load(); n != 1 {
				t.Errorf("admissions = %d, want 1", n)
			}
		})
	}
}

func TestVerifyLoopSettlesAPolicyCancelledFetch(t *testing.T) {
	dir := t.TempDir()
	at := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	now := func() time.Time { return at }
	monitor := newFakeMonitor(unrestricted)
	artifacts := &closureArtifacts{fakeStore: newFakeStore()}
	artifacts.missing.Store(1)
	fetching := make(chan struct{}, 1)
	cat := catalog.New(filepath.Join(dir, "catalog.json"), "me@host", now)
	me := consumer.New(consumer.Config{
		Catalog:   cat,
		Publisher: hookedPublisher(func() {}),
		Artifacts: artifacts,
		Verifier:  blockingVerifier{fetching: fetching},
		Network:   monitor,
		StampDir:  dir,
	})
	relayPending(t, dir, now, me, false)
	readiness := func() catalog.Readiness {
		t.Helper()
		snap, err := cat.Load()
		if err != nil {
			t.Fatal(err)
		}
		if len(snap.Readiness) != 1 {
			t.Fatalf("readiness = %+v, want one relayed checkpoint", snap.Readiness)
		}
		return slices.Collect(maps.Values(snap.Readiness))[0]
	}
	if got, want := readiness(), (catalog.Readiness{Missing: []string{catalog.MissingClosure}}); !reflect.DeepEqual(got, want) {
		t.Fatalf("readiness after the partial apply = %+v, want %+v", got, want)
	}
	artifacts.missing.Store(0)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- verifyLoop(ctx, monitor, time.Hour, make(chan struct{}), me.VerifyDeferred) }()

	<-fetching
	monitor.set(netpolicy.State{Status: netpolicy.StatusConnected, Cellular: true})
	want := catalog.Readiness{Missing: []string{"trunk"}, Deferred: catalog.MissingPrerequisites}
	deadline := time.After(2 * time.Second)
	for got := readiness(); !reflect.DeepEqual(got, want); got = readiness() {
		select {
		case <-deadline:
			t.Fatalf("readiness after the policy cancelled the prerequisite fetch = %+v, want %+v", got, want)
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Errorf("verifyLoop = %v, want context.Canceled", err)
	}
}
