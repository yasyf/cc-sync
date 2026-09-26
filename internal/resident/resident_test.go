package resident

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/cc-sync/internal/catalog"
	"github.com/yasyf/cc-sync/internal/config"
	"github.com/yasyf/cc-sync/internal/consumer"
	"github.com/yasyf/cc-sync/internal/scheduler"
	"github.com/yasyf/cc-sync/internal/version"
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
	monitor := newFakeMonitor(netpolicy.State{Status: netpolicy.StatusConnected, Cellular: true})
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
				return errors.New("fetch failed")
			}
			return nil
		})
	}()

	nudges <- struct{}{}
	select {
	case <-calls:
		t.Fatal("verify ran while the network was metered")
	case <-time.After(50 * time.Millisecond):
	}
	monitor.set(unrestricted)
	for range 2 {
		select {
		case <-calls:
		case <-time.After(5 * time.Second):
			t.Fatal("verify did not run after the network became unrestricted")
		}
		nudges <- struct{}{}
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Errorf("verifyLoop = %v, want context.Canceled", err)
	}
}

func TestVerifyLoopCancelsInFlightFetchOnRestriction(t *testing.T) {
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
			within:   policyPoll + time.Second,
			reason:   "local: manual metered",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			monitor := newFakeMonitor(unrestricted)
			ctx, cancel := context.WithCancel(t.Context())
			entered := make(chan struct{})
			cancelled := make(chan error, 1)
			done := make(chan error, 1)
			go func() {
				done <- verifyLoop(ctx, monitor, time.Hour, make(chan struct{}), func(fetchCtx context.Context) error {
					close(entered)
					<-fetchCtx.Done()
					cancelled <- context.Cause(fetchCtx)
					return fetchCtx.Err()
				})
			}()
			<-entered
			monitor.awaitReads(2)
			tt.restrict(monitor)
			select {
			case cause := <-cancelled:
				var paused *netpolicy.PausedError
				if !errors.As(cause, &paused) || paused.Reason != tt.reason {
					t.Errorf("in-flight fetch cancelled with cause %v, want a pause for %q", cause, tt.reason)
				}
			case <-time.After(tt.within):
				t.Errorf("in-flight prerequisite fetch was not cancelled within %v of the restriction", tt.within)
			}
			cancel()
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Errorf("verifyLoop = %v, want context.Canceled", err)
			}
		})
	}
}

func TestVerifyLoopResumesWhenUnrestricted(t *testing.T) {
	monitor := newFakeMonitor(unrestricted)
	ctx, cancel := context.WithCancel(t.Context())
	calls := make(chan int, 8)
	var n atomic.Int32
	done := make(chan error, 1)
	go func() {
		done <- verifyLoop(ctx, monitor, time.Hour, make(chan struct{}), func(fetchCtx context.Context) error {
			call := int(n.Add(1))
			calls <- call
			if call > 1 {
				return nil
			}
			<-fetchCtx.Done()
			return fetchCtx.Err()
		})
	}()
	if call := <-calls; call != 1 {
		t.Fatalf("first verification = call %d, want 1", call)
	}
	monitor.set(netpolicy.State{Status: netpolicy.StatusConnected, Cellular: true})
	select {
	case call := <-calls:
		t.Fatalf("verification %d started while the network was cellular", call)
	case <-time.After(100 * time.Millisecond):
	}
	monitor.set(unrestricted)
	select {
	case call := <-calls:
		if call != 2 {
			t.Fatalf("resumed verification = call %d, want 2", call)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("deferred verification did not resume after the network became unrestricted")
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
			paused := make(chan struct{})
			var n atomic.Int32
			done := make(chan error, 1)
			go func() {
				done <- verifyLoop(ctx, monitor, time.Hour, make(chan struct{}), func(fetchCtx context.Context) error {
					call := int(n.Add(1))
					calls <- call
					if !tt.inFlight || call > 1 {
						return nil
					}
					<-fetchCtx.Done()
					close(paused)
					return fetchCtx.Err()
				})
			}()
			want := 1
			if tt.inFlight {
				<-calls
				monitor.meter(true)
				select {
				case <-paused:
				case <-time.After(policyPoll + time.Second):
					t.Fatal("manual metering did not pause the in-flight verification")
				}
				want = 2
			} else {
				monitor.awaitReads(1)
			}
			select {
			case call := <-calls:
				t.Fatalf("verification %d started while the network was manually metered", call)
			case <-time.After(50 * time.Millisecond):
			}
			monitor.meter(false)
			select {
			case call := <-calls:
				if call != want {
					t.Fatalf("resumed verification = call %d, want %d", call, want)
				}
			case <-time.After(policyPoll + time.Second):
				t.Fatalf("deferred verification did not resume within %v of manual metering clearing", policyPoll+time.Second)
			}
			cancel()
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Errorf("verifyLoop = %v, want context.Canceled", err)
			}
		})
	}
}
