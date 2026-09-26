package resident

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/cc-sync/internal/config"
	"github.com/yasyf/cc-sync/internal/consumer"
	"github.com/yasyf/cc-sync/internal/scheduler"
	"github.com/yasyf/synckit/artifact"
	"github.com/yasyf/synckit/netpolicy"
	"github.com/yasyf/synckit/rpc"
	"github.com/yasyf/synckit/syncservice"
)

var unrestricted = netpolicy.State{Status: netpolicy.StatusConnected}

type fakeStore struct {
	mu     sync.Mutex
	pins   map[string][]artifact.Ref
	log    []string
	closed bool
}

func newFakeStore() *fakeStore { return &fakeStore{pins: map[string][]artifact.Ref{}} }

func (s *fakeStore) SetPins(_ context.Context, owner string, roots []artifact.Ref) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(roots) == 0 {
		delete(s.pins, owner)
		s.log = append(s.log, "unpin "+owner)
		return nil
	}
	s.pins[owner] = slices.Clone(roots)
	s.log = append(s.log, "pin "+owner)
	return nil
}

func (s *fakeStore) GC(context.Context) (artifact.GCReport, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log = append(s.log, "gc")
	return artifact.GCReport{}, nil
}

func (s *fakeStore) Complete(context.Context, []artifact.Ref) (int, error) { return 0, nil }

func (s *fakeStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func (s *fakeStore) pinned(owner string) []artifact.Ref {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pins[owner]
}

func (s *fakeStore) ops() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.log)
}

type fakeMonitor struct {
	mu      sync.Mutex
	state   netpolicy.State
	changed chan struct{}
	reads   int
	closed  bool
}

func (m *fakeMonitor) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

func newFakeMonitor(state netpolicy.State) *fakeMonitor {
	return &fakeMonitor{state: state, changed: make(chan struct{})}
}

func (m *fakeMonitor) Current() (netpolicy.State, <-chan struct{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reads++
	return m.state, m.changed
}

func (m *fakeMonitor) awaitReads(n int) {
	for {
		m.mu.Lock()
		reads := m.reads
		m.mu.Unlock()
		if reads >= n {
			return
		}
		time.Sleep(time.Millisecond)
	}
}

func (m *fakeMonitor) set(state netpolicy.State) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = state
	close(m.changed)
	m.changed = make(chan struct{})
}

func (m *fakeMonitor) meter(on bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state.ManualMetered = on
}

type fakeInventory struct {
	units []scheduler.Unit
	scans atomic.Int64
}

func (f *fakeInventory) Scan(context.Context) ([]scheduler.Unit, error) {
	n := f.scans.Add(1)
	units := slices.Clone(f.units)
	for i := range units {
		units[i].MetaStamp = fmt.Sprintf("%s-%d", units[i].MetaStamp, n)
	}
	return units, nil
}

type fakeCapturer struct{}

func (fakeCapturer) Capture(_ context.Context, u scheduler.Unit) (scheduler.Result, error) {
	return scheduler.Result{Outcome: scheduler.OutcomeCaptured, Checkpoint: "cp-" + u.WorktreeID}, nil
}

type fakeVerifier struct{}

func (fakeVerifier) VerifyCode(context.Context, artifact.Ref, bool) (consumer.CodeVerdict, error) {
	return consumer.CodeVerdict{Ready: true}, nil
}

type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

type harness struct {
	resident   *Resident
	dispatcher *rpc.Dispatcher
	store      *fakeStore
	monitor    *fakeMonitor
	clock      *clock
	layout     config.Layout
	capture    Capture[*fakeStore]
	registered syncservice.ArtifactConsumer
	stops      chan error
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	dir := t.TempDir()
	h := &harness{
		dispatcher: rpc.NewDispatcher(),
		store:      newFakeStore(),
		monitor:    newFakeMonitor(unrestricted),
		clock:      &clock{now: time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)},
		layout:     config.At(filepath.Join(dir, "config"), filepath.Join(dir, "checkouts")),
		stops:      make(chan error, 1),
	}
	deps := Deps[*fakeStore]{
		Layout:        h.layout,
		Self:          "me@host",
		Now:           h.clock.Now,
		OpenArtifacts: func() (*fakeStore, error) { return h.store, nil },
		Monitor:       func() (netpolicy.Monitor, error) { return h.monitor, nil },
		Register: func(d *rpc.Dispatcher, svc syncservice.ArtifactConsumer, s *fakeStore, m netpolicy.Monitor) {
			if d != h.dispatcher || s != h.store || m != h.monitor {
				t.Errorf("Register got dispatcher/store/monitor %p/%p/%p, want %p/%p/%p", d, s, m, h.dispatcher, h.store, h.monitor)
			}
			h.registered = svc
		},
		Pipeline: func(c Capture[*fakeStore]) (Pipeline, error) {
			h.capture = c
			units := []scheduler.Unit{{
				WorktreeID: "wt1",
				RepoKey:    "repo1",
				Sessions:   []scheduler.Session{{ID: "s1", LastActivity: time.Now()}},
				MetaStamp:  "m1",
			}}
			return Pipeline{Inventory: &fakeInventory{units: units}, Stamper: &fakeStamper{}, Capturer: fakeCapturer{}, Verifier: fakeVerifier{}, Expirer: &fakeExpirer{}}, nil
		},
	}
	r, err := Prepare(t.Context(), h.dispatcher, deps, func(err error) { h.stops <- err })
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	h.resident = r
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := r.Drain(ctx); err != nil {
			t.Errorf("Drain: %v", err)
		}
	})
	return h
}

func (h *harness) call(t *testing.T, method string, params any) (json.RawMessage, string) {
	t.Helper()
	data, err := json.Marshal(params)
	if err != nil {
		t.Fatal(err)
	}
	var p map[string]any
	if err := json.Unmarshal(data, &p); err != nil {
		t.Fatal(err)
	}
	resp := h.dispatcher.Dispatch(t.Context(), &rpc.Request{Method: method, Params: p})
	return resp.Result, resp.Error
}

func exists(t *testing.T, path string) bool {
	t.Helper()
	_, err := os.Stat(path)
	return err == nil
}

type fakeStamper struct{ n atomic.Int64 }

func (f *fakeStamper) CodeStamp(context.Context, scheduler.Unit) (string, error) {
	return strconv.FormatInt(f.n.Add(1), 10), nil
}

type fakeExpirer struct{ n atomic.Int64 }

func (f *fakeExpirer) ExpirePins(context.Context) error {
	f.n.Add(1)
	return nil
}
