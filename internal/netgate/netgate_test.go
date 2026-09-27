package netgate

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/synckit/netpolicy"
)

type fakeMonitor struct {
	mu      sync.Mutex
	state   netpolicy.State
	changed chan struct{}
	reads   int
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

func (m *fakeMonitor) Close() error { return nil }

func (m *fakeMonitor) set(state netpolicy.State, pathChange bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = state
	if pathChange {
		close(m.changed)
		m.changed = make(chan struct{})
	}
}

func TestRun(t *testing.T) {
	connected := netpolicy.State{Status: netpolicy.StatusConnected}
	cellular := netpolicy.State{Status: netpolicy.StatusConnected, Cellular: true}
	metered := netpolicy.State{Status: netpolicy.StatusConnected, ManualMetered: true}
	failed := errors.New("fetch failed")
	tests := []struct {
		name       string
		start      netpolicy.State
		restrict   *netpolicy.State
		pathChange bool
		fnErr      error
		wantRan    bool
		wantReason string
		wantErr    error
	}{
		{name: "refused while restricted", start: cellular, wantReason: "local: cellular"},
		{name: "refused while unknown", start: netpolicy.State{}, wantReason: "local: unknown"},
		{name: "interrupted by a path change", start: connected, restrict: &cellular, pathChange: true, wantRan: true, wantReason: "local: cellular"},
		{name: "interrupted by a manual override edit", start: connected, restrict: &metered, wantRan: true, wantReason: "local: manual metered"},
		{name: "fetch error passes through", start: connected, fnErr: failed, wantRan: true, wantErr: failed},
		{name: "allowed", start: connected, wantRan: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			monitor := &fakeMonitor{state: tt.start, changed: make(chan struct{})}
			ran := false
			err := Run(t.Context(), monitor, func(ctx context.Context) error {
				ran = true
				if tt.restrict == nil {
					return tt.fnErr
				}
				monitor.awaitReads(2)
				monitor.set(*tt.restrict, tt.pathChange)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(Poll + time.Second):
					return errors.New("fetch not cancelled after the policy turned restrictive")
				}
			})
			if ran != tt.wantRan {
				t.Fatalf("fetch ran = %v, want %v", ran, tt.wantRan)
			}
			var paused *netpolicy.PausedError
			switch {
			case tt.wantReason != "":
				if !errors.As(err, &paused) || paused.Reason != tt.wantReason {
					t.Errorf("Run = %v, want a pause for %q", err, tt.wantReason)
				}
			case !errors.Is(err, tt.wantErr) || errors.As(err, &paused):
				t.Errorf("Run = %v, want %v", err, tt.wantErr)
			}
		})
	}
}
