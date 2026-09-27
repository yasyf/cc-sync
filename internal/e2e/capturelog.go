//go:build e2e

package e2e

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/cc-sync/internal/scheduler"
)

const captureWait = 2 * time.Minute

// Dispatch is one scheduler attempt reaching the code stamp, the first step
// of every attempt.
type Dispatch struct {
	WorktreeID string
	At         time.Time
}

// CaptureRecord is one finished Capture call.
type CaptureRecord struct {
	WorktreeID string
	RepoKey    string
	Start, End time.Time
	Result     scheduler.Result
	Err        error
}

// CaptureLog instruments a host's capture pipeline across boots: it records
// every attempt's dispatch, every Capture call, and the peak number of
// Capture calls in flight overall and within one repo. While gated, each
// Capture call parks until Release admits its worktree.
type CaptureLog struct {
	mu         sync.Mutex
	changed    chan struct{}
	gated      bool
	admit      map[string]int
	parked     map[string]bool
	inflight   map[string]string
	peak       int
	peakRepo   int
	dispatches []Dispatch
	records    []CaptureRecord
}

func newCaptureLog() *CaptureLog {
	return &CaptureLog{changed: make(chan struct{}), admit: map[string]int{}, parked: map[string]bool{}, inflight: map[string]string{}}
}

// Gate parks every later Capture call until Release admits it.
func (l *CaptureLog) Gate() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.gated = true
}

// Ungate admits every parked and later Capture call.
func (l *CaptureLog) Ungate() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.gated = false
	clear(l.parked)
	l.broadcast()
}

// Release admits worktreeID's parked Capture call.
func (l *CaptureLog) Release(worktreeID string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.parked, worktreeID)
	l.admit[worktreeID]++
	l.broadcast()
}

// WaitParked waits until exactly n Capture calls are parked and returns
// their worktrees, sorted.
func (l *CaptureLog) WaitParked(t *testing.T, n int) []string {
	t.Helper()
	var ids []string
	l.await(t, "parked captures", func() bool {
		if len(l.parked) != n {
			return false
		}
		ids = make([]string, 0, n)
		for id := range l.parked {
			ids = append(ids, id)
		}
		slices.Sort(ids)
		return true
	})
	return ids
}

// WaitRecords waits until n Capture calls have finished and returns them in
// finishing order.
func (l *CaptureLog) WaitRecords(t *testing.T, n int) []CaptureRecord {
	t.Helper()
	l.await(t, "finished captures", func() bool { return len(l.records) >= n })
	return l.Records()
}

// Records returns every finished Capture call in finishing order.
func (l *CaptureLog) Records() []CaptureRecord {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.records)
}

// Dispatches returns every attempt's dispatch in order.
func (l *CaptureLog) Dispatches() []Dispatch {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.dispatches)
}

// Peak reports the most Capture calls ever in flight at once, overall and
// within one repo.
func (l *CaptureLog) Peak() (all, perRepo int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.peak, l.peakRepo
}

func (l *CaptureLog) await(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.After(captureWait)
	for {
		l.mu.Lock()
		ok, changed := cond(), l.changed
		l.mu.Unlock()
		if ok {
			return
		}
		select {
		case <-changed:
		case <-deadline:
			t.Fatalf("capture log: timed out waiting for %s", what)
		}
	}
}

func (l *CaptureLog) broadcast() {
	close(l.changed)
	l.changed = make(chan struct{})
}

func (l *CaptureLog) enter(ctx context.Context, u scheduler.Unit) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.inflight[u.WorktreeID] = u.RepoKey
	l.peak = max(l.peak, len(l.inflight))
	same := 0
	for _, repo := range l.inflight {
		if repo == u.RepoKey {
			same++
		}
	}
	l.peakRepo = max(l.peakRepo, same)
	if l.gated {
		l.parked[u.WorktreeID] = true
	}
	l.broadcast()
	for l.gated && l.admit[u.WorktreeID] == 0 {
		changed := l.changed
		l.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
		}
		l.mu.Lock()
		if err := ctx.Err(); err != nil {
			delete(l.parked, u.WorktreeID)
			delete(l.inflight, u.WorktreeID)
			l.broadcast()
			return err
		}
	}
	if l.admit[u.WorktreeID] > 0 {
		l.admit[u.WorktreeID]--
	}
	return nil
}

func (l *CaptureLog) leave(u scheduler.Unit, start time.Time, res scheduler.Result, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.inflight, u.WorktreeID)
	l.records = append(l.records, CaptureRecord{WorktreeID: u.WorktreeID, RepoKey: u.RepoKey, Start: start, End: time.Now(), Result: res, Err: err})
	l.broadcast()
}

type loggedStamper struct {
	log  *CaptureLog
	next scheduler.Stamper
}

func (s loggedStamper) CodeStamp(ctx context.Context, u scheduler.Unit) (string, error) {
	s.log.mu.Lock()
	s.log.dispatches = append(s.log.dispatches, Dispatch{WorktreeID: u.WorktreeID, At: time.Now()})
	s.log.broadcast()
	s.log.mu.Unlock()
	return s.next.CodeStamp(ctx, u)
}

type loggedCapturer struct {
	log  *CaptureLog
	next scheduler.Capturer
}

func (c loggedCapturer) Capture(ctx context.Context, u scheduler.Unit) (scheduler.Result, error) {
	start := time.Now()
	if err := c.log.enter(ctx, u); err != nil {
		return scheduler.Result{}, err
	}
	res, err := c.next.Capture(ctx, u)
	c.log.leave(u, start, res, err)
	return res, err
}
