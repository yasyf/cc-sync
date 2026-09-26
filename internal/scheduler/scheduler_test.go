package scheduler

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

type fakeInventory struct {
	mu    sync.Mutex
	units map[string]Unit
}

func newInventory(units ...Unit) *fakeInventory {
	f := &fakeInventory{units: map[string]Unit{}}
	for _, u := range units {
		f.units[u.WorktreeID] = u
	}
	return f
}

func (f *fakeInventory) Scan(context.Context) ([]Unit, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]Unit, 0, len(f.units))
	for _, u := range f.units {
		u.Sessions = slices.Clone(u.Sessions)
		out = append(out, u)
	}
	return out, nil
}

type stallingInventory struct {
	*fakeInventory
	stallAt int
	entered chan time.Time

	mu       sync.Mutex
	scans    int
	observed error
}

func newStallingInventory(stallAt int, units ...Unit) *stallingInventory {
	return &stallingInventory{fakeInventory: newInventory(units...), stallAt: stallAt, entered: make(chan time.Time, 1)}
}

func (f *stallingInventory) Scan(ctx context.Context) ([]Unit, error) {
	f.mu.Lock()
	f.scans++
	stall := f.scans == f.stallAt
	f.mu.Unlock()
	if !stall {
		return f.fakeInventory.Scan(ctx)
	}
	f.entered <- time.Now()
	<-ctx.Done()
	f.mu.Lock()
	defer f.mu.Unlock()
	f.observed = ctx.Err()
	return nil, ctx.Err()
}

func (f *stallingInventory) state() (scans int, observed error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.scans, f.observed
}

func (f *fakeInventory) update(id string, fn func(*Unit)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	u := f.units[id]
	fn(&u)
	f.units[id] = u
}

func (f *fakeInventory) remove(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.units, id)
}

type call struct {
	worktree   string
	start, end time.Time
	canceled   bool
}

type fakeCapturer struct {
	duration func(Unit) time.Duration
	result   func(n int, u Unit) (Result, error)

	mu         sync.Mutex
	running    int
	perRepo    map[string]int
	maxRunning int
	maxPerRepo int
	calls      []call
}

func newCapturer() *fakeCapturer {
	return &fakeCapturer{
		duration: func(Unit) time.Duration { return time.Second },
		result:   func(_ int, u Unit) (Result, error) { return captured(u), nil },
		perRepo:  map[string]int{},
	}
}

func captured(u Unit) Result {
	return Result{Outcome: OutcomeCaptured, Checkpoint: u.WorktreeID + "@" + u.MetaStamp}
}

func (f *fakeCapturer) Capture(ctx context.Context, u Unit) (Result, error) {
	n := f.begin(u)
	select {
	case <-time.After(f.duration(u)):
		f.end(n, u, false)
		return f.result(n, u)
	case <-ctx.Done():
		f.end(n, u, true)
		return Result{}, ctx.Err()
	}
}

func (f *fakeCapturer) begin(u Unit) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.running++
	f.perRepo[u.RepoKey]++
	f.maxRunning = max(f.maxRunning, f.running)
	f.maxPerRepo = max(f.maxPerRepo, f.perRepo[u.RepoKey])
	f.calls = append(f.calls, call{worktree: u.WorktreeID, start: time.Now()})
	return len(f.calls) - 1
}

func (f *fakeCapturer) end(n int, u Unit, canceled bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.running--
	f.perRepo[u.RepoKey]--
	f.calls[n].end, f.calls[n].canceled = time.Now(), canceled
}

func (f *fakeCapturer) snapshot() []call {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.calls)
}

type fakePublisher struct {
	mu sync.Mutex
	at []time.Time
}

func (f *fakePublisher) Publish(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.at = append(f.at, time.Now())
	return nil
}

func (f *fakePublisher) times() []time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.at)
}

type fakeStamper struct {
	mu    sync.Mutex
	code  map[string]string
	fail  map[string]error
	calls map[string][]time.Time
}

func newStamper() *fakeStamper {
	return &fakeStamper{code: map[string]string{}, fail: map[string]error{}, calls: map[string][]time.Time{}}
}

func (f *fakeStamper) CodeStamp(_ context.Context, u Unit) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls[u.WorktreeID] = append(f.calls[u.WorktreeID], time.Now())
	if err := f.fail[u.WorktreeID]; err != nil {
		delete(f.fail, u.WorktreeID)
		return "", err
	}
	return cmp.Or(f.code[u.WorktreeID], "c0"), nil
}

func (f *fakeStamper) set(id, code string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.code[id] = code
}

func (f *fakeStamper) failOnce(id string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fail[id] = err
}

func (f *fakeStamper) callsFor(id string) []time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.calls[id])
}

func start(t *testing.T, cfg Config, inv Inventory, capt Capturer, pub Publisher) (*Scheduler, func()) {
	t.Helper()
	return startWith(t, cfg, inv, newStamper(), capt, pub)
}

func startWith(t *testing.T, cfg Config, inv Inventory, stamper Stamper, capt Capturer, pub Publisher) (*Scheduler, func()) {
	t.Helper()
	s, err := New(cfg, inv, stamper, capt, pub)
	if err != nil {
		t.Fatalf("New() = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- s.Run(ctx) }()
	return s, func() {
		t.Helper()
		defer cancel()
		stopWithin(t, s, cancel)
		if err := <-errc; err != nil {
			t.Fatalf("Run() = %v, want nil", err)
		}
	}
}

func stopWithin(t *testing.T, s *Scheduler, rescue context.CancelFunc) {
	t.Helper()
	t0 := time.Now()
	done := make(chan struct{})
	go func() {
		s.Stop()
		close(done)
	}()
	select {
	case <-done:
		if d := time.Since(t0); d != 0 {
			t.Errorf("Stop() returned after %v, want at once", d)
		}
	case <-time.After(time.Hour):
		t.Errorf("Stop() still blocked after 1h")
		rescue()
		<-done
	}
}

func ago(d time.Duration) time.Time { return time.Now().Add(-d) }

func unit(id, repo string, sessions ...Session) Unit {
	return Unit{WorktreeID: id, RepoKey: repo, MetaStamp: "s0", Sessions: sessions}
}

func fleet() []Unit {
	units := make([]Unit, 12)
	for w := range units {
		units[w] = unit(fmt.Sprintf("wt%02d", w), fmt.Sprintf("repo%d", w%5))
	}
	for i := range 40 {
		s := Session{ID: fmt.Sprintf("sess%02d", i)}
		switch i % 4 {
		case 0:
			s.LastHumanInput = time.Now()
		case 1:
			s.LastAutonomous = time.Now()
		case 2:
			s.LastActivity = ago(30 * time.Minute)
		case 3:
			s.LastActivity = ago(2 * time.Hour)
		}
		units[i%12].Sessions = append(units[i%12].Sessions, s)
	}
	return units
}

func offsets(t0 time.Time, times []time.Time) []time.Duration {
	out := make([]time.Duration, len(times))
	for i, at := range times {
		out[i] = at.Sub(t0)
	}
	return out
}

func starts(calls []call) []time.Time {
	out := make([]time.Time, len(calls))
	for i, c := range calls {
		out[i] = c.start
	}
	return out
}

func minutes(ms ...int) []time.Duration {
	out := make([]time.Duration, len(ms))
	for i, m := range ms {
		out[i] = time.Duration(m) * time.Minute
	}
	return out
}

func worktrees(calls []call) []string {
	out := make([]string, len(calls))
	for i, c := range calls {
		out[i] = c.worktree
	}
	return out
}

func sameAttempts(got, want []Attempt) bool {
	return slices.EqualFunc(got, want, func(a, b Attempt) bool {
		return a.WorktreeID == b.WorktreeID && a.At.Equal(b.At) && a.Result == b.Result
	})
}

func TestConcurrencyLimits(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		units := fleet()
		inv := newInventory(units...)
		capt := newCapturer()
		capt.duration = func(Unit) time.Duration { return 45 * time.Second }
		_, stop := start(t, Config{}, inv, capt, &fakePublisher{})
		for round := 1; round <= 10; round++ {
			time.Sleep(3 * time.Minute)
			for _, u := range units {
				inv.update(u.WorktreeID, func(u *Unit) {
					u.MetaStamp = fmt.Sprintf("s%d", round)
					for i := range u.Sessions {
						if !u.Sessions[i].LastHumanInput.IsZero() {
							u.Sessions[i].LastHumanInput = time.Now()
						}
						if !u.Sessions[i].LastAutonomous.IsZero() {
							u.Sessions[i].LastAutonomous = time.Now()
						}
					}
				})
			}
		}
		stop()

		if capt.maxRunning != 2 {
			t.Errorf("max concurrent captures = %d, want 2", capt.maxRunning)
		}
		if capt.maxPerRepo != 1 {
			t.Errorf("max concurrent captures in one repo = %d, want 1", capt.maxPerRepo)
		}
		calls := capt.snapshot()
		for _, u := range units {
			if !slices.Contains(worktrees(calls), u.WorktreeID) {
				t.Errorf("%s never captured", u.WorktreeID)
			}
		}
		if len(calls) < 40 {
			t.Errorf("captures = %d, want at least 40 under 30m of contention", len(calls))
		}
	})
}

func TestPriorityOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		inv := newInventory(
			unit("idle", "r1", Session{ID: "a", LastActivity: ago(2 * time.Hour)}),
			unit("recent", "r2", Session{ID: "b", LastActivity: ago(30 * time.Minute)}),
			unit("recent-human", "r3", Session{ID: "c", LastHumanInput: ago(40 * time.Minute)}),
			unit("autonomous", "r4", Session{ID: "d", LastAutonomous: ago(time.Minute)}),
			unit("human-old", "r5", Session{ID: "e", LastHumanInput: ago(10 * time.Minute)}),
			unit("human-new", "r6", Session{ID: "f", LastHumanFocus: ago(time.Minute)}),
		)
		capt := newCapturer()
		_, stop := start(t, Config{Workers: 1}, inv, capt, &fakePublisher{})
		time.Sleep(time.Minute)
		stop()

		want := []string{"human-new", "human-old", "autonomous", "recent-human", "recent", "idle"}
		if got := worktrees(capt.snapshot()); !slices.Equal(got, want) {
			t.Errorf("capture order = %v, want %v", got, want)
		}
	})
}

func TestHumanActivityPreemptsQueuedIdle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		inv := newInventory(
			unit("a", "r1", Session{ID: "sa", LastActivity: ago(2 * time.Hour)}),
			unit("b", "r2", Session{ID: "sb", LastActivity: ago(2 * time.Hour)}),
			unit("c", "r3", Session{ID: "sc", LastActivity: ago(2 * time.Hour)}),
		)
		capt := newCapturer()
		capt.duration = func(u Unit) time.Duration {
			if u.WorktreeID == "a" {
				return 10 * time.Minute
			}
			return time.Second
		}
		_, stop := start(t, Config{Workers: 1}, inv, capt, &fakePublisher{})
		time.Sleep(time.Minute)
		inv.update("c", func(u *Unit) { u.Sessions[0].LastHumanInput = time.Now() })
		time.Sleep(15 * time.Minute)
		stop()

		calls := capt.snapshot()
		if got, want := worktrees(calls), []string{"a", "c", "b"}; !slices.Equal(got, want) {
			t.Fatalf("capture order = %v, want %v", got, want)
		}
		if calls[0].canceled || calls[0].end.Sub(calls[0].start) != 10*time.Minute {
			t.Errorf("running idle capture = %+v, want it to finish its full 10m", calls[0])
		}
		if !calls[1].start.Equal(calls[0].end) {
			t.Errorf("human unit started at %v, want the moment the worker freed at %v", calls[1].start, calls[0].end)
		}
	})
}

func TestUnchangedStampNeverRecaptured(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		t0 := time.Now()
		inv := newInventory(fleet()...)
		stamper := newStamper()
		capt := newCapturer()
		pub := &fakePublisher{}
		s, stop := startWith(t, Config{}, inv, stamper, capt, pub)
		time.Sleep(6 * time.Hour)
		synctest.Wait()
		if got := len(capt.snapshot()); got != 12 {
			t.Fatalf("captures after 6h unchanged = %d, want 12", got)
		}
		wantStamps := append(minutes(0, 2, 4, 6, 8, 10, 12, 14, 29, 44, 59), minutes(119, 179, 239, 299, 359)...)
		if got := offsets(t0, stamper.callsFor("wt00")); !slices.Equal(got, wantStamps) {
			t.Errorf("wt00 code stamps at %v, want %v: once per due interval as it cools from human to idle", got, wantStamps)
		}
		published := len(pub.times())

		inv.update("wt03", func(u *Unit) { u.MetaStamp = "s1" })
		time.Sleep(2 * time.Hour)
		synctest.Wait()
		st := s.Status()
		stop()

		calls := capt.snapshot()
		if len(calls) != 13 || calls[12].worktree != "wt03" {
			t.Errorf("captures = %v, want the 12 initial plus wt03", worktrees(calls))
		}
		if got := len(pub.times()); got != published+1 {
			t.Errorf("publishes = %d, want %d: unchanged units publish nothing", got, published+1)
		}
		if st.QueuedByTier != (TierCounts{}) || st.Workers != 0 {
			t.Errorf("status = %+v, want nothing queued or running", st)
		}
	})
}

func TestCodeOnlyChangeCapturesAtNextInterval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		t0 := time.Now()
		inv := newInventory(unit("wt", "r", Session{ID: "s", LastHumanFocus: t0}))
		stamper := newStamper()
		capt := newCapturer()
		s, stop := startWith(t, Config{}, inv, stamper, capt, &fakePublisher{})
		time.Sleep(30 * time.Second)
		stamper.set("wt", "c1")
		time.Sleep(7 * time.Minute)
		synctest.Wait()
		st := s.Status()
		stop()

		if got, want := offsets(t0, starts(capt.snapshot())), minutes(0, 2); !slices.Equal(got, want) {
			t.Errorf("captures at %v, want %v: the code-only edit captured once, at the next 2m interval", got, want)
		}
		if got, want := offsets(t0, stamper.callsFor("wt")), minutes(0, 2, 4, 6); !slices.Equal(got, want) {
			t.Errorf("code stamps at %v, want %v: only when due, never on the 30s scans", got, want)
		}
		if got := st.LastRoundAt.Sub(t0); got != 7*time.Minute+30*time.Second {
			t.Errorf("last scan at %v, want 7m30s", got)
		}
		if last := st.Units[0].Last; last.Outcome != OutcomeUnchanged || last.At.Sub(t0) != 6*time.Minute {
			t.Errorf("last attempt = %+v, want unchanged at 6m", last)
		}
	})
}

func TestConfiguredTiersSetCadence(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		t0 := time.Now()
		cfg := Config{Tiers: Tiers{HumanInterval: time.Minute, HumanWindow: 5 * time.Minute, RecentInterval: 7 * time.Minute, RecentWindow: 30 * time.Minute}}
		inv := newInventory(unit("wt", "r", Session{ID: "s", LastHumanInput: t0}))
		stamper := newStamper()
		capt := newCapturer()
		s, stop := startWith(t, cfg, inv, stamper, capt, &fakePublisher{})
		time.Sleep(13 * time.Minute)
		synctest.Wait()
		st := s.Status()
		stop()

		if got, want := offsets(t0, stamper.callsFor("wt")), minutes(0, 1, 2, 3, 4, 5, 12); !slices.Equal(got, want) {
			t.Errorf("code stamps at %v, want %v: every 1m through the 5m human window, then every 7m", got, want)
		}
		if n := len(capt.snapshot()); n != 1 {
			t.Errorf("captures = %d, want 1", n)
		}
		if u := st.Units[0]; u.Tier != TierRecent || u.Due.Sub(t0) != 19*time.Minute {
			t.Errorf("unit status = %+v, want recent and due at 19m", u)
		}
	})
}

func TestNewRejectsInvalidTiers(t *testing.T) {
	_, err := New(Config{Tiers: Tiers{HumanWindow: 2 * time.Hour}}, newInventory(), newStamper(), newCapturer(), &fakePublisher{})
	if want := "new scheduler: invalid capture tiers: human_window 2h0m0s exceeds recent_window 1h0m0s"; !errors.Is(err, ErrInvalidTiers) || err.Error() != want {
		t.Errorf("New() error = %v, want %q", err, want)
	}
}

func TestCodeStampFailureRetries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		t0 := time.Now()
		inv := newInventory(unit("wt", "r", Session{ID: "s", LastHumanFocus: t0}))
		stamper := newStamper()
		stamper.failOnce("wt", errors.New("git status: exit 128"))
		capt := newCapturer()
		pub := &fakePublisher{}
		s, stop := startWith(t, Config{}, inv, stamper, capt, pub)
		time.Sleep(time.Minute)
		st := s.Status()
		time.Sleep(2 * time.Minute)
		stop()

		want := Attempt{WorktreeID: "wt", At: t0, Result: Result{Outcome: OutcomeFailed, Reason: "code stamp: git status: exit 128"}}
		if last := st.Units[0].Last; !sameAttempts([]Attempt{*last}, []Attempt{want}) {
			t.Errorf("last attempt = %+v, want %+v", last, want)
		}
		if got, want := offsets(t0, starts(capt.snapshot())), minutes(2); !slices.Equal(got, want) {
			t.Errorf("captures at %v, want %v: no capture on a failed stamp, a retry at the tier interval", got, want)
		}
	})
}

func TestOutcomeRetry(t *testing.T) {
	tests := []struct {
		name  string
		first Result
		err   error
		retry time.Duration
	}{
		{"partial resumes at once", Result{Outcome: OutcomePartial, Reason: "budget 1GiB"}, nil, time.Second},
		{"deferred waits an interval", Result{Outcome: OutcomeDeferred, Reason: "rebase in progress"}, nil, 2 * time.Minute},
		{"busy waits an interval", Result{Outcome: OutcomeBusy, Reason: "store busy"}, nil, 2 * time.Minute},
		{"missing lfs waits an interval", Result{Outcome: OutcomeMissingLFS, Reason: "assets/model.bin"}, nil, 2 * time.Minute},
		{"failure waits an interval", Result{}, errors.New("disk full"), 2 * time.Minute},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				inv := newInventory(unit("wt", "r", Session{ID: "s", LastHumanFocus: time.Now()}))
				capt := newCapturer()
				capt.result = func(n int, u Unit) (Result, error) {
					if n == 0 {
						return tt.first, tt.err
					}
					return captured(u), nil
				}
				s, stop := start(t, Config{}, inv, capt, &fakePublisher{})
				time.Sleep(5 * time.Minute)
				st := s.Status()
				stop()

				calls := capt.snapshot()
				if len(calls) != 2 {
					t.Fatalf("captures = %d, want the failed attempt and one retry", len(calls))
				}
				if got := calls[1].start.Sub(calls[0].start); got != tt.retry {
					t.Errorf("retry after %v, want %v", got, tt.retry)
				}
				if last := st.Units[0].Last; last.Outcome != OutcomeUnchanged {
					t.Errorf("last attempt = %+v, want unchanged once the retry captured", last)
				}
			})
		})
	}
}

func TestKick(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		inv := newInventory(
			unit("wt1", "r1", Session{ID: "s1", LastActivity: ago(2 * time.Hour)}),
			unit("wt2", "r2", Session{ID: "s2", LastActivity: ago(2 * time.Hour)}, Session{ID: "s3"}),
		)
		capt := newCapturer()
		s, stop := start(t, Config{}, inv, capt, &fakePublisher{})
		ctx := context.Background()
		time.Sleep(time.Minute)

		inv.update("wt1", func(u *Unit) { u.MetaStamp = "s1" })
		got, err := s.Kick(ctx, "s1")
		want := []Attempt{{WorktreeID: "wt1", At: time.Now(), Result: Result{Outcome: OutcomeCaptured, Checkpoint: "wt1@s1"}}}
		if err != nil || !sameAttempts(got, want) {
			t.Errorf("Kick(s1) = %+v, %v; want %+v", got, err, want)
		}

		got, err = s.Kick(ctx, "s1", "s3", "s2")
		want = []Attempt{
			{WorktreeID: "wt1", At: time.Now(), Result: Result{Outcome: OutcomeUnchanged}},
			{WorktreeID: "wt2", At: time.Now(), Result: Result{Outcome: OutcomeUnchanged}},
		}
		if err != nil || !sameAttempts(got, want) {
			t.Errorf("Kick(s1, s3, s2) = %+v, %v; want %+v", got, err, want)
		}

		inv.update("wt2", func(u *Unit) { u.MetaStamp = "s1" })
		kicked := time.Now()
		got, err = s.Kick(ctx)
		want = []Attempt{
			{WorktreeID: "wt1", At: kicked, Result: Result{Outcome: OutcomeUnchanged}},
			{WorktreeID: "wt2", At: kicked.Add(time.Second), Result: Result{Outcome: OutcomeCaptured, Checkpoint: "wt2@s1"}},
		}
		if err != nil || !sameAttempts(got, want) {
			t.Errorf("Kick() = %+v, %v; want %+v", got, err, want)
		}

		if _, err := s.Kick(ctx, "s1", "nope"); !errors.Is(err, ErrUnknownSession) {
			t.Errorf("Kick(s1, nope) error = %v, want ErrUnknownSession", err)
		}
		stop()

		order := worktrees(capt.snapshot())
		slices.Sort(order[:2])
		if !slices.Equal(order, []string{"wt1", "wt2", "wt1", "wt2"}) {
			t.Errorf("captures = %v, want the initial pair plus one per changed kick", order)
		}
		if _, err := s.Kick(ctx, "s1"); !errors.Is(err, ErrStopped) {
			t.Errorf("Kick after Stop error = %v, want ErrStopped", err)
		}
	})
}

func TestKickReportsRemovedUnit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		inv := newInventory(unit("wt", "r", Session{ID: "s"}))
		capt := newCapturer()
		capt.duration = func(Unit) time.Duration { return time.Minute }
		s, stop := start(t, Config{}, inv, capt, &fakePublisher{})
		time.Sleep(10 * time.Second)

		inv.update("wt", func(u *Unit) { u.MetaStamp = "s1" })
		done := make(chan []Attempt)
		go func() {
			got, err := s.Kick(context.Background(), "s")
			if err != nil {
				t.Errorf("Kick(s) error = %v", err)
			}
			done <- got
		}()
		time.Sleep(10 * time.Second)
		inv.remove("wt")
		got := <-done
		st := s.Status()
		stop()

		want := []Attempt{{WorktreeID: "wt", At: time.Now(), Result: Result{Outcome: OutcomeRemoved}}}
		if !sameAttempts(got, want) {
			t.Errorf("Kick(s) = %+v, want %+v", got, want)
		}
		if len(st.Units) != 0 {
			t.Errorf("status units = %+v, want the removed unit forgotten", st.Units)
		}
		if n := len(capt.snapshot()); n != 1 {
			t.Errorf("captures = %d, want only the one in flight at removal", n)
		}
	})
}

func TestStampCoalescing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		units := fleet()
		capt := newCapturer()
		capt.duration = func(u Unit) time.Duration {
			return time.Duration(1+slices.IndexFunc(units, func(v Unit) bool { return v.WorktreeID == u.WorktreeID })) * time.Second
		}
		pub := &fakePublisher{}
		_, stop := start(t, Config{}, newInventory(units...), capt, pub)
		time.Sleep(5 * time.Minute)
		stop()

		calls := capt.snapshot()
		if len(calls) != 12 {
			t.Fatalf("captures = %d, want 12", len(calls))
		}
		firstEnd, lastEnd := calls[0].end, calls[0].end
		for _, c := range calls {
			firstEnd, lastEnd = earlier(firstEnd, c.end), later(lastEnd, c.end)
		}
		times := pub.times()
		if len(times) < 2 {
			t.Fatalf("publishes = %v, want the burst announced more than once", times)
		}
		for i := 1; i < len(times); i++ {
			if gap := times[i].Sub(times[i-1]); gap < 10*time.Second {
				t.Errorf("publishes %d and %d are %v apart, want at least 10s", i-1, i, gap)
			}
		}
		if !times[0].Equal(firstEnd) {
			t.Errorf("first publish at %v, want the first capture's end %v", times[0], firstEnd)
		}
		if last := times[len(times)-1]; last.Before(lastEnd) || last.Sub(lastEnd) > 10*time.Second {
			t.Errorf("last publish at %v, want within 10s after the last capture ended at %v", last, lastEnd)
		}
	})
}

func TestStopDrains(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		idle := func(id string) Session { return Session{ID: id, LastActivity: ago(2 * time.Hour)} }
		inv := newInventory(unit("a", "r1", idle("sa")), unit("b", "r2", idle("sb")), unit("c", "r3", idle("sc")))
		capt := newCapturer()
		capt.duration = func(Unit) time.Duration { return 24 * time.Hour }
		pub := &fakePublisher{}
		s, stop := start(t, Config{}, inv, capt, pub)
		time.Sleep(time.Minute)

		st := s.Status()
		if st.Workers != 2 || st.QueuedByTier != (TierCounts{Idle: 1}) {
			t.Errorf("status = %+v, want 2 workers busy and 1 idle unit queued", st)
		}
		stop()

		calls := capt.snapshot()
		if len(calls) != 2 {
			t.Fatalf("captures = %d, want 2", len(calls))
		}
		for _, c := range calls {
			if !c.canceled || c.end.Sub(c.start) != time.Minute {
				t.Errorf("capture %+v, want canceled at Stop after 1m", c)
			}
		}
		if capt.running != 0 {
			t.Errorf("captures still running after Stop = %d, want 0", capt.running)
		}
		if got := pub.times(); len(got) != 0 {
			t.Errorf("publishes = %v, want none for canceled captures", got)
		}
	})
}

func TestRunReturnsContextError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		s, err := New(Config{}, newInventory(unit("wt", "r")), newStamper(), newCapturer(), &fakePublisher{})
		if err != nil {
			t.Fatal(err)
		}
		errc := make(chan error, 1)
		go func() { errc <- s.Run(ctx) }()
		time.Sleep(time.Minute)
		cancel()
		if err := <-errc; !errors.Is(err, context.Canceled) {
			t.Errorf("Run() = %v, want context.Canceled", err)
		}
		stopWithin(t, s, cancel)
	})
}

func TestStopCancelsBlockedScan(t *testing.T) {
	tests := []struct {
		name    string
		stallAt int
		kick    bool
		wantAt  time.Duration
	}{
		{"initial", 1, false, 0},
		{"periodic", 2, false, 30 * time.Second},
		{"kick", 2, true, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				inv := newStallingInventory(tt.stallAt, unit("wt", "r", Session{ID: "s", LastActivity: ago(2 * time.Hour)}))
				t0 := time.Now()
				s, stop := start(t, Config{}, inv, newCapturer(), &fakePublisher{})
				kicked := make(chan error, 1)
				if tt.kick {
					go func() {
						_, err := s.Kick(context.Background(), "s")
						kicked <- err
					}()
				}
				if at := <-inv.entered; at.Sub(t0) != tt.wantAt {
					t.Errorf("scan stalled at %v, want %v", at.Sub(t0), tt.wantAt)
				}
				stop()

				if _, observed := inv.state(); !errors.Is(observed, context.Canceled) {
					t.Errorf("stalled scan observed %v, want context.Canceled", observed)
				}
				if tt.kick {
					if err := <-kicked; !errors.Is(err, ErrStopped) {
						t.Errorf("Kick(s) error = %v, want ErrStopped", err)
					}
				}
			})
		})
	}
}

func TestStopBeforeRun(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		inv := newStallingInventory(0, unit("wt", "r"))
		capt := newCapturer()
		s, err := New(Config{}, inv, newStamper(), capt, &fakePublisher{})
		if err != nil {
			t.Fatal(err)
		}
		t0 := time.Now()
		stopped := make(chan struct{})
		go func() {
			s.Stop()
			close(stopped)
		}()
		synctest.Wait()
		select {
		case <-stopped:
		default:
			t.Error("Stop() before Run blocked, want it to return at once")
		}

		errc := make(chan error, 1)
		go func() { errc <- s.Run(context.Background()) }()
		if err := <-errc; err != nil {
			t.Errorf("Run() after Stop = %v, want nil", err)
		}
		<-stopped
		if d := time.Since(t0); d != 0 {
			t.Errorf("Stop then Run took %v, want no time", d)
		}
		if scans, _ := inv.state(); scans != 0 {
			t.Errorf("scans = %d, want none after Stop", scans)
		}
		if n := len(capt.snapshot()); n != 0 {
			t.Errorf("captures = %d, want none after Stop", n)
		}
	})
}

func TestStopBeforeRunReleasesKicks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, err := New(Config{}, newInventory(unit("wt", "r", Session{ID: "s"})), newStamper(), newCapturer(), &fakePublisher{})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		kick := func() <-chan error {
			errc := make(chan error, 1)
			go func() {
				_, err := s.Kick(ctx, "s")
				errc <- err
			}()
			return errc
		}
		t0 := time.Now()
		pending := kick()
		synctest.Wait()
		stopWithin(t, s, cancel)
		after := kick()
		synctest.Wait()
		cancel()
		for _, k := range []struct {
			name string
			errc <-chan error
		}{{"pending", pending}, {"after", after}} {
			if err := <-k.errc; !errors.Is(err, ErrStopped) {
				t.Errorf("%s Kick(s) error = %v, want ErrStopped", k.name, err)
			}
		}
		if d := time.Since(t0); d != 0 {
			t.Errorf("Kicks returned after %v, want at once", d)
		}

		if err := s.Run(context.Background()); err != nil {
			t.Errorf("Run() after Stop = %v, want nil", err)
		}
		stopWithin(t, s, cancel)
	})
}

func TestStopTwice(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, err := New(Config{}, newInventory(unit("wt", "r")), newStamper(), newCapturer(), &fakePublisher{})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		errc := make(chan error, 1)
		go func() { errc <- s.Run(ctx) }()
		time.Sleep(time.Minute)
		stopWithin(t, s, cancel)
		stopWithin(t, s, cancel)
		if err := <-errc; err != nil {
			t.Errorf("Run() = %v, want nil", err)
		}
	})
}

func TestStatusJSON(t *testing.T) {
	at := func(m int) time.Time { return time.Date(2026, 9, 26, 12, m, 0, 0, time.UTC) }
	st := Status{
		QueuedByTier: TierCounts{Human: 1, Idle: 2},
		Workers:      1,
		LastRoundAt:  at(0),
		Units: []UnitStatus{{
			WorktreeID: "wt",
			RepoKey:    "/src/mono/.git",
			Tier:       TierHuman,
			Due:        at(2),
			Running:    true,
			Last:       &Attempt{WorktreeID: "wt", At: at(1), Result: Result{Outcome: OutcomeDeferred, Reason: "rebase in progress"}},
		}},
	}
	got, err := json.Marshal(st)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"queued_by_tier":{"human":1,"autonomous":0,"recent":0,"idle":2},"workers":1,"last_round_at":"2026-09-26T12:00:00Z",` +
		`"units":[{"worktree_id":"wt","repo_key":"/src/mono/.git","tier":"human","due":"2026-09-26T12:02:00Z","running":true,` +
		`"last":{"worktree_id":"wt","at":"2026-09-26T12:01:00Z","outcome":"deferred","reason":"rebase in progress"}}]}`
	if string(got) != want {
		t.Errorf("json =\n%s\nwant\n%s", got, want)
	}
}
