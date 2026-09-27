package scheduler

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"time"
)

// ErrInvalidTiers reports Tiers with a non-positive duration or an activity
// window wider than the recent window.
var ErrInvalidTiers = errors.New("invalid capture tiers")

// Tier ranks a unit's capture urgency; a lower Tier is more urgent and is dispatched first.
type Tier int

// The tiers, most urgent first.
const (
	TierHuman Tier = iota
	TierAutonomous
	TierRecent
	TierIdle
)

var tierNames = [...]string{"human", "autonomous", "recent", "idle"}

// String names the tier as `cc-sync status` spells it.
func (t Tier) String() string { return tierNames[t] }

// MarshalText renders the tier by name in JSON status output.
func (t Tier) MarshalText() ([]byte, error) { return []byte(t.String()), nil }

// UnmarshalText parses a tier name, so status replies decode on the client.
func (t *Tier) UnmarshalText(text []byte) error {
	i := slices.Index(tierNames[:], string(text))
	if i < 0 {
		return fmt.Errorf("scheduler: unknown tier %q", text)
	}
	*t = Tier(i)
	return nil
}

// Tiers sets how often a unit in each tier comes due and how recent activity
// must be to earn the human, autonomous, and recent tiers.
type Tiers struct {
	HumanInterval      time.Duration
	AutonomousInterval time.Duration
	RecentInterval     time.Duration
	IdleInterval       time.Duration
	HumanWindow        time.Duration
	AutonomousWindow   time.Duration
	RecentWindow       time.Duration
}

// DefaultTiers is the approved cadence: human input or focus within 15m
// captures every 2m, autonomous activity within 15m every 5m, any activity
// within 1h every 15m, anything older hourly.
func DefaultTiers() Tiers {
	return Tiers{
		HumanInterval:      2 * time.Minute,
		AutonomousInterval: 5 * time.Minute,
		RecentInterval:     15 * time.Minute,
		IdleInterval:       time.Hour,
		HumanWindow:        15 * time.Minute,
		AutonomousWindow:   15 * time.Minute,
		RecentWindow:       time.Hour,
	}
}

// Validate reports ErrInvalidTiers unless every duration is positive and the
// human and autonomous windows fit within the recent window.
func (t Tiers) Validate() error {
	for _, f := range []struct {
		name string
		d    time.Duration
	}{
		{"human_interval", t.HumanInterval},
		{"autonomous_interval", t.AutonomousInterval},
		{"recent_interval", t.RecentInterval},
		{"idle_interval", t.IdleInterval},
		{"human_window", t.HumanWindow},
		{"autonomous_window", t.AutonomousWindow},
		{"recent_window", t.RecentWindow},
	} {
		if f.d <= 0 {
			return fmt.Errorf("%w: %s %v is not positive", ErrInvalidTiers, f.name, f.d)
		}
	}
	if t.HumanWindow > t.RecentWindow {
		return fmt.Errorf("%w: human_window %v exceeds recent_window %v", ErrInvalidTiers, t.HumanWindow, t.RecentWindow)
	}
	if t.AutonomousWindow > t.RecentWindow {
		return fmt.Errorf("%w: autonomous_window %v exceeds recent_window %v", ErrInvalidTiers, t.AutonomousWindow, t.RecentWindow)
	}
	return nil
}

// Interval is how often a unit in tier comes due.
func (t Tiers) Interval(tier Tier) time.Duration {
	return [...]time.Duration{t.HumanInterval, t.AutonomousInterval, t.RecentInterval, t.IdleInterval}[tier]
}

// Classify maps a unit's latest activity to its tier and capture interval:
// human input or focus within HumanWindow earns TierHuman, autonomous
// activity within AutonomousWindow TierAutonomous, any activity within
// RecentWindow TierRecent, anything older TierIdle. A zero time means no such
// activity.
func (t Tiers) Classify(now, lastHuman, lastHumanFocus, lastAutonomous, lastAny time.Time) (Tier, time.Duration) {
	tier := TierIdle
	switch {
	case within(now, t.HumanWindow, lastHuman, lastHumanFocus):
		tier = TierHuman
	case within(now, t.AutonomousWindow, lastAutonomous):
		tier = TierAutonomous
	case within(now, t.RecentWindow, lastHuman, lastHumanFocus, lastAutonomous, lastAny):
		tier = TierRecent
	}
	return tier, t.Interval(tier)
}

func (t Tiers) orDefaults() Tiers {
	d := DefaultTiers()
	return Tiers{
		HumanInterval:      cmp.Or(t.HumanInterval, d.HumanInterval),
		AutonomousInterval: cmp.Or(t.AutonomousInterval, d.AutonomousInterval),
		RecentInterval:     cmp.Or(t.RecentInterval, d.RecentInterval),
		IdleInterval:       cmp.Or(t.IdleInterval, d.IdleInterval),
		HumanWindow:        cmp.Or(t.HumanWindow, d.HumanWindow),
		AutonomousWindow:   cmp.Or(t.AutonomousWindow, d.AutonomousWindow),
		RecentWindow:       cmp.Or(t.RecentWindow, d.RecentWindow),
	}
}

func within(now time.Time, window time.Duration, times ...time.Time) bool {
	return slices.ContainsFunc(times, func(t time.Time) bool { return now.Sub(t) <= window })
}

// Session is one Claude session's activity as the inventory observed it.
type Session struct {
	ID             string
	LastHumanInput time.Time
	LastHumanFocus time.Time
	LastAutonomous time.Time
	LastActivity   time.Time
}

// Unit is one capture unit: a worktree and every session whose cwd resolves into it.
type Unit struct {
	WorktreeID string
	// RepoKey names the repository (its git common dir); captures sharing a RepoKey never overlap.
	RepoKey  string
	Sessions []Session
	// MetaStamp digests the unit's session metadata. A due unit is recaptured
	// only when MetaStamp or its Stamper code stamp differs from the pair its
	// last capture recorded.
	MetaStamp string
}

// Classify applies t.Classify to the newest activity across the unit's
// sessions, so a unit takes the tier of its most urgent session.
func (u Unit) Classify(now time.Time, t Tiers) (Tier, time.Duration) {
	l := u.latest()
	return t.Classify(now, l.LastHumanInput, l.LastHumanFocus, l.LastAutonomous, l.LastActivity)
}

func (u Unit) latest() Session {
	var l Session
	for _, s := range u.Sessions {
		l.LastHumanInput = later(l.LastHumanInput, s.LastHumanInput)
		l.LastHumanFocus = later(l.LastHumanFocus, s.LastHumanFocus)
		l.LastAutonomous = later(l.LastAutonomous, s.LastAutonomous)
		l.LastActivity = later(l.LastActivity, s.LastActivity)
	}
	return l
}

func (u Unit) humanAt() time.Time {
	l := u.latest()
	return later(l.LastHumanInput, l.LastHumanFocus)
}

func (u Unit) holds(sessionID string) bool {
	return slices.ContainsFunc(u.Sessions, func(s Session) bool { return s.ID == sessionID })
}

func later(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}
