package scheduler

import (
	"slices"
	"time"
)

const (
	humanWindow  = 15 * time.Minute
	recentWindow = time.Hour
)

// Tier ranks a unit's capture urgency; a lower Tier is more urgent and is dispatched first.
type Tier int

// The tiers, most urgent first.
const (
	TierHuman Tier = iota
	TierAutonomous
	TierRecent
	TierIdle
)

var (
	tierIntervals = [...]time.Duration{2 * time.Minute, 5 * time.Minute, 15 * time.Minute, time.Hour}
	tierNames     = [...]string{"human", "autonomous", "recent", "idle"}
)

// Interval is how often a unit in the tier comes due.
func (t Tier) Interval() time.Duration { return tierIntervals[t] }

// String names the tier as `cc-sync status` spells it.
func (t Tier) String() string { return tierNames[t] }

// MarshalText renders the tier by name in JSON status output.
func (t Tier) MarshalText() ([]byte, error) { return []byte(t.String()), nil }

// Classify maps a unit's latest activity to its tier and capture interval:
// human input or focus within 15m captures every 2m, autonomous activity
// within 15m every 5m, any activity within 1h every 15m, anything older
// hourly. A zero time means no such activity.
func Classify(now, lastHuman, lastHumanFocus, lastAutonomous, lastAny time.Time) (Tier, time.Duration) {
	tier := TierIdle
	switch {
	case within(now, humanWindow, lastHuman, lastHumanFocus):
		tier = TierHuman
	case within(now, humanWindow, lastAutonomous):
		tier = TierAutonomous
	case within(now, recentWindow, lastHuman, lastHumanFocus, lastAutonomous, lastAny):
		tier = TierRecent
	}
	return tier, tier.Interval()
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
	// MetaStamp digests the unit's capture-relevant metadata; a unit is never recaptured under an unchanged stamp.
	MetaStamp string
}

// Classify applies Classify to the newest activity across the unit's
// sessions, so a unit takes the tier of its most urgent session.
func (u Unit) Classify(now time.Time) (Tier, time.Duration) {
	l := u.latest()
	return Classify(now, l.LastHumanInput, l.LastHumanFocus, l.LastAutonomous, l.LastActivity)
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
