package scheduler

import (
	"testing"
	"time"
)

func TestClassify(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	at := func(d time.Duration) time.Time { return now.Add(-d) }
	var never time.Time
	tests := []struct {
		name                        string
		human, focus, auto, lastAny time.Time
		wantTier                    Tier
		wantInterval                time.Duration
	}{
		{"no activity", never, never, never, never, TierIdle, time.Hour},
		{"human input at 15m", at(15 * time.Minute), never, never, never, TierHuman, 2 * time.Minute},
		{"human input past 15m", at(15*time.Minute + time.Nanosecond), never, never, never, TierRecent, 15 * time.Minute},
		{"human focus at 15m", never, at(15 * time.Minute), never, never, TierHuman, 2 * time.Minute},
		{"human focus past 15m", never, at(15*time.Minute + time.Nanosecond), never, never, TierRecent, 15 * time.Minute},
		{"autonomous at 15m", never, never, at(15 * time.Minute), never, TierAutonomous, 5 * time.Minute},
		{"autonomous past 15m", never, never, at(15*time.Minute + time.Nanosecond), never, TierRecent, 15 * time.Minute},
		{"human outranks newer autonomous", at(14 * time.Minute), never, at(0), at(0), TierHuman, 2 * time.Minute},
		{"any at 1h", never, never, never, at(time.Hour), TierRecent, 15 * time.Minute},
		{"any past 1h", never, never, never, at(time.Hour + time.Nanosecond), TierIdle, time.Hour},
		{"human input at 1h", at(time.Hour), never, never, never, TierRecent, 15 * time.Minute},
		{"autonomous past 1h", never, never, at(time.Hour + time.Nanosecond), never, TierIdle, time.Hour},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tier, interval := Classify(now, tt.human, tt.focus, tt.auto, tt.lastAny)
			if tier != tt.wantTier || interval != tt.wantInterval {
				t.Errorf("Classify() = (%v, %v), want (%v, %v)", tier, interval, tt.wantTier, tt.wantInterval)
			}
		})
	}
}

func TestUnitClassify(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	at := func(d time.Duration) time.Time { return now.Add(-d) }
	tests := []struct {
		name     string
		sessions []Session
		want     Tier
	}{
		{"no sessions", nil, TierIdle},
		{
			"most urgent session wins",
			[]Session{{ID: "a", LastActivity: at(2 * time.Hour)}, {ID: "b", LastAutonomous: at(time.Minute)}, {ID: "c", LastActivity: at(30 * time.Minute)}},
			TierAutonomous,
		},
		{
			"focus in one session",
			[]Session{{ID: "a", LastAutonomous: at(time.Minute)}, {ID: "b", LastHumanFocus: at(3 * time.Minute)}},
			TierHuman,
		},
		{
			"all stale",
			[]Session{{ID: "a", LastHumanInput: at(2 * time.Hour)}, {ID: "b", LastAutonomous: at(3 * time.Hour)}},
			TierIdle,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tier, interval := Unit{Sessions: tt.sessions}.Classify(now)
			if tier != tt.want || interval != tt.want.Interval() {
				t.Errorf("Classify() = (%v, %v), want (%v, %v)", tier, interval, tt.want, tt.want.Interval())
			}
		})
	}
}
