package scheduler

import (
	"errors"
	"testing"
	"time"
)

func TestClassify(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	at := func(d time.Duration) time.Time { return now.Add(-d) }
	var never time.Time
	custom := Tiers{
		HumanInterval:      time.Minute,
		AutonomousInterval: 3 * time.Minute,
		RecentInterval:     7 * time.Minute,
		IdleInterval:       20 * time.Minute,
		HumanWindow:        5 * time.Minute,
		AutonomousWindow:   10 * time.Minute,
		RecentWindow:       30 * time.Minute,
	}
	tests := []struct {
		name                        string
		tiers                       Tiers
		human, focus, auto, lastAny time.Time
		wantTier                    Tier
		wantInterval                time.Duration
	}{
		{"no activity", DefaultTiers(), never, never, never, never, TierIdle, time.Hour},
		{"human input at 15m", DefaultTiers(), at(15 * time.Minute), never, never, never, TierHuman, 2 * time.Minute},
		{"human input past 15m", DefaultTiers(), at(15*time.Minute + time.Nanosecond), never, never, never, TierRecent, 15 * time.Minute},
		{"human focus at 15m", DefaultTiers(), never, at(15 * time.Minute), never, never, TierHuman, 2 * time.Minute},
		{"human focus past 15m", DefaultTiers(), never, at(15*time.Minute + time.Nanosecond), never, never, TierRecent, 15 * time.Minute},
		{"autonomous at 15m", DefaultTiers(), never, never, at(15 * time.Minute), never, TierAutonomous, 5 * time.Minute},
		{"autonomous past 15m", DefaultTiers(), never, never, at(15*time.Minute + time.Nanosecond), never, TierRecent, 15 * time.Minute},
		{"human outranks newer autonomous", DefaultTiers(), at(14 * time.Minute), never, at(0), at(0), TierHuman, 2 * time.Minute},
		{"any at 1h", DefaultTiers(), never, never, never, at(time.Hour), TierRecent, 15 * time.Minute},
		{"any past 1h", DefaultTiers(), never, never, never, at(time.Hour + time.Nanosecond), TierIdle, time.Hour},
		{"human input at 1h", DefaultTiers(), at(time.Hour), never, never, never, TierRecent, 15 * time.Minute},
		{"autonomous past 1h", DefaultTiers(), never, never, at(time.Hour + time.Nanosecond), never, TierIdle, time.Hour},
		{"custom human at window", custom, at(5 * time.Minute), never, never, never, TierHuman, time.Minute},
		{"custom human past window", custom, never, at(5*time.Minute + time.Nanosecond), never, never, TierRecent, 7 * time.Minute},
		{"custom autonomous at window", custom, never, never, at(10 * time.Minute), never, TierAutonomous, 3 * time.Minute},
		{"custom autonomous past window", custom, never, never, at(10*time.Minute + time.Nanosecond), never, TierRecent, 7 * time.Minute},
		{"custom any at recent window", custom, never, never, never, at(30 * time.Minute), TierRecent, 7 * time.Minute},
		{"custom any past recent window", custom, never, never, never, at(30*time.Minute + time.Nanosecond), TierIdle, 20 * time.Minute},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tier, interval := tt.tiers.Classify(now, tt.human, tt.focus, tt.auto, tt.lastAny)
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
		name         string
		sessions     []Session
		want         Tier
		wantInterval time.Duration
	}{
		{"no sessions", nil, TierIdle, time.Hour},
		{
			"most urgent session wins",
			[]Session{{ID: "a", LastActivity: at(2 * time.Hour)}, {ID: "b", LastAutonomous: at(time.Minute)}, {ID: "c", LastActivity: at(30 * time.Minute)}},
			TierAutonomous, 5 * time.Minute,
		},
		{
			"focus in one session",
			[]Session{{ID: "a", LastAutonomous: at(time.Minute)}, {ID: "b", LastHumanFocus: at(3 * time.Minute)}},
			TierHuman, 2 * time.Minute,
		},
		{
			"all stale",
			[]Session{{ID: "a", LastHumanInput: at(2 * time.Hour)}, {ID: "b", LastAutonomous: at(3 * time.Hour)}},
			TierIdle, time.Hour,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tier, interval := Unit{Sessions: tt.sessions}.Classify(now, DefaultTiers())
			if tier != tt.want || interval != tt.wantInterval {
				t.Errorf("Classify() = (%v, %v), want (%v, %v)", tier, interval, tt.want, tt.wantInterval)
			}
		})
	}
}

func TestDefaultTiers(t *testing.T) {
	want := Tiers{
		HumanInterval:      2 * time.Minute,
		AutonomousInterval: 5 * time.Minute,
		RecentInterval:     15 * time.Minute,
		IdleInterval:       time.Hour,
		HumanWindow:        15 * time.Minute,
		AutonomousWindow:   15 * time.Minute,
		RecentWindow:       time.Hour,
	}
	if got := DefaultTiers(); got != want {
		t.Errorf("DefaultTiers() = %+v, want %+v", got, want)
	}
	if got := (Tiers{}).orDefaults(); got != want {
		t.Errorf("zero Tiers with defaults = %+v, want %+v", got, want)
	}
	if got := (Tiers{HumanInterval: time.Minute}).orDefaults().HumanInterval; got != time.Minute {
		t.Errorf("set HumanInterval with defaults = %v, want 1m", got)
	}
}

func TestTiersValidate(t *testing.T) {
	with := func(fn func(*Tiers)) Tiers {
		t := DefaultTiers()
		fn(&t)
		return t
	}
	tests := []struct {
		name    string
		tiers   Tiers
		wantErr string
	}{
		{"defaults", DefaultTiers(), ""},
		{"windows equal", with(func(t *Tiers) { t.HumanWindow, t.AutonomousWindow = time.Hour, time.Hour }), ""},
		{"zero human interval", with(func(t *Tiers) { t.HumanInterval = 0 }), "invalid capture tiers: human_interval 0s is not positive"},
		{"negative idle interval", with(func(t *Tiers) { t.IdleInterval = -time.Minute }), "invalid capture tiers: idle_interval -1m0s is not positive"},
		{"zero recent window", with(func(t *Tiers) { t.RecentWindow = 0 }), "invalid capture tiers: recent_window 0s is not positive"},
		{"human window past recent", with(func(t *Tiers) { t.HumanWindow = time.Hour + time.Second }), "invalid capture tiers: human_window 1h0m1s exceeds recent_window 1h0m0s"},
		{"autonomous window past recent", with(func(t *Tiers) { t.RecentWindow = 10 * time.Minute }), "invalid capture tiers: human_window 15m0s exceeds recent_window 10m0s"},
		{"only autonomous window past recent", with(func(t *Tiers) { t.AutonomousWindow = 2 * time.Hour }), "invalid capture tiers: autonomous_window 2h0m0s exceeds recent_window 1h0m0s"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.tiers.Validate()
			if tt.wantErr == "" {
				if err != nil {
					t.Errorf("Validate() = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, ErrInvalidTiers) || err.Error() != tt.wantErr {
				t.Errorf("Validate() = %v, want %q wrapping ErrInvalidTiers", err, tt.wantErr)
			}
		})
	}
}
