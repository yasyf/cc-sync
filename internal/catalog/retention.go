package catalog

import (
	"slices"
	"time"
)

// Retain classifies cps at now and returns the retained checkpoints newest
// first. Complete checkpoints alone claim the tiers: the newest is latest,
// and each is kept as the newest of its UTC hour within HourlyWindow or of
// its UTC day within DailyWindow. Beside them, the newest mixed and the
// newest otherwise incomplete checkpoint captured after the last complete
// one are kept as latest, so neither ever evicts a complete checkpoint. A
// checkpoint expires at ExpiresAt exactly.
func Retain(cps []Checkpoint, now time.Time) []Checkpoint {
	live := unexpired(cps, now)
	slices.SortFunc(live, compareCheckpoints)
	latestComplete := slices.IndexFunc(live, Checkpoint.Complete)
	hours := make(map[int64]bool)
	days := make(map[int64]bool)
	partial := make(map[bool]bool)
	kept := live[:0]
	for i, cp := range live {
		var classes []Class
		switch {
		case !cp.Complete():
			if (latestComplete < 0 || i < latestComplete) && !partial[cp.Mixed()] {
				partial[cp.Mixed()] = true
				classes = append(classes, ClassLatest)
			}
		default:
			if i == latestComplete {
				classes = append(classes, ClassLatest)
			}
			age := now.Sub(cp.CapturedAt)
			if hour := cp.CapturedAt.Truncate(time.Hour).Unix(); age < HourlyWindow && !hours[hour] {
				hours[hour] = true
				classes = append(classes, ClassHourly)
			}
			if day := cp.CapturedAt.Truncate(24 * time.Hour).Unix(); age < DailyWindow && !days[day] {
				days[day] = true
				classes = append(classes, ClassDaily)
			}
		}
		if len(classes) > 0 {
			cp.Classes = classes
			kept = append(kept, cp)
		}
	}
	return kept
}

func unexpired(cps []Checkpoint, now time.Time) []Checkpoint {
	return slices.DeleteFunc(slices.Clone(cps), func(cp Checkpoint) bool { return !now.Before(cp.ExpiresAt) })
}
