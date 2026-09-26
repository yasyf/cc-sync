package catalog

import (
	"slices"
	"time"
)

// Retain classifies cps at now and returns the retained checkpoints newest
// first: unexpired ones that are the latest, the newest of their UTC hour
// within HourlyWindow, or the newest of their UTC day within DailyWindow.
// Mixed checkpoints claim no hour or day, and the newest complete checkpoint
// is also latest, so a mixed checkpoint never evicts the last complete one.
// A checkpoint expires at ExpiresAt exactly.
func Retain(cps []Checkpoint, now time.Time) []Checkpoint {
	live := make([]Checkpoint, 0, len(cps))
	for _, cp := range cps {
		if now.Before(cp.ExpiresAt) {
			live = append(live, cp)
		}
	}
	slices.SortFunc(live, compareCheckpoints)
	hours := make(map[int64]bool)
	days := make(map[int64]bool)
	kept := live[:0]
	latestComplete := slices.IndexFunc(live, func(cp Checkpoint) bool { return !cp.Mixed() })
	for i, cp := range live {
		var classes []Class
		if i == 0 || i == latestComplete {
			classes = append(classes, ClassLatest)
		}
		if cp.Mixed() {
			if len(classes) > 0 {
				cp.Classes = classes
				kept = append(kept, cp)
			}
			continue
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
