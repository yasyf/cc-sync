package cli

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	sessionIDPattern    = regexp.MustCompile(`^[0-9a-f][0-9a-f-]{0,35}$`)
	checkpointIDPattern = regexp.MustCompile(`^[0-9a-f]+$`)
	hourlyPattern       = regexp.MustCompile(`^-([0-9]+)h$`)
)

// Target selects an item either directly or through one of its sessions.
type Target interface {
	fmt.Stringer
	isTarget()
}

// ItemRef selects an item as <source_host_id>/<workspace_id>.
type ItemRef struct {
	SourceHostID string
	WorkspaceID  string
}

func (ItemRef) isTarget() {}

func (r ItemRef) String() string { return r.SourceHostID + "/" + r.WorkspaceID }

// SessionRef selects a session by full id or unique prefix, optionally scoped
// to the source host named by Source.
type SessionRef struct {
	Source string
	ID     string
}

func (SessionRef) isTarget() {}

func (r SessionRef) String() string {
	if r.Source == "" {
		return r.ID
	}
	return r.Source + ":" + r.ID
}

// ParseTarget parses an item selector (<source_host_id>/<workspace_id>) or a
// session selector (<sid>, a unique prefix, or <source>:<sid>).
func ParseTarget(s string) (Target, error) {
	host, workspace, ok := strings.Cut(s, "/")
	if !ok {
		ref, err := ParseSession(s)
		if err != nil {
			return nil, err
		}
		return ref, nil
	}
	if host == "" || workspace == "" || strings.Contains(workspace, "/") {
		return nil, Errorf(CodeUsage, "invalid item selector %q: want <source_host_id>/<workspace_id>", s)
	}
	return ItemRef{SourceHostID: host, WorkspaceID: workspace}, nil
}

// ParseSession parses a session selector: a full id, a unique prefix, or
// <source>:<sid>.
func ParseSession(s string) (SessionRef, error) {
	source, id, scoped := strings.Cut(s, ":")
	if !scoped {
		source, id = "", s
	}
	if scoped && source == "" {
		return SessionRef{}, Errorf(CodeUsage, "invalid session selector %q: empty source", s)
	}
	if err := validateSessionID(id); err != nil {
		return SessionRef{}, err
	}
	return SessionRef{Source: source, ID: id}, nil
}

func validateSessionID(id string) error {
	if !sessionIDPattern.MatchString(id) {
		return Errorf(CodeUsage, "invalid session id %q: want a lowercase session id or prefix", id)
	}
	return nil
}

// CheckpointSelector picks one checkpoint of an item.
type CheckpointSelector interface {
	fmt.Stringer
	isCheckpointSelector()
}

// LatestCheckpoint selects the newest checkpoint.
type LatestCheckpoint struct{}

func (LatestCheckpoint) isCheckpointSelector() {}

func (LatestCheckpoint) String() string { return "latest" }

// CheckpointID selects the checkpoint whose id starts with Prefix.
type CheckpointID struct{ Prefix string }

func (CheckpointID) isCheckpointSelector() {}

func (c CheckpointID) String() string { return c.Prefix }

// CheckpointAt selects the newest checkpoint captured at or before Time.
type CheckpointAt struct{ Time time.Time }

func (CheckpointAt) isCheckpointSelector() {}

func (c CheckpointAt) String() string { return "at:" + c.Time.UTC().Format(time.RFC3339Nano) }

// CheckpointHourly selects the hourly checkpoint HoursAgo hours back.
type CheckpointHourly struct{ HoursAgo int }

func (CheckpointHourly) isCheckpointSelector() {}

func (c CheckpointHourly) String() string { return "hourly:-" + strconv.Itoa(c.HoursAgo) + "h" }

// CheckpointDaily selects the daily checkpoint for a calendar date.
type CheckpointDaily struct {
	Year  int
	Month time.Month
	Day   int
}

func (CheckpointDaily) isCheckpointSelector() {}

func (c CheckpointDaily) String() string {
	return fmt.Sprintf("daily:%04d-%02d-%02d", c.Year, c.Month, c.Day)
}

// ParseCheckpoint parses a --checkpoint value: latest, <id-prefix>,
// at:<RFC3339>, hourly:-<N>h, or daily:<YYYY-MM-DD>.
func ParseCheckpoint(s string) (CheckpointSelector, error) {
	kind, arg, qualified := strings.Cut(s, ":")
	if !qualified {
		switch {
		case s == "latest":
			return LatestCheckpoint{}, nil
		case checkpointIDPattern.MatchString(s):
			return CheckpointID{Prefix: s}, nil
		}
		return nil, Errorf(CodeUsage, "invalid checkpoint %q: want latest, <id-prefix>, at:<RFC3339>, hourly:-<N>h, or daily:<YYYY-MM-DD>", s)
	}
	switch kind {
	case "at":
		t, err := time.Parse(time.RFC3339, arg)
		if err != nil {
			return nil, Errorf(CodeUsage, "invalid checkpoint %q: %w", s, err)
		}
		return CheckpointAt{Time: t}, nil
	case "hourly":
		m := hourlyPattern.FindStringSubmatch(arg)
		if m == nil {
			return nil, Errorf(CodeUsage, "invalid checkpoint %q: want hourly:-<N>h", s)
		}
		n, err := strconv.Atoi(m[1])
		if err != nil {
			return nil, Errorf(CodeUsage, "invalid checkpoint %q: %w", s, err)
		}
		return CheckpointHourly{HoursAgo: n}, nil
	case "daily":
		d, err := time.Parse(time.DateOnly, arg)
		if err != nil {
			return nil, Errorf(CodeUsage, "invalid checkpoint %q: %w", s, err)
		}
		return CheckpointDaily{Year: d.Year(), Month: d.Month(), Day: d.Day()}, nil
	}
	return nil, Errorf(CodeUsage, "invalid checkpoint %q: unknown kind %q", s, kind)
}
