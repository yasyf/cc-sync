package claudenative

import (
	"bufio"
	"bytes"
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"
	"time"
)

// Activity says who was driving a session when a transcript record was written.
type Activity uint8

// Activity classes, from least to most attention-worthy for the scheduler.
const (
	ActivityNone Activity = iota
	ActivityAutonomous
	ActivityHuman
)

type origin struct {
	Kind string `json:"kind"`
}

type attachment struct {
	PlanFilePath string `json:"planFilePath"`
	PlanExists   *bool  `json:"planExists"`
}

type record struct {
	Type          string          `json:"type"`
	UUID          string          `json:"uuid"`
	Timestamp     string          `json:"timestamp"`
	Cwd           string          `json:"cwd"`
	GitBranch     string          `json:"gitBranch"`
	Version       string          `json:"version"`
	Entrypoint    string          `json:"entrypoint"`
	Slug          string          `json:"slug"`
	IsMeta        bool            `json:"isMeta"`
	Origin        *origin         `json:"origin"`
	ToolUseResult json.RawMessage `json:"toolUseResult"`
	Operation     string          `json:"operation"`
	RelocatedCwd  string          `json:"relocatedCwd"`
	CustomTitle   string          `json:"customTitle"`
	AITitle       string          `json:"aiTitle"`
	Attachment    *attachment     `json:"attachment"`
}

type toolUseInputs struct {
	Message struct {
		Content []struct {
			Type  string `json:"type"`
			Input struct {
				TeamName string `json:"team_name"`
			} `json:"input"`
		} `json:"content"`
	} `json:"message"`
}

var teamNameKey = []byte(`"team_name"`)

// ClassifyRecord decodes one transcript line and reports its activity class
// and timestamp (zero for metadata records that carry none). Human input is a
// non-meta user record with origin.kind "human" and no tool result, or a
// queue-operation enqueue; assistant, system, attachment, tool-result, and
// task-notification or peer user records are autonomous; everything else,
// including tail metadata, is none.
func ClassifyRecord(line []byte) (Activity, time.Time, error) {
	r, ts, err := decodeRecord(line)
	if err != nil {
		return ActivityNone, time.Time{}, err
	}
	return r.activity(), ts, nil
}

func decodeRecord(line []byte) (record, time.Time, error) {
	var r record
	if err := json.Unmarshal(line, &r); err != nil {
		return record{}, time.Time{}, fmt.Errorf("decode record: %w", err)
	}
	if r.Timestamp == "" {
		return r, time.Time{}, nil
	}
	ts, err := time.Parse(time.RFC3339Nano, r.Timestamp)
	if err != nil {
		return record{}, time.Time{}, fmt.Errorf("parse record timestamp: %w", err)
	}
	return r, ts, nil
}

func (r *record) activity() Activity {
	switch r.Type {
	case "user":
		return r.userActivity()
	case "queue-operation":
		if r.Operation == "enqueue" {
			return ActivityHuman
		}
		return ActivityNone
	case "assistant", "system", "attachment":
		return ActivityAutonomous
	default:
		return ActivityNone
	}
}

func (r *record) userActivity() Activity {
	switch {
	case len(r.ToolUseResult) > 0 && !bytes.Equal(r.ToolUseResult, []byte("null")):
		return ActivityAutonomous
	case r.Origin == nil:
		return ActivityNone
	case r.Origin.Kind == "task-notification", r.Origin.Kind == "peer":
		return ActivityAutonomous
	case r.Origin.Kind == "human" && !r.IsMeta:
		return ActivityHuman
	default:
		return ActivityNone
	}
}

// TranscriptSummary is the state folded from a transcript's complete records.
// Cursors carry it so a later scan folds only appended bytes.
type TranscriptSummary struct {
	Records        int
	LeafUUID       string
	OriginalCwd    string
	RelocatedCwd   string
	GitBranch      string
	Version        string
	Entrypoint     string
	CustomTitle    string
	AITitle        string
	LastHuman      time.Time
	LastAutonomous time.Time
	PlanFiles      []string
	Slugs          []string
	TeamNames      []string
}

// Cwd is Claude's effective project cwd: the last relocated cwd, else the first recorded cwd.
func (s TranscriptSummary) Cwd() string {
	if s.RelocatedCwd != "" {
		return s.RelocatedCwd
	}
	return s.OriginalCwd
}

// Title is the user's custom title, else Claude's generated one.
func (s TranscriptSummary) Title() string {
	if s.CustomTitle != "" {
		return s.CustomTitle
	}
	return s.AITitle
}

func (s TranscriptSummary) clone() TranscriptSummary {
	s.PlanFiles = slices.Clone(s.PlanFiles)
	s.Slugs = slices.Clone(s.Slugs)
	s.TeamNames = slices.Clone(s.TeamNames)
	return s
}

func (s *TranscriptSummary) fold(r *record, ts time.Time, line []byte) error {
	s.Records++
	if r.UUID != "" {
		s.LeafUUID = r.UUID
	}
	if s.OriginalCwd == "" {
		s.OriginalCwd = r.Cwd
	}
	s.GitBranch = cmp.Or(r.GitBranch, s.GitBranch)
	s.Version = cmp.Or(r.Version, s.Version)
	s.Entrypoint = cmp.Or(r.Entrypoint, s.Entrypoint)
	if r.Slug != "" {
		s.Slugs = insertSorted(s.Slugs, r.Slug)
	}
	switch r.Type {
	case "relocated":
		s.RelocatedCwd = r.RelocatedCwd
	case "custom-title":
		s.CustomTitle = r.CustomTitle
	case "ai-title":
		s.AITitle = r.AITitle
	case "attachment":
		if a := r.Attachment; a != nil && a.PlanFilePath != "" && (a.PlanExists == nil || *a.PlanExists) {
			s.PlanFiles = insertSorted(s.PlanFiles, a.PlanFilePath)
		}
	case "assistant":
		if err := s.foldTeamNames(line); err != nil {
			return err
		}
	}
	switch r.activity() {
	case ActivityHuman:
		s.LastHuman = later(s.LastHuman, ts)
	case ActivityAutonomous:
		s.LastAutonomous = later(s.LastAutonomous, ts)
	}
	return nil
}

func (s *TranscriptSummary) foldTeamNames(line []byte) error {
	if !bytes.Contains(line, teamNameKey) {
		return nil
	}
	var in toolUseInputs
	if err := json.Unmarshal(line, &in); err != nil {
		return fmt.Errorf("decode tool_use inputs: %w", err)
	}
	for _, c := range in.Message.Content {
		if c.Type == "tool_use" && c.Input.TeamName != "" {
			s.TeamNames = insertSorted(s.TeamNames, c.Input.TeamName)
		}
	}
	return nil
}

func later(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

func insertSorted(s []string, v string) []string {
	i, found := slices.BinarySearch(s, v)
	if found {
		return s
	}
	return slices.Insert(s, i, v)
}

// foldCompleteLines feeds each '\n'-terminated line of path within [from,
// size) to fold and returns the offset just past the last line fold accepted.
// A line fold rejects is skipped when a later line is accepted and otherwise
// left unconsumed, like a partial tail; bytes at or past size are never read.
func foldCompleteLines(path string, from, size int64, fold func(line []byte) error) (int64, error) {
	f, err := os.Open(path) //nolint:gosec // G304: a transcript or history.jsonl under the scanned Claude config dir, opened read-only.
	if err != nil {
		return 0, fmt.Errorf("open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()
	br := bufio.NewReaderSize(io.NewSectionReader(f, from, size-from), 64<<10)
	off, end := from, from
	for {
		line, err := br.ReadBytes('\n')
		if errors.Is(err, io.EOF) {
			return end, nil
		}
		if err != nil {
			return 0, fmt.Errorf("read %s: %w", path, err)
		}
		start := off
		off += int64(len(line))
		body := bytes.TrimSpace(line)
		if len(body) == 0 {
			end = off
			continue
		}
		if err := fold(body); err != nil {
			slog.Warn("claudenative: unparseable JSONL line", "path", path, "offset", start, "err", err)
			continue
		}
		end = off
	}
}
