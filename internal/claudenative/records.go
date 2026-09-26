package claudenative

import (
	"bufio"
	"bytes"
	"cmp"
	"crypto/sha256"
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
	Type         string  `json:"type"`
	PlanFilePath string  `json:"planFilePath"`
	PlanExists   *bool   `json:"planExists"`
	CommandMode  string  `json:"commandMode"`
	Origin       *origin `json:"origin"`
	IsMeta       bool    `json:"isMeta"`
}

type record struct {
	Type          string          `json:"type"`
	Subtype       string          `json:"subtype"`
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
// and timestamp (zero for metadata records that carry none).
//
// Human input is a user record or queued_command attachment whose
// origin.kind is "human" and that is not isMeta. Autonomous activity is work
// the agent did or input nobody typed: assistant records, user records
// carrying a tool result, user records and queued_command attachments whose
// origin.kind is "task-notification" or "peer" or whose commandMode is
// "task-notification", and scheduled_task_fire system records. Everything
// else is none: queue-operation records (a queued input counts where its
// user record or queued_command attachment delivers it, and history.jsonl
// logs a human's submission time), every other attachment and system
// subtype (prompt_snapshot, environment, hook results, reminders,
// compact_boundary, turn_duration, stop_hook_summary, local_command, and the
// like), user records without an origin, and tail metadata.
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
	case "assistant":
		return ActivityAutonomous
	case "user":
		if len(r.ToolUseResult) > 0 && !bytes.Equal(r.ToolUseResult, []byte("null")) {
			return ActivityAutonomous
		}
		return originActivity(r.Origin, r.IsMeta)
	case "attachment":
		if a := r.Attachment; a != nil && a.Type == "queued_command" {
			return a.queuedActivity()
		}
		return ActivityNone
	case "system":
		if r.Subtype == "scheduled_task_fire" {
			return ActivityAutonomous
		}
		return ActivityNone
	default:
		return ActivityNone
	}
}

func (a *attachment) queuedActivity() Activity {
	if a.CommandMode == "task-notification" {
		return ActivityAutonomous
	}
	return originActivity(a.Origin, a.IsMeta)
}

func originActivity(o *origin, isMeta bool) Activity {
	switch {
	case o == nil:
		return ActivityNone
	case o.Kind == "task-notification", o.Kind == "peer":
		return ActivityAutonomous
	case o.Kind == "human" && !isMeta:
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

func foldCompleteLines(f *os.File, from, size int64, fold func(line []byte) error) (int64, error) {
	br := bufio.NewReaderSize(io.NewSectionReader(f, from, size-from), 64<<10)
	off, end := from, from
	for {
		line, err := br.ReadBytes('\n')
		if errors.Is(err, io.EOF) {
			return end, nil
		}
		if err != nil {
			return 0, fmt.Errorf("read %s: %w", f.Name(), err)
		}
		start := off
		off += int64(len(line))
		body := bytes.TrimSpace(line)
		if len(body) == 0 {
			end = off
			continue
		}
		if err := fold(body); err != nil {
			slog.Warn("claudenative: unparseable JSONL line", "path", f.Name(), "offset", start, "err", err)
			continue
		}
		end = off
	}
}

// Digest is a SHA-256 sum Scan keeps to notice rewritten bytes.
type Digest [sha256.Size]byte

const boundaryWindow = 4 << 10

func boundaryDigest(f *os.File, offset int64) (Digest, error) {
	head := min(boundaryWindow, offset)
	tail := max(head, offset-boundaryWindow)
	h := sha256.New()
	if _, err := io.Copy(h, io.NewSectionReader(f, 0, head)); err != nil {
		return Digest{}, fmt.Errorf("read %s head: %w", f.Name(), err)
	}
	if _, err := io.Copy(h, io.NewSectionReader(f, tail, offset-tail)); err != nil {
		return Digest{}, fmt.Errorf("read %s boundary: %w", f.Name(), err)
	}
	var d Digest
	h.Sum(d[:0])
	return d, nil
}

func prefixIntact(f *os.File, size, offset int64, want Digest) (bool, error) {
	if size < offset {
		return false, nil
	}
	got, err := boundaryDigest(f, offset)
	if err != nil {
		return false, err
	}
	return got == want, nil
}
