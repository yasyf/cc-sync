package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"text/tabwriter"
	"time"
)

const outputVersion = 1

type header struct {
	Version int  `json:"version"`
	OK      bool `json:"ok"`
}

var okHeader = header{Version: outputVersion, OK: true}

type failureDetail struct {
	Code    Code         `json:"code"`
	Message string       `json:"message"`
	Details ErrorDetails `json:"details,omitempty"`
}

type failure struct {
	header
	Error failureDetail `json:"error"`
}

func newFailure(code Code, err error) failure {
	f := failure{
		header: header{Version: outputVersion, OK: false},
		Error:  failureDetail{Code: code, Message: err.Error()},
	}
	if e, ok := errors.AsType[*Error](err); ok {
		f.Error.Details = e.Details
	}
	return f
}

type listedItem struct {
	Selector string `json:"selector"`
	Item
}

func listed(item Item) listedItem {
	return listedItem{Selector: item.Ref().String(), Item: item}
}

type listJSON struct {
	header
	GeneratedAt Time              `json:"generated_at"`
	Local       Host              `json:"local"`
	Items       Array[listedItem] `json:"items"`
}

func newListJSON(res ListResult) listJSON {
	items := make(Array[listedItem], 0, len(res.Items))
	for _, item := range res.Items {
		items = append(items, listed(item))
	}
	return listJSON{header: okHeader, GeneratedAt: res.GeneratedAt, Local: res.Local, Items: items}
}

type inspectJSON struct {
	header
	Selector string `json:"selector"`
	InspectResult
}

func writeJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return fmt.Errorf("write json: %w", err)
	}
	return nil
}

type ndjsonProgress struct {
	mu sync.Mutex
	w  io.Writer
}

func (p *ndjsonProgress) report(line Progress) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := writeJSON(p.w, line); err != nil {
		slog.Warn("write pickup progress", "phase", line.Phase, "err", err)
	}
}

func formatTime(t Time) string { return t.UTC().Format(time.RFC3339) }

func formatOptionalTime(t *Time, absent string) string {
	if t == nil {
		return absent
	}
	return formatTime(*t)
}

func formatOptional(s *string, absent string) string {
	if s == nil {
		return absent
	}
	return *s
}

func formatPause(p *Pause) string {
	if p == nil {
		return ""
	}
	return fmt.Sprintf("paused: %s (%s) since %s", p.Reason, p.Endpoint, formatTime(p.Since))
}

func formatNetwork(n Network) string {
	parts := []string{string(n.Status)}
	for _, flag := range []struct {
		on   bool
		name string
	}{
		{n.Expensive, "expensive"},
		{n.Constrained, "constrained"},
		{n.Cellular, "cellular"},
		{n.ManualMetered, "manual-metered"},
	} {
		if flag.on {
			parts = append(parts, flag.name)
		}
	}
	return strings.Join(parts, ", ")
}

func joinOrDash(values []string) string {
	if len(values) == 0 {
		return "-"
	}
	return strings.Join(values, ", ")
}

func formatCode(c Completeness) string {
	if c.CodeCapturedAt == nil {
		return string(c.Code)
	}
	return fmt.Sprintf("%s from %s", c.Code, formatTime(*c.CodeCapturedAt))
}

func formatNewerPartial(c *PartialCheckpoint) string {
	return fmt.Sprintf("newer partial checkpoint (sessions %s, code %s)",
		formatOptionalTime(c.SessionActivityAt, "none"), formatOptionalTime(c.CodeCapturedAt, "none"))
}

func formatReadiness(c Completeness) string {
	if c.Ready {
		return "ready"
	}
	if len(c.Missing) == 0 {
		return "not ready"
	}
	return "missing " + strings.Join(c.Missing, ", ")
}

type printer struct {
	w   io.Writer
	err error
}

func (p *printer) printf(format string, args ...any) {
	if p.err == nil {
		_, p.err = fmt.Fprintf(p.w, format, args...)
	}
}

func (p *printer) table(rows [][]string) {
	if p.err != nil {
		return
	}
	tw := tabwriter.NewWriter(p.w, 0, 4, 2, ' ', 0)
	for _, row := range rows {
		if _, p.err = io.WriteString(tw, strings.Join(row, "\t")+"\n"); p.err != nil {
			return
		}
	}
	p.err = tw.Flush()
}

func renderStatus(p *printer, res StatusResult) {
	helper := "not running"
	if res.Helper.Running {
		helper = "running"
	}
	p.printf("helper: %s (build %s)\n", helper, res.Helper.Build)
	p.printf("local: %s (%s), network %s\n", res.Local.HostName, res.Local.HostID, formatNetwork(res.Local.Network))
	for _, peer := range res.Peers {
		reach := "unreachable"
		if peer.Reachable {
			reach = "reachable"
		}
		line := fmt.Sprintf("peer %s (%s): %s, last seen %s", peer.HostName, peer.HostID, reach, formatOptionalTime(peer.LastSeenAt, "never"))
		if peer.AckedRevision != nil {
			line += fmt.Sprintf(", acked revision %d", *peer.AckedRevision)
		}
		if peer.PendingRevision != nil {
			line += fmt.Sprintf(", pending revision %d since %s", *peer.PendingRevision, formatOptionalTime(peer.PendingSince, "unknown"))
		}
		if peer.Pause != nil {
			line += ", " + formatPause(peer.Pause)
		}
		p.printf("%s\n", line)
	}
	q := res.Scheduler.QueuedByTier
	p.printf("scheduler: %d workers, queued human=%d autonomous=%d recent=%d idle=%d, last round %s\n",
		res.Scheduler.Workers, q.Human, q.Autonomous, q.Recent, q.Idle, formatOptionalTime(res.Scheduler.LastRoundAt, "never"))
	t := res.Scheduler.Tiers
	p.printf("capture tiers: human every %s within %s, autonomous every %s within %s, recent every %s within %s, idle every %s\n",
		t.HumanInterval, t.HumanWindow, t.AutonomousInterval, t.AutonomousWindow, t.RecentInterval, t.RecentWindow, t.IdleInterval)
}

func renderList(p *printer, res ListResult) {
	if len(res.Items) == 0 {
		p.printf("no recoverable workspaces\n")
		return
	}
	rows := make([][]string, 0, 1+len(res.Items))
	rows = append(rows, []string{"SELECTOR", "SOURCE", "REPO", "BRANCH", "SESSIONS", "CAPTURED", "CODE", "STATE"})
	for _, item := range res.Items {
		state := formatReadiness(item.Completeness)
		if item.NewerPartial != nil {
			state += "; " + formatNewerPartial(item.NewerPartial)
		}
		if item.Pause != nil {
			state += "; " + formatPause(item.Pause)
		}
		rows = append(rows, []string{
			item.Ref().String(), item.Source.HostName, item.Workspace.RepoName, formatOptional(item.Workspace.Branch, "(detached)"),
			strconv.Itoa(len(item.Sessions)), formatTime(item.Checkpoint.CapturedAt), formatCode(item.Completeness), state,
		})
	}
	p.table(rows)
}

func renderInspect(p *printer, res InspectResult) {
	p.printf("%s: %s on %s (%s)\n", res.Ref(), res.Workspace.RepoName, formatOptional(res.Workspace.Branch, "(detached)"), res.Workspace.SourcePath)
	p.printf("source: %s (%s)\n", res.Source.HostName, res.Source.HostID)
	p.printf("checkpoint: %s (%s) captured %s, %s\n", res.Checkpoint.ID, res.Checkpoint.Tier, formatTime(res.Checkpoint.CapturedAt), formatReadiness(res.Completeness))
	p.printf("code: %s\n", formatCode(res.Completeness))
	if res.NewerPartial != nil {
		p.printf("%s: %s captured %s (pickup --allow-partial)\n", formatNewerPartial(res.NewerPartial), res.NewerPartial.ID, formatTime(res.NewerPartial.CapturedAt))
	}
	for _, s := range res.Sessions {
		p.printf("session %s: %s [%s]\n", s.SessionID, s.Title, s.Activity)
	}
	rows := make([][]string, 0, 1+len(res.Checkpoints))
	rows = append(rows, []string{"CHECKPOINT", "TIER", "CAPTURED", "READY", "MISSING", "DEFERRED"})
	for _, c := range res.Checkpoints {
		rows = append(rows, []string{
			c.ID, string(c.Tier), formatTime(c.CapturedAt), strconv.FormatBool(c.Ready), joinOrDash(c.Missing), joinOrDash(c.Deferred),
		})
	}
	p.table(rows)
	for _, d := range res.Delivery {
		state := string(d.State)
		if d.Pause != nil {
			state = formatPause(d.Pause)
		}
		p.printf("delivery to %s: %s\n", d.Peer, state)
	}
}

func renderPickup(p *printer, res PickupResult, launching *PickedSession) {
	verb := "restored"
	if res.Checkout.Reused {
		verb = "reused"
	}
	p.printf("checkout %s: %s on %s\n", verb, res.Checkout.Path, formatOptional(res.Checkout.Branch, "(detached)"))
	if res.Checkpoint.Partial {
		p.printf("partial checkpoint %s: code deferred (%s)\n", res.Checkpoint.ID, res.Checkpoint.CodeDeferred)
	}
	if s := res.Checkout.Sparse; s != nil {
		if s.Expanded {
			p.printf("sparse checkout expanded to full (patterns %s); rerun with --apply-sparse to keep it sparse\n", strings.Join(s.Patterns, ", "))
		} else {
			p.printf("sparse checkout kept (patterns %s)\n", strings.Join(s.Patterns, ", "))
		}
	}
	for _, s := range res.Sessions {
		line := fmt.Sprintf("session %s: %s", s.SessionID, s.Status)
		if s.Reason != "" {
			line += " (" + string(s.Reason) + ")"
		}
		if s.ForkedFrom != nil {
			line += ", forked from " + *s.ForkedFrom
		}
		p.printf("%s\n", line)
	}
	if res.Orca != nil {
		p.printf("orca worktree %s: %d resumed, %d dormant\n", res.Orca.WorktreeID, len(res.Orca.Resumed), len(res.Orca.Dormant))
	}
	for _, s := range res.Sessions {
		switch {
		case launching != nil && s.SessionID == launching.SessionID:
			p.printf("resuming %s with claude in %s\n", s.SessionID, s.Launch.Dir)
		case res.Orca == nil && s.Status == SessionRestored:
			p.printf("resume with: cc-sync resume %s\n", s.SessionID)
		}
	}
}

func renderSync(p *printer, res SyncResult) {
	for _, wt := range res.Worktrees {
		line := wt.WorkspaceID + ": "
		if wt.Checkpoint == nil {
			line += "no checkpoint"
		} else {
			line += fmt.Sprintf("checkpoint %s captured %s", wt.Checkpoint.ID, formatTime(wt.Checkpoint.CapturedAt))
		}
		if len(wt.Deferred) > 0 {
			line += "; deferred: " + strings.Join(wt.Deferred, ", ")
		}
		p.printf("%s\n", line)
	}
}
