package claudenative

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// LiveProcess is a running Claude Code process that owns a session. Entries
// found only by the argv scan carry just PID, ProcStart, and SessionID.
type LiveProcess struct {
	PID        int
	ProcStart  string
	SessionID  SessionID
	Cwd        string
	Version    string
	Kind       string
	Entrypoint string
	Status     string
	UpdatedAt  time.Time
}

// Process is one row of the host process table; Start is ps lstart in UTC,
// the form Claude records as procStart.
type Process struct {
	PID   int
	Start string
	Argv  []string
}

// ProcessLister snapshots the host process table.
type ProcessLister interface {
	Processes(ctx context.Context) ([]Process, error)
}

// SystemProcesses lists host processes with ps.
func SystemProcesses() ProcessLister {
	return psLister{}
}

type psLister struct{}

func (psLister) Processes(ctx context.Context) ([]Process, error) {
	cmd := exec.CommandContext(ctx, "ps", "-A", "-ww", "-o", "pid=,lstart=,command=")
	cmd.Env = append(os.Environ(), "TZ=UTC", "LC_ALL=C")
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("run ps: %w", err)
	}
	return parsePS(string(out))
}

func parsePS(out string) ([]Process, error) {
	var procs []Process
	for line := range strings.Lines(out) {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		if len(fields) < 6 {
			return nil, fmt.Errorf("parse ps row %q: want pid and lstart", line)
		}
		pid, err := strconv.Atoi(fields[0])
		if err != nil {
			return nil, fmt.Errorf("parse ps pid: %w", err)
		}
		procs = append(procs, Process{PID: pid, Start: strings.Join(fields[1:6], " "), Argv: fields[6:]})
	}
	return procs, nil
}

type registryEntry struct {
	PID        int    `json:"pid"`
	SessionID  string `json:"sessionId"`
	Cwd        string `json:"cwd"`
	ProcStart  string `json:"procStart"`
	Version    string `json:"version"`
	Kind       string `json:"kind"`
	Entrypoint string `json:"entrypoint"`
	Status     string `json:"status"`
	UpdatedAt  int64  `json:"updatedAt"`
}

// LiveSessions reports every session a running Claude process owns: entries
// of <config>/sessions/<pid>.json whose pid is alive with the recorded
// procStart (so a reused pid never counts), unioned with claude processes
// whose argv carries --resume, -r, or --session-id with a session UUID. The
// registry's .key files are secrets and are never opened.
func LiveSessions(ctx context.Context, l Layout, procs ProcessLister) (map[SessionID]LiveProcess, error) {
	table, err := procs.Processes(ctx)
	if err != nil {
		return nil, fmt.Errorf("list processes: %w", err)
	}
	return liveSessions(os.DirFS(l.registryDir()), table)
}

func liveSessions(registry fs.FS, table []Process) (map[SessionID]LiveProcess, error) {
	byPID := make(map[int]Process, len(table))
	for _, p := range table {
		byPID[p.PID] = p
	}
	live := make(map[SessionID]LiveProcess)
	entries, err := fs.ReadDir(registry, ".")
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("read session registry: %w", err)
	}
	for _, e := range entries {
		if e.IsDir() || path.Ext(e.Name()) != ".json" {
			continue
		}
		entry, ok, err := readRegistryEntry(registry, e.Name())
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		p, alive := byPID[entry.PID]
		if !alive || !sameStart(p.Start, entry.ProcStart) {
			continue
		}
		id, err := ParseSessionID(entry.SessionID)
		if err != nil {
			return nil, fmt.Errorf("session registry %s: %w", e.Name(), err)
		}
		live[id] = LiveProcess{
			PID:        entry.PID,
			ProcStart:  entry.ProcStart,
			SessionID:  id,
			Cwd:        entry.Cwd,
			Version:    entry.Version,
			Kind:       entry.Kind,
			Entrypoint: entry.Entrypoint,
			Status:     entry.Status,
			UpdatedAt:  time.UnixMilli(entry.UpdatedAt),
		}
	}
	for _, p := range table {
		id, ok := argvSession(p.Argv)
		if !ok {
			continue
		}
		if _, seen := live[id]; !seen {
			live[id] = LiveProcess{PID: p.PID, ProcStart: p.Start, SessionID: id}
		}
	}
	return live, nil
}

func readRegistryEntry(registry fs.FS, name string) (registryEntry, bool, error) {
	data, err := fs.ReadFile(registry, name)
	if errors.Is(err, fs.ErrNotExist) {
		return registryEntry{}, false, nil
	}
	if err != nil {
		return registryEntry{}, false, fmt.Errorf("read session registry %s: %w", name, err)
	}
	var entry registryEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return registryEntry{}, false, fmt.Errorf("decode session registry %s: %w", name, err)
	}
	return entry, true, nil
}

func sameStart(a, b string) bool {
	return strings.Join(strings.Fields(a), " ") == strings.Join(strings.Fields(b), " ")
}

func argvSession(argv []string) (SessionID, bool) {
	if !isClaude(argv) {
		return "", false
	}
	for i, arg := range argv {
		var value string
		switch {
		case (arg == "--resume" || arg == "-r" || arg == "--session-id") && i+1 < len(argv):
			value = argv[i+1]
		case strings.HasPrefix(arg, "--resume="), strings.HasPrefix(arg, "--session-id="):
			_, value, _ = strings.Cut(arg, "=")
		default:
			continue
		}
		if id, err := ParseSessionID(value); err == nil {
			return id, true
		}
	}
	return "", false
}

func isClaude(argv []string) bool {
	for _, arg := range argv[:min(2, len(argv))] {
		if filepath.Base(arg) == "claude" {
			return true
		}
	}
	return false
}
