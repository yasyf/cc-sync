// Package inventory turns the native Claude session inventory into scheduler
// capture units: every session whose cwd lies inside a registered,
// propagating reposync worktree, grouped by that worktree. Sessions outside
// every such worktree are unprotected and never become units.
package inventory

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/yasyf/cc-sync/internal/claudenative"
	"github.com/yasyf/cc-sync/internal/orcabridge"
	"github.com/yasyf/cc-sync/internal/scheduler"
	"github.com/yasyf/daemonkit/durable"
	"github.com/yasyf/reposync/registry"
	"github.com/yasyf/reposync/worktree"
)

// DefaultOrcaInterval is how often Scan refreshes Orca workspace activity.
const DefaultOrcaInterval = time.Minute

// Worktrees lists the worktrees of every registered, propagating repository.
type Worktrees func(ctx context.Context) ([]worktree.Worktree, error)

// Activity reports Orca's per-worktree human input and focus.
type Activity interface {
	Activity(ctx context.Context) ([]orcabridge.WorkspaceActivity, error)
}

// Config wires an Inventory. Orca may be nil when Orca is not wired at all;
// a zero OrcaInterval takes DefaultOrcaInterval.
type Config struct {
	Layout       claudenative.Layout
	Processes    claudenative.ProcessLister
	Worktrees    Worktrees
	Orca         Activity
	OrcaInterval time.Duration
	CursorPath   string
	Now          func() time.Time
}

// Target is everything a capture of one unit needs: the worktree and the
// native sessions whose cwd resolves into it, as of the last Scan.
type Target struct {
	Worktree worktree.Worktree
	Sessions []claudenative.Session
}

// Inventory is the scheduler's Inventory over native Claude state.
type Inventory struct {
	cfg Config

	mu      sync.Mutex
	cursor  claudenative.Cursor
	targets map[string]Target
	orca    []orcabridge.WorkspaceActivity
	orcaAt  time.Time
}

// RegisteredWorktrees discovers the worktrees of every registered,
// propagating reposync repository; local-only and origin-less repositories
// never appear.
func RegisteredWorktrees(ctx context.Context) ([]worktree.Worktree, error) {
	reg, err := registry.Load()
	if err != nil {
		return nil, fmt.Errorf("load reposync registry: %w", err)
	}
	wts, skips, err := worktree.Discover(ctx, reg)
	if err != nil {
		return nil, fmt.Errorf("discover worktrees: %w", err)
	}
	for _, s := range skips {
		slog.Debug("worktree skipped", "path", s.Path, "reason", s.Reason)
	}
	return wts, nil
}

// New returns an Inventory resuming from the cursor persisted at
// cfg.CursorPath, starting fresh when none was written yet.
func New(cfg Config) (*Inventory, error) {
	if cfg.OrcaInterval == 0 {
		cfg.OrcaInterval = DefaultOrcaInterval
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	inv := &Inventory{cfg: cfg, targets: map[string]Target{}}
	data, err := os.ReadFile(cfg.CursorPath)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return inv, nil
	case err != nil:
		return nil, fmt.Errorf("read scan cursor: %w", err)
	}
	if err := json.Unmarshal(data, &inv.cursor); err != nil {
		return nil, fmt.Errorf("decode scan cursor %s: %w", cfg.CursorPath, err)
	}
	return inv, nil
}

// Scan inventories native sessions incrementally, groups those inside
// registered worktrees into one unit per worktree, and persists the cursor.
func (i *Inventory) Scan(ctx context.Context) ([]scheduler.Unit, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	wts, err := i.cfg.Worktrees(ctx)
	if err != nil {
		return nil, err
	}
	live, err := claudenative.LiveSessions(ctx, i.cfg.Layout, i.cfg.Processes)
	if err != nil {
		return nil, fmt.Errorf("live sessions: %w", err)
	}
	res := &resolver{worktrees: wts, located: map[string]worktree.Worktree{}}
	sessions, cursor, err := claudenative.Scan(ctx, claudenative.ScanOptions{Layout: i.cfg.Layout, Repos: res, Live: live}, i.cursor)
	if err != nil {
		return nil, fmt.Errorf("scan sessions: %w", err)
	}
	if err := i.persist(cursor); err != nil {
		return nil, err
	}
	activity, err := i.activity(ctx, wts)
	if err != nil {
		return nil, err
	}
	targets := map[string]Target{}
	unprotected := 0
	for _, s := range sessions {
		wt, ok := res.located[s.Cwd]
		if !ok {
			unprotected++
			continue
		}
		t := targets[wt.ID]
		t.Worktree = wt
		t.Sessions = append(t.Sessions, s)
		targets[wt.ID] = t
	}
	if unprotected > 0 {
		slog.Debug("sessions outside registered worktrees", "count", unprotected)
	}
	i.targets = targets
	units := make([]scheduler.Unit, 0, len(targets))
	for _, id := range slices.Sorted(maps.Keys(targets)) {
		u, err := unit(targets[id], activity[id])
		if err != nil {
			return nil, err
		}
		units = append(units, u)
	}
	return units, nil
}

// Target returns the worktree and sessions the last Scan grouped under id.
func (i *Inventory) Target(worktreeID string) (Target, bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	t, ok := i.targets[worktreeID]
	return t, ok
}

func (i *Inventory) persist(cursor claudenative.Cursor) error {
	data, err := json.Marshal(cursor)
	if err != nil {
		return fmt.Errorf("encode scan cursor: %w", err)
	}
	if err := durable.WriteFile(i.cfg.CursorPath, data, 0o600); err != nil {
		return fmt.Errorf("write scan cursor: %w", err)
	}
	i.cursor = cursor
	return nil
}

func (i *Inventory) activity(ctx context.Context, wts []worktree.Worktree) (map[string]orcabridge.WorkspaceActivity, error) {
	if i.cfg.Orca == nil {
		return nil, nil
	}
	now := i.cfg.Now()
	if i.orcaAt.IsZero() || now.Sub(i.orcaAt) >= i.cfg.OrcaInterval {
		activity, err := i.cfg.Orca.Activity(ctx)
		var unavailable *orcabridge.UnavailableError
		switch {
		case errors.As(err, &unavailable):
			slog.Debug("orca activity unavailable", "reason", unavailable.Reason)
			activity = nil
		case err != nil:
			return nil, fmt.Errorf("orca activity: %w", err)
		}
		i.orca, i.orcaAt = activity, now
	}
	byWorktree := map[string]orcabridge.WorkspaceActivity{}
	for _, a := range i.orca {
		if wt, ok := worktree.Locate(wts, a.Path); ok {
			byWorktree[wt.ID] = a
		}
	}
	return byWorktree, nil
}

func unit(t Target, orca orcabridge.WorkspaceActivity) (scheduler.Unit, error) {
	u := scheduler.Unit{WorktreeID: t.Worktree.ID, RepoKey: t.Worktree.CommonDir}
	for _, s := range t.Sessions {
		u.Sessions = append(u.Sessions, scheduler.Session{
			ID:             string(s.ID),
			LastHumanInput: later(s.LastHumanInput, orca.LastHumanInputAt),
			LastHumanFocus: orca.LastHumanFocusAt,
			LastAutonomous: s.LastAutonomousActivity,
			LastActivity:   s.LastActivity,
		})
	}
	stamp, err := metaStamp(t)
	if err != nil {
		return scheduler.Unit{}, err
	}
	u.MetaStamp = stamp
	return u, nil
}

type sessionStamp struct {
	ID           claudenative.SessionID  `json:"id"`
	Inode        uint64                  `json:"inode"`
	CompleteSize int64                   `json:"complete_size"`
	LeafUUID     string                  `json:"leaf_uuid"`
	Title        string                  `json:"title"`
	Sidecars     claudenative.SidecarSet `json:"sidecars"`
}

type fileStamp struct {
	Path    string `json:"path"`
	Size    int64  `json:"size"`
	ModTime int64  `json:"mod_time"`
}

func metaStamp(t Target) (string, error) {
	var stamp struct {
		Worktree string         `json:"worktree"`
		Files    []fileStamp    `json:"files"`
		Sessions []sessionStamp `json:"sessions"`
	}
	stamp.Worktree = t.Worktree.ID
	if t.Worktree.GitDir != "" {
		for _, name := range []string{"HEAD", "index"} {
			f, err := statStamp(filepath.Join(t.Worktree.GitDir, name))
			if err != nil {
				return "", err
			}
			stamp.Files = append(stamp.Files, f)
		}
	}
	for _, s := range t.Sessions {
		stamp.Sessions = append(stamp.Sessions, sessionStamp{
			ID:           s.ID,
			Inode:        s.Transcript.Inode,
			CompleteSize: s.Transcript.CompleteSize,
			LeafUUID:     s.Transcript.LeafUUID,
			Title:        s.Title,
			Sidecars:     s.Sidecars,
		})
	}
	data, err := json.Marshal(stamp)
	if err != nil {
		return "", fmt.Errorf("encode meta stamp: %w", err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func statStamp(path string) (fileStamp, error) {
	info, err := os.Stat(path)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return fileStamp{Path: path}, nil
	case err != nil:
		return fileStamp{}, fmt.Errorf("stat %s: %w", path, err)
	}
	return fileStamp{Path: path, Size: info.Size(), ModTime: info.ModTime().UnixNano()}, nil
}

type resolver struct {
	worktrees []worktree.Worktree
	located   map[string]worktree.Worktree
}

func (r *resolver) Resolve(_ context.Context, cwd string) (claudenative.RepoBinding, bool, error) {
	wt, ok := worktree.Locate(r.worktrees, cwd)
	if !ok {
		return claudenative.RepoBinding{}, false, nil
	}
	resolved, err := filepath.EvalSymlinks(cwd)
	if err != nil {
		resolved = filepath.Clean(cwd)
	}
	rel, err := filepath.Rel(wt.Root, resolved)
	if err != nil {
		return claudenative.RepoBinding{}, false, fmt.Errorf("relate %s to worktree %s: %w", cwd, wt.Root, err)
	}
	r.located[cwd] = wt
	return claudenative.RepoBinding{
		Origin:       wt.Origin,
		Relpath:      wt.Relpath,
		RegistryPath: r.mainRoot(wt),
		CheckoutRoot: wt.Root,
		WorktreeRel:  filepath.ToSlash(rel),
		Linked:       wt.Name != "",
	}, true, nil
}

func (r *resolver) mainRoot(wt worktree.Worktree) string {
	for _, w := range r.worktrees {
		if w.CommonDir == wt.CommonDir && w.Name == "" {
			return w.Root
		}
	}
	return ""
}

func later(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}
