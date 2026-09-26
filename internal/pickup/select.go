package pickup

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/yasyf/cc-sync/internal/catalog"
	"github.com/yasyf/cc-sync/internal/claudenative"
	"github.com/yasyf/cc-sync/internal/cli"
	"github.com/yasyf/cc-sync/internal/codesnap"
	"github.com/yasyf/cc-sync/internal/orcabridge"
	"github.com/yasyf/cc-sync/internal/sessionarchive"
	"github.com/yasyf/synckit/artifact"
)

type pick struct {
	origin     string
	worktree   catalog.Worktree
	checkpoint catalog.Checkpoint
	session    claudenative.SessionID
}

func selectCheckpoint(snap catalog.Snapshot, target cli.Target, sel cli.CheckpointSelector, allowPartial bool, now time.Time) (pick, error) {
	pk, candidates, err := selectWorktree(snap, target)
	if err != nil {
		return pick{}, err
	}
	if sel == nil {
		sel = cli.LatestCheckpoint{}
	}
	if byID, ok := sel.(cli.CheckpointID); ok {
		return pickByID(pk, candidates, byID.Prefix, allowPartial)
	}
	for _, cp := range candidates {
		if cp.Deferred == "" && matches(cp, sel, now) {
			pk.checkpoint = cp
			return pk, nil
		}
	}
	for _, cp := range candidates {
		if matches(cp, sel, now) {
			return pick{}, partialError(cp)
		}
	}
	return pick{}, fmt.Errorf("%w: no checkpoint of %s matches %s", ErrNotFound, pk.worktree.ID, sel)
}

func selectWorktree(snap catalog.Snapshot, target cli.Target) (pick, []catalog.Checkpoint, error) {
	switch t := target.(type) {
	case cli.ItemRef:
		for _, o := range snap.Origins {
			if o.Origin != t.SourceHostID {
				continue
			}
			for _, wt := range o.Worktrees {
				if wt.ID == t.WorkspaceID {
					return pick{origin: o.Origin, worktree: wt}, wt.Checkpoints, nil
				}
			}
		}
		return pick{}, nil, fmt.Errorf("%w: item %s", ErrNotFound, t)
	case cli.SessionRef:
		return selectBySession(snap, t)
	}
	return pick{}, nil, fmt.Errorf("%w: target %v", ErrNotFound, target)
}

func selectBySession(snap catalog.Snapshot, ref cli.SessionRef) (pick, []catalog.Checkpoint, error) {
	var found pick
	var candidates []catalog.Checkpoint
	ids := map[string]bool{}
	for _, o := range snap.Origins {
		if ref.Source != "" && o.Origin != ref.Source {
			continue
		}
		for _, wt := range o.Worktrees {
			var holding []catalog.Checkpoint
			for _, cp := range wt.Checkpoints {
				hits := sessionMatches(cp.Sessions, ref.ID)
				for _, id := range hits {
					ids[id] = true
				}
				if len(hits) > 0 {
					holding = append(holding, cp)
				}
			}
			if len(holding) > 0 && (candidates == nil || holding[0].CapturedAt.After(candidates[0].CapturedAt)) {
				found, candidates = pick{origin: o.Origin, worktree: wt}, holding
			}
		}
	}
	switch len(ids) {
	case 0:
		return pick{}, nil, fmt.Errorf("%w: session %s", ErrNotFound, ref)
	case 1:
		for id := range ids {
			found.session = claudenative.SessionID(id)
		}
		return found, candidates, nil
	}
	return pick{}, nil, fmt.Errorf("%w: session %s matches %d sessions", ErrAmbiguous, ref, len(ids))
}

func sessionMatches(sessions []catalog.Session, prefix string) []string {
	var ids []string
	for _, s := range sessions {
		if strings.HasPrefix(s.ID, prefix) {
			ids = append(ids, s.ID)
		}
	}
	return ids
}

func pickByID(pk pick, candidates []catalog.Checkpoint, prefix string, allowPartial bool) (pick, error) {
	var hits []catalog.Checkpoint
	for _, cp := range candidates {
		if strings.HasPrefix(cp.ID, prefix) {
			hits = append(hits, cp)
		}
	}
	switch {
	case len(hits) == 0:
		return pick{}, fmt.Errorf("%w: checkpoint %s", ErrNotFound, prefix)
	case len(hits) > 1:
		return pick{}, fmt.Errorf("%w: checkpoint %s matches %d checkpoints", ErrAmbiguous, prefix, len(hits))
	case hits[0].Deferred != "" && !allowPartial:
		return pick{}, partialError(hits[0])
	}
	pk.checkpoint = hits[0]
	return pk, nil
}

func partialError(cp catalog.Checkpoint) error {
	return &NotReadyError{CheckpointID: cp.ID, Missing: []string{"code deferred: " + cp.Deferred}}
}

func matches(cp catalog.Checkpoint, sel cli.CheckpointSelector, now time.Time) bool {
	switch s := sel.(type) {
	case cli.LatestCheckpoint:
		return true
	case cli.CheckpointAt:
		return !cp.CapturedAt.After(s.Time)
	case cli.CheckpointHourly:
		return hasClass(cp, catalog.ClassHourly) && !cp.CapturedAt.After(now.Add(-time.Duration(s.HoursAgo)*time.Hour))
	case cli.CheckpointDaily:
		y, m, d := cp.CapturedAt.In(now.Location()).Date()
		return hasClass(cp, catalog.ClassDaily) && y == s.Year && m == s.Month && d == s.Day
	}
	panic(fmt.Sprintf("pickup: unknown checkpoint selector %T", sel))
}

func hasClass(cp catalog.Checkpoint, c catalog.Class) bool {
	for _, have := range cp.Classes {
		if have == c {
			return true
		}
	}
	return false
}

func selectSessions(resume []string, named claudenative.SessionID, cp catalog.Checkpoint, archived []sessionarchive.Manifest) (map[claudenative.SessionID]bool, error) {
	ids := make([]string, 0, len(archived))
	for _, m := range archived {
		ids = append(ids, m.SessionID)
	}
	selected := map[claudenative.SessionID]bool{}
	for _, want := range resume {
		var hits []string
		for _, id := range ids {
			if strings.HasPrefix(id, want) {
				hits = append(hits, id)
			}
		}
		switch len(hits) {
		case 0:
			return nil, fmt.Errorf("%w: session %s in checkpoint %s", ErrNotFound, want, cp.ID)
		case 1:
			selected[claudenative.SessionID(hits[0])] = true
		default:
			return nil, fmt.Errorf("%w: session %s matches %d sessions", ErrAmbiguous, want, len(hits))
		}
	}
	if len(resume) > 0 {
		return selected, nil
	}
	if named != "" {
		selected[named] = true
		return selected, nil
	}
	if id, ok := mostRecentHuman(cp.Sessions); ok {
		selected[claudenative.SessionID(id)] = true
	}
	return selected, nil
}

func mostRecentHuman(sessions []catalog.Session) (string, bool) {
	var best *catalog.Session
	for i := range sessions {
		s := &sessions[i]
		if best == nil || s.LastHumanActivity.After(best.LastHumanActivity) ||
			(s.LastHumanActivity.Equal(best.LastHumanActivity) && s.LastActivity.After(best.LastActivity)) {
			best = s
		}
	}
	if best == nil {
		return "", false
	}
	return best.ID, true
}

type rootParts struct {
	code       *artifact.Ref
	descriptor *artifact.Ref
	sessions   []sessionarchive.Manifest
}

func readRoot(ctx context.Context, store Store, root artifact.Ref) (rootParts, error) {
	m, err := store.Manifest(ctx, root)
	if err != nil {
		return rootParts{}, fmt.Errorf("read root: %w", err)
	}
	var parts rootParts
	for _, dep := range m.Deps {
		dm, err := store.Manifest(ctx, dep)
		if err != nil {
			return rootParts{}, fmt.Errorf("read root dependency %s: %w", dep.Digest, err)
		}
		switch dm.Media {
		case codesnap.MediaCode:
			if parts.code != nil {
				return rootParts{}, fmt.Errorf("%w: two code groups", ErrInvalid)
			}
			parts.code = &dep
		case orcabridge.MediaDescriptor:
			if parts.descriptor != nil {
				return rootParts{}, fmt.Errorf("%w: two orca descriptors", ErrInvalid)
			}
			parts.descriptor = &dep
		case sessionarchive.MediaSession:
			if len(dm.Deps) == 0 {
				return rootParts{}, fmt.Errorf("%w: session group %s has no manifest", ErrInvalid, dep.Digest)
			}
			sm, err := readSession(ctx, store, dm.Deps[0])
			if err != nil {
				return rootParts{}, err
			}
			parts.sessions = append(parts.sessions, sm)
		default:
			return rootParts{}, fmt.Errorf("%w: root dependency %s has media %q", ErrInvalid, dep.Digest, dm.Media)
		}
	}
	return parts, nil
}

func readSession(ctx context.Context, store Store, ref artifact.Ref) (sessionarchive.Manifest, error) {
	rc, err := store.Open(ctx, ref)
	if err != nil {
		return sessionarchive.Manifest{}, fmt.Errorf("open session manifest %s: %w", ref.Digest, err)
	}
	defer func() { _ = rc.Close() }()
	data, err := io.ReadAll(rc)
	if err != nil {
		return sessionarchive.Manifest{}, fmt.Errorf("read session manifest %s: %w", ref.Digest, err)
	}
	var m sessionarchive.Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return sessionarchive.Manifest{}, fmt.Errorf("decode session manifest %s: %w", ref.Digest, err)
	}
	if _, err := claudenative.ParseSessionID(m.SessionID); err != nil {
		return sessionarchive.Manifest{}, fmt.Errorf("session manifest %s: %w", ref.Digest, err)
	}
	return m, nil
}
