package service

import (
	"context"
	"fmt"
	"strings"

	"github.com/yasyf/cc-sync/internal/claudenative"
	"github.com/yasyf/cc-sync/internal/cli"
	"github.com/yasyf/synckit/hostregistry"
)

// Resume replaces the process with `claude --resume <id>` in the directory a
// pickup restored the session into, refusing a session live on this host.
// It returns only when Exec does.
func (s *Service) Resume(ctx context.Context, req cli.ResumeRequest) (cli.ResumeResult, error) {
	id, err := s.resolveSession(req.Session)
	if err != nil {
		return cli.ResumeResult{}, err
	}
	live, err := s.cfg.Live(ctx)
	if err != nil {
		return cli.ResumeResult{}, fmt.Errorf("live sessions: %w", err)
	}
	if p, ok := live[id]; ok {
		return cli.ResumeResult{}, cli.Errorf(cli.CodeLiveLocalCollision, "session %s is live on this host (pid %d in %s)", id, p.PID, p.Cwd)
	}
	copies, err := s.cfg.Sessions(ctx, id)
	if err != nil {
		return cli.ResumeResult{}, fmt.Errorf("find local session %s: %w", id, err)
	}
	switch len(copies) {
	case 0:
		return cli.ResumeResult{}, cli.Errorf(cli.CodeNotFound, "session %s is not restored on this host; run cc-sync pickup first", id)
	case 1:
	default:
		paths := make([]string, 0, len(copies))
		for _, c := range copies {
			paths = append(paths, c.TranscriptPath)
		}
		return cli.ResumeResult{}, cli.Errorf(cli.CodeCheckoutConflict, "session %s has %d local copies: %s", id, len(copies), strings.Join(paths, ", "))
	}
	cwd := copies[0].Cwd
	if err := s.cfg.Exec([]string{"claude", "--resume", string(id)}, cwd, s.cfg.Environ); err != nil {
		return cli.ResumeResult{}, fmt.Errorf("resume %s: %w", id, err)
	}
	return cli.ResumeResult{SessionID: string(id), Cwd: cwd}, nil
}

func (s *Service) resolveSession(ref cli.SessionRef) (claudenative.SessionID, error) {
	if id, err := claudenative.ParseSessionID(ref.ID); err == nil {
		return id, nil
	}
	snap, err := s.cfg.Catalog.Load()
	if err != nil {
		return "", fmt.Errorf("load catalog: %w", err)
	}
	ids := map[string]bool{}
	for _, o := range snap.Origins {
		if ref.Source != "" && ref.Source != o.Origin && ref.Source != hostregistry.HostNode(o.Origin) {
			continue
		}
		for _, w := range o.Worktrees {
			for _, cp := range w.Checkpoints {
				for _, sess := range cp.Sessions {
					if strings.HasPrefix(sess.ID, ref.ID) {
						ids[sess.ID] = true
					}
				}
			}
		}
	}
	switch len(ids) {
	case 0:
		return "", cli.Errorf(cli.CodeNotFound, "no captured session %s", ref)
	case 1:
		for id := range ids {
			return claudenative.ParseSessionID(id)
		}
	}
	return "", cli.Errorf(cli.CodeUsage, "session %s is ambiguous across %d sessions", ref, len(ids))
}
