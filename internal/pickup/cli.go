package pickup

import (
	"context"
	"errors"

	"github.com/yasyf/cc-sync/internal/cli"
	"github.com/yasyf/cc-sync/internal/orcabridge"
)

// Pickup runs the `cc-sync pickup` request req, reporting each failure under
// its cli code.
func (p *Pickup) Pickup(ctx context.Context, req cli.PickupRequest) (cli.PickupResult, error) {
	res, err := p.Run(ctx, Request{
		Target:       req.Target,
		Checkpoint:   req.Checkpoint,
		AllowPartial: req.AllowPartial,
		ApplySparse:  req.ApplySparse,
		Resume:       req.Resume,
		OnDivergence: Divergence(req.OnDivergence),
		NoOrca:       req.NoOrca,
		DryRun:       req.DryRun,
		Progress:     req.Progress,
	})
	if err != nil {
		return cli.PickupResult{}, classify(err)
	}
	return res.CLI(), nil
}

// CLI is res as the `cc-sync pickup --json` payload.
func (res Result) CLI() cli.PickupResult {
	out := cli.PickupResult{
		Checkpoint: cli.PickupCheckpoint{
			ID:           res.Checkpoint.ID,
			CapturedAt:   cli.Time{Time: res.Checkpoint.CapturedAt},
			Partial:      res.Checkpoint.Partial,
			CodeDeferred: res.Checkpoint.CodeDeferred,
		},
		Checkout: cli.PickupCheckout{
			Path:        res.Checkout.Path,
			Branch:      optional(res.Checkout.Branch),
			Reused:      res.Checkout.Reused,
			Newer:       res.Checkout.Newer,
			LFSPending:  res.Checkout.LFSPending,
			Exact:       res.Checkout.Exact,
			Differences: res.Checkout.Differences,
		},
		Sessions: make(cli.Array[cli.PickedSession], 0, len(res.Sessions)),
	}
	if s := res.Checkout.Sparse; s != nil {
		out.Checkout.Sparse = &cli.SparseCheckout{Cone: s.Cone, Patterns: s.Patterns, Expanded: res.Checkout.SparseExpanded}
	}
	for _, s := range res.Sessions {
		ps := cli.PickedSession{
			SessionID:  s.SessionID,
			Status:     cli.SessionStatus(s.Status),
			Selected:   s.Selected,
			Reason:     cli.Code(s.Reason),
			ForkedFrom: optional(s.ForkedFrom),
		}
		if l := s.Launch; l != nil {
			ps.Launch = &cli.Launch{Argv: l.Argv, Dir: l.Dir, EnvUnset: l.EnvUnset, EnvSet: l.EnvSet}
		}
		out.Sessions = append(out.Sessions, ps)
	}
	if o := res.Orca; o != nil {
		out.Orca = &cli.OrcaPickup{WorktreeID: o.WorktreeID, Resumed: make(cli.Array[cli.ResumedTab], 0, len(o.Resumed)), Dormant: o.Dormant}
		for _, t := range o.Resumed {
			out.Orca.Resumed = append(out.Orca.Resumed, cli.ResumedTab{SessionID: t.SessionID, TabID: t.TabID})
		}
	}
	return out
}

func optional(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func classify(err error) error {
	if div, ok := errors.AsType[*DivergentLocalError](err); ok {
		return cli.DivergentLocalCopy(cli.DivergenceDetails{
			SessionID:           string(div.SessionID),
			LocalLastActivityAt: cli.Time{Time: div.LocalLastActivity},
			PickedCapturedAt:    cli.Time{Time: div.PickedCapturedAt},
		}, err)
	}
	if code, ok := codeOf(err); ok {
		return &cli.Error{Code: code, Err: err}
	}
	return err
}

func codeOf(err error) (cli.Code, bool) {
	if _, ok := errors.AsType[*cli.Error](err); ok {
		return "", false
	}
	if _, ok := errors.AsType[*NotReadyError](err); ok {
		return cli.CodeNotReady, true
	}
	if _, ok := errors.AsType[*IncompatibleError](err); ok {
		return cli.CodeIncompatible, true
	}
	if u, ok := errors.AsType[*orcabridge.UnavailableError](err); ok {
		if u.Reason == orcabridge.ReasonNotLocal {
			return cli.CodeOrcaNotLocal, true
		}
		return cli.CodeOrcaUnavailable, true
	}
	if r, ok := errors.AsType[*orcabridge.RefusedError](err); ok {
		switch r.Code {
		case orcabridge.CodeSessionLiveLocally:
			return cli.CodeLiveLocalCollision, true
		case orcabridge.CodeDestinationNotEmpty:
			return cli.CodeCheckoutConflict, true
		case orcabridge.CodeBindingAmbiguous:
			return cli.CodeUnsupported, true
		}
	}
	switch {
	case errors.Is(err, ErrLiveLocal):
		return cli.CodeLiveLocalCollision, true
	case errors.Is(err, ErrCheckoutConflict):
		return cli.CodeCheckoutConflict, true
	case errors.Is(err, ErrNotFound):
		return cli.CodeNotFound, true
	case errors.Is(err, ErrAmbiguous):
		return cli.CodeUsage, true
	case errors.Is(err, context.Canceled):
		return cli.CodeCancelled, true
	}
	return "", false
}
