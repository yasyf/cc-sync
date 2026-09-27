package pickup

import (
	"context"
	"fmt"
	"time"

	"github.com/yasyf/cc-sync/internal/claudenative"
	"github.com/yasyf/cc-sync/internal/sessionrestore"
)

// Native is the SessionRestorer over sessionrestore. It probes the
// destination claude through Run once, on the first Prepare, and moves
// divergent local copies a replace displaces below DisplacedRoot.
type Native struct {
	Run           sessionrestore.Runner
	Procs         claudenative.ProcessLister
	DisplacedRoot string
	Now           func() time.Time
	caps          *sessionrestore.Capabilities
}

// Prepare plans the native install of replica into t.
func (n *Native) Prepare(ctx context.Context, replica string, t SessionTarget, d Divergence) (PreparedSession, error) {
	if n.caps == nil {
		caps, err := sessionrestore.ProbeCapabilities(ctx, n.Run)
		if err != nil {
			return nil, fmt.Errorf("probe destination claude: %w", err)
		}
		n.caps = &caps
	}
	plan, err := sessionrestore.Prepare(ctx, replica, t, sessionrestore.Options{
		OnDivergence:  d,
		Now:           n.Now(),
		DisplacedRoot: n.DisplacedRoot,
		Procs:         n.Procs,
		Capabilities:  *n.caps,
	})
	if err != nil {
		return nil, err
	}
	return nativeSession{plan: plan}, nil
}

type nativeSession struct {
	plan sessionrestore.Plan
}

func (s nativeSession) Plan() SessionPlan {
	return s.plan
}

func (s nativeSession) Apply(ctx context.Context) error {
	_, err := sessionrestore.Apply(ctx, s.plan)
	return err
}
