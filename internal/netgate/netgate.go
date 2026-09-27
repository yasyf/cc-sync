// Package netgate runs a network fetch under this host's bulk-transfer
// policy: the fetch starts only while the policy allows bulk transfer and is
// cancelled the moment it stops allowing it.
package netgate

import (
	"context"
	"errors"
	"sync"

	"github.com/yasyf/synckit/netpolicy"
)

// Run runs fn if monitor allows bulk transfer now, under a context cancelled
// the moment the policy turns restrictive: cellular, expensive, constrained,
// manual metered, disconnected, or unknown. A restriction that began and
// cleared between two reads of monitor cancels fn too. It returns a
// *netpolicy.PausedError when the policy refused or interrupted fn, and fn's
// own result otherwise.
func Run(ctx context.Context, monitor netpolicy.Monitor, fn func(context.Context) error) error {
	start, _ := monitor.Current()
	if paused := blocked(start); paused != nil {
		return paused
	}
	var watcher sync.WaitGroup
	defer watcher.Wait()
	fnCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	watcher.Go(func() { cancelOnRestriction(fnCtx, monitor, start.RestrictedEpoch, cancel) })
	err := fn(fnCtx)
	var paused *netpolicy.PausedError
	if err != nil && errors.As(context.Cause(fnCtx), &paused) {
		return paused
	}
	return err
}

func blocked(state netpolicy.State) *netpolicy.PausedError {
	if verdict := netpolicy.Evaluate(state, netpolicy.State{Status: netpolicy.StatusConnected}); !verdict.Allowed {
		return &netpolicy.PausedError{Reason: verdict.Reason}
	}
	return nil
}

func cancelOnRestriction(ctx context.Context, monitor netpolicy.Monitor, epoch uint64, cancel context.CancelCauseFunc) {
	for {
		state, changed := monitor.Current()
		if paused := interrupted(state, epoch); paused != nil {
			cancel(paused)
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-changed:
		}
	}
}

func interrupted(state netpolicy.State, epoch uint64) *netpolicy.PausedError {
	if paused := blocked(state); paused != nil {
		return paused
	}
	if state.RestrictedEpoch != epoch {
		return &netpolicy.PausedError{Reason: "local: restricted mid-fetch"}
	}
	return nil
}
