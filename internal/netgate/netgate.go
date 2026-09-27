// Package netgate runs a network fetch under this host's bulk-transfer
// policy: the fetch starts only while the policy allows bulk transfer and is
// cancelled the moment it stops allowing it.
package netgate

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/yasyf/synckit/netpolicy"
)

// Poll is how often Run re-reads the policy between path changes while a
// fetch runs; the monitor surfaces manual override edits only on a read.
const Poll = time.Second

// Run runs fn if monitor allows bulk transfer now, under a context cancelled
// the moment the policy turns restrictive: cellular, expensive, constrained,
// manual metered, disconnected, or unknown. It returns a
// *netpolicy.PausedError when the policy refused or interrupted fn, and fn's
// own result otherwise.
func Run(ctx context.Context, monitor netpolicy.Monitor, fn func(context.Context) error) error {
	state, _ := monitor.Current()
	if paused := blocked(state); paused != nil {
		return paused
	}
	var watcher sync.WaitGroup
	defer watcher.Wait()
	fnCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	watcher.Go(func() { cancelOnRestriction(fnCtx, monitor, cancel) })
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

func cancelOnRestriction(ctx context.Context, monitor netpolicy.Monitor, cancel context.CancelCauseFunc) {
	poll := time.NewTicker(Poll)
	defer poll.Stop()
	for {
		state, changed := monitor.Current()
		if paused := blocked(state); paused != nil {
			cancel(paused)
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-changed:
		case <-poll.C:
		}
	}
}
