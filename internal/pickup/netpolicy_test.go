package pickup

import (
	"context"
	"errors"
	"os/exec"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/cc-sync/internal/cli"
	"github.com/yasyf/cc-sync/internal/codesnap"
	"github.com/yasyf/reposync/worktree"
	"github.com/yasyf/synckit/artifact"
	"github.com/yasyf/synckit/netpolicy"
)

type fakeNetwork struct {
	mu      sync.Mutex
	state   netpolicy.State
	changed chan struct{}
}

func newFakeNetwork() *fakeNetwork {
	return &fakeNetwork{state: netpolicy.State{Status: netpolicy.StatusConnected}, changed: make(chan struct{})}
}

func (n *fakeNetwork) Current() (netpolicy.State, <-chan struct{}) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state, n.changed
}

func (n *fakeNetwork) Close() error { return nil }

func (n *fakeNetwork) cellular() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.state.Cellular = true
	close(n.changed)
	n.changed = make(chan struct{})
}

type lfsCode struct {
	fakeCode
	local   func()
	fetches chan struct{}
}

func (c *lfsCode) Restore(ctx context.Context, store codesnap.Reader, code artifact.Ref, opts RestoreOptions) (Restored, error) {
	r, err := c.fakeCode.Restore(ctx, store, code, opts)
	if err != nil {
		return Restored{}, err
	}
	c.local()
	fetched, err := fetchLFS(ctx, opts, func(ctx context.Context) error {
		cmd := exec.CommandContext(ctx, "sleep", "30")
		if err := cmd.Start(); err != nil {
			return err
		}
		c.fetches <- struct{}{}
		return cmd.Wait()
	})
	if err != nil {
		return Restored{}, err
	}
	if !fetched {
		r.LFSPending = []string{"big.bin"}
	}
	return r, nil
}

func fetchLFS(ctx context.Context, opts RestoreOptions, fetch func(context.Context) error) (bool, error) {
	err := opts.FetchLFS(ctx, fetch)
	if errors.Is(err, worktree.ErrFetchDeferred) {
		return false, nil
	}
	return err == nil, err
}

func TestPickupGatesLFSFetchOnLivePolicy(t *testing.T) {
	tests := []struct {
		name       string
		duringRest bool
	}{
		{"restricted before the fetch starts", true},
		{"restricted mid-fetch", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := newWorld(t)
			network := newFakeNetwork()
			w.cfg.Network = network
			code := &lfsCode{fakeCode: *w.code, fetches: make(chan struct{}, 1), local: func() {}}
			if tt.duringRest {
				code.local = network.cellular
			}
			w.cfg.Code = code
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			type outcome struct {
				res Result
				err error
			}
			done := make(chan outcome, 1)
			go func() {
				res, err := New(w.cfg).Run(ctx, Request{Target: cli.ItemRef{SourceHostID: "alice", WorkspaceID: "wt-1"}})
				done <- outcome{res, err}
			}()
			if !tt.duringRest {
				select {
				case <-code.fetches:
				case <-time.After(5 * time.Second):
					t.Fatal("LFS fetch never started under an unrestricted network")
				}
				network.cellular()
			}
			var got outcome
			select {
			case got = <-done:
			case <-time.After(time.Second + 200*time.Millisecond):
				cancel()
				t.Fatal("LFS fetch still running 1.2s after the network turned cellular")
			}
			if got.err != nil {
				t.Fatalf("Run: %v, want the restore to complete locally with LFS pending", got.err)
			}
			select {
			case <-code.fetches:
				t.Error("LFS fetch started after the network turned cellular")
			default:
			}
			if want := []string{"big.bin"}; !reflect.DeepEqual(got.res.Checkout.LFSPending, want) {
				t.Errorf("LFSPending = %v, want %v", got.res.Checkout.LFSPending, want)
			}
		})
	}
}
