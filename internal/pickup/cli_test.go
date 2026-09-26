package pickup

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/yasyf/cc-sync/internal/cli"
	"github.com/yasyf/cc-sync/internal/orcabridge"
	"github.com/yasyf/reposync/worktree"
)

func TestClassify(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want cli.Code
	}{
		{"not ready", fmt.Errorf("open: %w", &NotReadyError{CheckpointID: "cp", Missing: []string{"code"}}), cli.CodeNotReady},
		{"live local", fmt.Errorf("prepare: %w", ErrLiveLocal), cli.CodeLiveLocalCollision},
		{"divergent", fmt.Errorf("prepare: %w", &DivergentLocalError{SessionID: sidHuman}), cli.CodeDivergentLocalCopy},
		{"incompatible", &IncompatibleError{Capability: "--resume", Detail: "absent"}, cli.CodeIncompatible},
		{"orca not local", &orcabridge.UnavailableError{Reason: orcabridge.ReasonNotLocal}, cli.CodeOrcaNotLocal},
		{"orca unavailable", &orcabridge.UnavailableError{Reason: orcabridge.ReasonNotRunning}, cli.CodeOrcaUnavailable},
		{"orca live locally", fmt.Errorf("orca import: %w", &orcabridge.RefusedError{Code: orcabridge.CodeSessionLiveLocally}), cli.CodeLiveLocalCollision},
		{"orca destination not empty", &orcabridge.RefusedError{Code: orcabridge.CodeDestinationNotEmpty}, cli.CodeCheckoutConflict},
		{"orca binding ambiguous", &orcabridge.RefusedError{Code: orcabridge.CodeBindingAmbiguous}, cli.CodeUnsupported},
		{"checkout conflict", fmt.Errorf("restore: %w", ErrCheckoutConflict), cli.CodeCheckoutConflict},
		{"not found", fmt.Errorf("%w: item x", ErrNotFound), cli.CodeNotFound},
		{"ambiguous", fmt.Errorf("%w: session 0f", ErrAmbiguous), cli.CodeUsage},
		{"cancelled", fmt.Errorf("restore: %w", context.Canceled), cli.CodeCancelled},
		{"coded", cli.Errorf(cli.CodeUnavailable, "pin: %w", ErrNotFound), cli.CodeUnavailable},
		{"internal", errors.New("boom"), cli.CodeInternal},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := cli.Classify(classify(tt.err)); got != tt.want {
				t.Errorf("code = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestClassifyDivergenceDetails(t *testing.T) {
	local, picked := time.Date(2026, 9, 26, 15, 0, 0, 0, time.UTC), time.Date(2026, 9, 26, 14, 0, 0, 0, time.UTC)
	err := classify(&DivergentLocalError{SessionID: sidHuman, LocalLastActivity: local, PickedCapturedAt: picked})
	e, ok := errors.AsType[*cli.Error](err)
	want := cli.DivergenceDetails{SessionID: sidHuman, LocalLastActivityAt: cli.Time{Time: local}, PickedCapturedAt: cli.Time{Time: picked}}
	if !ok || e.Details != want {
		t.Errorf("details = %+v, want %+v", e, want)
	}
}

func TestResultCLI(t *testing.T) {
	at := time.Date(2026, 9, 26, 14, 5, 0, 0, time.UTC)
	res := Result{
		Checkpoint: Checkpoint{ID: "cp-mixed", CapturedAt: at, Partial: true, CodeDeferred: "missing-lfs"},
		Checkout: Checkout{
			Path: "/co/x", Branch: "recovery/x", Newer: true, LFSPending: []string{"a.bin"},
			Differences: []string{"sparse checkout expanded to full"},
			Sparse:      &worktree.Sparse{Cone: true, Patterns: []string{"/web/"}}, SparseExpanded: true,
		},
		Sessions: []Session{
			{SessionID: sidFork, Status: StatusRestored, Selected: true, Launch: &Launch{Argv: []string{"claude", "--resume", sidFork}, Dir: "/co/x"}, ForkedFrom: sidHuman},
			{SessionID: sidIdle, Status: StatusRefused, Reason: ReasonLiveLocal},
		},
		Orca: &OrcaResult{ExecutionHostID: "local", WorktreeID: "wt-9", Resumed: []ResumedTab{{SessionID: sidFork, TabID: "tab-1"}}, Dormant: []string{}},
	}
	want := cli.PickupResult{
		Checkpoint: cli.PickupCheckpoint{ID: "cp-mixed", CapturedAt: cli.Time{Time: at}, Partial: true, CodeDeferred: "missing-lfs"},
		Checkout: cli.PickupCheckout{
			Path: "/co/x", Branch: new("recovery/x"), Newer: true, LFSPending: []string{"a.bin"},
			Differences: []string{"sparse checkout expanded to full"},
			Sparse:      &cli.SparseCheckout{Cone: true, Patterns: []string{"/web/"}, Expanded: true},
		},
		Sessions: cli.Array[cli.PickedSession]{
			{SessionID: sidFork, Status: cli.SessionRestored, Selected: true, Launch: &cli.Launch{Argv: []string{"claude", "--resume", sidFork}, Dir: "/co/x"}, ForkedFrom: new(sidHuman)},
			{SessionID: sidIdle, Status: cli.SessionRefused, Reason: cli.CodeLiveLocalCollision},
		},
		Orca: &cli.OrcaPickup{WorktreeID: "wt-9", Resumed: cli.Array[cli.ResumedTab]{{SessionID: sidFork, TabID: "tab-1"}}, Dormant: []string{}},
	}
	if got := res.CLI(); !reflect.DeepEqual(got, want) {
		t.Errorf("CLI() = %+v\nwant %+v", got, want)
	}
}

func TestApplySparse(t *testing.T) {
	sparse := &worktree.Sparse{Cone: true, Patterns: []string{"/web/"}}
	tests := []struct {
		name         string
		apply        bool
		wantExpanded bool
	}{
		{"default expands", false, true},
		{"apply keeps sparse", true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := newWorld(t)
			w.code.restored = Restored{Branch: "recovery/x", Sparse: sparse}
			res, err := w.run(Request{ApplySparse: tt.apply, NoOrca: true})
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			if w.code.opts[0].ApplySparse != tt.apply || res.Checkout.Sparse != sparse || res.Checkout.SparseExpanded != tt.wantExpanded {
				t.Errorf("opts %+v checkout %+v, want ApplySparse %v and expanded %v", w.code.opts[0], res.Checkout, tt.apply, tt.wantExpanded)
			}
		})
	}
}
