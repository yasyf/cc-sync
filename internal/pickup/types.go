package pickup

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/yasyf/cc-sync/internal/catalog"
	"github.com/yasyf/cc-sync/internal/codesnap"
	"github.com/yasyf/cc-sync/internal/orcabridge"
	"github.com/yasyf/cc-sync/internal/replica"
	"github.com/yasyf/cc-sync/internal/sessionrestore"
	"github.com/yasyf/reposync/worktree"
	"github.com/yasyf/synckit/artifact"
)

// Catalog is the durable catalog pickup selects from.
type Catalog interface {
	Load() (catalog.Snapshot, error)
}

// Pinner pins roots in the resident's artifact store through the helper's
// ccsync.pin.v1 method so GC cannot race a pickup; empty roots unpin owner.
type Pinner interface {
	Pin(ctx context.Context, owner string, roots []artifact.Ref, ttl time.Duration) error
}

// Store is the read-only view of the artifact store pickup reads through.
type Store interface {
	codesnap.Reader
	replica.Store
}

// CodeRestorer restores a checkpoint's code group as a recovery checkout and
// removes one pickup abandons. Restore reports a destination or path clash
// as an error wrapping ErrCheckoutConflict.
type CodeRestorer interface {
	Restore(ctx context.Context, store codesnap.Reader, code artifact.Ref, opts RestoreOptions) (Restored, error)
	Remove(ctx context.Context, r Restored) error
}

// RestoreOptions places a restored snapshot. ApplySparse re-applies the
// source's sparse-checkout patterns in a new recovery worktree instead of
// expanding it to a full checkout.
type RestoreOptions = worktree.RestoreOptions

// Restored describes a recovery checkout. Sparse holds the source's
// sparse-checkout patterns, set whenever the source worktree was sparse.
type Restored = worktree.Restored

// SessionRestorer plans the native install of one session replica without
// writing native state; the returned PreparedSession applies it.
type SessionRestorer interface {
	Prepare(ctx context.Context, replica string, t SessionTarget, d Divergence) (PreparedSession, error)
}

// PreparedSession is one planned session install; its Plan names the local
// id (new only for a fork), the source id, the install mode, the recovery
// context, and the launch.
type PreparedSession interface {
	Plan() SessionPlan
	Apply(ctx context.Context) error
}

// Orca imports a recovery descriptor into the local Orca runtime.
type Orca interface {
	Import(ctx context.Context, req orcabridge.ImportRequest) (orcabridge.ImportResult, error)
}

// PathRule maps one source path prefix to its destination.
type PathRule = sessionrestore.PathRule

// PathMap relocates source-layout paths.
type PathMap = sessionrestore.PathMap

// SessionTarget is where a session lands: layout, home, cwd, and checkout roots.
type SessionTarget = sessionrestore.Target

// Divergence is the --on-divergence choice.
type Divergence = sessionrestore.Divergence

// SessionMode is how the picked history lands.
type SessionMode = sessionrestore.Mode

// Launch is how to resume an installed session natively.
type Launch = sessionrestore.Launch

// SessionPlan is a prepared session install as sessionrestore planned it.
type SessionPlan = sessionrestore.Plan

// DivergentLocalError reports a local copy holding records the picked checkpoint lacks.
type DivergentLocalError = sessionrestore.DivergentLocalError

// IncompatibleError names a capability or format the destination claude lacks.
type IncompatibleError = sessionrestore.IncompatibleError

// Divergence choices; the zero value refuses.
const (
	DivergenceRefuse    = sessionrestore.DivergenceRefuse
	DivergenceKeepLocal = sessionrestore.DivergenceKeepLocal
	DivergenceReplace   = sessionrestore.DivergenceReplace
	DivergenceFork      = sessionrestore.DivergenceFork
)

// SessionMode values.
const (
	ModeFresh       = sessionrestore.ModeFresh
	ModeFastForward = sessionrestore.ModeFastForward
	ModeReplace     = sessionrestore.ModeReplace
	ModeFork        = sessionrestore.ModeFork
	ModeKeepLocal   = sessionrestore.ModeKeepLocal
)

// ErrLiveLocal reports a session running on this host; pickup never overrides it.
var ErrLiveLocal = sessionrestore.ErrLiveLocal

// Pickup errors callers branch on.
var (
	ErrNotFound         = errors.New("pickup: not found")
	ErrAmbiguous        = errors.New("pickup: ambiguous selector")
	ErrCheckoutConflict = errors.New("pickup: checkout conflict")
	ErrInvalid          = errors.New("pickup: invalid checkpoint")
)

// NotReadyError reports a checkpoint this host cannot pick up yet, naming
// what is missing.
type NotReadyError struct {
	CheckpointID string
	Missing      []string
}

func (e *NotReadyError) Error() string {
	return fmt.Sprintf("checkpoint %s is not ready: missing %v", e.CheckpointID, e.Missing)
}

// Status is how pickup left a session.
type Status string

// Status values.
const (
	StatusResumed  Status = "resumed"
	StatusDormant  Status = "dormant"
	StatusRestored Status = "restored"
	StatusRefused  Status = "refused"
)

// Reasons a session is refused, matching the pickup error codes.
const (
	ReasonLiveLocal    = "live-local-collision"
	ReasonDivergent    = "divergent-local-copy"
	ReasonIncompatible = "incompatible"
)

// Result is the payload of `cc-sync pickup`.
type Result struct {
	Checkpoint Checkpoint  `json:"checkpoint"`
	Checkout   Checkout    `json:"checkout"`
	Sessions   []Session   `json:"sessions"`
	Orca       *OrcaResult `json:"orca"`
}

// Checkpoint is the checkpoint pickup restored. Partial marks a checkpoint
// that is not a complete recovery point; CodeDeferred says why a mixed one's
// code is older than its sessions.
type Checkpoint struct {
	ID           string    `json:"id"`
	CapturedAt   time.Time `json:"captured_at"`
	Partial      bool      `json:"partial"`
	CodeDeferred string    `json:"code_deferred,omitempty"`
}

// Checkout is the checkout pickup restored or reused. Newer reports a reused
// sibling left at an older snapshot than the one picked; Sparse holds the
// source's sparse-checkout patterns, and SparseExpanded reports that pickup
// expanded them to a full checkout instead of re-applying them.
type Checkout struct {
	Path           string           `json:"path"`
	Branch         string           `json:"branch"`
	Reused         bool             `json:"reused"`
	Newer          bool             `json:"newer"`
	LFSPending     []string         `json:"lfs_pending,omitempty"`
	Exact          bool             `json:"exact"`
	Differences    []string         `json:"differences,omitempty"`
	Sparse         *worktree.Sparse `json:"sparse,omitempty"`
	SparseExpanded bool             `json:"sparse_expanded,omitempty"`
}

// Session is one session's outcome. Selected marks a session pickup was
// asked to resume; Launch is set for every installed session Orca did not
// resume; ForkedFrom names the source id of a fork.
type Session struct {
	SessionID  string  `json:"session_id"`
	Status     Status  `json:"status"`
	Selected   bool    `json:"selected"`
	Reason     string  `json:"reason,omitempty"`
	Launch     *Launch `json:"launch"`
	ForkedFrom string  `json:"forked_from,omitempty"`
}

// OrcaResult is the Orca import pickup performed on the local runtime.
type OrcaResult struct {
	ExecutionHostID string       `json:"execution_host_id"`
	WorktreeID      string       `json:"worktree_id"`
	Resumed         []ResumedTab `json:"resumed"`
	Dormant         []string     `json:"dormant"`
}

// ResumedTab is a session Orca resumed into a tab.
type ResumedTab struct {
	SessionID string `json:"session_id"`
	TabID     string `json:"tab_id"`
}
