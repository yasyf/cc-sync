package pickup

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/yasyf/cc-sync/internal/catalog"
	"github.com/yasyf/cc-sync/internal/claudenative"
	"github.com/yasyf/cc-sync/internal/codesnap"
	"github.com/yasyf/cc-sync/internal/orcabridge"
	"github.com/yasyf/cc-sync/internal/replica"
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

// RestoreOptions mirrors reposync's worktree.RestoreOptions.
type RestoreOptions struct {
	Dest     string
	Branch   string
	Fresh    bool
	FetchLFS bool
}

// Restored mirrors reposync's worktree.Restored.
type Restored struct {
	Path        string
	Branch      string
	Head        string
	Reused      bool
	Applied     string
	Newer       bool
	LFSPending  []string
	Exact       bool
	Differences []string
}

// SessionRestorer plans the native install of one session replica without
// writing native state; the returned PreparedSession applies it.
type SessionRestorer interface {
	Prepare(ctx context.Context, replica string, t SessionTarget, d Divergence) (PreparedSession, error)
}

// PreparedSession is one planned session install.
type PreparedSession interface {
	Plan() SessionPlan
	Apply(ctx context.Context) error
}

// Orca imports a recovery descriptor into the local Orca runtime.
type Orca interface {
	Import(ctx context.Context, req orcabridge.ImportRequest) (orcabridge.ImportResult, error)
}

// PathRule mirrors sessionrestore.PathRule.
type PathRule struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// PathMap mirrors sessionrestore.PathMap.
type PathMap []PathRule

// SessionTarget mirrors sessionrestore.Target: the destination layout and
// home, the recovered cwd, and the checkout roots the code restore mapped.
type SessionTarget struct {
	Layout    claudenative.Layout
	Home      string
	Cwd       string
	Checkouts PathMap
}

// Divergence mirrors sessionrestore.Divergence, the --on-divergence choice.
type Divergence string

// Divergence choices; the zero value refuses.
const (
	DivergenceRefuse    Divergence = "refuse"
	DivergenceKeepLocal Divergence = "keep-local"
	DivergenceReplace   Divergence = "replace"
	DivergenceFork      Divergence = "fork"
)

// SessionMode mirrors sessionrestore.Mode, how the picked history lands.
type SessionMode string

// SessionMode values.
const (
	ModeFresh       SessionMode = "fresh"
	ModeFastForward SessionMode = "fast-forward"
	ModeReplace     SessionMode = "replace"
	ModeFork        SessionMode = "fork"
	ModeKeepLocal   SessionMode = "keep-local"
)

// Launch mirrors sessionrestore.Launch: how to resume an installed session.
type Launch struct {
	Argv     []string          `json:"argv"`
	Dir      string            `json:"dir"`
	EnvUnset []string          `json:"env_unset"`
	EnvSet   map[string]string `json:"env_set"`
}

// SessionPlan is what pickup reads from a prepared session: its local id
// (new only for a fork), its source id, the install mode, and its launch.
type SessionPlan struct {
	SessionID       claudenative.SessionID
	SourceSessionID claudenative.SessionID
	Mode            SessionMode
	Launch          Launch
}

// ErrLiveLocal mirrors sessionrestore.ErrLiveLocal: the session runs on this host.
var ErrLiveLocal = errors.New("session is live locally")

// DivergentLocalError mirrors sessionrestore.DivergentLocalError: a local
// copy holds records the picked checkpoint lacks.
type DivergentLocalError struct {
	SessionID         claudenative.SessionID
	LocalPath         string
	LocalLeafUUID     string
	LocalLastActivity time.Time
	PickedLeafUUID    string
	PickedCapturedAt  time.Time
}

func (e *DivergentLocalError) Error() string {
	return fmt.Sprintf("session %s: local copy %s diverges from the picked checkpoint (local leaf %s at %s, picked leaf %s captured %s)",
		e.SessionID, e.LocalPath, e.LocalLeafUUID, e.LocalLastActivity.Format(time.RFC3339),
		e.PickedLeafUUID, e.PickedCapturedAt.Format(time.RFC3339))
}

// IncompatibleError mirrors sessionrestore.IncompatibleError: a concrete
// capability or storage format the destination lacks.
type IncompatibleError struct {
	Capability string
	Detail     string
}

func (e *IncompatibleError) Error() string {
	return fmt.Sprintf("incompatible destination: %s: %s", e.Capability, e.Detail)
}

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

// Checkpoint is the checkpoint pickup restored. Partial marks a mixed
// checkpoint whose code is older than its sessions; CodeDeferred says why.
type Checkpoint struct {
	ID           string    `json:"id"`
	CapturedAt   time.Time `json:"captured_at"`
	Partial      bool      `json:"partial"`
	CodeDeferred string    `json:"code_deferred,omitempty"`
}

// Checkout is the checkout pickup restored or reused. Newer reports a reused
// sibling left at an older snapshot than the one picked.
type Checkout struct {
	Path       string   `json:"path"`
	Branch     string   `json:"branch"`
	Reused     bool     `json:"reused"`
	Newer      bool     `json:"newer"`
	LFSPending []string `json:"lfs_pending,omitempty"`
}

// Session is one session's outcome. Launch is set for every installed
// session Orca did not resume; ForkedFrom names the source id of a fork.
type Session struct {
	SessionID  string  `json:"session_id"`
	Status     Status  `json:"status"`
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
