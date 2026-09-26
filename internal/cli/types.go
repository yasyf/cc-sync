package cli

import (
	"encoding/json"
	"time"
)

// Time is a timestamp that encodes as RFC3339 in UTC.
type Time struct{ time.Time }

// MarshalJSON encodes t as an RFC3339 UTC string.
func (t Time) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.UTC().Format(time.RFC3339Nano))
}

// At wraps t as a Time.
func At(t time.Time) Time { return Time{t} }

// AtPtr wraps t as a nullable Time.
func AtPtr(t time.Time) *Time { return &Time{t} }

// Duration is an interval that encodes as a Go duration string, matching
// synckit's codec.
type Duration time.Duration

// MarshalJSON encodes d as its Go duration string.
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.String())
}

// String formats d as a Go duration.
func (d Duration) String() string { return time.Duration(d).String() }

// Array is a slice that encodes nil as [] so every JSON array is present.
type Array[T any] []T

// MarshalJSON encodes a nil Array as [].
func (a Array[T]) MarshalJSON() ([]byte, error) {
	if a == nil {
		return []byte("[]"), nil
	}
	return json.Marshal([]T(a))
}

// NetworkStatus is an endpoint's reported route reachability.
type NetworkStatus string

// NetworkStatus values mirror synckit netpolicy.Status.
const (
	NetworkUnknown      NetworkStatus = "unknown"
	NetworkDisconnected NetworkStatus = "disconnected"
	NetworkConnected    NetworkStatus = "connected"
)

// PauseReason names why bulk transfer to or from a peer is paused.
type PauseReason string

// PauseReason values.
const (
	PauseCellular       PauseReason = "cellular"
	PauseExpensive      PauseReason = "expensive"
	PauseConstrained    PauseReason = "constrained"
	PauseUnknownNetwork PauseReason = "unknown-network"
	PauseDisconnected   PauseReason = "disconnected"
	PauseManualMetered  PauseReason = "manual-metered"
	PausePeerOffline    PauseReason = "peer-offline"
)

// Endpoint names which side of a transfer imposed a pause.
type Endpoint string

// Endpoint values.
const (
	EndpointLocal Endpoint = "local"
	EndpointPeer  Endpoint = "peer"
)

// Activity classifies a session's most recent activity.
type Activity string

// Activity values.
const (
	ActivityHuman      Activity = "human"
	ActivityAutonomous Activity = "autonomous"
	ActivityIdle       Activity = "idle"
)

// Tier is a checkpoint's retention class.
type Tier string

// Tier values.
const (
	TierLatest Tier = "latest"
	TierHourly Tier = "hourly"
	TierDaily  Tier = "daily"
)

// TranscriptState is how much of a checkpoint's session transcripts is present.
type TranscriptState string

// TranscriptState values.
const (
	TranscriptComplete TranscriptState = "complete"
	TranscriptPartial  TranscriptState = "partial"
	TranscriptMissing  TranscriptState = "missing"
)

// CodeState is the state of a checkpoint's code snapshot.
type CodeState string

// CodeState values.
const (
	CodeComplete CodeState = "complete"
	CodeDeferred CodeState = "deferred"
	CodeMissing  CodeState = "missing"
	CodeNone     CodeState = "none"
)

// LayoutState is the fidelity of a checkpoint's Orca layout.
type LayoutState string

// LayoutState values.
const (
	LayoutClientView LayoutState = "client-view"
	LayoutHostOnly   LayoutState = "host-only"
	LayoutNone       LayoutState = "none"
)

// OrcaKind is the kind of Orca workspace a checkout belongs to.
type OrcaKind string

// OrcaKind values.
const (
	OrcaWorktree OrcaKind = "worktree"
	OrcaFolder   OrcaKind = "folder"
)

// DeliveryState mirrors synckit delivery.State for one peer.
type DeliveryState string

// DeliveryState values.
const (
	DeliveryIdle         DeliveryState = "idle"
	DeliveryStaged       DeliveryState = "staged"
	DeliveryTransferring DeliveryState = "transferring"
	DeliveryApplying     DeliveryState = "applying"
	DeliveryPaused       DeliveryState = "paused"
	DeliveryBackoff      DeliveryState = "backoff"
)

// SessionStatus is how pickup left a session.
type SessionStatus string

// SessionStatus values.
const (
	SessionResumed  SessionStatus = "resumed"
	SessionDormant  SessionStatus = "dormant"
	SessionRestored SessionStatus = "restored"
)

// Phase is one pickup step reported by --progress ndjson.
type Phase string

// Phase values, in pickup order.
const (
	PhaseSelect          Phase = "select"
	PhaseRestoreCode     Phase = "restore-code"
	PhaseRestoreSessions Phase = "restore-sessions"
	PhaseOrcaImport      Phase = "orca-import"
	PhaseOrcaResume      Phase = "orca-resume"
)

// Host identifies a synckit host.
type Host struct {
	HostID   string `json:"host_id"`
	HostName string `json:"host_name"`
}

// Pause is an active transfer pause.
type Pause struct {
	Reason   PauseReason `json:"reason"`
	Endpoint Endpoint    `json:"endpoint"`
	Since    Time        `json:"since"`
}

// Helper reports the resident helper process.
type Helper struct {
	Running bool   `json:"running"`
	Build   string `json:"build"`
}

// Network is an endpoint's network policy state.
type Network struct {
	Status        NetworkStatus `json:"status"`
	Expensive     bool          `json:"expensive"`
	Constrained   bool          `json:"constrained"`
	Cellular      bool          `json:"cellular"`
	ManualMetered bool          `json:"manual_metered"`
}

// LocalHost is this host and its network state.
type LocalHost struct {
	Host
	Network Network `json:"network"`
}

// Peer is one mesh peer's delivery status.
type Peer struct {
	Host
	Reachable       bool    `json:"reachable"`
	LastSeenAt      *Time   `json:"last_seen_at"`
	AckedRevision   *uint64 `json:"acked_revision"`
	PendingRevision *uint64 `json:"pending_revision"`
	PendingSince    *Time   `json:"pending_since"`
	Pause           *Pause  `json:"pause"`
}

// QueuedByTier counts queued captures per scheduler tier.
type QueuedByTier struct {
	Human      int `json:"human"`
	Autonomous int `json:"autonomous"`
	Recent     int `json:"recent"`
	Idle       int `json:"idle"`
}

// CaptureTiers is the effective capture cadence: a worktree whose most urgent
// session had human, autonomous, or any activity within that tier's window is
// captured at the tier's interval, and at IdleInterval otherwise.
type CaptureTiers struct {
	HumanInterval      Duration `json:"human_interval"`
	AutonomousInterval Duration `json:"autonomous_interval"`
	RecentInterval     Duration `json:"recent_interval"`
	IdleInterval       Duration `json:"idle_interval"`
	HumanWindow        Duration `json:"human_window"`
	AutonomousWindow   Duration `json:"autonomous_window"`
	RecentWindow       Duration `json:"recent_window"`
}

// Scheduler reports the capture scheduler.
type Scheduler struct {
	QueuedByTier QueuedByTier `json:"queued_by_tier"`
	Workers      int          `json:"workers"`
	LastRoundAt  *Time        `json:"last_round_at"`
	Tiers        CaptureTiers `json:"tiers"`
}

// StatusResult is the payload of `cc-sync status`.
type StatusResult struct {
	Helper    Helper      `json:"helper"`
	Local     LocalHost   `json:"local"`
	Peers     Array[Peer] `json:"peers"`
	Scheduler Scheduler   `json:"scheduler"`
}

// Source is the host that captured an item.
type Source struct {
	Host
	LastSeenAt *Time `json:"last_seen_at"`
	Reachable  bool  `json:"reachable"`
}

// OrcaWorkspace is the Orca workspace a source checkout belonged to.
type OrcaWorkspace struct {
	Kind       OrcaKind `json:"kind"`
	Name       string   `json:"name"`
	InstanceID string   `json:"instance_id"`
}

// Workspace is the source worktree an item captures.
type Workspace struct {
	ID         string         `json:"id"`
	RepoName   string         `json:"repo_name"`
	RepoOrigin *string        `json:"repo_origin"`
	Branch     *string        `json:"branch"`
	SourcePath string         `json:"source_path"`
	Orca       *OrcaWorkspace `json:"orca"`
}

// Session is one Claude session captured in an item.
type Session struct {
	SessionID           string   `json:"session_id"`
	Title               string   `json:"title"`
	LastActivityAt      *Time    `json:"last_activity_at"`
	LastHumanActivityAt *Time    `json:"last_human_activity_at"`
	Activity            Activity `json:"activity"`
	BoundInOrca         bool     `json:"bound_in_orca"`
	LiveLocalCollision  bool     `json:"live_local_collision"`
}

// Checkpoint identifies one captured checkpoint.
type Checkpoint struct {
	ID               string `json:"id"`
	Tier             Tier   `json:"tier"`
	CapturedAt       Time   `json:"captured_at"`
	SourceActivityAt *Time  `json:"source_activity_at"`
}

// Completeness is a checkpoint's local readiness; Ready is never true while
// anything required is missing, deferred, partial, or omitted.
type Completeness struct {
	Ready      bool            `json:"ready"`
	Missing    Array[string]   `json:"missing"`
	Transcript TranscriptState `json:"transcript"`
	Code       CodeState       `json:"code"`
	Layout     LayoutState     `json:"layout"`
}

// LocalCheckout is an existing local checkout pickup could reuse.
type LocalCheckout struct {
	Path     string `json:"path"`
	Reusable bool   `json:"reusable"`
}

// Item is one recoverable worktree and its chosen checkpoint. Its selector is
// derived from Source.HostID and Workspace.ID when encoded.
type Item struct {
	Source          Source         `json:"source"`
	Workspace       Workspace      `json:"workspace"`
	Sessions        Array[Session] `json:"sessions"`
	Checkpoint      Checkpoint     `json:"checkpoint"`
	CheckpointCount int            `json:"checkpoint_count"`
	Completeness    Completeness   `json:"completeness"`
	Pause           *Pause         `json:"pause"`
	LocalCheckout   *LocalCheckout `json:"local_checkout"`
}

// Ref is the item's stable selector.
func (i Item) Ref() ItemRef {
	return ItemRef{SourceHostID: i.Source.HostID, WorkspaceID: i.Workspace.ID}
}

// ListResult is the payload of `cc-sync list`.
type ListResult struct {
	GeneratedAt Time
	Local       Host
	Items       []Item
}

// CheckpointDetail is one retained checkpoint of an inspected item.
type CheckpointDetail struct {
	ID         string        `json:"id"`
	Tier       Tier          `json:"tier"`
	CapturedAt Time          `json:"captured_at"`
	Ready      bool          `json:"ready"`
	Missing    Array[string] `json:"missing"`
	Deferred   Array[string] `json:"deferred"`
}

// Delivery is the delivery state toward one peer.
type Delivery struct {
	Peer  string        `json:"peer"`
	State DeliveryState `json:"state"`
	Pause *Pause        `json:"pause"`
}

// InspectResult is the payload of `cc-sync inspect`.
type InspectResult struct {
	Item
	Checkpoints Array[CheckpointDetail] `json:"checkpoints"`
	Delivery    Array[Delivery]         `json:"delivery"`
}

// PickupCheckout is the checkout pickup restored or reused.
type PickupCheckout struct {
	Path   string  `json:"path"`
	Branch *string `json:"branch"`
	Reused bool    `json:"reused"`
}

// PickedSession is one session's pickup outcome.
type PickedSession struct {
	SessionID string        `json:"session_id"`
	Status    SessionStatus `json:"status"`
}

// ResumedTab is a session Orca resumed into a tab.
type ResumedTab struct {
	SessionID string `json:"session_id"`
	TabID     string `json:"tab_id"`
}

type localExecutionHost struct{}

func (localExecutionHost) MarshalJSON() ([]byte, error) { return []byte(`"local"`), nil }

// OrcaPickup is the Orca import pickup performed; it always targets the local
// execution host.
type OrcaPickup struct {
	ExecutionHostID localExecutionHost `json:"execution_host_id"`
	WorktreeID      string             `json:"worktree_id"`
	Resumed         Array[ResumedTab]  `json:"resumed"`
	Dormant         Array[string]      `json:"dormant"`
}

// PickupResult is the payload of `cc-sync pickup`.
type PickupResult struct {
	Checkout PickupCheckout       `json:"checkout"`
	Sessions Array[PickedSession] `json:"sessions"`
	Orca     *OrcaPickup          `json:"orca"`
}

// Progress is one --progress ndjson line.
type Progress struct {
	Phase Phase `json:"phase"`
	Done  *int  `json:"done,omitempty"`
	Total *int  `json:"total,omitempty"`
}

// SyncedWorktree is one worktree a sync captured, with the checkpoint it now
// offers and why a fresh capture was deferred.
type SyncedWorktree struct {
	WorkspaceID string        `json:"workspace_id"`
	Sessions    Array[string] `json:"sessions"`
	Checkpoint  *Checkpoint   `json:"checkpoint"`
	Deferred    Array[string] `json:"deferred"`
}

// SyncResult is the payload of `cc-sync sync`.
type SyncResult struct {
	Worktrees Array[SyncedWorktree] `json:"worktrees"`
}

// InstallResult is the payload of `cc-sync install`.
type InstallResult struct {
	ConfigDir string `json:"config_dir"`
	Synckitd  bool   `json:"synckitd"`
	Helper    Helper `json:"helper"`
}

// UninstallResult is the payload of `cc-sync uninstall`.
type UninstallResult struct {
	Purged bool `json:"purged"`
}

// ResumeResult is the payload of `cc-sync resume` when the resumed Claude
// process returns control.
type ResumeResult struct {
	SessionID string `json:"session_id"`
	Cwd       string `json:"cwd"`
}
