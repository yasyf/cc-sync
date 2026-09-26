package cli

import "context"

// Service performs cc-sync's operations; the command tree only parses input
// and renders results. Failures return an *Error to pick the reported code.
type Service interface {
	Install(ctx context.Context, req InstallRequest) (InstallResult, error)
	Uninstall(ctx context.Context, req UninstallRequest) (UninstallResult, error)
	List(ctx context.Context, req ListRequest) (ListResult, error)
	Inspect(ctx context.Context, req InspectRequest) (InspectResult, error)
	Status(ctx context.Context) (StatusResult, error)
	Sync(ctx context.Context, req SyncRequest) (SyncResult, error)
	Pickup(ctx context.Context, req PickupRequest) (PickupResult, error)
	Resume(ctx context.Context, req ResumeRequest) (ResumeResult, error)
	HelperServe(ctx context.Context) error
}

// InstallRequest configures `cc-sync install`.
type InstallRequest struct {
	NoSynckitd bool
}

// UninstallRequest configures `cc-sync uninstall`.
type UninstallRequest struct {
	Purge bool
}

// ListRequest filters `cc-sync list`; empty Source and Repo match everything.
type ListRequest struct {
	Source string
	Repo   string
	All    bool
}

// InspectRequest selects the item and checkpoint `cc-sync inspect` reports.
type InspectRequest struct {
	Target     Target
	Checkpoint CheckpointSelector
}

// SyncRequest names the sessions `cc-sync sync` captures; empty means all.
type SyncRequest struct {
	Sessions []string
}

// PickupRequest configures `cc-sync pickup`. AllowPartial admits an
// explicitly selected partial checkpoint; ApplySparse keeps a sparse source's
// recovery checkout sparse. Resume lists the session ids to resume, empty
// meaning the default choice; OnDivergence resolves a divergent local copy of
// a picked session; Progress receives every phase.
type PickupRequest struct {
	Target       Target
	Checkpoint   CheckpointSelector
	AllowPartial bool
	ApplySparse  bool
	Resume       []string
	OnDivergence Divergence
	NoOrca       bool
	DryRun       bool
	Progress     func(Progress)
}

// ResumeRequest selects the session `cc-sync resume` continues.
type ResumeRequest struct {
	Session SessionRef
}
