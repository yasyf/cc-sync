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

// PickupRequest configures `cc-sync pickup`. Resume lists the session ids to
// resume, empty meaning the default choice; Progress receives every phase.
type PickupRequest struct {
	Target     Target
	Checkpoint CheckpointSelector
	Resume     []string
	NoOrca     bool
	DryRun     bool
	Progress   func(Progress)
}

// ResumeRequest selects the session `cc-sync resume` continues.
type ResumeRequest struct {
	Session SessionRef
}

// UnavailableService fails every operation as unavailable; it stands in until
// the resident service is wired.
type UnavailableService struct{}

func errUnwired(op string) error {
	return Errorf(CodeUnavailable, "%s: cc-sync service is not wired into this build", op)
}

// Install fails as unavailable.
func (UnavailableService) Install(context.Context, InstallRequest) (InstallResult, error) {
	return InstallResult{}, errUnwired("install")
}

// Uninstall fails as unavailable.
func (UnavailableService) Uninstall(context.Context, UninstallRequest) (UninstallResult, error) {
	return UninstallResult{}, errUnwired("uninstall")
}

// List fails as unavailable.
func (UnavailableService) List(context.Context, ListRequest) (ListResult, error) {
	return ListResult{}, errUnwired("list")
}

// Inspect fails as unavailable.
func (UnavailableService) Inspect(context.Context, InspectRequest) (InspectResult, error) {
	return InspectResult{}, errUnwired("inspect")
}

// Status fails as unavailable.
func (UnavailableService) Status(context.Context) (StatusResult, error) {
	return StatusResult{}, errUnwired("status")
}

// Sync fails as unavailable.
func (UnavailableService) Sync(context.Context, SyncRequest) (SyncResult, error) {
	return SyncResult{}, errUnwired("sync")
}

// Pickup fails as unavailable.
func (UnavailableService) Pickup(context.Context, PickupRequest) (PickupResult, error) {
	return PickupResult{}, errUnwired("pickup")
}

// Resume fails as unavailable.
func (UnavailableService) Resume(context.Context, ResumeRequest) (ResumeResult, error) {
	return ResumeResult{}, errUnwired("resume")
}

// HelperServe fails as unavailable.
func (UnavailableService) HelperServe(context.Context) error {
	return errUnwired("helper-serve")
}
