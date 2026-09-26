package orcabridge

import "fmt"

// Reason names why the local Orca runtime cannot serve cross-machine recovery.
type Reason string

// Reasons an UnavailableError carries.
const (
	ReasonNotInstalled      Reason = "not-installed"
	ReasonNotRunning        Reason = "not-running"
	ReasonCapabilityMissing Reason = "capability-missing"
	ReasonNotLocal          Reason = "not-local"
)

// UnavailableError reports that Orca cannot serve recovery on this machine:
// capture proceeds without a layout and pickup proceeds CLI-only.
type UnavailableError struct {
	Reason Reason
	Detail string
}

func (e *UnavailableError) Error() string {
	return fmt.Sprintf("orca unavailable (%s): %s", e.Reason, e.Detail)
}

// Orca error codes a RefusedError carries.
const (
	CodeRepoUnregistered    = "recovery_repo_unregistered"
	CodeCheckoutMissing     = "recovery_checkout_missing"
	CodeDestinationNotEmpty = "recovery_destination_not_empty"
	CodeDescriptorInvalid   = "recovery_descriptor_invalid"
	CodeDescriptorTooLarge  = "recovery_descriptor_too_large"
	CodeSessionLiveLocally  = "recovery_session_live_locally"
	CodeBindingNotFound     = "recovery_binding_not_found"
	CodeSelectorNotFound    = "selector_not_found"
	CodeRuntimeAccessDenied = "runtime_access_denied"
	CodeInvalidArgument     = "invalid_argument"
)

// RefusedError carries the Orca error code of a declined recovery call.
type RefusedError struct {
	Code    string
	Message string
}

func (e *RefusedError) Error() string {
	return fmt.Sprintf("orca refused (%s): %s", e.Code, e.Message)
}

var unavailableCodes = map[string]Reason{
	"recovery_local_only":  ReasonNotLocal,
	"recovery_unsupported": ReasonCapabilityMissing,
	"method_not_found":     ReasonCapabilityMissing,
	"incompatible_runtime": ReasonCapabilityMissing,
	"runtime_unavailable":  ReasonNotRunning,
	"runtime_timeout":      ReasonNotRunning,
}

func errorFromEnvelope(verb string, e envelopeError) error {
	if reason, ok := unavailableCodes[e.Code]; ok {
		return &UnavailableError{Reason: reason, Detail: fmt.Sprintf("orca %s: %s: %s", verb, e.Code, e.Message)}
	}
	return &RefusedError{Code: e.Code, Message: e.Message}
}
