package orcabridge

import (
	"context"
	"fmt"
	"slices"
)

const (
	// CapabilityWorkspace gates describe/export/import/resume/list/activity.
	CapabilityWorkspace = "cross-machine-recovery.workspace.v1"
	// CapabilityPresentation marks hosts whose exports carry client views.
	CapabilityPresentation = "cross-machine-recovery.presentation.v1"
	// CapabilityRecoveryLaunch marks runtimes whose import accepts
	// --recovery-launch-file.
	CapabilityRecoveryLaunch = "cross-machine-recovery.recovery-launch.v1"

	localTarget          = "local"
	localExecutionHostID = "local"
)

// Status is the `orca status --json` result.
type Status struct {
	Target  Target        `json:"target"`
	App     App           `json:"app"`
	Runtime RuntimeStatus `json:"runtime"`
}

// Target is the runtime the CLI routed to: "local" or "environment".
type Target struct {
	Kind string `json:"kind"`
}

// App describes the Orca desktop process.
type App struct {
	Running bool `json:"running"`
	PID     int  `json:"pid"`
}

// RuntimeStatus describes the runtime the CLI reached.
type RuntimeStatus struct {
	State        string   `json:"state"`
	Reachable    bool     `json:"reachable"`
	RuntimeID    string   `json:"runtimeId"`
	AppVersion   string   `json:"appVersion"`
	Capabilities []string `json:"capabilities"`
}

// HasCapability reports whether the runtime advertises capability.
func (s Status) HasCapability(capability string) bool {
	return slices.Contains(s.Runtime.Capabilities, capability)
}

func (s Status) check() error {
	if s.Target.Kind != localTarget {
		return &UnavailableError{Reason: ReasonNotLocal, Detail: fmt.Sprintf("status target kind %q", s.Target.Kind)}
	}
	if !s.Runtime.Reachable {
		return &UnavailableError{Reason: ReasonNotRunning, Detail: fmt.Sprintf("runtime state %q", s.Runtime.State)}
	}
	if !s.HasCapability(CapabilityWorkspace) {
		return &UnavailableError{Reason: ReasonCapabilityMissing, Detail: fmt.Sprintf("runtime %s lacks %s", s.Runtime.AppVersion, CapabilityWorkspace)}
	}
	return nil
}

// Description is the `orca recovery describe --json` result.
type Description struct {
	Protocol              int      `json:"protocol"`
	RuntimeID             string   `json:"runtimeId"`
	ExecutionHostID       string   `json:"executionHostId"`
	AppVersion            string   `json:"appVersion"`
	Platform              string   `json:"platform"`
	MachineName           string   `json:"machineName"`
	HostKind              string   `json:"hostKind"`
	LocalClientInstanceID string   `json:"localClientInstanceId"`
	Capabilities          []string `json:"capabilities"`
}

// Runtime is a local Orca runtime that passed verification.
type Runtime struct {
	Status      Status
	Description Description
}

// Status runs `orca status --json` without gating on its answer.
func (c *Client) Status(ctx context.Context) (Status, error) {
	var status Status
	if err := c.run(ctx, nil, &status, "status", "--json"); err != nil {
		return Status{}, err
	}
	return status, nil
}

// Verify proves the CLI reaches the local runtime with the recovery
// capability: status must report a reachable local target advertising
// CapabilityWorkspace, and describe must report execution host "local" on the
// same runtime ID.
func (c *Client) Verify(ctx context.Context) (Runtime, error) {
	status, err := c.Status(ctx)
	if err != nil {
		return Runtime{}, err
	}
	if err := status.check(); err != nil {
		return Runtime{}, err
	}
	var desc Description
	if err := c.run(ctx, nil, &desc, "recovery", "describe", "--json"); err != nil {
		return Runtime{}, err
	}
	if desc.ExecutionHostID != localExecutionHostID {
		return Runtime{}, &UnavailableError{Reason: ReasonNotLocal, Detail: fmt.Sprintf("describe executionHostId %q", desc.ExecutionHostID)}
	}
	if desc.RuntimeID != status.Runtime.RuntimeID {
		return Runtime{}, &UnavailableError{Reason: ReasonNotLocal, Detail: fmt.Sprintf("describe runtimeId %q differs from status runtimeId %q", desc.RuntimeID, status.Runtime.RuntimeID)}
	}
	return Runtime{Status: status, Description: desc}, nil
}
