package orcabridge

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Selector names an Orca worktree: "id:<worktreeId>" or "path:<absolute path>".
type Selector string

// ByID selects a worktree by its Orca worktree ID.
func ByID(worktreeID string) Selector {
	return Selector("id:" + worktreeID)
}

// ByPath selects a worktree by its absolute checkout path.
func ByPath(path string) Selector {
	return Selector("path:" + path)
}

// PathMapping rewrites a source-machine path prefix to its destination.
type PathMapping struct {
	From string
	To   string
}

// ImportRequest is one `orca recovery import`; Descriptor is passed on stdin verbatim.
type ImportRequest struct {
	Descriptor   []byte
	Checkout     string
	CheckpointID string
	PathMap      []PathMapping
	Resume       []string
	PreferClient string
	Activate     bool
	RegisterRepo bool
	DryRun       bool
}

// ImportResult is the `orca recovery import --json` result.
type ImportResult struct {
	ImportKey          string             `json:"importKey"`
	Disposition        string             `json:"disposition"`
	RepoID             string             `json:"repoId"`
	WorktreeID         string             `json:"worktreeId"`
	InstanceID         string             `json:"instanceId"`
	PresentationSource PresentationSource `json:"presentationSource"`
	IDMap              IDMap              `json:"idMap"`
	Bindings           []ImportedBinding  `json:"bindings"`
	Provenance         json.RawMessage    `json:"provenance"`
}

// PresentationSource names the layout an import applied: "client-view" (with ClientKey) or "host-layout".
type PresentationSource struct {
	Kind      string `json:"kind"`
	ClientKey string `json:"clientKey"`
}

// IDMap maps each source id to the id the import regenerated for it.
type IDMap struct {
	Tabs     map[string]string `json:"tabs"`
	Groups   map[string]string `json:"groups"`
	Leaves   map[string]string `json:"leaves"`
	Browsers map[string]string `json:"browsers"`
}

// ImportedBinding reports one agent binding's outcome: "dormant", "resumed", or "refused".
type ImportedBinding struct {
	SourcePaneKey     string `json:"sourcePaneKey"`
	LocalPaneKey      string `json:"localPaneKey"`
	ProviderSessionID string `json:"providerSessionId"`
	Status            string `json:"status"`
	Reason            string `json:"reason"`
	TerminalHandle    string `json:"terminalHandle"`
}

// ResumeResult is the `orca recovery resume --json` result; Disposition is "created" or "adopted".
type ResumeResult struct {
	TerminalHandle string `json:"terminalHandle"`
	Disposition    string `json:"disposition"`
	LocalPaneKey   string `json:"localPaneKey"`
}

// DormantBinding is a recovered agent session awaiting resume.
type DormantBinding struct {
	LocalPaneKey    string          `json:"localPaneKey"`
	WorktreeID      string          `json:"worktreeId"`
	ProviderSession ProviderSession `json:"providerSession"`
	Provenance      json.RawMessage `json:"provenance"`
}

// ProviderSession identifies an agent's own session.
type ProviderSession struct {
	Key            string `json:"key"`
	ID             string `json:"id"`
	TranscriptPath string `json:"transcriptPath"`
}

// WorkspaceActivity is the newest human input and focus Orca saw on a
// worktree; a zero time means never.
type WorkspaceActivity struct {
	WorktreeID       string
	Path             string
	LastHumanInputAt time.Time
	LastHumanFocusAt time.Time
}

// Export returns the compact recovery descriptor for the worktree checked out at path.
func (c *Client) Export(ctx context.Context, worktreePath string) ([]byte, error) {
	if _, err := c.Verify(ctx); err != nil {
		return nil, err
	}
	var result struct {
		Descriptor json.RawMessage `json:"descriptor"`
	}
	if err := c.run(ctx, nil, &result, "recovery", "export", "--worktree", string(ByPath(worktreePath)), "--json"); err != nil {
		return nil, err
	}
	var descriptor bytes.Buffer
	if err := json.Compact(&descriptor, result.Descriptor); err != nil {
		return nil, fmt.Errorf("orca recovery export: compact descriptor: %w", err)
	}
	if descriptor.Len() > MaxDescriptorBytes {
		return nil, descriptorTooLarge(descriptor.Len())
	}
	return descriptor.Bytes(), nil
}

// Import applies a descriptor to an existing checkout on the local runtime.
func (c *Client) Import(ctx context.Context, req ImportRequest) (ImportResult, error) {
	if len(req.Descriptor) > MaxDescriptorBytes {
		return ImportResult{}, descriptorTooLarge(len(req.Descriptor))
	}
	args, err := importArgs(req)
	if err != nil {
		return ImportResult{}, err
	}
	if _, err := c.Verify(ctx); err != nil {
		return ImportResult{}, err
	}
	var result ImportResult
	if err := c.run(ctx, req.Descriptor, &result, args...); err != nil {
		return ImportResult{}, err
	}
	return result, nil
}

func importArgs(req ImportRequest) ([]string, error) {
	args := []string{"recovery", "import", "--descriptor", "-", "--checkout", req.Checkout, "--checkpoint", req.CheckpointID}
	for _, m := range req.PathMap {
		if strings.Contains(m.From, "=") {
			return nil, fmt.Errorf("orca recovery import: path-map source %q contains '='", m.From)
		}
		args = append(args, "--path-map", m.From+"="+m.To)
	}
	for _, id := range req.Resume {
		args = append(args, "--resume", id)
	}
	if req.PreferClient != "" {
		args = append(args, "--prefer-client", req.PreferClient)
	}
	if req.Activate {
		args = append(args, "--activate")
	}
	if req.RegisterRepo {
		args = append(args, "--register-repo")
	}
	if req.DryRun {
		args = append(args, "--dry-run")
	}
	return append(args, "--json"), nil
}

// Resume launches one dormant recovered session in worktree; focus brings its pane forward.
func (c *Client) Resume(ctx context.Context, worktree Selector, providerSessionID string, focus bool) (ResumeResult, error) {
	if _, err := c.Verify(ctx); err != nil {
		return ResumeResult{}, err
	}
	args := []string{"recovery", "resume", "--worktree", string(worktree), "--session", providerSessionID}
	if focus {
		args = append(args, "--focus")
	}
	var result ResumeResult
	if err := c.run(ctx, nil, &result, append(args, "--json")...); err != nil {
		return ResumeResult{}, err
	}
	return result, nil
}

// List returns dormant recovered bindings, scoped to worktree unless it is empty.
func (c *Client) List(ctx context.Context, worktree Selector) ([]DormantBinding, error) {
	if _, err := c.Verify(ctx); err != nil {
		return nil, err
	}
	args := []string{"recovery", "list"}
	if worktree != "" {
		args = append(args, "--worktree", string(worktree))
	}
	var result struct {
		Bindings []DormantBinding `json:"bindings"`
	}
	if err := c.run(ctx, nil, &result, append(args, "--json")...); err != nil {
		return nil, err
	}
	return result.Bindings, nil
}

// Activity returns per-worktree human input and focus times from Orca's presentation store.
func (c *Client) Activity(ctx context.Context) ([]WorkspaceActivity, error) {
	if _, err := c.Verify(ctx); err != nil {
		return nil, err
	}
	var result struct {
		Workspaces []struct {
			WorktreeID       string `json:"worktreeId"`
			Path             string `json:"path"`
			LastHumanInputAt *int64 `json:"lastHumanInputAt"`
			LastHumanFocusAt *int64 `json:"lastHumanFocusAt"`
		} `json:"workspaces"`
	}
	if err := c.run(ctx, nil, &result, "recovery", "activity", "--json"); err != nil {
		return nil, err
	}
	activity := make([]WorkspaceActivity, 0, len(result.Workspaces))
	for _, w := range result.Workspaces {
		activity = append(activity, WorkspaceActivity{
			WorktreeID:       w.WorktreeID,
			Path:             w.Path,
			LastHumanInputAt: unixMilli(w.LastHumanInputAt),
			LastHumanFocusAt: unixMilli(w.LastHumanFocusAt),
		})
	}
	return activity, nil
}

func unixMilli(ms *int64) time.Time {
	if ms == nil {
		return time.Time{}
	}
	return time.UnixMilli(*ms).UTC()
}

func descriptorTooLarge(size int) error {
	return &RefusedError{Code: CodeDescriptorTooLarge, Message: fmt.Sprintf("descriptor is %d bytes, cap %d", size, MaxDescriptorBytes)}
}
