package orcabridge

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
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

// SessionMapping renames a descriptor binding's provider session From to the
// local id To that a forked pickup installed it under.
type SessionMapping struct {
	From string
	To   string
}

// MediaDescriptor is the artifact media of a stored Orca recovery descriptor.
const MediaDescriptor = "cc-sync.orca-descriptor"

// MaxAppendSystemPromptBytes caps one RecoveryLaunch.AppendSystemPrompt.
const MaxAppendSystemPromptBytes = 16 << 10

// AgentClaude is the RecoveryBindingKey agent of a Claude Code session.
const AgentClaude = "claude"

// RecoveryBindingKey identifies one agent binding: agent, key kind
// ("session_id" or "conversation_id"), provider id, and, for agents keyed by
// transcript, the transcript path.
type RecoveryBindingKey struct {
	Agent          string `json:"agent"`
	Key            string `json:"key"`
	ID             string `json:"id"`
	TranscriptPath string `json:"transcriptPath,omitempty"`
}

// BindingSelector is the bare-id form of Orca's RecoveryBindingSelector: it
// matches the one binding whose provider session id it is, and Orca refuses
// it with CodeBindingAmbiguous when several bindings share that id.
type BindingSelector string

// OmittedBinding is a descriptor binding Orca left out of the export, with why.
type OmittedBinding struct {
	Agent  string `json:"agent"`
	Key    string `json:"key"`
	ID     string `json:"id"`
	Reason string `json:"reason"`
}

// RecoveryLaunch extends the resume launch of one imported session;
// AppendSystemPrompt becomes claude's --append-system-prompt.
type RecoveryLaunch struct {
	AppendSystemPrompt string `json:"appendSystemPrompt"`
}

// ImportRequest is one `orca recovery import`; Descriptor is passed on stdin
// verbatim. Resume selects by local session id, SessionIDMap renames forked
// sessions, and RecoveryLaunch, keyed by source session id, reaches Orca only
// when its runtime advertises CapabilityRecoveryLaunch.
type ImportRequest struct {
	Descriptor     []byte
	Checkout       string
	CheckpointID   string
	PathMap        []PathMapping
	Resume         []BindingSelector
	SessionIDMap   []SessionMapping
	RecoveryLaunch map[string]RecoveryLaunch
	PreferClient   string
	Activate       bool
	RegisterRepo   bool
	DryRun         bool
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
	SourcePaneKey  string             `json:"sourcePaneKey"`
	LocalPaneKey   string             `json:"localPaneKey"`
	Binding        RecoveryBindingKey `json:"binding"`
	Status         string             `json:"status"`
	Reason         string             `json:"reason"`
	TerminalHandle string             `json:"terminalHandle"`
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

// Omitted returns the bindings descriptor lists as left out of its export.
func Omitted(descriptor []byte) ([]OmittedBinding, error) {
	var d struct {
		OmittedBindings *[]OmittedBinding `json:"omittedBindings"`
	}
	if err := json.Unmarshal(descriptor, &d); err != nil {
		return nil, fmt.Errorf("decode orca descriptor: %w", err)
	}
	if d.OmittedBindings == nil {
		return nil, &RefusedError{Code: CodeDescriptorInvalid, Message: "descriptor lacks omittedBindings"}
	}
	return *d.OmittedBindings, nil
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
	rt, err := c.Verify(ctx)
	if err != nil {
		return ImportResult{}, err
	}
	if len(req.RecoveryLaunch) > 0 && slices.Contains(rt.Description.Capabilities, CapabilityRecoveryLaunch) {
		path, err := writeRecoveryLaunch(req.RecoveryLaunch)
		if err != nil {
			return ImportResult{}, err
		}
		defer func() { _ = os.Remove(path) }()
		args = slices.Insert(args, len(args)-1, "--recovery-launch-file", path)
	}
	var result ImportResult
	if err := c.run(ctx, req.Descriptor, &result, args...); err != nil {
		return ImportResult{}, err
	}
	return result, nil
}

func writeRecoveryLaunch(launch map[string]RecoveryLaunch) (path string, err error) {
	data, err := json.Marshal(launch)
	if err != nil {
		return "", fmt.Errorf("encode recovery launch: %w", err)
	}
	f, err := os.CreateTemp("", "cc-sync-recovery-launch-*.json")
	if err != nil {
		return "", fmt.Errorf("create recovery launch file: %w", err)
	}
	defer func() {
		err = errors.Join(err, f.Close())
		if err != nil {
			_ = os.Remove(f.Name())
		}
	}()
	if _, err := f.Write(data); err != nil {
		return "", fmt.Errorf("write recovery launch file: %w", err)
	}
	return f.Name(), nil
}

func importArgs(req ImportRequest) ([]string, error) {
	for sid, l := range req.RecoveryLaunch {
		if len(l.AppendSystemPrompt) > MaxAppendSystemPromptBytes {
			return nil, fmt.Errorf("orca recovery import: session %s append-system-prompt is %d bytes, cap %d", sid, len(l.AppendSystemPrompt), MaxAppendSystemPromptBytes)
		}
	}
	args := []string{"recovery", "import", "--descriptor", "-", "--checkout", req.Checkout, "--checkpoint", req.CheckpointID}
	for _, m := range req.PathMap {
		if strings.Contains(m.From, "=") {
			return nil, fmt.Errorf("orca recovery import: path-map source %q contains '='", m.From)
		}
		args = append(args, "--path-map", m.From+"="+m.To)
	}
	for _, sel := range req.Resume {
		args = append(args, "--resume", string(sel))
	}
	for _, m := range req.SessionIDMap {
		args = append(args, "--session-map", m.From+"="+m.To)
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

// Resume launches the dormant recovered session binding selects in
// worktree; focus brings its pane forward.
func (c *Client) Resume(ctx context.Context, worktree Selector, binding BindingSelector, focus bool) (ResumeResult, error) {
	if _, err := c.Verify(ctx); err != nil {
		return ResumeResult{}, err
	}
	args := []string{"recovery", "resume", "--worktree", string(worktree), "--session", string(binding)}
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
