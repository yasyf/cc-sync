//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/yasyf/reposync/registry"
	"github.com/yasyf/reposync/worktree"

	"github.com/yasyf/cc-sync/internal/catalog"
	"github.com/yasyf/cc-sync/internal/claudenative"
	"github.com/yasyf/cc-sync/internal/cli"
	"github.com/yasyf/cc-sync/internal/codesnap"
	"github.com/yasyf/cc-sync/internal/config"
	"github.com/yasyf/cc-sync/internal/consumer"
	"github.com/yasyf/cc-sync/internal/pickup"
	"github.com/yasyf/cc-sync/internal/resident"
	"github.com/yasyf/cc-sync/internal/scheduler"
	"github.com/yasyf/cc-sync/internal/service"
	"github.com/yasyf/synckit/artifact"
	"github.com/yasyf/synckit/codec"
	"github.com/yasyf/synckit/delivery"
	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/netpolicy"
)

// ClaudeVersion is the version the fake claude binary reports.
const ClaudeVersion = "2.1.283"

const claudeHelp = `Usage: claude [options] [command] [prompt]

Options:
  -r, --resume [value]              Resume a conversation by session ID
  --append-system-prompt <prompt>   Append a system prompt to the default system prompt
  -h, --help                        Display help for command
`

// ExecCall is one process replacement the CLI asked for; the harness records
// it instead of exec'ing.
type ExecCall struct {
	Argv []string
	Dir  string
	Env  []string
}

// CLIResult is one in-process `cc-sync ... --json` run.
type CLIResult struct {
	Code   int
	Stdout []byte
	Stderr string
}

// Decode unmarshals the JSON stdout into v.
func (r CLIResult) Decode(v any) error {
	if err := json.Unmarshal(r.Stdout, v); err != nil {
		return fmt.Errorf("decode cc-sync output (exit %d, stderr %q): %w: %s", r.Code, r.Stderr, err, r.Stdout)
	}
	return nil
}

// CLI runs the real cc-sync command tree and service in-process against
// this host with --json appended.
func (h *Host) CLI(args ...string) CLIResult {
	h.t.Helper()
	var stdout, stderr bytes.Buffer
	code := cli.Execute(service.New(h.serviceConfig()), append(args, "--json"), &stdout, &stderr)
	return CLIResult{Code: code, Stdout: stdout.Bytes(), Stderr: stderr.String()}
}

// PickupOutput is the pickup JSON a scenario asserts on; Raw keeps the
// whole document.
type PickupOutput struct {
	Checkpoint struct {
		ID      string `json:"id"`
		Partial bool   `json:"partial"`
	} `json:"checkpoint"`
	Checkout struct {
		Path        string   `json:"path"`
		Reused      bool     `json:"reused"`
		Newer       bool     `json:"newer"`
		Exact       bool     `json:"exact"`
		Differences []string `json:"differences"`
	} `json:"checkout"`
	Sessions []map[string]any `json:"sessions"`
	Orca     map[string]any   `json:"orca"`
	Raw      []byte           `json:"-"`
}

// Pickup runs `cc-sync pickup <selector> <flags...> --json` and decodes the
// result, failing the test on a non-zero exit.
func (h *Host) Pickup(selector string, flags ...string) PickupOutput {
	h.t.Helper()
	res := h.CLI(append([]string{"pickup", selector}, flags...)...)
	if res.Code != 0 {
		h.t.Fatalf("%s: pickup %s exited %d: %s%s", h.Name, selector, res.Code, res.Stdout, res.Stderr)
	}
	out := PickupOutput{Raw: res.Stdout}
	if err := res.Decode(&out); err != nil {
		h.t.Fatal(err)
	}
	return out
}

// Execs lists the process replacements the CLI requested.
func (h *Host) Execs() []ExecCall {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]ExecCall(nil), h.execs...)
}

func (h *Host) serviceConfig() service.Config {
	orca, err := h.orcaClient()
	if err != nil {
		h.t.Fatalf("%s: orca client: %v", h.Name, err)
	}
	return service.Config{
		Catalog:    catalog.New(h.Layout.CatalogPath, h.Name, h.Clock.Now),
		Deliveries: noDeliveries{},
		Mesh:       staticMesh{reg: hostregistry.Registry{Self: h.Name, Hosts: h.peers}},
		Network: func(context.Context) (netpolicy.State, error) {
			state, _ := h.Net.Current()
			return state, nil
		},
		Orca: orca,
		Live: func(ctx context.Context) (map[claudenative.SessionID]claudenative.LiveProcess, error) {
			return claudenative.LiveSessions(ctx, h.Claude, h.Procs)
		},
		Sessions: func(ctx context.Context, id claudenative.SessionID) ([]claudenative.Session, error) {
			sessions, _, err := claudenative.Scan(ctx, claudenative.ScanOptions{Layout: h.Claude, Only: []claudenative.SessionID{id}}, claudenative.Cursor{})
			return sessions, err
		},
		Checkouts: service.CheckoutDir{Root: h.Layout.CheckoutRoot},
		Helper:    helperClient{h: h},
		Installer: noInstaller{},
		Picker:    pickup.New(h.pickupConfig()),
		Serve: func(context.Context) error {
			return fmt.Errorf("%w: the simulated resident runs in-process", service.ErrUnavailable)
		},
		Tiers:   func() (cli.CaptureTiers, error) { return captureTiers(h.Layout.ConfigPath) },
		Environ: []string{"HOME=" + h.Home, "PATH=/usr/bin:/bin"},
		Exec: func(argv []string, dir string, env []string) error {
			h.mu.Lock()
			defer h.mu.Unlock()
			h.execs = append(h.execs, ExecCall{Argv: argv, Dir: dir, Env: env})
			return nil
		},
		Now: h.Clock.Now,
	}
}

func (h *Host) pickupConfig() pickup.Config {
	code, err := worktree.OpenStore(h.Layout.CodeStore)
	if err != nil {
		h.t.Fatalf("%s: open code store: %v", h.Name, err)
	}
	orca, err := h.orcaClient()
	if err != nil {
		h.t.Fatalf("%s: orca client: %v", h.Name, err)
	}
	return pickup.Config{
		Catalog:   catalog.New(h.Layout.CatalogPath, h.Name, h.Clock.Now),
		Pinner:    pinner{h: h},
		OpenStore: func(context.Context) (pickup.Store, error) { return artifact.OpenReadOnly(h.Artifacts) },
		Verifier: codeVerifier{code: code, reg: h.Registry, open: func() (codesnap.Reader, func() error, error) {
			r, err := artifact.OpenReadOnly(h.Artifacts)
			if err != nil {
				return nil, nil, err
			}
			return r, func() error { return nil }, nil
		}},
		Code: pickup.Reposync{Worktrees: restorer{code: code, reg: h.Registry}},
		Sessions: &pickup.Native{
			Run:           fakeClaude,
			Procs:         h.Procs,
			DisplacedRoot: filepath.Join(h.Layout.Dir, "displaced"),
			Now:           h.Clock.Now,
		},
		Orca: orca,
		FetchAllowed: func() bool {
			state, _ := h.Net.Current()
			return state.Unrestricted()
		},
		Layout:       h.Claude,
		Home:         h.Home,
		ReplicaRoot:  h.Layout.ReplicaRoot,
		CheckoutRoot: h.Layout.CheckoutRoot,
		Now:          h.Clock.Now,
	}
}

func fakeClaude(_ context.Context, args ...string) ([]byte, error) {
	switch {
	case len(args) == 1 && args[0] == "--version":
		return []byte(ClaudeVersion + " (Claude Code)\n"), nil
	case len(args) == 1 && args[0] == "--help":
		return []byte(claudeHelp), nil
	}
	return nil, fmt.Errorf("fake claude: unexpected argv %q", args)
}

func captureTiers(path string) (cli.CaptureTiers, error) {
	cfg, err := config.Load(path)
	if err != nil {
		return cli.CaptureTiers{}, err
	}
	t := cfg.Capture
	return cli.CaptureTiers{
		HumanInterval:      cli.Duration(t.HumanInterval),
		AutonomousInterval: cli.Duration(t.AutonomousInterval),
		RecentInterval:     cli.Duration(t.RecentInterval),
		IdleInterval:       cli.Duration(t.IdleInterval),
		HumanWindow:        cli.Duration(t.HumanWindow),
		AutonomousWindow:   cli.Duration(t.AutonomousWindow),
		RecentWindow:       cli.Duration(t.RecentWindow),
	}, nil
}

type codeVerifier struct {
	open func() (codesnap.Reader, func() error, error)
	code *worktree.Store
	reg  registry.Registry
}

func (v codeVerifier) VerifyCode(ctx context.Context, root artifact.Ref, fetchOrigin bool) (_ consumer.CodeVerdict, err error) {
	store, closeStore, err := v.open()
	if err != nil {
		return consumer.CodeVerdict{}, err
	}
	defer func() { err = errors.Join(err, closeStore()) }()
	m, err := store.Manifest(ctx, root)
	if err != nil {
		return consumer.CodeVerdict{}, fmt.Errorf("read root %s: %w", root.Digest, err)
	}
	for _, dep := range m.Deps {
		dm, err := store.Manifest(ctx, dep)
		if err != nil {
			return consumer.CodeVerdict{}, fmt.Errorf("read root dependency %s: %w", dep.Digest, err)
		}
		if dm.Media != codesnap.MediaCode {
			continue
		}
		src, err := codesnap.SourceFromManifest(ctx, store, dep)
		if err != nil {
			return consumer.CodeVerdict{}, err
		}
		snap, err := src.Snapshot(ctx)
		if err != nil {
			return consumer.CodeVerdict{}, err
		}
		verdict, err := v.code.Verify(ctx, v.reg, snap, src, worktree.VerifyOptions{FetchOrigin: fetchOrigin})
		if err != nil {
			return consumer.CodeVerdict{}, err
		}
		return consumer.CodeVerdict{Ready: verdict.Ready, Missing: verdict.Missing}, nil
	}
	return consumer.CodeVerdict{Missing: []string{"code group"}}, nil
}

type restorer struct {
	code *worktree.Store
	reg  registry.Registry
}

func (r restorer) Restore(ctx context.Context, snap worktree.Snapshot, src worktree.ArtifactSource, opts pickup.RestoreOptions) (pickup.Restored, error) {
	got, err := r.code.Restore(ctx, r.reg, snap, src, worktree.RestoreOptions{
		Dest: opts.Dest, Branch: opts.Branch, Fresh: opts.Fresh, FetchLFS: opts.FetchLFS, ApplySparse: opts.ApplySparse,
	})
	if err != nil {
		return pickup.Restored{}, err
	}
	return pickup.Restored{
		Path: got.Path, Branch: got.Branch, Head: got.Head, Reused: got.Reused, Applied: got.Applied,
		Newer: got.Newer, LFSPending: got.LFSPending, Exact: got.Exact, Differences: got.Differences, Sparse: got.Sparse,
	}, nil
}

type pinner struct {
	h *Host
}

func (p pinner) Pin(ctx context.Context, owner string, roots []artifact.Ref, ttl time.Duration) error {
	return p.h.Call(ctx, resident.MethodPin, resident.PinRequest{Owner: owner, Roots: roots, TTL: codec.Duration(ttl)}, nil)
}

type helperClient struct {
	h *Host
}

func (c helperClient) Status(ctx context.Context) (service.HelperStatus, error) {
	var reply resident.StatusReply
	if err := c.h.Call(ctx, resident.MethodStatus, struct{}{}, &reply); err != nil {
		return service.HelperStatus{}, fmt.Errorf("%w: %w", service.ErrUnavailable, err)
	}
	return service.HelperStatus{Build: reply.Build, Scheduler: reply.Scheduler}, nil
}

func (c helperClient) Kick(ctx context.Context, sessionIDs []string) ([]scheduler.Attempt, error) {
	var reply resident.KickReply
	if err := c.h.Call(ctx, resident.MethodKick, resident.KickRequest{SessionIDs: sessionIDs}, &reply); err != nil {
		return nil, err
	}
	return reply.Attempts, nil
}

type staticMesh struct {
	reg hostregistry.Registry
}

func (m staticMesh) Load() (*hostregistry.Registry, error) {
	reg := m.reg
	return &reg, nil
}

type noDeliveries struct{}

func (noDeliveries) Status(context.Context, string) ([]delivery.PeerStatus, error) {
	return []delivery.PeerStatus{}, nil
}

type noInstaller struct{}

func (noInstaller) Install(context.Context, cli.InstallRequest) (cli.InstallResult, error) {
	return cli.InstallResult{}, fmt.Errorf("%w: install is not simulated", service.ErrUnavailable)
}

func (noInstaller) Uninstall(context.Context, cli.UninstallRequest) (cli.UninstallResult, error) {
	return cli.UninstallResult{}, fmt.Errorf("%w: uninstall is not simulated", service.ErrUnavailable)
}
