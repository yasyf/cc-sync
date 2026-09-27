package resident

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/yasyf/reposync/registry"
	"github.com/yasyf/reposync/worktree"

	"github.com/yasyf/cc-sync/internal/capture"
	"github.com/yasyf/cc-sync/internal/claudenative"
	"github.com/yasyf/cc-sync/internal/config"
	"github.com/yasyf/cc-sync/internal/consumer"
	"github.com/yasyf/cc-sync/internal/inventory"
	"github.com/yasyf/cc-sync/internal/orcabridge"
	"github.com/yasyf/synckit/artifact"
	"github.com/yasyf/synckit/syncservice"
)

// ArtifactRoot is cc-sync's synckit artifact store root.
func ArtifactRoot() (string, error) {
	return artifact.ServiceRoot(consumer.ServiceID)
}

// OpenArtifacts opens the owning artifact store at ArtifactRoot.
func OpenArtifacts() (*artifact.Store, error) {
	root, err := ArtifactRoot()
	if err != nil {
		return nil, err
	}
	return artifact.Open(root)
}

// SystemDeps wires Serve to this host: the mesh identity, the artifact store
// at ArtifactRoot, the live network monitor, synckit's artifact consumer
// registration, and BuildPipeline.
func SystemDeps(layout config.Layout) (Deps[*artifact.Store], error) {
	self, err := MeshSelf()
	if err != nil {
		return Deps[*artifact.Store]{}, err
	}
	return Deps[*artifact.Store]{
		Layout:        layout,
		Self:          self,
		Now:           time.Now,
		OpenArtifacts: OpenArtifacts,
		Monitor:       SystemMonitor,
		Register:      syncservice.RegisterArtifactConsumer,
		Pipeline:      BuildPipeline,
	}, nil
}

// BuildPipeline builds the concrete capture pipeline over c: an inventory of
// native Claude sessions in registered reposync worktrees, one capture.Job
// that stamps, captures, and expires partial-capture pins, and the reposync
// code verifier.
func BuildPipeline(c Capture[*artifact.Store]) (Pipeline, error) {
	claude, err := claudenative.DefaultLayout()
	if err != nil {
		return Pipeline{}, fmt.Errorf("resident: claude layout: %w", err)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return Pipeline{}, fmt.Errorf("resident: resolve home: %w", err)
	}
	inv, err := inventory.New(inventory.Config{
		Layout:     claude,
		Processes:  claudenative.SystemProcesses(),
		Worktrees:  inventory.RegisteredWorktrees,
		Orca:       systemOrca{},
		CursorPath: filepath.Join(c.Layout.Dir, "scan-cursor.json"),
	})
	if err != nil {
		return Pipeline{}, fmt.Errorf("resident: inventory: %w", err)
	}
	job := capture.New(capture.Config{
		Self:      c.Self,
		Layout:    claude,
		Home:      home,
		Store:     c.Artifacts,
		Code:      c.Code,
		Stamper:   capture.StampFunc(worktree.Stamp),
		Orca:      systemOrca{},
		Catalog:   c.Catalog,
		Publisher: c.Publisher,
		Targets:   inv,
		CodeIndex: c.Layout.CodeIndex,
		StateDir:  filepath.Join(c.Layout.Dir, "capture"),
		Tiers:     c.Config.Capture.Scheduler(),
	})
	return Pipeline{
		Inventory: inv,
		Stamper:   job,
		Capturer:  job,
		Verifier:  consumer.Reposync{Store: c.Artifacts, Code: c.Code, Registry: registry.Load},
		Expirer:   job,
	}, nil
}

type systemOrca struct{}

func (systemOrca) Export(ctx context.Context, worktreePath string) ([]byte, error) {
	c, err := orcabridge.New(orcabridge.Options{})
	if err != nil {
		return nil, err
	}
	return c.Export(ctx, worktreePath)
}

func (systemOrca) Activity(ctx context.Context) ([]orcabridge.WorkspaceActivity, error) {
	c, err := orcabridge.New(orcabridge.Options{})
	if err != nil {
		return nil, err
	}
	return c.Activity(ctx)
}
