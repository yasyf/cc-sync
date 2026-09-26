// Package consumer serves cc-sync's checkpoint catalog to synckit as an
// artifact consumer: one stamp watch item, snapshot exports whose roots are
// every ready checkpoint, and fenced applies that acknowledge a change only
// once every checkpoint in it is ready on this host.
package consumer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"github.com/yasyf/cc-sync/internal/catalog"
	"github.com/yasyf/synckit/artifact"
	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/syncservice"
)

// ServiceID is cc-sync's synckit service name.
const ServiceID = "cc-sync"

// WatchItemID names the single item List reports.
const WatchItemID = "checkpoints"

// PinOwner owns the pins on every retained checkpoint root.
const PinOwner = "cc-sync/catalog"

// DeferredFetch marks a checkpoint whose code could not verify while network
// policy withheld the origin fetch.
const DeferredFetch = "origin-fetch-paused"

const declaration = "payload:{identity:cc-sync-catalog-v1,version:1,exporter:string,origins:[origin]};" +
	"origin:{origin:string,revision:uint64,worktrees:[worktree],tombstones:[tombstone]};" +
	"worktree:{id:string,repo:{origin:string,relpath:string,branch:string?,source_path:string},orca:{kind:string,name:string,instance_id:string,freshness:time}?,checkpoints:[checkpoint]};" +
	"checkpoint:{id:sha256(origin,worktree,root),root:artifact.ref,classes:[latest|hourly|daily],captured_at:time,source_activity_at:time,expires_at:time(source_activity_at+7d)," +
	"sessions:[{id:string,title:string?,last_activity:time,last_human_activity:time,activity:string,claude_version:string?}],code:reposync.summary,deferred:string?,completeness:{complete:bool,missing:[string]?}};" +
	"tombstone:{id:string,revision:uint64,deleted_at:time,expires_at:time};" +
	"delivery:{kind:snapshot,base_revision:0,source_revision:uint64,artifacts:roots(latest,human_activity,captured_at)};" +
	"receipt:{origin:string,change_id:sha256,revision:uint64,payload_digest:sha256}"

// Fingerprint is the schema fingerprint cc-sync registers with synckit.
var Fingerprint = hostregistry.SchemaFingerprint(catalog.Identity, declaration)

var (
	// ErrV1 refuses the v1 export and apply methods; cc-sync changes always
	// carry artifacts.
	ErrV1 = errors.New("consumer: cc-sync serves only the v2 artifact methods")
	// ErrSchema refuses a request or change for another service or schema.
	ErrSchema = errors.New("consumer: service or schema fingerprint mismatch")
	// ErrRootsMismatch refuses a change whose artifacts are not the roots its
	// payload derives.
	ErrRootsMismatch = errors.New("consumer: change artifacts differ from the roots its payload derives")
)

// Artifacts is the slice of the artifact store Reconcile drives.
type Artifacts interface {
	SetPins(ctx context.Context, owner string, roots []artifact.Ref) error
	GC(ctx context.Context) (artifact.GCReport, error)
}

// CodeVerdict is whether a checkpoint's code snapshot restores on this host,
// and what it still lacks when it does not.
type CodeVerdict struct {
	Ready   bool
	Missing []string
}

// CodeVerifier checks a checkpoint's code snapshot against this host's
// checkouts, fetching the origin trunk only when fetchOrigin allows it.
type CodeVerifier interface {
	VerifyCode(ctx context.Context, root artifact.Ref, fetchOrigin bool) (CodeVerdict, error)
}

// Publisher announces catalog changes through the stamp.
type Publisher interface {
	Publish(ctx context.Context) error
}

// Config wires a Consumer. FetchAllowed reports whether network policy
// currently allows a bulk origin fetch.
type Config struct {
	Catalog      *catalog.Store
	Publisher    Publisher
	Artifacts    Artifacts
	Verifier     CodeVerifier
	FetchAllowed func() bool
	StampDir     string
}

// Consumer is cc-sync's syncservice.ArtifactConsumer.
type Consumer struct {
	cfg Config
}

var _ syncservice.ArtifactConsumer = (*Consumer)(nil)

// New returns a Consumer over cfg.
func New(cfg Config) *Consumer {
	return &Consumer{cfg: cfg}
}

// Capabilities reports the v2 artifact method set.
func (c *Consumer) Capabilities(context.Context) (syncservice.Capabilities, error) {
	return syncservice.ArtifactCapabilities(ServiceID), nil
}

// List reports the stamp as the single watch item; it reads the stamp file
// and nothing else.
func (c *Consumer) List(context.Context) ([]syncservice.WatchItem, error) {
	stamp, err := os.ReadFile(filepath.Join(c.cfg.StampDir, catalog.StampFile))
	if err != nil {
		return nil, fmt.Errorf("consumer: read stamp: %w", err)
	}
	return []syncservice.WatchItem{{ID: WatchItemID, WatchDirs: []string{c.cfg.StampDir}, Fingerprint: string(stamp)}}, nil
}

// Reconcile applies retention and expiry, pins every retained root, collects
// unpinned artifacts, and announces any resulting catalog change. It never
// captures.
func (c *Consumer) Reconcile(ctx context.Context, _ string) (syncservice.ReconcileResult, error) {
	gc, err := c.cfg.Catalog.GC(ctx)
	if err != nil {
		return syncservice.ReconcileResult{}, err
	}
	if err := c.cfg.Artifacts.SetPins(ctx, PinOwner, gc.Roots); err != nil {
		return syncservice.ReconcileResult{}, fmt.Errorf("consumer: pin retained roots: %w", err)
	}
	if _, err := c.cfg.Artifacts.GC(ctx); err != nil {
		return syncservice.ReconcileResult{}, fmt.Errorf("consumer: artifact gc: %w", err)
	}
	if err := c.cfg.Publisher.Publish(ctx); err != nil {
		return syncservice.ReconcileResult{}, err
	}
	return syncservice.ReconcileResult{Converged: gc.Ready}, nil
}

// Export refuses: cc-sync changes carry artifacts.
func (c *Consumer) Export(context.Context, syncservice.ExportRequest) (syncservice.ChangeEnvelope, error) {
	return syncservice.ChangeEnvelope{}, ErrV1
}

// Apply refuses: cc-sync changes carry artifacts.
func (c *Consumer) Apply(context.Context, syncservice.ChangeEnvelope) (syncservice.ApplyResult, error) {
	return syncservice.ApplyResult{}, ErrV1
}

// ExportArtifacts snapshots every locally ready checkpoint, own and relayed,
// with roots derived from that payload.
func (c *Consumer) ExportArtifacts(ctx context.Context, request syncservice.ExportRequest) (syncservice.ChangeEnvelope, error) {
	if err := request.Validate(); err != nil {
		return syncservice.ChangeEnvelope{}, err
	}
	if request.ServiceID != ServiceID || request.SchemaFingerprint != Fingerprint {
		return syncservice.ChangeEnvelope{}, fmt.Errorf("%w: %s %s", ErrSchema, request.ServiceID, request.SchemaFingerprint)
	}
	exported, err := c.cfg.Catalog.Export(ctx)
	if err != nil {
		return syncservice.ChangeEnvelope{}, err
	}
	roots, err := catalog.Roots(exported.Payload)
	if err != nil {
		return syncservice.ChangeEnvelope{}, err
	}
	return syncservice.NewExportedArtifactChange(ServiceID, Fingerprint, syncservice.ChangeSnapshot,
		syncservice.NewRevision(0), syncservice.NewRevision(exported.Revision), exported.Data, roots)
}

// ApplyArtifacts decodes change, refuses it unless its artifacts are exactly
// the roots its payload derives, fences it by origin, verifies the code of
// every checkpoint it would newly record whose root is in ready, and merges
// it. The result acknowledges the change only when every checkpoint in it is
// ready here; otherwise it is Partial with the prior receipt.
func (c *Consumer) ApplyArtifacts(ctx context.Context, change syncservice.ChangeEnvelope, ready []artifact.Ref) (syncservice.ApplyResult, error) {
	if change.ServiceID != ServiceID || change.SchemaFingerprint != Fingerprint {
		return syncservice.ApplyResult{}, fmt.Errorf("%w: %s %s", ErrSchema, change.ServiceID, change.SchemaFingerprint)
	}
	payload, err := catalog.Decode(change.Payload)
	if err != nil {
		return syncservice.ApplyResult{}, err
	}
	roots, err := catalog.Roots(payload)
	if err != nil {
		return syncservice.ApplyResult{}, err
	}
	if !slices.Equal(roots, change.Artifacts) {
		return syncservice.ApplyResult{}, fmt.Errorf("%w: derived %d roots, change carries %d", ErrRootsMismatch, len(roots), len(change.Artifacts))
	}
	decision, fenced, err := c.cfg.Catalog.Fence(change)
	if err != nil {
		return syncservice.ApplyResult{}, err
	}
	if decision != syncservice.FenceApply {
		return fenced, nil
	}
	evidence, err := c.verify(ctx, payload, ready)
	if err != nil {
		return syncservice.ApplyResult{}, err
	}
	result, err := c.cfg.Catalog.Apply(ctx, change, payload, evidence)
	if err != nil {
		return syncservice.ApplyResult{}, err
	}
	return result, c.cfg.Publisher.Publish(ctx)
}

func (c *Consumer) verify(ctx context.Context, payload catalog.Payload, ready []artifact.Ref) (catalog.Evidence, error) {
	evidence := catalog.Evidence{Roots: make(map[artifact.Digest]bool, len(ready)), Verified: map[string]catalog.Readiness{}}
	for _, root := range ready {
		evidence.Roots[root.Digest] = true
	}
	pending, err := c.cfg.Catalog.Unverified(payload)
	if err != nil {
		return catalog.Evidence{}, err
	}
	fetch := c.cfg.FetchAllowed()
	for _, cp := range pending {
		if !evidence.Roots[cp.Root.Digest] {
			continue
		}
		verdict, err := c.cfg.Verifier.VerifyCode(ctx, cp.Root, fetch)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return catalog.Evidence{}, ctxErr
		}
		readiness := catalog.Readiness{Ready: verdict.Ready, Missing: verdict.Missing}
		switch {
		case err != nil:
			readiness = catalog.Readiness{Missing: []string{err.Error()}}
		case !verdict.Ready && !fetch:
			readiness.Deferred = DeferredFetch
		}
		evidence.Verified[cp.ID] = readiness
	}
	return evidence, nil
}
