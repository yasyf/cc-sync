// Package consumer serves cc-sync's checkpoint catalog to synckit as an
// artifact consumer: one stamp watch item, snapshot exports that carry this
// host's block and every artifact-complete relayed block verbatim, and
// fenced applies that acknowledge a change only once this host holds ready
// every unexpired checkpoint it carries.
package consumer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"github.com/yasyf/reposync/worktree"

	"github.com/yasyf/cc-sync/internal/catalog"
	"github.com/yasyf/cc-sync/internal/netgate"
	"github.com/yasyf/synckit/artifact"
	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/netpolicy"
	"github.com/yasyf/synckit/syncservice"
)

// ServiceID is cc-sync's synckit service name.
const ServiceID = "cc-sync"

// WatchItemID names the single item List reports.
const WatchItemID = "checkpoints"

// PinOwner owns the pins on every retained checkpoint root.
const PinOwner = "cc-sync/catalog"

const declaration = "payload:{identity:cc-sync-catalog-v1,version:1,exporter:string,as_of:time,origins:[origin]};" +
	"origin:{origin:string,revision:max(prev+1,unix_micros),worktrees:[worktree],tombstones:[tombstone]};relay:verbatim,artifact-complete;" +
	"worktree:{id:string,repo:{origin:string,relpath:string,branch:string?,source_path:string},orca:{kind:string,name:string,instance_id:string,freshness:time}?,checkpoints:[checkpoint]};" +
	"checkpoint:{id:sha256(origin,worktree,root),root:artifact.ref,classes:[latest|hourly|daily],captured_at:time,source_activity_at:time,expires_at:time(source_activity_at+7d)," +
	"sessions:[{id:string,title:string?,last_activity:time,last_human_activity:time,activity:string,claude_version:string?}],code:reposync.summary,deferred:string?,completeness:{complete:bool,missing:[string]?}};" +
	"tombstone:{id:string,revision:uint64,deleted_at:time,expires_at:time};" +
	"delivery:{kind:snapshot,base_revision:0,source_revision:max(prev+1,unix_micros),artifacts:roots(unexpired_at(as_of),latest,hourly,daily,human_activity,captured_at,cap(max_roots))};" +
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

// Artifacts is the slice of the artifact store the consumer drives: pins
// and GC for Reconcile, closure checks for applies and background
// verification.
type Artifacts interface {
	SetPins(ctx context.Context, owner string, roots []artifact.Ref) error
	GC(ctx context.Context) (artifact.GCReport, error)
	Complete(ctx context.Context, roots []artifact.Ref) (missing int, err error)
}

// CodeVerdict is whether a checkpoint's code snapshot restores on this host,
// and what it still lacks when it does not.
type CodeVerdict struct {
	Ready   bool
	Missing []string
}

// CodeVerifier checks a checkpoint's code snapshot against this host's
// checkouts, fetching the origin trunk only through the fetchOrigin gate; nil
// never fetches.
type CodeVerifier interface {
	VerifyCode(ctx context.Context, root artifact.Ref, fetchOrigin worktree.FetchGate) (CodeVerdict, error)
}

// Publisher announces catalog changes through the stamp.
type Publisher interface {
	Publish(ctx context.Context) error
}

// Config wires a Consumer. Network is this host's network monitor: only
// VerifyDeferred consults it, starting each origin fetch only while Network
// allows bulk transfer and cancelling the fetch the moment it stops allowing
// it.
type Config struct {
	Catalog   *catalog.Store
	Publisher Publisher
	Artifacts Artifacts
	Verifier  CodeVerifier
	Network   netpolicy.Monitor
	StampDir  string
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

// ExportArtifacts snapshots this host's block and every relayed block that
// met the artifact-completeness bar, with roots derived from that payload.
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
// the roots its payload derives, fences it by origin, and merges it. Every
// checkpoint it would newly hold whose root closure is complete has its code
// verified without an origin fetch; one missing prerequisites records
// catalog.MissingPrerequisites for VerifyDeferred. The result acknowledges
// the change as processed only when every unexpired checkpoint it carries is
// held ready here, including when expiry disposed of all of them; otherwise
// it is Partial with the prior receipt.
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
	pending, err := c.cfg.Catalog.Unverified(payload)
	if err != nil {
		return syncservice.ApplyResult{}, err
	}
	evidence := catalog.Evidence{Roots: make(map[artifact.Digest]bool, len(ready)), Verified: map[string]catalog.Readiness{}}
	for _, root := range ready {
		evidence.Roots[root.Digest] = true
	}
	if _, err := c.settle(ctx, pending, evidence, false); err != nil {
		return syncservice.ApplyResult{}, err
	}
	result, err := c.cfg.Catalog.Apply(ctx, change, payload, evidence)
	if err != nil {
		return syncservice.ApplyResult{}, err
	}
	return result, c.cfg.Publisher.Publish(ctx)
}

// VerifyDeferred is the resident's background half of verification, run
// outside any apply: it re-checks the artifact closure of every held relayed
// checkpoint not yet ready, relays every held block that is now
// artifact-complete, and verifies each closure-complete checkpoint, fetching
// the origin trunk through a gate that re-checks network policy immediately
// before each fetch and cancels it once the policy turns restrictive. It then
// publishes, so the next delivery of a waiting change can acknowledge it. A
// fetch the policy refused or interrupted leaves its checkpoint deferred on
// the prerequisites it still lacks, and VerifyDeferred returns the
// *netpolicy.PausedError after publishing so the caller retries once the
// policy allows. A ctx cancelled mid-pass records nothing and leaves every
// checkpoint deferred as it was.
func (c *Consumer) VerifyDeferred(ctx context.Context) error {
	pending, err := c.cfg.Catalog.Pending()
	if err != nil {
		return err
	}
	evidence := catalog.Evidence{Roots: map[artifact.Digest]bool{}, Verified: map[string]catalog.Readiness{}}
	paused, err := c.settle(ctx, pending, evidence, true)
	if err != nil {
		return err
	}
	if err := c.cfg.Catalog.Settle(ctx, evidence); err != nil {
		return err
	}
	if err := c.cfg.Publisher.Publish(ctx); err != nil {
		return err
	}
	if paused != nil {
		return paused
	}
	return nil
}

func (c *Consumer) settle(ctx context.Context, pending []catalog.Checkpoint, evidence catalog.Evidence, background bool) (paused *netpolicy.PausedError, _ error) {
	var fetchOrigin worktree.FetchGate
	if background {
		fetchOrigin = func(ctx context.Context, fetch func(context.Context) error) error {
			err := netgate.Run(ctx, c.cfg.Network, fetch)
			if errors.As(err, &paused) {
				return fmt.Errorf("%w: %w", worktree.ErrFetchDeferred, paused)
			}
			return err
		}
	}
	for _, cp := range pending {
		if !evidence.Roots[cp.Root.Digest] {
			missing, err := c.cfg.Artifacts.Complete(ctx, []artifact.Ref{cp.Root})
			if err != nil {
				return nil, fmt.Errorf("consumer: closure of %s: %w", cp.ID, err)
			}
			evidence.Roots[cp.Root.Digest] = missing == 0
		}
		if !evidence.Roots[cp.Root.Digest] {
			continue
		}
		verdict, err := c.cfg.Verifier.VerifyCode(ctx, cp.Root, fetchOrigin)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		switch {
		case err != nil:
			evidence.Verified[cp.ID] = catalog.Readiness{Missing: []string{err.Error()}}
		case verdict.Ready:
			evidence.Verified[cp.ID] = catalog.Readiness{Ready: true}
		default:
			evidence.Verified[cp.ID] = catalog.Readiness{Missing: verdict.Missing, Deferred: catalog.MissingPrerequisites}
		}
	}
	return paused, nil
}
