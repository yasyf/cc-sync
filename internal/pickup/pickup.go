// Package pickup restores a picked checkpoint on this host from the CLI
// process: it pins the checkpoint in the resident's store, restores the code
// as a recovery checkout, installs every archived Claude session natively,
// and imports the Orca workspace that resumes the selected ones. Without
// Orca it returns each session's native launch instead.
package pickup

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/yasyf/cc-sync/internal/catalog"
	"github.com/yasyf/cc-sync/internal/claudenative"
	"github.com/yasyf/cc-sync/internal/cli"
	"github.com/yasyf/cc-sync/internal/consumer"
	"github.com/yasyf/cc-sync/internal/orcabridge"
	"github.com/yasyf/cc-sync/internal/replica"
	"github.com/yasyf/cc-sync/internal/sessionarchive"
	"github.com/yasyf/synckit/artifact"
	"github.com/yasyf/synckit/hostregistry"
)

// PinTTL bounds a pickup pin, so a crashed pickup never pins forever.
const PinTTL = 24 * time.Hour

// CheckoutStamp is the time layout of the capture minute ending a default
// recovery checkout's name.
const CheckoutStamp = "20060102-1504"

// PinOwnerPrefix prefixes every pickup's pin owner; one pickup's owner is
// PinOwnerPrefix followed by its operation id.
const PinOwnerPrefix = "cc-sync/pickup/"

// Config wires a Pickup. Orca nil means Orca is unavailable on this host.
// FetchAllowed reports whether network policy allows bulk fetches now.
// PreferClient is the Orca client instance whose saved view the import
// prefers ($CC_SYNC_ORCA_CLIENT_INSTANCE_ID).
type Config struct {
	Catalog      Catalog
	Pinner       Pinner
	OpenStore    func(ctx context.Context) (Store, error)
	Verifier     consumer.CodeVerifier
	Code         CodeRestorer
	Sessions     SessionRestorer
	Orca         Orca
	FetchAllowed func() bool
	Layout       claudenative.Layout
	Home         string
	ReplicaRoot  string
	CheckoutRoot string
	PreferClient string
	Now          func() time.Time
}

// Request is one pickup. Resume names the sessions to resume (full ids or
// unique prefixes); empty resumes the session a session target named, else
// the checkpoint's most recent human session. AllowPartial admits an
// explicitly selected mixed checkpoint.
type Request struct {
	Target       cli.Target
	Checkpoint   cli.CheckpointSelector
	AllowPartial bool
	Resume       []string
	OnDivergence Divergence
	NoOrca       bool
	DryRun       bool
	Progress     func(cli.Progress)
}

// Pickup runs pickups against one host's stores.
type Pickup struct {
	cfg Config
}

// New returns a Pickup over cfg.
func New(cfg Config) *Pickup {
	return &Pickup{cfg: cfg}
}

type run struct {
	*Pickup
	req      Request
	pick     pick
	store    Store
	restored Restored
	sessions []prepared
}

type prepared struct {
	source   claudenative.SessionID
	manifest sessionarchive.Manifest
	cwd      string
	session  PreparedSession
	refused  string
}

// Run performs req. A failure or cancellation before the Orca import removes
// a recovery checkout this run created; the pin is always released.
func (p *Pickup) Run(ctx context.Context, req Request) (res Result, err error) {
	r := &run{Pickup: p, req: req}
	r.progress(cli.PhaseSelect, nil, nil)
	snap, err := p.cfg.Catalog.Load()
	if err != nil {
		return Result{}, fmt.Errorf("load catalog: %w", err)
	}
	if r.pick, err = selectCheckpoint(snap, req.Target, req.Checkpoint, req.AllowPartial, p.cfg.Now()); err != nil {
		return Result{}, err
	}
	cp := r.pick.checkpoint
	if ready := snap.ReadinessOf(r.pick.origin, cp.ID); !ready.Ready {
		return Result{}, &NotReadyError{CheckpointID: cp.ID, Missing: ready.Missing}
	}
	owner := PinOwnerPrefix + rand.Text()
	if err := p.cfg.Pinner.Pin(ctx, owner, []artifact.Ref{cp.Root}, PinTTL); err != nil {
		return Result{}, fmt.Errorf("pin checkpoint %s: %w", cp.ID, err)
	}
	defer func() {
		if uerr := p.cfg.Pinner.Pin(context.WithoutCancel(ctx), owner, nil, 0); uerr != nil {
			err = errors.Join(err, fmt.Errorf("unpin checkpoint %s: %w", cp.ID, uerr))
		}
	}()
	parts, err := r.open(ctx)
	if err != nil {
		return Result{}, err
	}
	selected, err := selectSessions(req.Resume, r.pick.session, cp, parts.sessions)
	if err != nil {
		return Result{}, err
	}
	dest, err := p.dest(r.pick)
	if err != nil {
		return Result{}, err
	}
	imported := false
	defer func() {
		if err != nil && !imported && !req.DryRun && r.restored.Path != "" && !r.restored.Reused {
			err = errors.Join(err, r.discard(context.WithoutCancel(ctx)))
		}
	}()
	if err := r.restoreCode(ctx, parts.code, dest); err != nil {
		return Result{}, err
	}
	if err := r.restoreSessions(ctx, parts, selected); err != nil {
		return Result{}, err
	}
	res = r.result()
	if req.DryRun || req.NoOrca || p.cfg.Orca == nil || parts.descriptor == nil {
		return res, nil
	}
	descriptor, err := r.readDescriptor(ctx, *parts.descriptor)
	if err != nil {
		return Result{}, err
	}
	imported = true
	return r.importOrca(ctx, descriptor, selected, res)
}

func (r *run) progress(phase cli.Phase, done, total *int) {
	if r.req.Progress != nil {
		r.req.Progress(cli.Progress{Phase: phase, Done: done, Total: total})
	}
}

func (r *run) open(ctx context.Context) (rootParts, error) {
	cp := r.pick.checkpoint
	var err error
	if r.store, err = r.cfg.OpenStore(ctx); err != nil {
		return rootParts{}, fmt.Errorf("open artifact store: %w", err)
	}
	missing, err := r.store.Complete(ctx, []artifact.Ref{cp.Root})
	if err != nil {
		return rootParts{}, fmt.Errorf("check checkpoint %s closure: %w", cp.ID, err)
	}
	if missing > 0 {
		return rootParts{}, &NotReadyError{CheckpointID: cp.ID, Missing: []string{strconv.Itoa(missing) + " artifacts"}}
	}
	verdict, err := r.cfg.Verifier.VerifyCode(ctx, cp.Root, false)
	if err != nil {
		return rootParts{}, fmt.Errorf("verify checkpoint %s code: %w", cp.ID, err)
	}
	if !verdict.Ready {
		return rootParts{}, &NotReadyError{CheckpointID: cp.ID, Missing: verdict.Missing}
	}
	parts, err := readRoot(ctx, r.store, cp.Root)
	if err != nil {
		return rootParts{}, fmt.Errorf("read checkpoint %s: %w", cp.ID, err)
	}
	if parts.code == nil {
		return rootParts{}, &NotReadyError{CheckpointID: cp.ID, Missing: []string{"code"}}
	}
	return parts, nil
}

// CheckoutLocation places the default recovery checkouts of w picked from
// origin: each one lives in dir, <root>/<repo relpath>, named prefix followed
// by its capture minute in the CheckoutStamp layout.
func CheckoutLocation(root, origin string, w catalog.Worktree) (dir, prefix string, err error) {
	rel := filepath.Clean(filepath.FromSlash(w.Repo.RelPath))
	if !filepath.IsLocal(rel) {
		return "", "", fmt.Errorf("%w: repo relpath %q", ErrInvalid, w.Repo.RelPath)
	}
	name := firstNonEmpty(orcaName(w), w.Repo.Branch, filepath.Base(w.Repo.SourcePath))
	return filepath.Join(root, rel), sanitize(hostregistry.HostNode(origin)) + "-" + sanitize(name) + "-", nil
}

func (p *Pickup) dest(pk pick) (string, error) {
	dir, prefix, err := CheckoutLocation(p.cfg.CheckoutRoot, pk.origin, pk.worktree)
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, prefix+pk.checkpoint.CapturedAt.In(p.cfg.Now().Location()).Format(CheckoutStamp)), nil
}

func orcaName(wt catalog.Worktree) string {
	if wt.Orca == nil {
		return ""
	}
	return wt.Orca.Name
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

func sanitize(s string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.', r == '_', r == '-':
			return r
		}
		return '-'
	}, s)
}

func (r *run) restoreCode(ctx context.Context, code *artifact.Ref, dest string) error {
	r.progress(cli.PhaseRestoreCode, nil, nil)
	if r.req.DryRun {
		r.restored = Restored{Path: dest, Branch: r.pick.worktree.Repo.Branch}
		return nil
	}
	restored, err := r.cfg.Code.Restore(ctx, r.store, *code, RestoreOptions{Dest: dest, FetchLFS: r.cfg.FetchAllowed()})
	if err != nil {
		return fmt.Errorf("restore code to %s: %w", dest, err)
	}
	r.restored = restored
	return ctx.Err()
}

func (r *run) discard(ctx context.Context) error {
	if err := r.cfg.Code.Remove(ctx, r.restored); err != nil {
		return fmt.Errorf("remove recovery checkout %s: %w", r.restored.Path, err)
	}
	return nil
}

func (r *run) restoreSessions(ctx context.Context, parts rootParts, selected map[claudenative.SessionID]bool) error {
	total := len(parts.sessions)
	zero := 0
	r.progress(cli.PhaseRestoreSessions, &zero, &total)
	mode := orRefuse(r.req.OnDivergence)
	source := r.pick.worktree.Repo.SourcePath
	for _, m := range parts.sessions {
		sid := claudenative.SessionID(m.SessionID)
		dir := replica.Dir(r.cfg.ReplicaRoot, r.pick.origin, m.SessionID, r.pick.checkpoint.ID)
		if err := replica.Materialize(ctx, m, r.pick.checkpoint.ID, r.store, dir); err != nil {
			return fmt.Errorf("materialize session %s: %w", sid, err)
		}
		cwd := relocate(source, m.Cwd, r.restored.Path)
		ps, err := r.cfg.Sessions.Prepare(ctx, dir, SessionTarget{
			Layout: r.cfg.Layout, Home: r.cfg.Home, Cwd: cwd,
			Checkouts: PathMap{{From: source, To: r.restored.Path}},
		}, mode)
		if reason, ok := refusal(err); ok && !selected[sid] {
			slog.Info("pickup: session refused", "session", sid, "reason", reason, "err", err)
			r.sessions = append(r.sessions, prepared{source: sid, manifest: m, cwd: cwd, refused: reason})
			continue
		}
		if err != nil {
			return fmt.Errorf("prepare session %s: %w", sid, err)
		}
		r.sessions = append(r.sessions, prepared{source: sid, manifest: m, cwd: cwd, session: ps})
	}
	if r.req.DryRun {
		return nil
	}
	done := 0
	for _, s := range r.sessions {
		if s.session == nil {
			continue
		}
		if err := s.session.Apply(ctx); err != nil {
			return fmt.Errorf("install session %s: %w", s.source, err)
		}
		done++
		r.progress(cli.PhaseRestoreSessions, &done, &total)
	}
	return ctx.Err()
}

func orRefuse(d Divergence) Divergence {
	if d == "" {
		return DivergenceRefuse
	}
	return d
}

func relocate(source, cwd, dest string) string {
	rel, err := filepath.Rel(source, cwd)
	if err != nil || !filepath.IsLocal(rel) {
		return dest
	}
	return filepath.Join(dest, rel)
}

func refusal(err error) (string, bool) {
	var div *DivergentLocalError
	var inc *IncompatibleError
	switch {
	case errors.Is(err, ErrLiveLocal):
		return ReasonLiveLocal, true
	case errors.As(err, &div):
		return ReasonDivergent, true
	case errors.As(err, &inc):
		return ReasonIncompatible, true
	}
	return "", false
}

func (r *run) result() Result {
	cp := r.pick.checkpoint
	res := Result{
		Checkpoint: Checkpoint{ID: cp.ID, CapturedAt: cp.CapturedAt, Partial: cp.Deferred != "", CodeDeferred: cp.Deferred},
		Checkout: Checkout{
			Path: r.restored.Path, Branch: r.restored.Branch, Reused: r.restored.Reused,
			Newer: r.restored.Newer, LFSPending: r.restored.LFSPending,
		},
		Sessions: make([]Session, 0, len(r.sessions)),
	}
	for _, s := range r.sessions {
		if s.session == nil {
			res.Sessions = append(res.Sessions, Session{SessionID: string(s.source), Status: StatusRefused, Reason: s.refused})
			continue
		}
		plan := s.session.Plan()
		out := Session{SessionID: string(plan.SessionID), Status: StatusRestored, Launch: &plan.Launch}
		if plan.Mode == ModeFork {
			out.ForkedFrom = string(plan.SourceSessionID)
		}
		res.Sessions = append(res.Sessions, out)
	}
	return res
}

func (r *run) importOrca(ctx context.Context, descriptor []byte, selected map[claudenative.SessionID]bool, res Result) (Result, error) {
	req := orcabridge.ImportRequest{
		Descriptor:   descriptor,
		Checkout:     r.restored.Path,
		CheckpointID: r.pick.checkpoint.ID,
		PathMap:      r.orcaPathMap(),
		PreferClient: r.cfg.PreferClient,
		RegisterRepo: true,
	}
	for _, s := range r.sessions {
		if s.session == nil {
			continue
		}
		plan := s.session.Plan()
		if selected[s.source] {
			req.Resume = append(req.Resume, string(plan.SessionID))
		}
		if plan.SessionID != plan.SourceSessionID {
			req.SessionIDMap = append(req.SessionIDMap, orcabridge.SessionMapping{From: string(plan.SourceSessionID), To: string(plan.SessionID)})
		}
	}
	r.progress(cli.PhaseOrcaImport, nil, nil)
	out, err := r.cfg.Orca.Import(ctx, req)
	var unavailable *orcabridge.UnavailableError
	if errors.As(err, &unavailable) {
		slog.Info("pickup: orca unavailable, sessions launch natively", "reason", unavailable.Reason, "detail", unavailable.Detail)
		return res, nil
	}
	if err != nil {
		return Result{}, fmt.Errorf("orca import: %w", err)
	}
	orca := &OrcaResult{ExecutionHostID: "local", WorktreeID: out.WorktreeID, Resumed: []ResumedTab{}, Dormant: []string{}}
	bindings := make(map[string]orcabridge.ImportedBinding, len(out.Bindings))
	for _, b := range out.Bindings {
		bindings[b.ProviderSessionID] = b
	}
	for i, s := range res.Sessions {
		b, ok := bindings[s.SessionID]
		if !ok || s.Status == StatusRefused {
			continue
		}
		switch b.Status {
		case "resumed":
			tab, _, _ := strings.Cut(b.LocalPaneKey, ":")
			orca.Resumed = append(orca.Resumed, ResumedTab{SessionID: s.SessionID, TabID: tab})
			res.Sessions[i].Status, res.Sessions[i].Launch = StatusResumed, nil
		case "dormant":
			orca.Dormant = append(orca.Dormant, s.SessionID)
			res.Sessions[i].Status = StatusDormant
		case "refused":
			res.Sessions[i].Status, res.Sessions[i].Reason = StatusRefused, b.Reason
		default:
			return Result{}, fmt.Errorf("orca import: binding %s has status %q", b.ProviderSessionID, b.Status)
		}
	}
	resumed, requested := len(orca.Resumed), len(req.Resume)
	r.progress(cli.PhaseOrcaResume, &resumed, &requested)
	res.Orca = orca
	return res, nil
}

func (r *run) readDescriptor(ctx context.Context, ref artifact.Ref) ([]byte, error) {
	rc, err := r.store.Open(ctx, ref)
	if err != nil {
		return nil, fmt.Errorf("open orca descriptor: %w", err)
	}
	defer func() { _ = rc.Close() }()
	data, err := io.ReadAll(io.LimitReader(rc, orcabridge.MaxDescriptorBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read orca descriptor: %w", err)
	}
	return data, nil
}

func (r *run) orcaPathMap() []orcabridge.PathMapping {
	source := r.pick.worktree.Repo.SourcePath
	m := []orcabridge.PathMapping{{From: source, To: r.restored.Path}}
	for _, s := range r.sessions {
		if s.session == nil {
			continue
		}
		m = append(m,
			orcabridge.PathMapping{
				From: filepath.Join(claudenative.ProjectsDir(s.manifest.ConfigDir), claudenative.ProjectDirName(s.manifest.Cwd)),
				To:   filepath.Join(claudenative.ProjectsDir(r.cfg.Layout.ConfigDir), claudenative.ProjectDirName(s.cwd)),
			},
			orcabridge.PathMapping{
				From: filepath.Join(s.manifest.TmpRoot, "claude-"+strconv.Itoa(s.manifest.UID)),
				To:   filepath.Join(r.cfg.Layout.TmpRoot, "claude-"+strconv.Itoa(r.cfg.Layout.UID)),
			},
		)
	}
	slices.SortFunc(m, func(a, b orcabridge.PathMapping) int { return strings.Compare(a.From, b.From) })
	return slices.CompactFunc(m, func(a, b orcabridge.PathMapping) bool { return a.From == b.From })
}
