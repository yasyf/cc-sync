// Package capture runs one worktree capture: its code through reposync and
// codesnap, every session through sessionarchive, and its Orca layout through
// orcabridge, grouped under one checkpoint root recorded in the catalog.
package capture

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/yasyf/cc-sync/internal/catalog"
	"github.com/yasyf/cc-sync/internal/claudenative"
	"github.com/yasyf/cc-sync/internal/codesnap"
	"github.com/yasyf/cc-sync/internal/inventory"
	"github.com/yasyf/cc-sync/internal/orcabridge"
	"github.com/yasyf/cc-sync/internal/scheduler"
	"github.com/yasyf/cc-sync/internal/sessionarchive"
	"github.com/yasyf/reposync/worktree"
	"github.com/yasyf/synckit/artifact"
)

// Media labels of the artifacts a capture stores itself.
const (
	MediaCheckpoint = "cc-sync.checkpoint"
	MediaPartial    = "cc-sync.capture-partial"
)

// BusyRetry is how soon a worktree whose code capture hit ErrBusy is due
// again. PartialPinTTL bounds how long the content a partial code capture
// already stored stays pinned without a complete snapshot.
const (
	BusyRetry     = scheduler.BusyRetry
	PartialPinTTL = 24 * time.Hour
)

// PinOwnerPrefix prefixes the per-worktree pin owner holding partial code
// capture progress.
const PinOwnerPrefix = "cc-sync/capture/"

const maxNamed = 32

// ErrUnknownWorktree reports a unit the inventory no longer holds.
var ErrUnknownWorktree = errors.New("capture: unknown worktree")

// Store is the synckit artifact store surface a capture writes through: the
// code and session writes, plus the per-root closure check and partial pins.
type Store interface {
	codesnap.Store
	Closure(ctx context.Context, roots []artifact.Ref, bound artifact.ClosureBound) (artifact.Closure, error)
	SetPins(ctx context.Context, owner string, roots []artifact.Ref) error
}

// Code captures a worktree's work in progress; reposync's worktree.Store is one.
type Code interface {
	Capture(ctx context.Context, wt worktree.Worktree, sink worktree.ArtifactSink, opts worktree.CaptureOptions) (worktree.Snapshot, error)
}

// Stamper digests a worktree's work in progress without capturing it, the
// shape of reposync's worktree.Stamp.
type Stamper interface {
	Stamp(ctx context.Context, wt worktree.Worktree) (string, error)
}

// StampFunc adapts a function such as reposync's worktree.Stamp to Stamper.
type StampFunc func(ctx context.Context, wt worktree.Worktree) (string, error)

// Stamp calls f.
func (f StampFunc) Stamp(ctx context.Context, wt worktree.Worktree) (string, error) {
	return f(ctx, wt)
}

// Orca exports a worktree's Orca recovery descriptor.
type Orca interface {
	Export(ctx context.Context, worktreePath string) ([]byte, error)
}

// Catalog records a captured checkpoint in this host's block.
type Catalog interface {
	Record(ctx context.Context, wt catalog.Worktree, cp catalog.Checkpoint) (catalog.Checkpoint, error)
}

// Publisher announces a catalog change.
type Publisher interface {
	Publish(ctx context.Context) error
}

// Targets resolves a unit to the worktree and sessions the inventory grouped.
type Targets interface {
	Target(worktreeID string) (inventory.Target, bool)
}

// Config wires a Job. Self is this host's synckit id; a zero Bound takes
// artifact.DefaultClosureBound, a zero Limits takes reposync's defaults, and
// a zero Tiers takes scheduler.DefaultTiers for labeling session activity.
type Config struct {
	Self      string
	Layout    claudenative.Layout
	Home      string
	Store     Store
	Code      Code
	Stamper   Stamper
	Orca      Orca
	Catalog   Catalog
	Publisher Publisher
	Targets   Targets
	CodeIndex string
	StateDir  string
	Limits    worktree.Limits
	Bound     artifact.ClosureBound
	Tiers     scheduler.Tiers
	Now       func() time.Time
}

// Job is the scheduler's Capturer: one checkpoint per changed worktree.
type Job struct {
	cfg Config

	mu    sync.Mutex
	locks map[string]*sync.Mutex
}

// New returns a Job over cfg.
func New(cfg Config) *Job {
	if cfg.Bound == (artifact.ClosureBound{}) {
		cfg.Bound = artifact.DefaultClosureBound
	}
	if cfg.Tiers == (scheduler.Tiers{}) {
		cfg.Tiers = scheduler.DefaultTiers()
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Job{cfg: cfg, locks: map[string]*sync.Mutex{}}
}

// CodeStamp digests the unit's worktree's work in progress for the scheduler
// to compare at due time; it makes Job the scheduler's Stamper.
func (j *Job) CodeStamp(ctx context.Context, u scheduler.Unit) (string, error) {
	t, ok := j.cfg.Targets.Target(u.WorktreeID)
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrUnknownWorktree, u.WorktreeID)
	}
	stamp, err := j.cfg.Stamper.Stamp(ctx, t.Worktree)
	if err != nil {
		return "", fmt.Errorf("stamp worktree %s: %w", t.Worktree.Root, err)
	}
	return stamp, nil
}

// Capture checkpoints u's worktree unless its metadata and code stamps match
// the last complete capture. Code that cannot be captured completely still
// yields a checkpoint of the sessions over the last complete code manifest,
// labeled with the reason; ErrBusy yields no checkpoint at all.
func (j *Job) Capture(ctx context.Context, u scheduler.Unit) (scheduler.Result, error) {
	t, ok := j.cfg.Targets.Target(u.WorktreeID)
	if !ok {
		return scheduler.Result{}, fmt.Errorf("%w: %s", ErrUnknownWorktree, u.WorktreeID)
	}
	unlock := j.lock(u.WorktreeID)
	defer unlock()
	st, err := j.load(u.WorktreeID)
	if err != nil {
		return scheduler.Result{}, err
	}
	now := j.cfg.Now().UTC()
	if err := j.revalidate(ctx, &st); err != nil {
		return scheduler.Result{}, err
	}
	if st.Partial != nil && now.Sub(st.Partial.Since) >= PartialPinTTL {
		if err := j.unpin(ctx, u.WorktreeID, &st); err != nil {
			return scheduler.Result{}, err
		}
	}
	codeStamp, err := j.cfg.Stamper.Stamp(ctx, t.Worktree)
	if err != nil {
		return scheduler.Result{}, fmt.Errorf("stamp worktree %s: %w", t.Worktree.Root, err)
	}
	stamp := combine(u.MetaStamp, codeStamp)
	if st.Stamp == stamp {
		return scheduler.Result{Outcome: scheduler.OutcomeUnchanged, Checkpoint: st.Checkpoint}, nil
	}
	code, err := j.code(ctx, t.Worktree, &st, now)
	if err != nil {
		return scheduler.Result{}, err
	}
	if code.outcome == scheduler.OutcomeBusy {
		return scheduler.Result{Outcome: scheduler.OutcomeBusy, Reason: code.state}, nil
	}
	sessions, err := j.sessions(ctx, t.Sessions, st.Sessions, now)
	if err != nil {
		return scheduler.Result{}, err
	}
	st.Sessions = sessions
	if err := j.orca(ctx, t.Worktree, &st, now); err != nil {
		return scheduler.Result{}, err
	}
	deps := st.deps(t.Sessions)
	if slices.Equal(deps, st.Deps) {
		if code.complete() {
			st.Stamp = stamp
		}
		if err := j.save(u.WorktreeID, st); err != nil {
			return scheduler.Result{}, err
		}
		return scheduler.Result{Outcome: code.unchanged(), Checkpoint: st.Checkpoint, Reason: code.state}, nil
	}
	root, err := j.cfg.Store.PutGroup(ctx, MediaCheckpoint, deps)
	if err != nil {
		return scheduler.Result{}, fmt.Errorf("group checkpoint of %s: %w", t.Worktree.Root, err)
	}
	if _, err := j.cfg.Store.Closure(ctx, []artifact.Ref{root}, j.cfg.Bound); err != nil {
		return scheduler.Result{}, fmt.Errorf("checkpoint of %s not published: %w", t.Worktree.Root, err)
	}
	recorded, err := j.cfg.Catalog.Record(ctx, j.catalogWorktree(t.Worktree, st), j.checkpoint(t, st, code, root, now))
	if err != nil {
		return scheduler.Result{}, fmt.Errorf("record checkpoint of %s: %w", t.Worktree.Root, err)
	}
	st.Deps, st.Checkpoint, st.Stamp = deps, recorded.ID, ""
	if code.complete() {
		st.Stamp = stamp
	}
	if err := j.save(u.WorktreeID, st); err != nil {
		return scheduler.Result{}, err
	}
	if err := j.cfg.Publisher.Publish(ctx); err != nil {
		return scheduler.Result{}, fmt.Errorf("publish catalog: %w", err)
	}
	return scheduler.Result{Outcome: code.outcome, Checkpoint: recorded.ID, Reason: code.state}, nil
}

// ExpirePins drops the partial-capture pins of every worktree whose oldest
// pinned partial capture is PartialPinTTL old.
func (j *Job) ExpirePins(ctx context.Context) error {
	ids, err := j.stateIDs()
	if err != nil {
		return err
	}
	now := j.cfg.Now().UTC()
	for _, id := range ids {
		if err := j.expire(ctx, id, now); err != nil {
			return err
		}
	}
	return nil
}

func (j *Job) expire(ctx context.Context, id string, now time.Time) error {
	unlock := j.lock(id)
	defer unlock()
	st, err := j.load(id)
	if err != nil {
		return err
	}
	if st.Partial == nil || now.Sub(st.Partial.Since) < PartialPinTTL {
		return nil
	}
	return j.unpin(ctx, id, &st)
}

type codeResult struct {
	outcome scheduler.Outcome
	state   string
	named   []string
}

func (c codeResult) complete() bool { return c.outcome == scheduler.OutcomeCaptured }

func (c codeResult) unchanged() scheduler.Outcome {
	if c.complete() {
		return scheduler.OutcomeUnchanged
	}
	return c.outcome
}

func (j *Job) code(ctx context.Context, wt worktree.Worktree, st *state, now time.Time) (codeResult, error) {
	rec := &recorder{Store: j.cfg.Store, seen: map[artifact.Digest]bool{}}
	sink, err := codesnap.NewSink(rec, j.cfg.CodeIndex)
	if err != nil {
		return codeResult{}, err
	}
	snap, err := j.cfg.Code.Capture(ctx, wt, sink, worktree.CaptureOptions{Source: j.cfg.Self, Limits: j.cfg.Limits})
	var (
		deferred *worktree.DeferredError
		partial  *worktree.PartialError
		lfs      *worktree.MissingLFSError
	)
	switch {
	case errors.Is(err, worktree.ErrBusy):
		return codeResult{outcome: scheduler.OutcomeBusy, state: "busy"}, nil
	case errors.As(err, &deferred):
		return codeResult{outcome: scheduler.OutcomeDeferred, state: "deferred:" + deferred.Reason}, nil
	case errors.As(err, &partial):
		if err := j.pin(ctx, wt.ID, st, rec.refs, now); err != nil {
			return codeResult{}, err
		}
		return codeResult{
			outcome: scheduler.OutcomePartial,
			state:   "deferred:partial-" + string(partial.Reason),
			named:   named("code:", partial.Remaining),
		}, nil
	case errors.As(err, &lfs):
		objects := make([]string, len(lfs.Objects))
		for i, o := range lfs.Objects {
			objects[i] = o.Path + "@" + o.OID
		}
		return codeResult{outcome: scheduler.OutcomeMissingLFS, state: "deferred:missing-lfs", named: named("lfs:", objects)}, nil
	case err != nil:
		return codeResult{}, fmt.Errorf("capture code of %s: %w", wt.Root, err)
	case !snap.Complete:
		omitted := make([]string, len(snap.Omitted))
		for i, o := range snap.Omitted {
			omitted[i] = string(o.Reason) + ":" + o.Path
		}
		return codeResult{outcome: scheduler.OutcomeDeferred, state: "deferred:omitted", named: named("code:", omitted)}, nil
	}
	if st.Code == nil || st.Code.Summary.Digest != snap.Digest {
		root, err := sink.BuildCodeManifest(ctx, snap)
		if err != nil {
			return codeResult{}, fmt.Errorf("build code manifest of %s: %w", wt.Root, err)
		}
		st.Code = &codeState{Root: root}
	}
	st.Code.Summary = snap.Summary()
	if st.Partial != nil {
		if err := j.unpin(ctx, wt.ID, st); err != nil {
			return codeResult{}, err
		}
	}
	return codeResult{outcome: scheduler.OutcomeCaptured}, nil
}

func (j *Job) pin(ctx context.Context, id string, st *state, refs []artifact.Ref, now time.Time) error {
	if len(refs) == 0 {
		return nil
	}
	root, err := groupTree(ctx, j.cfg.Store, MediaPartial, refs)
	if err != nil {
		return fmt.Errorf("group partial capture of %s: %w", id, err)
	}
	p := partialPins{Since: now}
	if st.Partial != nil {
		p = *st.Partial
	}
	if !slices.Contains(p.Roots, root) {
		p.Roots = append(p.Roots, root)
	}
	if err := j.cfg.Store.SetPins(ctx, PinOwnerPrefix+id, p.Roots); err != nil {
		return fmt.Errorf("pin partial capture of %s: %w", id, err)
	}
	st.Partial = &p
	return j.savePartial(id, st.Partial)
}

func (j *Job) unpin(ctx context.Context, id string, st *state) error {
	if err := j.cfg.Store.SetPins(ctx, PinOwnerPrefix+id, nil); err != nil {
		return fmt.Errorf("unpin partial capture of %s: %w", id, err)
	}
	st.Partial = nil
	return j.savePartial(id, nil)
}

func (j *Job) sessions(ctx context.Context, sessions []claudenative.Session, prev map[string]sessionarchive.Archive, now time.Time) (map[string]sessionarchive.Archive, error) {
	archives := make(map[string]sessionarchive.Archive, len(sessions))
	for _, s := range sessions {
		var last *sessionarchive.Archive
		if a, ok := prev[string(s.ID)]; ok {
			last = &a
		}
		a, err := sessionarchive.Capture(ctx, j.source(s, now), last, j.cfg.Store)
		if err != nil {
			return nil, fmt.Errorf("archive session %s: %w", s.ID, err)
		}
		archives[string(s.ID)] = a
	}
	return archives, nil
}

func (j *Job) orca(ctx context.Context, wt worktree.Worktree, st *state, now time.Time) error {
	descriptor, err := j.cfg.Orca.Export(ctx, wt.Root)
	var (
		unavailable *orcabridge.UnavailableError
		refused     *orcabridge.RefusedError
	)
	switch {
	case errors.As(err, &unavailable):
		return nil
	case errors.As(err, &refused) && refused.Code == orcabridge.CodeSelectorNotFound:
		st.Orca = nil
		return nil
	case err != nil:
		return fmt.Errorf("export orca layout of %s: %w", wt.Root, err)
	}
	summary, err := summarize(descriptor, now)
	if err != nil {
		return err
	}
	omitted, err := orcabridge.Omitted(descriptor)
	if err != nil {
		return fmt.Errorf("orca descriptor of %s: %w", wt.Root, err)
	}
	ref, err := j.cfg.Store.Put(ctx, bytes.NewReader(descriptor), orcabridge.MediaDescriptor)
	if err != nil {
		return fmt.Errorf("store orca descriptor of %s: %w", wt.Root, err)
	}
	st.Orca = &orcaState{Descriptor: ref, Summary: summary}
	for _, o := range omitted {
		st.Orca.Omitted = append(st.Orca.Omitted, catalog.OmittedBinding(o))
	}
	return nil
}

func (j *Job) source(s claudenative.Session, now time.Time) sessionarchive.Source {
	config := s.ConfigDir
	src := sessionarchive.Source{
		SessionID:      string(s.ID),
		SourceHost:     j.cfg.Self,
		Home:           j.cfg.Home,
		TmpRoot:        j.cfg.Layout.TmpRoot,
		UID:            j.cfg.Layout.UID,
		ConfigDir:      config,
		ProjectDirName: s.ProjectDirName,
		TranscriptPath: s.TranscriptPath,
		PlanFiles:      s.Sidecars.Plans,
		ScratchpadDir:  s.Sidecars.Scratchpad,
		Cwd:            s.Cwd,
		OriginalCwd:    s.OriginalCwd,
		GitBranch:      s.GitBranch,
		Title:          s.Title,
		ClaudeVersion:  s.Version,
		LastHuman:      s.LastHumanInput,
		LastAutonomous: s.LastAutonomousActivity,
		CapturedAt:     now,
	}
	if s.Sidecars.SessionDir {
		src.SessionDir = strings.TrimSuffix(s.TranscriptPath, ".jsonl")
	}
	if s.Sidecars.FileHistory {
		src.FileHistoryDir = filepath.Join(config, "file-history", string(s.ID))
	}
	for _, list := range s.Sidecars.TaskLists {
		src.TaskListDirs = append(src.TaskListDirs, filepath.Join(config, "tasks", list))
	}
	for _, hash := range s.Sidecars.PasteHashes {
		src.PasteFiles = append(src.PasteFiles, filepath.Join(config, "paste-cache", hash+".txt"))
	}
	return src
}

func (j *Job) catalogWorktree(wt worktree.Worktree, st state) catalog.Worktree {
	cw := catalog.Worktree{
		ID:   wt.ID,
		Repo: catalog.Repo{Origin: wt.Origin, RelPath: wt.Relpath, Branch: wt.Branch, SourcePath: wt.Root},
	}
	if st.Orca != nil {
		orca := st.Orca.Summary
		cw.Orca = &orca
	}
	return cw
}

func (j *Job) checkpoint(t inventory.Target, st state, code codeResult, root artifact.Ref, now time.Time) catalog.Checkpoint {
	cp := catalog.Checkpoint{Root: root, CapturedAt: now, Deferred: code.state}
	if st.Code != nil {
		cp.Code = st.Code.Summary
	}
	if st.Orca != nil {
		cp.Omitted = st.Orca.Omitted
	}
	missing := code.named
	complete := code.complete()
	for _, s := range t.Sessions {
		cp.SourceActivityAt = later(cp.SourceActivityAt, s.LastActivity)
		tier, _ := j.cfg.Tiers.Classify(now, s.LastHumanInput, time.Time{}, s.LastAutonomousActivity, s.LastActivity)
		cp.Sessions = append(cp.Sessions, catalog.Session{
			ID:                string(s.ID),
			Title:             s.Title,
			LastActivity:      s.LastActivity,
			LastHumanActivity: s.LastHumanInput,
			Activity:          activity(tier),
			ClaudeVersion:     s.Version,
		})
		c := st.Sessions[string(s.ID)].Manifest.Completeness
		prefix := "session:" + string(s.ID) + ":"
		missing = append(missing, named(prefix+"missing:", c.Missing)...)
		missing = append(missing, named(prefix+"deferred:", c.Deferred)...)
		complete = complete && len(c.Missing) == 0 && len(c.Deferred) == 0
	}
	if cp.SourceActivityAt.IsZero() {
		cp.SourceActivityAt = now
	}
	cp.Completeness = catalog.Completeness{Complete: complete, Missing: missing}
	return cp
}

func activity(t scheduler.Tier) string {
	switch t {
	case scheduler.TierHuman:
		return "human"
	case scheduler.TierAutonomous:
		return "autonomous"
	}
	return "idle"
}

func named(prefix string, items []string) []string {
	out := make([]string, 0, min(len(items), maxNamed+1))
	for _, item := range items[:min(len(items), maxNamed)] {
		out = append(out, prefix+item)
	}
	if extra := len(items) - maxNamed; extra > 0 {
		out = append(out, fmt.Sprintf("%s+%d more", prefix, extra))
	}
	return out
}

func combine(meta, code string) string {
	sum := sha256.Sum256([]byte(meta + "\x00" + code))
	return hex.EncodeToString(sum[:])
}

func groupTree(ctx context.Context, store Store, media string, refs []artifact.Ref) (artifact.Ref, error) {
	for len(refs) > artifact.MaxDeps {
		level := make([]artifact.Ref, 0, len(refs)/artifact.MaxDeps+1)
		for chunk := range slices.Chunk(refs, artifact.MaxDeps) {
			group, err := store.PutGroup(ctx, media, chunk)
			if err != nil {
				return artifact.Ref{}, err
			}
			level = append(level, group)
		}
		refs = level
	}
	return store.PutGroup(ctx, media, refs)
}

func later(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

type recorder struct {
	Store
	mu   sync.Mutex
	seen map[artifact.Digest]bool
	refs []artifact.Ref
}

func (r *recorder) Put(ctx context.Context, rd io.Reader, media string) (artifact.Ref, error) {
	ref, err := r.Store.Put(ctx, rd, media)
	if err != nil {
		return artifact.Ref{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.seen[ref.Digest] {
		r.seen[ref.Digest] = true
		r.refs = append(r.refs, ref)
	}
	return ref, nil
}
