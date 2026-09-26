// Package catalog is cc-sync's durable checkpoint catalog: the per-origin
// payload peers exchange, local readiness, retention and expiry, tombstones,
// and the stamp that announces exported changes to synckit.
package catalog

import (
	"bytes"
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/yasyf/daemonkit/durable"
	"github.com/yasyf/reposync/worktree"
	"github.com/yasyf/synckit/artifact"
)

// Identity and Version name the exchanged payload schema.
const (
	Identity = "cc-sync-catalog-v1"
	Version  = 1
)

// ExpiryWindow is how long a checkpoint lives after its source activity.
// HourlyWindow and DailyWindow bound the hourly and daily retention tiers.
const (
	ExpiryWindow = 7 * 24 * time.Hour
	HourlyWindow = 24 * time.Hour
	DailyWindow  = 7 * 24 * time.Hour
)

// ErrInvalid marks every payload or state validation failure.
var ErrInvalid = errors.New("catalog: invalid")

// Class is one retention tier a checkpoint is kept for.
type Class string

// The retention tiers, in canonical order.
const (
	ClassLatest Class = "latest"
	ClassHourly Class = "hourly"
	ClassDaily  Class = "daily"
)

var classOrder = []Class{ClassLatest, ClassHourly, ClassDaily}

// Payload is the catalog one exporter sends: its own block and, verbatim,
// the newest block of every relayed origin whose unexpired roots are all
// artifact-complete on the exporter. AsOf is the time its roots derive at.
type Payload struct {
	Identity string    `json:"identity"`
	Version  uint64    `json:"version"`
	Exporter string    `json:"exporter"`
	AsOf     time.Time `json:"as_of"`
	Origins  []Origin  `json:"origins"`
}

// Origin is one source host's block. Only that host changes it, moving
// Revision to max(previous+1, now in Unix microseconds) on every change;
// relays copy it verbatim and never edit it.
type Origin struct {
	Origin     string      `json:"origin"`
	Revision   uint64      `json:"revision"`
	Worktrees  []Worktree  `json:"worktrees"`
	Tombstones []Tombstone `json:"tombstones"`
}

// Worktree is one captured worktree and its retained checkpoints, newest first.
type Worktree struct {
	ID          string       `json:"id"`
	Repo        Repo         `json:"repo"`
	Orca        *Orca        `json:"orca,omitempty"`
	Checkpoints []Checkpoint `json:"checkpoints"`
}

// Repo locates a worktree's repository on its source host.
type Repo struct {
	Origin     string `json:"origin"`
	RelPath    string `json:"relpath"`
	Branch     string `json:"branch,omitempty"`
	SourcePath string `json:"source_path"`
}

// Orca summarizes the Orca workspace a worktree belonged to.
type Orca struct {
	Kind       string    `json:"kind"`
	Name       string    `json:"name"`
	InstanceID string    `json:"instance_id"`
	Freshness  time.Time `json:"freshness"`
}

// Checkpoint is one captured state of a worktree: Root is the artifact group
// holding its code manifest, Orca descriptor, and session manifests.
type Checkpoint struct {
	ID               string           `json:"id"`
	Root             artifact.Ref     `json:"root"`
	Classes          []Class          `json:"classes"`
	CapturedAt       time.Time        `json:"captured_at"`
	SourceActivityAt time.Time        `json:"source_activity_at"`
	ExpiresAt        time.Time        `json:"expires_at"`
	Sessions         []Session        `json:"sessions"`
	Code             worktree.Summary `json:"code"`
	Deferred         string           `json:"deferred,omitempty"`
	Completeness     Completeness     `json:"completeness"`
	Omitted          []OmittedBinding `json:"omitted,omitempty"`
}

// Mixed reports whether cp pairs newer sessions with deferred, older code.
func (cp Checkpoint) Mixed() bool { return cp.Deferred != "" }

// Complete reports whether cp is a complete recovery point: its code is not
// deferred, every session artifact it references was archived, and Orca
// omitted no binding. Only complete checkpoints claim retention tiers, count
// as pick-up ready, or are reported durable on a peer.
func (cp Checkpoint) Complete() bool {
	return !cp.Mixed() && cp.Completeness.Complete && len(cp.Omitted) == 0
}

// Session summarizes one Claude session archived in a checkpoint.
type Session struct {
	ID                string    `json:"id"`
	Title             string    `json:"title,omitempty"`
	LastActivity      time.Time `json:"last_activity"`
	LastHumanActivity time.Time `json:"last_human_activity"`
	Activity          string    `json:"activity"`
	ClaudeVersion     string    `json:"claude_version,omitempty"`
}

// OmittedBinding is an agent session Orca bound to the worktree but left out
// of the checkpoint's descriptor, so pickup cannot restore it.
type OmittedBinding struct {
	Agent  string `json:"agent"`
	Key    string `json:"key"`
	ID     string `json:"id"`
	Reason string `json:"reason"`
}

// Completeness reports what a capture referenced but could not archive.
type Completeness struct {
	Complete bool     `json:"complete"`
	Missing  []string `json:"missing,omitempty"`
}

// Tombstone records that its origin removed worktree ID at Revision. It
// outlives the removed checkpoints so a stale relayed block cannot bring
// them back.
type Tombstone struct {
	ID        string    `json:"id"`
	Revision  uint64    `json:"revision"`
	DeletedAt time.Time `json:"deleted_at"`
	ExpiresAt time.Time `json:"expires_at"`
}

// CheckpointID derives a checkpoint's ID from its origin, worktree, and root.
func CheckpointID(origin, worktreeID string, root artifact.Ref) string {
	sum := sha256.Sum256([]byte(origin + "\x00" + worktreeID + "\x00" + string(root.Digest)))
	return hex.EncodeToString(sum[:])
}

// Validate checks identity, canonical order, and every block.
func (p Payload) Validate() error {
	if p.Identity != Identity || p.Version != Version {
		return fmt.Errorf("%w: payload identity %q version %d", ErrInvalid, p.Identity, p.Version)
	}
	if p.Exporter == "" || p.AsOf.IsZero() {
		return fmt.Errorf("%w: payload exporter %q as of %s", ErrInvalid, p.Exporter, p.AsOf)
	}
	return validateOrigins(p.Origins)
}

// Validate checks one block against its own revision.
func (o Origin) Validate() error {
	if o.Origin == "" || o.Revision == 0 {
		return fmt.Errorf("%w: origin %q revision %d", ErrInvalid, o.Origin, o.Revision)
	}
	if err := strictlyIncreasing(o.Worktrees, func(w Worktree) string { return w.ID }); err != nil {
		return fmt.Errorf("origin %s worktrees: %w", o.Origin, err)
	}
	if err := strictlyIncreasing(o.Tombstones, func(t Tombstone) string { return t.ID }); err != nil {
		return fmt.Errorf("origin %s tombstones: %w", o.Origin, err)
	}
	for _, w := range o.Worktrees {
		if err := w.validate(o.Origin); err != nil {
			return err
		}
	}
	for _, t := range o.Tombstones {
		if t.Revision == 0 || t.Revision > o.Revision || t.DeletedAt.IsZero() || t.ExpiresAt.Before(t.DeletedAt) {
			return fmt.Errorf("%w: origin %s tombstone %s", ErrInvalid, o.Origin, t.ID)
		}
		if _, live := findWorktree(o.Worktrees, t.ID); live {
			return fmt.Errorf("%w: origin %s tombstones live worktree %s", ErrInvalid, o.Origin, t.ID)
		}
	}
	return nil
}

func (w Worktree) validate(origin string) error {
	if w.ID == "" || w.Repo.Origin == "" || w.Repo.SourcePath == "" || len(w.Checkpoints) == 0 {
		return fmt.Errorf("%w: origin %s worktree %q", ErrInvalid, origin, w.ID)
	}
	if !slices.IsSortedFunc(w.Checkpoints, compareCheckpoints) {
		return fmt.Errorf("%w: origin %s worktree %s checkpoints are not newest first", ErrInvalid, origin, w.ID)
	}
	seen := make(map[string]bool, len(w.Checkpoints))
	for _, cp := range w.Checkpoints {
		if seen[cp.ID] {
			return fmt.Errorf("%w: origin %s worktree %s repeats checkpoint %s", ErrInvalid, origin, w.ID, cp.ID)
		}
		seen[cp.ID] = true
		if err := cp.validate(origin, w.ID); err != nil {
			return err
		}
	}
	return nil
}

func (cp Checkpoint) validate(origin, worktreeID string) error {
	if err := cp.Root.Validate(); err != nil {
		return fmt.Errorf("checkpoint %s root: %w", cp.ID, err)
	}
	switch {
	case cp.Root.Kind != artifact.KindManifest:
		return fmt.Errorf("%w: checkpoint %s root is a %s", ErrInvalid, cp.ID, cp.Root.Kind)
	case cp.ID != CheckpointID(origin, worktreeID, cp.Root):
		return fmt.Errorf("%w: checkpoint id %s does not derive from its root", ErrInvalid, cp.ID)
	case cp.CapturedAt.IsZero() || cp.SourceActivityAt.IsZero():
		return fmt.Errorf("%w: checkpoint %s lacks capture or activity time", ErrInvalid, cp.ID)
	case !cp.ExpiresAt.Equal(cp.SourceActivityAt.Add(ExpiryWindow)):
		return fmt.Errorf("%w: checkpoint %s expires_at is not source activity + %s", ErrInvalid, cp.ID, ExpiryWindow)
	case !canonicalClasses(cp.Classes):
		return fmt.Errorf("%w: checkpoint %s classes %v", ErrInvalid, cp.ID, cp.Classes)
	}
	if err := strictlyIncreasing(cp.Sessions, func(s Session) string { return s.ID }); err != nil {
		return fmt.Errorf("checkpoint %s sessions: %w", cp.ID, err)
	}
	return nil
}

func validateOrigins(origins []Origin) error {
	if err := strictlyIncreasing(origins, func(o Origin) string { return o.Origin }); err != nil {
		return fmt.Errorf("origins: %w", err)
	}
	for _, o := range origins {
		if err := o.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func strictlyIncreasing[T any](items []T, key func(T) string) error {
	for i, item := range items {
		if key(item) == "" {
			return fmt.Errorf("%w: empty key", ErrInvalid)
		}
		if i > 0 && key(items[i-1]) >= key(item) {
			return fmt.Errorf("%w: %q does not follow %q", ErrInvalid, key(item), key(items[i-1]))
		}
	}
	return nil
}

func canonicalClasses(classes []Class) bool {
	if len(classes) == 0 {
		return false
	}
	next := 0
	for _, c := range classes {
		i := slices.Index(classOrder, c)
		if i < next {
			return false
		}
		next = i + 1
	}
	return true
}

func compareCheckpoints(a, b Checkpoint) int {
	return cmp.Or(b.CapturedAt.Compare(a.CapturedAt), strings.Compare(a.ID, b.ID))
}

func findWorktree(worktrees []Worktree, id string) (int, bool) {
	return slices.BinarySearchFunc(worktrees, id, func(w Worktree, id string) int { return strings.Compare(w.ID, id) })
}

// Encode validates p and returns its canonical bytes.
func Encode(p Payload) ([]byte, error) {
	return durable.Marshal(p)
}

// Decode strictly decodes a payload: unknown fields, duplicate keys, invalid
// blocks, and any encoding other than the canonical one are refused.
func Decode(data []byte) (Payload, error) {
	p, err := durable.Unmarshal[Payload](data)
	if err != nil {
		return Payload{}, fmt.Errorf("catalog: decode payload: %w", err)
	}
	canonical, err := Encode(p)
	if err != nil {
		return Payload{}, err
	}
	if !bytes.Equal(canonical, data) {
		return Payload{}, fmt.Errorf("%w: payload is not canonically encoded", ErrInvalid)
	}
	return p, nil
}

// Roots derives the payload's artifact roots in delivery priority order: the
// checkpoints unexpired at AsOf, every worktree's latest first, then hourly,
// then daily, each by most recent human activity and then newest capture,
// capped at artifact.MaxRoots. Equal payloads always derive equal roots.
func Roots(p Payload) ([]artifact.Ref, error) {
	return rootsAt(p.Origins, p.AsOf)
}

func rootsAt(origins []Origin, at time.Time) ([]artifact.Ref, error) {
	type ranked struct {
		cp    Checkpoint
		class int
		human time.Time
	}
	var all []ranked
	for _, o := range origins {
		for _, cp := range live(o, at) {
			r := ranked{cp: cp, class: slices.Index(classOrder, cp.Classes[0])}
			for _, s := range cp.Sessions {
				if s.LastHumanActivity.After(r.human) {
					r.human = s.LastHumanActivity
				}
			}
			all = append(all, r)
		}
	}
	slices.SortFunc(all, func(a, b ranked) int {
		return cmp.Or(cmp.Compare(a.class, b.class), b.human.Compare(a.human), compareCheckpoints(a.cp, b.cp))
	})
	roots := make([]artifact.Ref, 0, min(len(all), artifact.MaxRoots))
	seen := make(map[artifact.Digest]bool, len(all))
	for _, r := range all {
		if len(roots) == artifact.MaxRoots {
			break
		}
		if seen[r.cp.Root.Digest] {
			continue
		}
		seen[r.cp.Root.Digest] = true
		roots = append(roots, r.cp.Root)
	}
	if err := artifact.ValidateRoots(roots); err != nil {
		return nil, fmt.Errorf("catalog: roots: %w", err)
	}
	return roots, nil
}
