package capture

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/yasyf/cc-sync/internal/catalog"
	"github.com/yasyf/cc-sync/internal/claudenative"
	"github.com/yasyf/cc-sync/internal/sessionarchive"
	"github.com/yasyf/daemonkit/durable"
	"github.com/yasyf/reposync/worktree"
	"github.com/yasyf/synckit/artifact"
)

const stateSuffix = ".json"

type state struct {
	Stamp      string                            `json:"stamp,omitempty"`
	Checkpoint string                            `json:"checkpoint,omitempty"`
	Deps       []artifact.Ref                    `json:"deps,omitempty"`
	Code       *codeState                        `json:"code,omitempty"`
	Orca       *orcaState                        `json:"orca,omitempty"`
	Sessions   map[string]sessionarchive.Archive `json:"sessions,omitempty"`
	Partial    *partialPins                      `json:"partial,omitempty"`
}

type codeState struct {
	Root    artifact.Ref     `json:"root"`
	Summary worktree.Summary `json:"summary"`
}

type orcaState struct {
	Descriptor artifact.Ref `json:"descriptor"`
	Summary    catalog.Orca `json:"summary"`
}

type partialPins struct {
	Roots []artifact.Ref `json:"roots"`
	Since time.Time      `json:"since"`
}

func (s state) Validate() error {
	refs := append([]artifact.Ref(nil), s.Deps...)
	if s.Code != nil {
		refs = append(refs, s.Code.Root)
	}
	if s.Orca != nil {
		refs = append(refs, s.Orca.Descriptor)
	}
	if s.Partial != nil {
		refs = append(refs, s.Partial.Roots...)
	}
	for _, r := range refs {
		if err := r.Validate(); err != nil {
			return fmt.Errorf("capture state: %w", err)
		}
	}
	return nil
}

func (s state) deps(sessions []claudenative.Session) []artifact.Ref {
	var deps []artifact.Ref
	if s.Code != nil {
		deps = append(deps, s.Code.Root)
	}
	if s.Orca != nil {
		deps = append(deps, s.Orca.Descriptor)
	}
	for _, sess := range sessions {
		deps = append(deps, s.Sessions[string(sess.ID)].Ref)
	}
	return deps
}

func (j *Job) revalidate(ctx context.Context, st *state) error {
	if len(st.Deps) == 0 {
		return nil
	}
	digests := make([]artifact.Digest, len(st.Deps))
	for i, d := range st.Deps {
		digests[i] = d.Digest
	}
	missing, err := j.cfg.Store.Has(ctx, digests)
	if err != nil {
		return fmt.Errorf("check last checkpoint: %w", err)
	}
	if len(missing) > 0 {
		*st = state{Partial: st.Partial}
	}
	return nil
}

func (j *Job) lock(id string) func() {
	j.mu.Lock()
	l, ok := j.locks[id]
	if !ok {
		l = &sync.Mutex{}
		j.locks[id] = l
	}
	j.mu.Unlock()
	l.Lock()
	return l.Unlock
}

func (j *Job) path(id string) string {
	return filepath.Join(j.cfg.StateDir, id+stateSuffix)
}

func (j *Job) load(id string) (state, error) {
	st, err := durable.ReadFile[state](j.path(id))
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return state{}, nil
	case err != nil:
		return state{}, fmt.Errorf("read capture state of %s: %w", id, err)
	}
	return st, nil
}

func (j *Job) save(id string, st state) error {
	if err := os.MkdirAll(j.cfg.StateDir, 0o700); err != nil {
		return fmt.Errorf("create capture state dir: %w", err)
	}
	data, err := durable.Marshal(st)
	if err != nil {
		return fmt.Errorf("encode capture state of %s: %w", id, err)
	}
	if err := durable.WriteFile(j.path(id), data, 0o600); err != nil {
		return fmt.Errorf("write capture state of %s: %w", id, err)
	}
	return nil
}

func (j *Job) savePartial(id string, p *partialPins) error {
	st, err := j.load(id)
	if err != nil {
		return err
	}
	st.Partial = p
	return j.save(id, st)
}

func (j *Job) stateIDs() ([]string, error) {
	entries, err := os.ReadDir(j.cfg.StateDir)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("list capture state: %w", err)
	}
	var ids []string
	for _, e := range entries {
		if id, ok := strings.CutSuffix(e.Name(), stateSuffix); ok && e.Type().IsRegular() {
			ids = append(ids, id)
		}
	}
	return ids, nil
}
