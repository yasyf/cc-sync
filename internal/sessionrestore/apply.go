package sessionrestore

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strconv"

	"github.com/yasyf/cc-sync/internal/claudenative"
)

const (
	stagingDirName = ".cc-sync-staging"
	journalName    = "journal.json"
)

// Result reports what Apply changed.
type Result struct {
	Installed []string `json:"installed"`
	Skipped   []string `json:"skipped,omitempty"`
	Displaced []string `json:"displaced,omitempty"`
}

type moveKind string

const (
	moveDisplace moveKind = "displace"
	moveInstall  moveKind = "install"
)

type move struct {
	Kind   moveKind `json:"kind"`
	From   string   `json:"from"`
	To     string   `json:"to"`
	Backup string   `json:"backup,omitempty"`
}

type journal struct {
	SessionID claudenative.SessionID `json:"session_id"`
	Committed bool                   `json:"committed"`
	Scratch   []string               `json:"scratch"`
	Created   []string               `json:"created"`
	Moves     []move                 `json:"moves"`
	path      string
}

// Apply installs p: it stages every unit, journals the moves, displaces
// divergent local copies, swaps sidecars into place, and renames the
// transcript last. Any failure rolls every move back.
func Apply(ctx context.Context, p Plan) (Result, error) {
	return applier{rename: os.Rename}.apply(ctx, p, true)
}

// Recover finishes every journal a crashed Apply left under the layout's
// staging dir: committed installs are cleaned up, the rest rolled back.
// Call it only while no Apply runs against the layout.
func Recover(ctx context.Context, l claudenative.Layout) error {
	root := filepath.Join(l.ConfigDir, stagingDirName)
	entries, err := os.ReadDir(root)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read staging: %w", err)
	}
	var errs []error
	for _, e := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		dir := filepath.Join(root, e.Name())
		j, err := readJournal(filepath.Join(dir, journalName))
		if errors.Is(err, fs.ErrNotExist) {
			errs = append(errs, os.RemoveAll(dir))
			continue
		}
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if !j.Committed {
			errs = append(errs, j.rollback(os.Rename))
		}
		errs = append(errs, j.cleanup())
	}
	return errors.Join(errs...)
}

type applier struct {
	rename func(from, to string) error
}

func (a applier) apply(ctx context.Context, p Plan, rollback bool) (Result, error) {
	if p.Mode == ModeKeepLocal {
		return Result{}, nil
	}
	nonce := make([]byte, 8)
	if _, err := rand.Read(nonce); err != nil {
		return Result{}, fmt.Errorf("staging nonce: %w", err)
	}
	stage := filepath.Join(p.Layout.ConfigDir, stagingDirName, hex.EncodeToString(nonce))
	j := &journal{SessionID: p.SessionID, path: filepath.Join(stage, journalName)}
	scratchStage := filepath.Join(p.Layout.TmpRoot, "claude-"+strconv.Itoa(p.Layout.UID), stagingDirName, hex.EncodeToString(nonce))
	roots := []string{stage}
	for _, u := range p.Installs {
		if u.Kind == UnitScratchpad {
			roots = append(roots, scratchStage)
		}
	}
	for _, root := range roots {
		j.Scratch = append(j.Scratch, missingDirs(root)...)
		if err := os.MkdirAll(root, 0o700); err != nil {
			return Result{}, errors.Join(fmt.Errorf("create staging: %w", err), j.cleanup())
		}
	}
	if err := j.write(); err != nil {
		return Result{}, errors.Join(err, j.cleanup())
	}
	res, err := a.stage(ctx, p, j, stage, scratchStage)
	if err != nil {
		return Result{}, errors.Join(err, j.rollback(os.Rename), j.cleanup())
	}
	if err := a.commit(j); err != nil {
		if !rollback {
			return Result{}, err
		}
		return Result{}, errors.Join(err, j.rollback(os.Rename), j.cleanup())
	}
	return res, j.cleanup()
}

func (a applier) stage(ctx context.Context, p Plan, j *journal, stage, scratchStage string) (Result, error) {
	var res Result
	for _, d := range p.Displace {
		j.Moves = append(j.Moves, move{Kind: moveDisplace, From: d.From, To: d.To})
		res.Displaced = append(res.Displaced, d.To)
	}
	stats := relocation{}
	rel := p.relocator(&stats)
	for i, u := range p.Installs {
		if err := ctx.Err(); err != nil {
			return Result{}, err
		}
		exists, err := lexists(u.Dest)
		if err != nil {
			return Result{}, err
		}
		displaced := false
		for _, d := range p.Displace {
			displaced = displaced || d.From == u.Dest
		}
		if exists && !u.Replace && !displaced {
			res.Skipped = append(res.Skipped, u.Dest)
			continue
		}
		root := stage
		if u.Kind == UnitScratchpad {
			root = scratchStage
		}
		m := move{Kind: moveInstall, From: filepath.Join(root, "new", strconv.Itoa(i)), To: u.Dest}
		if exists && !displaced {
			m.Backup = filepath.Join(root, "old", strconv.Itoa(i))
		}
		for _, dir := range []string{filepath.Join(root, "new"), filepath.Join(root, "old")} {
			if err := os.MkdirAll(dir, 0o700); err != nil {
				return Result{}, fmt.Errorf("create staging: %w", err)
			}
		}
		if err := p.materialize(u, diskSink{root: m.From}, rel); err != nil {
			return Result{}, err
		}
		j.Moves = append(j.Moves, m)
		res.Installed = append(res.Installed, u.Dest)
	}
	for _, m := range j.Moves {
		j.Created = append(j.Created, missingDirs(filepath.Dir(m.To))...)
	}
	if err := j.write(); err != nil {
		return Result{}, err
	}
	for _, m := range j.Moves {
		if err := os.MkdirAll(filepath.Dir(m.To), 0o700); err != nil {
			return Result{}, fmt.Errorf("create %s: %w", filepath.Dir(m.To), err)
		}
	}
	return res, nil
}

func (a applier) commit(j *journal) error {
	for _, m := range j.Moves {
		if m.Backup != "" {
			if err := a.rename(m.To, m.Backup); err != nil {
				return fmt.Errorf("set aside %s: %w", m.To, err)
			}
		}
		if err := a.rename(m.From, m.To); err != nil {
			return fmt.Errorf("move %s into place: %w", m.To, err)
		}
	}
	j.Committed = true
	return j.write()
}

func (j *journal) rollback(rename func(from, to string) error) error {
	var errs []error
	for i := len(j.Moves) - 1; i >= 0; i-- {
		m := j.Moves[i]
		fromExists, err := lexists(m.From)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		toExists, err := lexists(m.To)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		switch m.Kind {
		case moveDisplace:
			if toExists && !fromExists {
				errs = append(errs, rename(m.To, m.From))
			}
		case moveInstall:
			if toExists && !fromExists {
				errs = append(errs, os.RemoveAll(m.To))
			}
			if m.Backup == "" {
				continue
			}
			backup, err := lexists(m.Backup)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			if backup {
				errs = append(errs, rename(m.Backup, m.To))
			}
		}
	}
	for i := len(j.Created) - 1; i >= 0; i-- {
		removeEmpty(j.Created[i])
	}
	return errors.Join(errs...)
}

func (j *journal) cleanup() error {
	var errs []error
	for _, dir := range j.Scratch {
		if filepath.Base(filepath.Dir(dir)) == stagingDirName {
			errs = append(errs, os.RemoveAll(dir))
		}
	}
	for i := len(j.Scratch) - 1; i >= 0; i-- {
		removeEmpty(j.Scratch[i])
	}
	return errors.Join(errs...)
}

func (j *journal) write() error {
	b, err := json.Marshal(j)
	if err != nil {
		return fmt.Errorf("encode journal: %w", err)
	}
	tmp := j.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600) //nolint:gosec // G304: the journal inside this Apply's staging dir.
	if err != nil {
		return fmt.Errorf("write journal: %w", err)
	}
	if _, err := f.Write(b); err != nil {
		_ = f.Close()
		return fmt.Errorf("write journal: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("sync journal: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close journal: %w", err)
	}
	if err := os.Rename(tmp, j.path); err != nil {
		return fmt.Errorf("commit journal: %w", err)
	}
	return nil
}

func readJournal(p string) (*journal, error) {
	b, err := os.ReadFile(p) //nolint:gosec // G304: a journal under the layout's staging dir.
	if err != nil {
		return nil, fmt.Errorf("read journal: %w", err)
	}
	j := &journal{path: p}
	if err := json.Unmarshal(b, j); err != nil {
		return nil, fmt.Errorf("decode journal %s: %w", p, err)
	}
	return j, nil
}

func missingDirs(dir string) []string {
	var out []string
	for {
		if _, err := os.Lstat(dir); err == nil {
			break
		}
		out = append([]string{dir}, out...)
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return out
}

func removeEmpty(dir string) {
	_ = os.Remove(dir)
}

func lexists(p string) (bool, error) {
	_, err := os.Lstat(p)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, fs.ErrNotExist):
		return false, nil
	}
	return false, fmt.Errorf("stat %s: %w", p, err)
}

type sink interface {
	dir(rel string, mode fs.FileMode) error
	file(rel string, mode fs.FileMode, write func(io.Writer) error) error
	link(rel, target string) error
}

type discardSink struct{}

func (discardSink) dir(string, fs.FileMode) error { return nil }

func (discardSink) file(_ string, _ fs.FileMode, write func(io.Writer) error) error {
	return write(io.Discard)
}

func (discardSink) link(string, string) error { return nil }

type diskSink struct{ root string }

func (s diskSink) path(rel string) string {
	return filepath.Join(s.root, rel)
}

func (s diskSink) dir(rel string, mode fs.FileMode) error {
	if err := os.Mkdir(s.path(rel), mode.Perm()|0o700); err != nil {
		return fmt.Errorf("stage dir: %w", err)
	}
	return nil
}

func (s diskSink) file(rel string, mode fs.FileMode, write func(io.Writer) error) error {
	f, err := os.OpenFile(s.path(rel), os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode.Perm()|0o600)
	if err != nil {
		return fmt.Errorf("stage file: %w", err)
	}
	if err := write(f); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("sync staged file: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close staged file: %w", err)
	}
	return nil
}

func (s diskSink) link(rel, target string) error {
	if err := os.Symlink(target, s.path(rel)); err != nil {
		return fmt.Errorf("stage symlink: %w", err)
	}
	return nil
}

func (p *Plan) materialize(u Install, s sink, r *relocator) error {
	if u.Kind == UnitTranscript {
		r.begin(transcriptFile)
		return s.file(".", 0o600, func(w io.Writer) error {
			f, err := os.Open(u.Source)
			if err != nil {
				return fmt.Errorf("open replica transcript: %w", err)
			}
			defer func() { _ = f.Close() }()
			if err := r.jsonl(f, w); err != nil {
				return err
			}
			if _, err := w.Write(p.relocatedSuffix()); err != nil {
				return fmt.Errorf("append relocated record: %w", err)
			}
			return nil
		})
	}
	return filepath.WalkDir(u.Source, func(src string, d fs.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("walk replica: %w", err)
		}
		rel, err := filepath.Rel(u.Source, src)
		if err != nil {
			return fmt.Errorf("walk replica: %w", err)
		}
		name, err := filepath.Rel(p.Replica, src)
		if err != nil {
			return fmt.Errorf("walk replica: %w", err)
		}
		r.begin(filepath.ToSlash(name))
		info, err := d.Info()
		if err != nil {
			return fmt.Errorf("stat replica %s: %w", name, err)
		}
		switch {
		case d.Type()&fs.ModeSymlink != 0:
			target, err := os.Readlink(src)
			if err != nil {
				return fmt.Errorf("read replica link %s: %w", name, err)
			}
			return s.link(rel, r.link(target))
		case d.IsDir():
			return s.dir(rel, info.Mode())
		}
		return s.file(rel, info.Mode(), func(w io.Writer) error {
			f, err := os.Open(src) //nolint:gosec // G304: a file inside the replica being restored.
			if err != nil {
				return fmt.Errorf("open replica %s: %w", name, err)
			}
			defer func() { _ = f.Close() }()
			slash := filepath.ToSlash(rel)
			switch {
			case u.Kind == UnitSession && path.Ext(slash) == ".jsonl":
				return r.jsonl(f, w)
			case u.Kind == UnitSession && path.Dir(slash) == "workflows" && path.Ext(slash) == ".json":
				return r.document(f, w, workflowFields)
			}
			if _, err := io.Copy(w, f); err != nil {
				return fmt.Errorf("copy replica %s: %w", name, err)
			}
			return nil
		})
	})
}
