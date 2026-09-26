package service

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/yasyf/cc-sync/internal/catalog"
	"github.com/yasyf/cc-sync/internal/cli"
	"github.com/yasyf/synckit/hostregistry"
)

// CheckoutDir finds pickup's default recovery checkouts under Root, named
// <Root>/<repo relpath>/<source host>-<worktree name>-<YYYYMMDD-HHMM>; the
// newest one wins and is reusable when it is still a git checkout.
type CheckoutDir struct {
	Root string
}

// Find returns the newest recovery checkout of w captured on source.
func (d CheckoutDir) Find(source string, w catalog.Worktree) (*cli.LocalCheckout, error) {
	dir := filepath.Join(d.Root, filepath.FromSlash(w.Repo.RelPath))
	entries, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read checkouts %s: %w", dir, err)
	}
	prefix := hostregistry.HostNode(source) + "-" + filepath.Base(w.Repo.SourcePath) + "-"
	for i := len(entries) - 1; i >= 0; i-- {
		e := entries[i]
		if !e.IsDir() || !strings.HasPrefix(e.Name(), prefix) {
			continue
		}
		path := filepath.Join(dir, e.Name())
		_, err := os.Lstat(filepath.Join(path, ".git"))
		switch {
		case errors.Is(err, fs.ErrNotExist):
			return &cli.LocalCheckout{Path: path}, nil
		case err != nil:
			return nil, fmt.Errorf("inspect checkout %s: %w", path, err)
		}
		return &cli.LocalCheckout{Path: path, Reusable: true}, nil
	}
	return nil, nil
}
