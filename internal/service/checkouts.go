package service

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/yasyf/cc-sync/internal/catalog"
	"github.com/yasyf/cc-sync/internal/cli"
	"github.com/yasyf/cc-sync/internal/pickup"
)

// CheckoutDir finds pickup's default recovery checkouts under Root, placed by
// pickup.CheckoutLocation; the newest one wins and is reusable when it is
// still a git checkout.
type CheckoutDir struct {
	Root string
}

// Find returns the newest recovery checkout of w captured on source.
func (d CheckoutDir) Find(source string, w catalog.Worktree) (*cli.LocalCheckout, error) {
	dir, prefix, err := pickup.CheckoutLocation(d.Root, source, w)
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read checkouts %s: %w", dir, err)
	}
	for i := len(entries) - 1; i >= 0; i-- {
		e := entries[i]
		stamp, ok := strings.CutPrefix(e.Name(), prefix)
		if !e.IsDir() || !ok {
			continue
		}
		if _, err := time.Parse(pickup.CheckoutStamp, stamp); err != nil {
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
