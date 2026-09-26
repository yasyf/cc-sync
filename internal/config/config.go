// Package config resolves cc-sync's on-disk layout and loads the user
// configuration in <Dir>/config.json.
package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/yasyf/synckit/codec"
	"github.com/yasyf/synckit/hostregistry"

	"github.com/yasyf/cc-sync/internal/scheduler"
)

const (
	// ToolName names cc-sync's config directory and synckit service.
	ToolName = "cc-sync"
	// DirEnv overrides the config directory verbatim.
	DirEnv = "CC_SYNC_CONFIG_DIR"
)

// ErrInvalid reports a config.json that does not describe a valid config.
var ErrInvalid = errors.New("config: invalid")

// Layout is cc-sync's on-disk layout. StampDir is the only directory synckit
// watches; CodeStore is the reposync worktree.Store root.
type Layout struct {
	Dir          string
	StampDir     string
	CatalogPath  string
	LedgerPath   string
	PinsPath     string
	ConfigPath   string
	CodeStore    string
	CodeIndex    string
	ReplicaRoot  string
	JournalDir   string
	CheckoutRoot string
}

// Tiers are the capture cadences and the activity windows that earn them.
type Tiers struct {
	HumanInterval      codec.Duration `json:"human_interval"`
	AutonomousInterval codec.Duration `json:"autonomous_interval"`
	RecentInterval     codec.Duration `json:"recent_interval"`
	IdleInterval       codec.Duration `json:"idle_interval"`
	HumanWindow        codec.Duration `json:"human_window"`
	AutonomousWindow   codec.Duration `json:"autonomous_window"`
	RecentWindow       codec.Duration `json:"recent_window"`
}

// Config is the decoded config.json.
type Config struct {
	Capture Tiers `json:"capture"`
}

// DefaultTiers are the approved capture cadences.
var DefaultTiers = TiersOf(scheduler.DefaultTiers())

// TiersOf renders scheduler tiers as their config.json form.
func TiersOf(t scheduler.Tiers) Tiers {
	return Tiers{
		HumanInterval:      codec.Duration(t.HumanInterval),
		AutonomousInterval: codec.Duration(t.AutonomousInterval),
		RecentInterval:     codec.Duration(t.RecentInterval),
		IdleInterval:       codec.Duration(t.IdleInterval),
		HumanWindow:        codec.Duration(t.HumanWindow),
		AutonomousWindow:   codec.Duration(t.AutonomousWindow),
		RecentWindow:       codec.Duration(t.RecentWindow),
	}
}

// Scheduler converts the tiers to the scheduler's form.
func (t Tiers) Scheduler() scheduler.Tiers {
	return scheduler.Tiers{
		HumanInterval:      time.Duration(t.HumanInterval),
		AutonomousInterval: time.Duration(t.AutonomousInterval),
		RecentInterval:     time.Duration(t.RecentInterval),
		IdleInterval:       time.Duration(t.IdleInterval),
		HumanWindow:        time.Duration(t.HumanWindow),
		AutonomousWindow:   time.Duration(t.AutonomousWindow),
		RecentWindow:       time.Duration(t.RecentWindow),
	}
}

// Resolve returns the layout under the config directory: DirEnv when set,
// otherwise ~/.config/cc-sync, with recovered checkouts under
// ~/.cc-sync/checkouts.
func Resolve() (Layout, error) {
	dir, err := hostregistry.Config{Name: ToolName, DirEnv: DirEnv}.Dir()
	if err != nil {
		return Layout{}, fmt.Errorf("config: resolve dir: %w", err)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return Layout{}, fmt.Errorf("config: resolve home: %w", err)
	}
	return At(dir, filepath.Join(home, ".cc-sync", "checkouts")), nil
}

// At returns the layout rooted at dir with checkouts under checkoutRoot.
func At(dir, checkoutRoot string) Layout {
	return Layout{
		Dir:          dir,
		StampDir:     filepath.Join(dir, "stamp"),
		CatalogPath:  filepath.Join(dir, "catalog-v1.json"),
		LedgerPath:   filepath.Join(dir, "transfer-v1.json"),
		PinsPath:     filepath.Join(dir, "pins-v1.json"),
		ConfigPath:   filepath.Join(dir, "config.json"),
		CodeStore:    filepath.Join(dir, "reposync"),
		CodeIndex:    filepath.Join(dir, "codesnap"),
		ReplicaRoot:  filepath.Join(dir, "replicas"),
		JournalDir:   filepath.Join(dir, "journal"),
		CheckoutRoot: checkoutRoot,
	}
}

// Ensure creates every directory the resident owns.
func (l Layout) Ensure() error {
	for _, dir := range []string{l.Dir, l.StampDir, l.CodeStore, l.CodeIndex, l.ReplicaRoot, l.JournalDir} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return fmt.Errorf("config: create %s: %w", dir, err)
		}
	}
	return nil
}

// Load reads path strictly over the defaults; a missing file is the defaults.
func Load(path string) (Config, error) {
	cfg := Config{Capture: DefaultTiers}
	data, err := os.ReadFile(path) //nolint:gosec // G304: the layout's own config.json, read-only.
	if errors.Is(err, os.ErrNotExist) {
		return cfg, nil
	}
	if err != nil {
		return Config{}, fmt.Errorf("config: read %s: %w", path, err)
	}
	if err := decode(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("%w: %s: %w", ErrInvalid, path, err)
	}
	if err := cfg.Capture.Validate(); err != nil {
		return Config{}, fmt.Errorf("%s: %w", path, err)
	}
	return cfg, nil
}

// Validate reports ErrInvalid wrapping scheduler.ErrInvalidTiers unless every
// duration is positive and the human and autonomous windows fit within the
// recent window.
func (t Tiers) Validate() error {
	if err := t.Scheduler().Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	return nil
}

func decode(data []byte, cfg *Config) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(cfg); err != nil {
		return err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return errors.New("trailing data after the config object")
	}
	return nil
}
