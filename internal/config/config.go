// Package config loads cc-sync's optional <Dir>/config.json. An absent file
// or key keeps its default; an unknown key, trailing data, or an invalid
// value fails the load.
package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/yasyf/synckit/codec"

	"github.com/yasyf/cc-sync/internal/scheduler"
)

// FileName is the config file's name within cc-sync's state dir.
const FileName = "config.json"

// Config is the decoded config.json.
type Config struct {
	Capture Capture `json:"capture"`
}

// Capture is the "capture" key: the scheduler's tier intervals and activity
// windows as canonical Go duration strings.
type Capture struct {
	HumanInterval      codec.Duration `json:"human_interval"`
	AutonomousInterval codec.Duration `json:"autonomous_interval"`
	RecentInterval     codec.Duration `json:"recent_interval"`
	IdleInterval       codec.Duration `json:"idle_interval"`
	HumanWindow        codec.Duration `json:"human_window"`
	AutonomousWindow   codec.Duration `json:"autonomous_window"`
	RecentWindow       codec.Duration `json:"recent_window"`
}

// Default is the config an empty or absent config.json yields.
func Default() Config {
	t := scheduler.DefaultTiers()
	return Config{Capture: Capture{
		HumanInterval:      codec.Duration(t.HumanInterval),
		AutonomousInterval: codec.Duration(t.AutonomousInterval),
		RecentInterval:     codec.Duration(t.RecentInterval),
		IdleInterval:       codec.Duration(t.IdleInterval),
		HumanWindow:        codec.Duration(t.HumanWindow),
		AutonomousWindow:   codec.Duration(t.AutonomousWindow),
		RecentWindow:       codec.Duration(t.RecentWindow),
	}}
}

// Load reads dir's config.json over Default and validates the capture tiers.
func Load(dir string) (Config, error) {
	path := filepath.Join(dir, FileName)
	cfg := Default()
	data, err := os.ReadFile(path) //nolint:gosec // G304: the fixed config.json name under cc-sync's own state dir.
	if errors.Is(err, fs.ErrNotExist) {
		return cfg, nil
	}
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}
	if err := decode(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("decode %s: %w", path, err)
	}
	if err := cfg.Capture.Tiers().Validate(); err != nil {
		return Config{}, fmt.Errorf("load %s: %w", path, err)
	}
	return cfg, nil
}

// Tiers converts the capture key to the scheduler's tiers.
func (c Capture) Tiers() scheduler.Tiers {
	return scheduler.Tiers{
		HumanInterval:      time.Duration(c.HumanInterval),
		AutonomousInterval: time.Duration(c.AutonomousInterval),
		RecentInterval:     time.Duration(c.RecentInterval),
		IdleInterval:       time.Duration(c.IdleInterval),
		HumanWindow:        time.Duration(c.HumanWindow),
		AutonomousWindow:   time.Duration(c.AutonomousWindow),
		RecentWindow:       time.Duration(c.RecentWindow),
	}
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
