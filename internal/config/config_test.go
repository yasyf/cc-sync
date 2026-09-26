package config

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-sync/internal/scheduler"
)

func TestLoad(t *testing.T) {
	defaults := scheduler.Tiers{
		HumanInterval:      2 * time.Minute,
		AutonomousInterval: 5 * time.Minute,
		RecentInterval:     15 * time.Minute,
		IdleInterval:       time.Hour,
		HumanWindow:        15 * time.Minute,
		AutonomousWindow:   15 * time.Minute,
		RecentWindow:       time.Hour,
	}
	with := func(fn func(*scheduler.Tiers)) scheduler.Tiers {
		t := defaults
		fn(&t)
		return t
	}
	tests := []struct {
		name    string
		file    *string
		want    scheduler.Tiers
		wantErr string
	}{
		{name: "absent file", file: nil, want: defaults},
		{name: "empty object", file: ptr(`{}`), want: defaults},
		{name: "empty capture", file: ptr(`{"capture": {}}`), want: defaults},
		{name: "null capture", file: ptr(`{"capture": null}`), want: defaults},
		{
			name: "one override",
			file: ptr(`{"capture": {"human_interval": "1m"}}`),
			want: with(func(t *scheduler.Tiers) { t.HumanInterval = time.Minute }),
		},
		{
			name: "every override",
			file: ptr(`{"capture": {
				"human_interval": "30s", "autonomous_interval": "90s", "recent_interval": "10m", "idle_interval": "2h",
				"human_window": "5m", "autonomous_window": "20m", "recent_window": "3h"
			}}`),
			want: scheduler.Tiers{
				HumanInterval:      30 * time.Second,
				AutonomousInterval: 90 * time.Second,
				RecentInterval:     10 * time.Minute,
				IdleInterval:       2 * time.Hour,
				HumanWindow:        5 * time.Minute,
				AutonomousWindow:   20 * time.Minute,
				RecentWindow:       3 * time.Hour,
			},
		},
		{
			name: "windows equal",
			file: ptr(`{"capture": {"human_window": "1h", "autonomous_window": "1h"}}`),
			want: with(func(t *scheduler.Tiers) { t.HumanWindow, t.AutonomousWindow = time.Hour, time.Hour }),
		},
		{name: "unknown top-level key", file: ptr(`{"captures": {}}`), wantErr: `json: unknown field "captures"`},
		{name: "unknown capture key", file: ptr(`{"capture": {"human": "1m"}}`), wantErr: `json: unknown field "human"`},
		{name: "unparseable duration", file: ptr(`{"capture": {"idle_interval": "soon"}}`), wantErr: `parse duration "soon"`},
		{name: "numeric duration", file: ptr(`{"capture": {"idle_interval": 3600}}`), wantErr: "decode duration"},
		{name: "null duration", file: ptr(`{"capture": {"idle_interval": null}}`), wantErr: `parse duration ""`},
		{name: "zero interval", file: ptr(`{"capture": {"recent_interval": "0s"}}`), wantErr: "invalid capture tiers: recent_interval 0s is not positive"},
		{name: "negative window", file: ptr(`{"capture": {"autonomous_window": "-5m"}}`), wantErr: "invalid capture tiers: autonomous_window -5m0s is not positive"},
		{name: "human window past recent", file: ptr(`{"capture": {"human_window": "2h"}}`), wantErr: "invalid capture tiers: human_window 2h0m0s exceeds recent_window 1h0m0s"},
		{name: "recent window under autonomous", file: ptr(`{"capture": {"human_window": "5m", "recent_window": "10m"}}`), wantErr: "invalid capture tiers: autonomous_window 15m0s exceeds recent_window 10m0s"},
		{name: "trailing data", file: ptr(`{} {}`), wantErr: "trailing data after the config object"},
		{name: "malformed", file: ptr(`{"capture": `), wantErr: "unexpected EOF"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			if tt.file != nil {
				if err := os.WriteFile(path, []byte(*tt.file), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			cfg, err := Load(path)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("Load() error = %v, want one containing %q", err, tt.wantErr)
				}
				if !errors.Is(err, ErrInvalid) {
					t.Errorf("Load() error = %v, want it to wrap ErrInvalid", err)
				}
				if strings.HasPrefix(tt.wantErr, "invalid capture tiers") && !errors.Is(err, scheduler.ErrInvalidTiers) {
					t.Errorf("Load() error = %v, want it to wrap ErrInvalidTiers", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if got := cfg.Capture.Scheduler(); got != tt.want {
				t.Errorf("Load().Capture.Scheduler() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestLoadUnreadable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil || !strings.HasPrefix(err.Error(), "config: read ") {
		t.Errorf("Load() error = %v, want a read config error", err)
	}
}

func TestDefaultJSON(t *testing.T) {
	got, err := json.Marshal(Config{Capture: DefaultTiers})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"capture":{"human_interval":"2m0s","autonomous_interval":"5m0s","recent_interval":"15m0s","idle_interval":"1h0m0s",` +
		`"human_window":"15m0s","autonomous_window":"15m0s","recent_window":"1h0m0s"}}`
	if string(got) != want {
		t.Errorf("json =\n%s\nwant\n%s", got, want)
	}
	if tiers := DefaultTiers.Scheduler(); tiers != scheduler.DefaultTiers() {
		t.Errorf("DefaultTiers.Scheduler() = %+v, want scheduler.DefaultTiers()", tiers)
	}
}

func TestResolveHonorsDirEnv(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(DirEnv, dir)
	got, err := Resolve()
	if err != nil {
		t.Fatal(err)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	want := Layout{
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
		CheckoutRoot: filepath.Join(home, ".cc-sync", "checkouts"),
	}
	if got != want {
		t.Errorf("Resolve = %+v, want %+v", got, want)
	}
}

func TestEnsureCreatesPrivateDirs(t *testing.T) {
	l := At(filepath.Join(t.TempDir(), "cc-sync"), t.TempDir())
	if err := l.Ensure(); err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{l.Dir, l.StampDir, l.CodeStore, l.CodeIndex, l.ReplicaRoot, l.JournalDir} {
		info, err := os.Stat(dir)
		if err != nil {
			t.Fatalf("stat %s: %v", dir, err)
		}
		if !info.IsDir() || info.Mode().Perm() != 0o700 {
			t.Errorf("%s mode = %v, want a 0700 dir", dir, info.Mode())
		}
	}
}

func ptr(s string) *string { return &s }
