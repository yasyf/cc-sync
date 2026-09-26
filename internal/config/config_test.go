package config

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/synckit/codec"
)

func TestLoad(t *testing.T) {
	overridden := DefaultTiers
	overridden.HumanInterval = codec.Duration(time.Minute)
	overridden.RecentWindow = codec.Duration(2 * time.Hour)
	tests := []struct {
		name    string
		body    *string
		want    Tiers
		wantErr error
	}{
		{name: "missing file", want: DefaultTiers},
		{name: "empty object", body: new("{}"), want: DefaultTiers},
		{name: "partial override", body: new(`{"capture":{"human_interval":"1m","recent_window":"2h"}}`), want: overridden},
		{name: "unknown key", body: new(`{"capture":{"human_intervals":"1m"}}`), wantErr: ErrInvalid},
		{name: "unknown section", body: new(`{"network":{}}`), wantErr: ErrInvalid},
		{name: "zero duration", body: new(`{"capture":{"idle_interval":"0s"}}`), wantErr: ErrInvalid},
		{name: "negative duration", body: new(`{"capture":{"human_window":"-1m"}}`), wantErr: ErrInvalid},
		{name: "malformed", body: new(`{"capture":`), wantErr: ErrInvalid},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			if tt.body != nil {
				if err := os.WriteFile(path, []byte(*tt.body), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			got, err := Load(path)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Load error = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr == nil && got.Capture != tt.want {
				t.Errorf("Load capture = %+v, want %+v", got.Capture, tt.want)
			}
		})
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
