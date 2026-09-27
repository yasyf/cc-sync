package resident

import (
	"context"
	"errors"
	"flag"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	"github.com/yasyf/cc-sync/internal/catalog"
	"github.com/yasyf/cc-sync/internal/config"
	"github.com/yasyf/synckit/manifest"
)

var update = flag.Bool("update", false, "rewrite golden files")

const manifestGolden = "testdata/manifest.json"

type fakeRunner struct {
	calls    [][]string
	manifest []byte
	fail     string
}

func (r *fakeRunner) run(_ context.Context, name string, args ...string) error {
	call := append([]string{name}, args...)
	r.calls = append(r.calls, call)
	if len(args) == 2 && args[0] == "register" {
		data, err := os.ReadFile(args[1])
		if err != nil {
			return err
		}
		r.manifest = data
	}
	if len(args) > 0 && args[0] == r.fail {
		return errors.New(r.fail + " failed")
	}
	return nil
}

func TestExecRunner(t *testing.T) {
	missing := "cc-sync-realproc-absent-binary"
	tests := []struct {
		name     string
		argv     []string
		notFound bool
		msg      string
	}{
		{name: "success", argv: []string{"/bin/sh", "-c", "exit 0"}},
		{name: "failure with output", argv: []string{"/bin/sh", "-c", "echo boom >&2; exit 3"}, msg: "/bin/sh -c echo boom >&2; exit 3: exit status 3: boom"},
		{name: "silent failure", argv: []string{"/bin/sh", "-c", "exit 4"}, msg: "/bin/sh -c exit 4: exit status 4"},
		{name: "missing binary", argv: []string{missing, "install"}, notFound: true, msg: missing + ` install: exec: "` + missing + `": executable file not found in $PATH`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ExecRunner(context.Background(), tt.argv[0], tt.argv[1:]...)
			if tt.msg == "" {
				if err != nil {
					t.Fatalf("ExecRunner() = %v, want nil", err)
				}
				return
			}
			if err == nil || err.Error() != tt.msg {
				t.Fatalf("ExecRunner() = %v, want %q", err, tt.msg)
			}
			if got := errors.Is(err, exec.ErrNotFound); got != tt.notFound {
				t.Fatalf("errors.Is(%v, exec.ErrNotFound) = %t, want %t", err, got, tt.notFound)
			}
		})
	}
}

func TestManifestGolden(t *testing.T) {
	m := Manifest()
	if err := m.Validate(); err != nil {
		t.Fatalf("Manifest().Validate: %v", err)
	}
	r := &fakeRunner{}
	layout := config.At(filepath.Join(t.TempDir(), "config"), t.TempDir())
	if err := Install(t.Context(), r.run, layout, "me@host"); err != nil {
		t.Fatalf("Install: %v", err)
	}
	if *update {
		if err := os.WriteFile(manifestGolden, r.manifest, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(manifestGolden)
	if err != nil {
		t.Fatal(err)
	}
	if string(r.manifest) != string(want) {
		t.Errorf("registered manifest =\n%s\nwant\n%s", r.manifest, want)
	}
	loaded, err := manifest.Load(manifestGolden)
	if err != nil {
		t.Fatalf("manifest.Load: %v", err)
	}
	if !reflect.DeepEqual(*loaded, m) {
		t.Errorf("golden round trip = %+v, want %+v", *loaded, m)
	}
}

func TestInstall(t *testing.T) {
	tests := []struct {
		name    string
		fail    string
		want    []string
		wantErr bool
	}{
		{name: "register then install", want: []string{"register", "install"}},
		{name: "register failure stops", fail: "register", want: []string{"register"}, wantErr: true},
		{name: "install failure surfaces", fail: "install", want: []string{"register", "install"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("TMPDIR", t.TempDir())
			r := &fakeRunner{fail: tt.fail}
			layout := config.At(filepath.Join(t.TempDir(), "config"), t.TempDir())
			err := Install(t.Context(), r.run, layout, "me@host")
			if (err != nil) != tt.wantErr {
				t.Fatalf("Install = %v, wantErr %v", err, tt.wantErr)
			}
			var verbs []string
			for _, call := range r.calls {
				if call[0] != Synckitd {
					t.Errorf("ran %v, want only %s", call, Synckitd)
				}
				verbs = append(verbs, call[1])
			}
			if !slices.Equal(verbs, tt.want) {
				t.Errorf("synckitd verbs = %v, want %v", verbs, tt.want)
			}
			if manifestPath := r.calls[0][2]; exists(t, manifestPath) {
				t.Errorf("temp manifest %s left behind", manifestPath)
			}
			if !exists(t, filepath.Join(layout.StampDir, catalog.StampFile)) {
				t.Errorf("stamp missing after Install")
			}
		})
	}
}

func TestUninstall(t *testing.T) {
	tests := []struct {
		name    string
		fail    string
		purge   bool
		want    [][]string
		wantErr bool
		kept    bool
	}{
		{name: "keeps data", want: [][]string{{Synckitd, "unregister", "cc-sync"}, {Synckitd, "install"}}, kept: true},
		{name: "purges data", purge: true, want: [][]string{{Synckitd, "unregister", "cc-sync"}, {Synckitd, "install"}}},
		{name: "unregister failure keeps data", fail: "unregister", purge: true, want: [][]string{{Synckitd, "unregister", "cc-sync"}}, wantErr: true, kept: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "config")
			if err := os.MkdirAll(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			var purge []string
			if tt.purge {
				purge = []string{dir}
			}
			r := &fakeRunner{fail: tt.fail}
			err := Uninstall(t.Context(), r.run, purge)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Uninstall = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(r.calls, tt.want) {
				t.Errorf("calls = %v, want %v", r.calls, tt.want)
			}
			if exists(t, dir) != tt.kept {
				t.Errorf("data dir exists = %v, want %v", !tt.kept, tt.kept)
			}
		})
	}
}
