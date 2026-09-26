//go:build e2e

package e2e

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
)

type sparseBlock struct {
	Cone     bool     `json:"cone"`
	Patterns []string `json:"patterns"`
	Expanded bool     `json:"expanded"`
}

func TestPickupSparseSourceUnavailable(t *testing.T) {
	const staged, edited, untracked = "staged inside\n", "\x00edited inside\x01", "untracked inside\n"
	outside := map[string]string{"api/server.go": "package api\n", "docs/guide.md": "guide\n"}
	tests := []struct {
		name     string
		set      []string
		cone     bool
		patterns []string
		list     string
	}{
		{"cone", []string{"--cone", "web"}, true, []string{"/*", "!/*/", "/web/"}, "web"},
		{"non-cone", []string{"--no-cone", "/README.md", "/web/"}, false, []string{"/README.md", "/web/"}, "/README.md\n/web/"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			files := map[string]string{"README.md": "hello\n", "web/app.txt": "published inside\n"}
			maps.Copy(files, outside)
			origin := NewOrigin(t, "acme/app", files)
			mesh := NewMesh(t, NewClock(Now()))
			a := mesh.Add(hostA, origin)
			b := mesh.Add(hostB, origin)
			c := mesh.Add(hostC, origin)
			src := a.Checkout("acme/app")
			a.Git(src, append([]string{"sparse-checkout", "set"}, tt.set...)...)
			if list := a.Git(src, "sparse-checkout", "list"); list != tt.list {
				t.Fatalf("source sparse-checkout list = %q, want %q", list, tt.list)
			}
			requireOutside(t, src, outside, false)

			a.WriteFile(filepath.Join(src, "web", "app.txt"), staged, 0o644)
			a.Git(src, "add", "web/app.txt")
			a.WriteFile(filepath.Join(src, "web", "app.txt"), edited, 0o644)
			a.WriteFile(filepath.Join(src, "web", "new.txt"), untracked, 0o644)
			const wantStatus = "MM web/app.txt\n?? web/new.txt"
			if got := a.Git(src, "status", "--porcelain=v1", "-uall"); got != wantStatus {
				t.Fatalf("source status %q, want %q", got, wantStatus)
			}
			requireWIP := func(h *Host, dir string) {
				t.Helper()
				if got := h.Git(dir, "status", "--porcelain=v1", "-uall"); got != wantStatus {
					t.Errorf("%s: restored status %q, want %q", h.Name, got, wantStatus)
				}
				if got := string(rawGit(t, dir, "cat-file", "-p", ":web/app.txt")); got != staged {
					t.Errorf("%s: restored :web/app.txt = %q, want %q", h.Name, got, staged)
				}
				for name, want := range map[string]string{"web/app.txt": edited, "web/new.txt": untracked} {
					if got := readFile(t, filepath.Join(dir, name)); got != want {
						t.Errorf("%s: restored %s = %q, want %q", h.Name, name, got, want)
					}
				}
			}

			sess := a.WriteSession(SessionSpec{Cwd: src, Branch: "main", Turns: []Turn{{Human: true, Text: "edit the web app"}, {Text: "Edited."}}})
			captured(t, a, sess)
			for _, peer := range []string{hostB, hostC} {
				requireAcked(t, deliver(t, mesh, hostA, peer))
			}
			takeDown(t, a, src)

			full := b.Pickup(hostA + ":" + sess.ID)
			expansion := fmt.Sprintf("sparse checkout expanded to full: source cone %t, patterns %q, exceptions []", tt.cone, tt.patterns)
			if full.Checkout.Exact || !slices.Equal(full.Checkout.Differences, []string{expansion}) {
				t.Fatalf("default pickup checkout %+v, want inexact with the one difference %q", full.Checkout, expansion)
			}
			requireSparseBlock(t, full, sparseBlock{Cone: tt.cone, Patterns: tt.patterns, Expanded: true})
			requireOutside(t, full.Checkout.Path, outside, true)
			requireWIP(b, full.Checkout.Path)

			kept := c.Pickup(hostA+":"+sess.ID, "--apply-sparse")
			if kept.Checkout.Reused || !kept.Checkout.Exact || len(kept.Checkout.Differences) != 0 {
				t.Fatalf("--apply-sparse pickup checkout %+v, want a fresh exact restore", kept.Checkout)
			}
			requireSparseBlock(t, kept, sparseBlock{Cone: tt.cone, Patterns: tt.patterns})
			if list := c.Git(kept.Checkout.Path, "sparse-checkout", "list"); list != tt.list {
				t.Errorf("restored sparse-checkout list = %q, want %q", list, tt.list)
			}
			requireOutside(t, kept.Checkout.Path, outside, false)
			requireWIP(c, kept.Checkout.Path)
		})
	}
}

func requireSparseBlock(t *testing.T, res PickupOutput, want sparseBlock) {
	t.Helper()
	var out struct {
		Checkout struct {
			Sparse *sparseBlock `json:"sparse"`
		} `json:"checkout"`
	}
	if err := json.Unmarshal(res.Raw, &out); err != nil {
		t.Fatal(err)
	}
	if out.Checkout.Sparse == nil || !reflect.DeepEqual(*out.Checkout.Sparse, want) {
		t.Errorf("pickup checkout.sparse = %+v, want %+v", out.Checkout.Sparse, want)
	}
}

func requireOutside(t *testing.T, dir string, outside map[string]string, present bool) {
	t.Helper()
	for name, want := range outside {
		path := filepath.Join(dir, name)
		if present {
			if got := readFile(t, path); got != want {
				t.Errorf("%s = %q, want %q", path, got, want)
			}
			continue
		}
		if _, err := os.Lstat(path); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("%s exists (%v), want it outside the sparse checkout", path, err)
		}
	}
}
