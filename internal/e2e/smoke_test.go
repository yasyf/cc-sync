//go:build e2e

package e2e

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/yasyf/cc-sync/internal/scheduler"
)

func TestSmokeRecoverOnSecondHost(t *testing.T) {
	origin := NewOrigin(t, "acme/app", map[string]string{"README.md": "hello\n", "main.go": "package main\n"})
	mesh := NewMesh(t, NewClock(Now()))
	a := mesh.Add("host-a", origin)
	b := mesh.Add("host-b", origin)

	src := a.Checkout("acme/app")
	a.WriteFile(filepath.Join(src, "main.go"), "package main\n\nfunc main() {}\n", 0o644)
	a.WriteFile(filepath.Join(src, "notes.txt"), "untracked work\n", 0o644)
	sess := a.WriteSession(SessionSpec{
		Cwd: src, Branch: "main",
		Turns:       []Turn{{Human: true, Text: "add a main func"}, {Text: "Added func main."}},
		ToolResults: 1,
	})

	attempts := a.Kick(sess.ID)
	if len(attempts) != 1 || attempts[0].Outcome != scheduler.OutcomeCaptured {
		t.Fatalf("kick = %+v, want one captured attempt", attempts)
	}

	d, err := mesh.Deliver(t.Context(), "host-a", "host-b")
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if d.Result.Partial || d.Result.AckedRevision != d.Change.SourceRevision {
		t.Fatalf("apply = %+v, want a full ACK of %s", d.Result, d.Change.SourceRevision)
	}

	a.Offline()
	for _, p := range []string{src, a.Claude.ConfigDir} {
		if err := os.RemoveAll(p); err != nil {
			t.Fatal(err)
		}
	}

	res := b.Pickup("host-a:" + sess.ID)
	restored := res.Checkout.Path
	for name, want := range map[string]string{"main.go": "package main\n\nfunc main() {}\n", "notes.txt": "untracked work\n"} {
		got, err := os.ReadFile(filepath.Join(restored, name))
		if err != nil || string(got) != want {
			t.Fatalf("restored %s = %q, %v; want %q", name, got, err, want)
		}
	}
	if _, err := os.Stat(filepath.Join(b.Claude.ConfigDir, "projects")); err != nil {
		t.Fatalf("no native projects dir on host-b: %v", err)
	}
	imports := b.Orca.CallsTo("recovery import")
	if len(imports) != 1 {
		t.Fatalf("orca imports = %d, want 1", len(imports))
	}
	if !slices.Contains(imports[0].Argv, "--register-repo") {
		t.Fatalf("orca import argv %q lacks --register-repo", imports[0].Argv)
	}
}
