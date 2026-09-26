//go:build e2e

package e2e

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-sync/internal/catalog"
	"github.com/yasyf/cc-sync/internal/claudenative"
	"github.com/yasyf/cc-sync/internal/scheduler"
)

func TestPickupSourceUnavailableCodeFidelity(t *testing.T) {
	origin := NewOrigin(t, "acme/app", map[string]string{
		"README.md":         "hello\n",
		"main.go":           "package main\n",
		".gitignore":        "node_modules/\n",
		"app.txt":           "published text\n",
		"blob.dat":          "\x00published\x01",
		"gone-staged.txt":   "staged delete\n",
		"gone-unstaged.txt": "unstaged delete\n",
		"script.sh":         "#!/bin/sh\necho hi\n",
	})
	mesh := NewMesh(t, NewClock(Now()))
	a := mesh.Add("host-a", origin)
	b := mesh.Add("host-b", origin)
	src := a.Checkout("acme/app")
	published := a.Git(src, "rev-parse", "HEAD")

	a.WriteFile(filepath.Join(src, "lib", "one.go"), "package lib\n", 0o644)
	symlink(t, "README.md", filepath.Join(src, "link"))
	a.Git(src, "add", "lib/one.go", "link")
	a.Git(src, "commit", "-q", "-m", "one")
	a.WriteFile(filepath.Join(src, "main.go"), "package main\n\nfunc main() {}\n", 0o644)
	a.Git(src, "commit", "-q", "-am", "two")

	const stagedText, worktreeText = "staged text\n", "worktree text\n"
	const stagedBin, worktreeBin = "\x00staged\x01\xfe", "\x00worktree\x02\xff"
	a.WriteFile(filepath.Join(src, "app.txt"), stagedText, 0o644)
	a.WriteFile(filepath.Join(src, "blob.dat"), stagedBin, 0o644)
	a.Git(src, "add", "app.txt", "blob.dat")
	a.WriteFile(filepath.Join(src, "app.txt"), worktreeText, 0o644)
	a.WriteFile(filepath.Join(src, "blob.dat"), worktreeBin, 0o644)
	a.Git(src, "rm", "-q", "gone-staged.txt")
	remove(t, filepath.Join(src, "gone-unstaged.txt"))
	if err := os.Chmod(filepath.Join(src, "script.sh"), 0o755); err != nil {
		t.Fatal(err)
	}
	remove(t, filepath.Join(src, "link"))
	symlink(t, "main.go", filepath.Join(src, "link"))
	a.WriteFile(filepath.Join(src, "cmd", "new.go"), "package main\n", 0o644)
	a.WriteFile(filepath.Join(src, "node_modules", "left-pad", "index.js"), "module.exports = 1\n", 0o644)

	if n := a.Git(src, "rev-list", "--count", published+"..HEAD"); n != "2" {
		t.Fatalf("source has %s unpublished commits, want 2", n)
	}
	wantStatus := a.Git(src, "status", "--porcelain=v1", "-uall")
	for _, line := range []string{"MM app.txt", "MM blob.dat", "D  gone-staged.txt", " D gone-unstaged.txt", " M script.sh", " M link", "?? cmd/new.go"} {
		if !slices.Contains(strings.Split(wantStatus, "\n"), line) {
			t.Fatalf("source status lacks %q:\n%s", line, wantStatus)
		}
	}
	wantLog := a.Git(src, "log", "--format=%H %s")
	sess := a.WriteSession(SessionSpec{Cwd: src, Branch: "main", Turns: []Turn{{Human: true, Text: "stage some work"}, {Text: "Staged."}}})

	before := treeDigest(t, src, true)
	captureAndDeliver(t, mesh, a, sess.ID)
	if after := treeDigest(t, src, true); after != before {
		t.Errorf("capture changed the source checkout: digest %s, was %s", after, before)
	}
	takeDown(t, a, src)

	res := b.Pickup("host-a:" + sess.ID)
	r := res.Checkout.Path
	if res.Checkpoint.Partial || !res.Checkout.Exact || len(res.Checkout.Differences) != 0 {
		t.Fatalf("pickup checkpoint %+v checkout %+v, want a complete exact restore", res.Checkpoint, res.Checkout)
	}
	if got := b.Git(r, "log", "--format=%H %s"); got != wantLog {
		t.Errorf("restored history:\n%s\nwant:\n%s", got, wantLog)
	}
	if got := b.Git(r, "status", "--porcelain=v1", "-uall"); got != wantStatus {
		t.Errorf("restored status:\n%s\nwant:\n%s", got, wantStatus)
	}
	for _, c := range []struct{ rev, want string }{
		{":app.txt", stagedText},
		{":blob.dat", stagedBin},
		{":link", "README.md"},
		{"HEAD:link", "README.md"},
	} {
		if got := string(rawGit(t, r, "cat-file", "-p", c.rev)); got != c.want {
			t.Errorf("restored %s = %q, want %q", c.rev, got, c.want)
		}
	}
	for name, want := range map[string]string{"app.txt": worktreeText, "blob.dat": worktreeBin, "main.go": "package main\n\nfunc main() {}\n", "lib/one.go": "package lib\n", "cmd/new.go": "package main\n"} {
		if got := readFile(t, filepath.Join(r, name)); got != want {
			t.Errorf("restored %s = %q, want %q", name, got, want)
		}
	}
	for _, name := range []string{"gone-staged.txt", "gone-unstaged.txt", "node_modules"} {
		if _, err := os.Lstat(filepath.Join(r, name)); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("restored %s exists (%v), want absent", name, err)
		}
	}
	if got := b.Git(r, "ls-files", "gone-staged.txt", "gone-unstaged.txt"); got != "gone-unstaged.txt" {
		t.Errorf("restored index holds %q, want only the unstaged delete", got)
	}
	if fi, err := os.Lstat(filepath.Join(r, "script.sh")); err != nil || fi.Mode().Perm()&0o111 != 0o111 {
		t.Errorf("restored script.sh mode %v (%v), want executable", fi.Mode(), err)
	}
	if got := b.Git(r, "ls-files", "-s", "script.sh"); !strings.HasPrefix(got, "100644 ") {
		t.Errorf("restored index entry %q, want the unstaged mode change left out of the index", got)
	}
	if target, err := os.Readlink(filepath.Join(r, "link")); err != nil || target != "main.go" {
		t.Errorf("restored link -> %q (%v), want main.go", target, err)
	}
}

func TestPickupUnpublishedLFSObjectOffline(t *testing.T) {
	const base, hist = "published\x00asset", "unpublished\x00history\x01"
	origin := NewLFSOrigin(t, "acme/assets", map[string]string{"README.md": "assets\n", "base.bin": base}, "*.bin")
	mesh := NewMesh(t, NewClock(Now()))
	a := mesh.Add("host-a", origin)
	b := mesh.Add("host-b", origin)
	a.InstallLFS("acme/assets")
	b.InstallLFS("acme/assets")
	src := a.Checkout("acme/assets")

	a.WriteFile(filepath.Join(src, "hist.bin"), hist, 0o644)
	a.Git(src, "add", "hist.bin")
	a.Git(src, "commit", "-q", "-m", "hist")
	oid := sha256Hex(hist)
	if ptr := a.Git(src, "cat-file", "-p", "HEAD:hist.bin"); !strings.Contains(ptr, "oid sha256:"+oid) {
		t.Fatalf("hist.bin is committed as %q, want an LFS pointer", ptr)
	}
	if _, err := os.Stat(LFSObjectPath(origin.URL, oid)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("hist.bin's object is on the origin LFS remote (%v), want it unpublished", err)
	}
	if _, err := os.Stat(LFSObjectPath(filepath.Join(src, ".git"), oid)); err != nil {
		t.Fatalf("hist.bin's object is not in the source LFS store: %v", err)
	}
	sess := a.WriteSession(SessionSpec{Cwd: src, Branch: "main", Turns: []Turn{{Human: true, Text: "commit an asset"}, {Text: "Committed."}}})
	captureAndDeliver(t, mesh, a, sess.ID)
	takeDown(t, a, src)

	b.Git(b.Checkout("acme/assets"), "config", "lfs.url", "file://"+filepath.Join(b.Root, "no-such-lfs-remote"))
	b.SetNetwork(Disconnected)
	res := b.Pickup("host-a:" + sess.ID)
	var extra struct {
		Checkout struct {
			LFSPending []string `json:"lfs_pending"`
		} `json:"checkout"`
	}
	if err := json.Unmarshal(res.Raw, &extra); err != nil {
		t.Fatal(err)
	}
	if !res.Checkout.Exact || len(res.Checkout.Differences) != 0 || len(extra.Checkout.LFSPending) != 0 {
		t.Fatalf("checkout %+v lfs_pending %q, want an exact, fully hydrated restore", res.Checkout, extra.Checkout.LFSPending)
	}
	for name, want := range map[string]string{"hist.bin": hist, "base.bin": base} {
		if got := readFile(t, filepath.Join(res.Checkout.Path, name)); got != want {
			t.Errorf("restored %s = %q, want %q", name, got, want)
		}
	}
	if got := b.Git(res.Checkout.Path, "status", "--porcelain=v1", "-uall"); got != "" {
		t.Errorf("restored status %q, want clean", got)
	}
}

func TestPickupLargeTranscriptByteExact(t *testing.T) {
	origin := NewOrigin(t, "acme/app", map[string]string{"README.md": "hello\n"})
	mesh := NewMesh(t, NewClock(Now()))
	a := mesh.Add("host-a", origin)
	b := mesh.Add("host-b", origin)
	src := a.Checkout("acme/app")
	sess := a.WriteSession(SessionSpec{
		Cwd: src, Branch: "main",
		Turns:       []Turn{{Human: true, Text: "write a lot"}, {Text: "Writing."}},
		ToolResults: 2, Subagents: 1, PadTo: 9 << 20, PartialTail: true,
	})
	raw := []byte(readFile(t, sess.Transcript))
	complete := raw[:bytes.LastIndexByte(raw, '\n')+1]
	if len(complete) <= 8<<20 || len(complete) == len(raw) {
		t.Fatalf("transcript has %d complete of %d bytes, want over 8 MiB of complete records and a partial tail", len(complete), len(raw))
	}
	sidecars := map[string]string{}
	for _, rel := range []string{"tool-results/toolu_e2e0000.txt", "tool-results/toolu_e2e0001.txt", "subagents/agent-a0000000000000001.jsonl", "subagents/agent-a0000000000000001.meta.json"} {
		sidecars[rel] = readFile(t, filepath.Join(sess.Dir, rel))
	}
	captureAndDeliver(t, mesh, a, sess.ID)
	takeDown(t, a, src)

	res := b.Pickup("host-a:" + sess.ID)
	var out struct {
		Sessions []struct {
			SessionID string `json:"session_id"`
			Status    string `json:"status"`
			Launch    *struct {
				Argv []string `json:"argv"`
				Dir  string   `json:"dir"`
			} `json:"launch"`
		} `json:"sessions"`
	}
	if err := json.Unmarshal(res.Raw, &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Sessions) != 1 || out.Sessions[0].SessionID != sess.ID || out.Sessions[0].Launch == nil {
		t.Fatalf("pickup sessions %s, want one launchable %s", res.Raw, sess.ID)
	}
	launch := out.Sessions[0].Launch
	if launch.Dir != res.Checkout.Path || !slices.Contains(launch.Argv, sess.ID) || !slices.Contains(launch.Argv, "--resume") {
		t.Fatalf("launch %+v, want claude --resume %s in %s", launch, sess.ID, res.Checkout.Path)
	}
	id := claudenative.SessionID(sess.ID)
	oldCwd, newCwd := []byte(`"cwd":"`+src+`"`), []byte(`"cwd":"`+res.Checkout.Path+`"`)
	if n, lines := bytes.Count(complete, oldCwd), bytes.Count(complete, []byte("\n")); n != lines {
		t.Fatalf("%d of %d records carry the source cwd", n, lines)
	}
	want := bytes.ReplaceAll(complete, oldCwd, newCwd)
	got := []byte(readFile(t, claudenative.TranscriptPath(b.Claude.ConfigDir, launch.Dir, id)))
	if !bytes.HasPrefix(got, want) {
		t.Fatalf("restored transcript is %d bytes, want the %d relocated complete source records first; first difference at byte %d", len(got), len(want), firstDiff(got, want))
	}
	marker := got[len(want):]
	var relocated map[string]any
	if bytes.Count(marker, []byte("\n")) != 1 || !bytes.HasSuffix(marker, []byte("\n")) || json.Unmarshal(marker, &relocated) != nil {
		t.Fatalf("restored transcript ends %q past the source records, want one relocation marker record", marker)
	}
	if wantMarker := map[string]any{"type": "relocated", "sessionId": sess.ID, "relocatedCwd": launch.Dir}; !maps.Equal(relocated, wantMarker) {
		t.Fatalf("relocation marker %v, want %v", relocated, wantMarker)
	}
	dir := claudenative.SessionDir(b.Claude.ConfigDir, launch.Dir, id)
	for rel, content := range sidecars {
		if rel == "subagents/agent-a0000000000000001.jsonl" {
			content = strings.ReplaceAll(content, string(oldCwd), string(newCwd))
		}
		if got := readFile(t, filepath.Join(dir, rel)); got != content {
			t.Errorf("restored %s = %q, want %q", rel, got, content)
		}
	}
}

func TestPickupOrcaMultiSessionResumesMostRecentHuman(t *testing.T) {
	origin := NewOrigin(t, "acme/app", map[string]string{"README.md": "hello\n"})
	clock := NewClock(Now())
	mesh := NewMesh(t, clock)
	a := mesh.Add("host-a", origin)
	b := mesh.Add("host-b", origin)
	src := a.Checkout("acme/app")
	a.WriteFile(filepath.Join(src, "work.txt"), "in progress\n", 0o644)

	oldest := a.WriteSession(SessionSpec{Cwd: src, Branch: "main", Turns: []Turn{{Human: true, Text: "first"}, {Text: "ok"}}})
	clock.Advance(5 * time.Minute)
	middle := a.WriteSession(SessionSpec{Cwd: src, Branch: "main", Turns: []Turn{{Human: true, Text: "second"}, {Text: "ok"}}})
	clock.Advance(5 * time.Minute)
	human := a.WriteSession(SessionSpec{Cwd: src, Branch: "main", Turns: []Turn{{Human: true, Text: "latest human prompt"}, {Text: "ok"}}})
	clock.Advance(5 * time.Minute)
	a.AppendTurns(oldest, Turn{Text: "autonomous follow-up"})
	a.AppendTurns(middle, Turn{Text: "autonomous follow-up"})

	descriptor := fmt.Sprintf(`{"version": 1, "workspace": {"instanceId": "inst-app", "path": %q, "branch": "main", "meta": {"displayName": "app"}},
		"tabs": [{"id": "t1", "leaves": [
			{"id": "l1", "binding": {"agent": "claude", "key": "session_id", "id": %q}},
			{"id": "l2", "binding": {"agent": "claude", "key": "session_id", "id": %q}},
			{"id": "l3", "binding": {"agent": "claude", "key": "session_id", "id": %q}}]}],
		"omittedBindings": []}`, src, oldest.ID, middle.ID, human.ID)
	a.Orca.Respond("recovery-export", `{"ok":true,"result":{"descriptor":`+descriptor+`}}`)
	binding := func(leaf, id, status string) string {
		return fmt.Sprintf(`{"sourcePaneKey":"t1:%s","localPaneKey":"t9:%s","binding":{"agent":"claude","key":"session_id","id":%q},"status":%q}`, leaf, leaf, id, status)
	}
	b.Orca.Respond("recovery-import", `{"id":"local","ok":true,"result":{"importKey":"e2e-import","disposition":"imported","repoId":"repo-e2e","worktreeId":"repo-e2e::/dst","instanceId":"inst-app",`+
		`"presentationSource":{"kind":"client-view","clientKey":"local-renderer"},"idMap":{"tabs":{"t1":"t9"},"groups":{},"leaves":{},"browsers":{}},"bindings":[`+
		binding("l1", oldest.ID, "dormant")+","+binding("l2", middle.ID, "dormant")+","+binding("l3", human.ID, "resumed")+`],"provenance":{"importKey":"e2e-import"}}}`)

	captureAndDeliver(t, mesh, a)
	takeDown(t, a, src)

	items := list(t, b)
	if len(items) != 1 || len(items[0].Sessions) != 3 {
		t.Fatalf("list items %+v, want one item holding all three sessions", items)
	}
	res := b.Pickup(items[0].Selector)
	imports := b.Orca.CallsTo("recovery import")
	if len(imports) != 1 {
		t.Fatalf("orca imports = %d, want 1", len(imports))
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, []byte(descriptor)); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(imports[0].Stdin, compact.Bytes()) {
		t.Errorf("import descriptor %s, want the source export %s", imports[0].Stdin, compact.Bytes())
	}
	if got := flagValues(imports[0].Argv, "--resume"); !slices.Equal(got, []string{human.ID}) {
		t.Errorf("import resumes %q, want only the most recent human session %s", got, human.ID)
	}
	if got := flagValues(imports[0].Argv, "--path-map"); !slices.Contains(got, src+"="+res.Checkout.Path) {
		t.Errorf("import path maps %q lack %s=%s", got, src, res.Checkout.Path)
	}
	var out struct {
		Sessions []struct {
			SessionID string `json:"session_id"`
			Status    string `json:"status"`
			Selected  bool   `json:"selected"`
		} `json:"sessions"`
		Orca struct {
			Resumed []struct {
				SessionID string `json:"session_id"`
				TabID     string `json:"tab_id"`
			} `json:"resumed"`
			Dormant []string `json:"dormant"`
		} `json:"orca"`
	}
	if err := json.Unmarshal(res.Raw, &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Orca.Resumed) != 1 || out.Orca.Resumed[0].SessionID != human.ID || out.Orca.Resumed[0].TabID != "t9" {
		t.Errorf("orca resumed %+v, want %s in t9", out.Orca.Resumed, human.ID)
	}
	slices.Sort(out.Orca.Dormant)
	wantDormant := []string{oldest.ID, middle.ID}
	slices.Sort(wantDormant)
	if !slices.Equal(out.Orca.Dormant, wantDormant) {
		t.Errorf("orca dormant %q, want %q", out.Orca.Dormant, wantDormant)
	}
	want := map[string]string{oldest.ID: "dormant", middle.ID: "dormant", human.ID: "resumed"}
	if len(out.Sessions) != len(want) {
		t.Fatalf("pickup sessions %+v, want %d", out.Sessions, len(want))
	}
	for _, s := range out.Sessions {
		if s.Status != want[s.SessionID] || s.Selected != (s.SessionID == human.ID) {
			t.Errorf("session %s status %q selected %v, want %q selected %v", s.SessionID, s.Status, s.Selected, want[s.SessionID], s.SessionID == human.ID)
		}
		if _, err := os.Stat(claudenative.TranscriptPath(b.Claude.ConfigDir, res.Checkout.Path, claudenative.SessionID(s.SessionID))); err != nil {
			t.Errorf("session %s not installed natively: %v", s.SessionID, err)
		}
	}
}

func TestPickupRepeatedSiblingNeverRollsBack(t *testing.T) {
	origin := NewOrigin(t, "acme/app", map[string]string{"README.md": "hello\n", "main.go": "package main\n"})
	clock := NewClock(Now())
	mesh := NewMesh(t, clock)
	a := mesh.Add("host-a", origin)
	b := mesh.Add("host-b", origin)
	src := a.Checkout("acme/app")
	a.WriteFile(filepath.Join(src, "main.go"), "package main // first on a\n", 0o644)
	sess := a.WriteSession(SessionSpec{Cwd: src, Branch: "main", Turns: []Turn{{Human: true, Text: "edit main"}, {Text: "Edited."}}})
	captureAndDeliver(t, mesh, a, sess.ID)

	first := b.Pickup("host-a:" + sess.ID)
	r := first.Checkout.Path
	if first.Checkout.Reused || first.Checkout.Newer {
		t.Fatalf("first pickup checkout %+v, want a fresh restore", first.Checkout)
	}
	b.WriteFile(filepath.Join(r, "main.go"), "package main // edited on b\n", 0o644)
	b.WriteFile(filepath.Join(r, "b-notes.txt"), "b's own work\n", 0o644)
	edited := treeDigest(t, r, false)

	clock.Advance(10 * time.Minute)
	a.WriteFile(filepath.Join(src, "main.go"), "package main // newer on a\n", 0o644)
	a.AppendTurns(sess, Turn{Human: true, Text: "keep going"}, Turn{Text: "Kept going."})
	captureAndDeliver(t, mesh, a, sess.ID)
	takeDown(t, a, src)

	second := b.Pickup("host-a:" + sess.ID)
	if second.Checkpoint.ID == first.Checkpoint.ID {
		t.Fatalf("second pickup chose %s again, want the newer checkpoint", second.Checkpoint.ID)
	}
	if second.Checkout.Path != r || !second.Checkout.Reused || !second.Checkout.Newer {
		t.Fatalf("second pickup checkout %+v, want %s reused and left behind the newer snapshot", second.Checkout, r)
	}
	if got := treeDigest(t, r, false); got != edited {
		t.Errorf("second pickup changed host-b's edited checkout: digest %s, was %s", got, edited)
	}
	for name, want := range map[string]string{"main.go": "package main // edited on b\n", "b-notes.txt": "b's own work\n"} {
		if got := readFile(t, filepath.Join(r, name)); got != want {
			t.Errorf("%s = %q after repeated pickup, want host-b's %q", name, got, want)
		}
	}
}

func TestPickupMixedDeferredCheckpointD7(t *testing.T) {
	origin := NewOrigin(t, "acme/app", map[string]string{"README.md": "hello\n", "main.go": "package main\n"})
	clock := NewClock(Now())
	mesh := NewMesh(t, clock)
	a := mesh.Add("host-a", origin)
	b := mesh.Add("host-b", origin)
	src := a.Checkout("acme/app")
	a.WriteFile(filepath.Join(src, "main.go"), "package main // complete\n", 0o644)
	sess := a.WriteSession(SessionSpec{Cwd: src, Branch: "main", Turns: []Turn{{Human: true, Text: "edit main"}, {Text: "Edited."}}})
	captureAndDeliver(t, mesh, a, sess.ID)
	complete := checkpoints(t, b)
	if len(complete) != 1 || complete[0].Deferred != "" {
		t.Fatalf("checkpoints after the first capture %+v, want one complete", complete)
	}

	clock.Advance(10 * time.Minute)
	if err := os.MkdirAll(filepath.Join(src, ".git", "rebase-merge"), 0o750); err != nil {
		t.Fatal(err)
	}
	a.WriteFile(filepath.Join(src, "main.go"), "package main // mid-rebase\n", 0o644)
	a.AppendTurns(sess, Turn{Human: true, Text: "rebase onto main"}, Turn{Text: "Rebasing."})
	if attempts := a.Kick(sess.ID); len(attempts) != 1 || attempts[0].Outcome != scheduler.OutcomeDeferred {
		t.Fatalf("kick mid-rebase = %+v, want one deferred attempt", attempts)
	}
	if _, err := mesh.Deliver(t.Context(), "host-a", "host-b"); err != nil {
		t.Fatalf("deliver: %v", err)
	}
	var mixed catalog.Checkpoint
	for _, cp := range checkpoints(t, b) {
		if cp.ID != complete[0].ID {
			mixed = cp
		}
	}
	if mixed.ID == "" || !strings.Contains(mixed.Deferred, "rebase") {
		t.Fatalf("checkpoints %+v, want a newer mixed checkpoint deferred by the rebase", checkpoints(t, b))
	}
	takeDown(t, a, src)

	items := list(t, b)
	if len(items) != 1 || items[0].Checkpoint.ID != complete[0].ID || items[0].NewerPartial == nil || items[0].NewerPartial.ID != mixed.ID {
		t.Fatalf("list items %+v, want pick-up target %s with newer partial %s", items, complete[0].ID, mixed.ID)
	}
	def := b.Pickup("host-a:" + sess.ID)
	if def.Checkpoint.ID != complete[0].ID || def.Checkpoint.Partial {
		t.Fatalf("default pickup checkpoint %+v, want complete %s", def.Checkpoint, complete[0].ID)
	}
	if got := readFile(t, filepath.Join(def.Checkout.Path, "main.go")); got != "package main // complete\n" {
		t.Errorf("default pickup main.go = %q, want the complete checkpoint's code", got)
	}
	if refused := b.CLI("pickup", "host-a:"+sess.ID, "--checkpoint", mixed.ID); refused.Code == 0 || !strings.Contains(string(refused.Stdout)+refused.Stderr, "deferred") {
		t.Fatalf("pickup --checkpoint %s without --allow-partial exited %d: %s%s", mixed.ID, refused.Code, refused.Stdout, refused.Stderr)
	}
	partial := b.Pickup("host-a:"+sess.ID, "--checkpoint", mixed.ID, "--allow-partial")
	if partial.Checkpoint.ID != mixed.ID || !partial.Checkpoint.Partial {
		t.Fatalf("--allow-partial pickup checkpoint %+v, want partial %s", partial.Checkpoint, mixed.ID)
	}
}

type listedItem struct {
	Selector   string `json:"selector"`
	Checkpoint struct {
		ID string `json:"id"`
	} `json:"checkpoint"`
	NewerPartial *struct {
		ID string `json:"id"`
	} `json:"newer_partial"`
	Sessions []struct {
		SessionID string `json:"session_id"`
	} `json:"sessions"`
}

func list(t *testing.T, h *Host) []listedItem {
	t.Helper()
	res := h.CLI("list")
	if res.Code != 0 {
		t.Fatalf("%s: list exited %d: %s%s", h.Name, res.Code, res.Stdout, res.Stderr)
	}
	var out struct {
		Items []listedItem `json:"items"`
	}
	if err := res.Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out.Items
}

func checkpoints(t *testing.T, h *Host) []catalog.Checkpoint {
	t.Helper()
	var cps []catalog.Checkpoint
	for _, o := range h.Catalog().Origins {
		for _, wt := range o.Worktrees {
			cps = append(cps, wt.Checkpoints...)
		}
	}
	return cps
}

func captureAndDeliver(t *testing.T, mesh *Mesh, a *Host, sessionIDs ...string) {
	t.Helper()
	attempts := a.Kick(sessionIDs...)
	if len(attempts) != 1 || attempts[0].Outcome != scheduler.OutcomeCaptured {
		t.Fatalf("kick = %+v, want one captured attempt", attempts)
	}
	d, err := mesh.Deliver(t.Context(), a.Name, "host-b")
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if d.Result.Partial || d.Result.AckedRevision != d.Change.SourceRevision {
		t.Fatalf("apply = %+v, want a full ACK of %s", d.Result, d.Change.SourceRevision)
	}
}

func takeDown(t *testing.T, h *Host, src string) {
	t.Helper()
	h.Offline()
	for _, p := range []string{src, h.Claude.ConfigDir} {
		if err := os.RemoveAll(p); err != nil {
			t.Fatal(err)
		}
	}
}

func treeDigest(t *testing.T, dir string, withGit bool) string {
	t.Helper()
	sum := sha256.New()
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		if !withGit && d.Name() == ".git" {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		fmt.Fprintf(sum, "%s\x00%v\x00", rel, info.Mode())
		switch {
		case info.Mode()&fs.ModeSymlink != 0:
			target, err := os.Readlink(path)
			if err != nil {
				return err
			}
			fmt.Fprintf(sum, "%s\x00", target)
		case info.Mode().IsRegular():
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			sum.Write(b)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(sum.Sum(nil))
}

func rawGit(t *testing.T, dir string, args ...string) []byte {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	cmd.Env = append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("git %s: %v", strings.Join(args, " "), err)
	}
	return out
}

func flagValues(argv []string, flag string) []string {
	var out []string
	for i := 0; i+1 < len(argv); i++ {
		if argv[i] == flag {
			out = append(out, argv[i+1])
		}
	}
	return out
}

func firstDiff(a, b []byte) int {
	for i := range min(len(a), len(b)) {
		if a[i] != b[i] {
			return i
		}
	}
	return min(len(a), len(b))
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func remove(t *testing.T, path string) {
	t.Helper()
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
}

func symlink(t *testing.T, target, path string) {
	t.Helper()
	if err := os.Symlink(target, path); err != nil {
		t.Fatal(err)
	}
}

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}
