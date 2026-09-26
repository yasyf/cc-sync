//go:build e2e

package e2e

import (
	"encoding/json"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/cc-sync/internal/catalog"
	"github.com/yasyf/cc-sync/internal/scheduler"
	"github.com/yasyf/synckit/artifact"
	"github.com/yasyf/synckit/delivery"
	"github.com/yasyf/synckit/netpolicy"
	"github.com/yasyf/synckit/rpc"
	"github.com/yasyf/synckit/syncservice"
)

var bulkMethods = []string{
	artifact.MethodHave, artifact.MethodBatchBegin, artifact.MethodBatchPut, artifact.MethodBatchCommit, syncservice.MethodApplyV2,
}

func TestPolicyPausesOnEitherEndpointAndAutoResumes(t *testing.T) {
	connected := netpolicy.StatusConnected
	restricted := []struct {
		name        string
		state       netpolicy.State
		local, peer delivery.PauseReason
	}{
		{"cellular", netpolicy.State{Status: connected, Cellular: true}, delivery.PauseLocalCellular, delivery.PausePeerCellular},
		{"expensive", netpolicy.State{Status: connected, Expensive: true}, delivery.PauseLocalExpensive, delivery.PausePeerExpensive},
		{"constrained", netpolicy.State{Status: connected, Constrained: true}, delivery.PauseLocalConstrained, delivery.PausePeerConstrained},
		{"unknown", netpolicy.State{Status: netpolicy.StatusUnknown}, delivery.PauseLocalUnknown, delivery.PausePeerUnknown},
		{"disconnected", Disconnected, delivery.PauseLocalDisconnected, delivery.PausePeerDisconnected},
		{"manual metered", netpolicy.State{Status: connected, ManualMetered: true}, delivery.PauseLocalManualMetered, delivery.PausePeerManualMetered},
	}
	for _, r := range restricted {
		for _, remote := range []bool{false, true} {
			name, want := "local "+r.name, r.local
			if remote {
				name, want = "remote "+r.name, r.peer
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				origin := NewOrigin(t, "acme/app", map[string]string{"main.go": "package main\n"})
				mesh := NewMesh(t, NewClock(Now()))
				a := mesh.Add("host-a", origin)
				b := mesh.Add("host-b", origin)
				blocked := a
				if remote {
					blocked = b
				}
				blocked.SetNetwork(r.state)

				src := a.Checkout("acme/app")
				a.WriteFile(filepath.Join(src, "main.go"), "package main\n\nfunc main() {}\n", 0o644)
				sess := a.WriteSession(SessionSpec{
					Cwd: src, Branch: "main",
					Turns: []Turn{{Human: true, Text: "add a main func"}, {Text: "Added func main."}},
				})
				checkpoint(t, a, sess.ID)

				lanes := mesh.Lanes()
				link := mesh.Link("host-a", "host-b")
				paused := lanes.Await("host-a", "host-b", "pause", func(s delivery.PeerStatus) bool { return s.State == delivery.StatePaused })
				if paused.PauseReason != want {
					t.Fatalf("pause reason = %q, want %q (status %+v)", paused.PauseReason, want, paused)
				}
				requireNoBulk(t, link)
				if remote {
					if n := link.Calls(artifact.MethodNetStatus); n == 0 {
						t.Fatal("the lane paused on the peer's network without a net.status probe")
					}
					if paused.PeerNetwork == nil || paused.PeerNetwork.Status != r.state.Status || paused.PeerNetwork.Unrestricted() {
						t.Fatalf("paused lane reports peer network %+v, want the peer's %+v", paused.PeerNetwork, r.state)
					}
				}
				control, err := syncservice.NewClient(transport{host: b, link: link}).NetStatus(t.Context())
				if err != nil {
					t.Fatalf("net.status over the paused link: %v", err)
				}
				if actual, _ := b.Net.Current(); control.Unrestricted() == remote || control.Status != actual.Status {
					t.Fatalf("net.status over the paused link = %+v, want host-b's %+v", control, actual)
				}
				if held := checkpointIDs(b.Catalog(), "host-a"); len(held) != 0 {
					t.Fatalf("host-b holds %v from host-a while the lane is paused", held)
				}

				mesh.Clock.Advance(time.Minute)
				a.WriteFile(filepath.Join(src, "main.go"), "package main\n\nfunc main() { println(1) }\n", 0o644)
				a.AppendTurns(sess, Turn{Human: true, Text: "print one"}, Turn{Text: "Printed one."})
				second := checkpoint(t, a, sess.ID)
				if own := checkpointIDs(a.Catalog(), "host-a"); !slices.Contains(own, second[0].ID) {
					t.Fatalf("host-a's catalog %v lacks the checkpoint captured while paused %s", own, second[0].ID)
				}
				if again := lanes.Lane("host-a", "host-b"); again.State != delivery.StatePaused || again.PauseReason != want {
					t.Fatalf("lane after a paused capture = %s/%s, want still paused with %s", again.State, again.PauseReason, want)
				}
				requireNoBulk(t, link)

				blocked.SetNetwork(Unmetered)
				resumed := lanes.Await("host-a", "host-b", "auto-resume", func(s delivery.PeerStatus) bool {
					return s.State == delivery.StateIdle && s.AckedChangeID != ""
				})
				if resumed.PauseReason != "" {
					t.Fatalf("resumed lane still carries pause reason %q", resumed.PauseReason)
				}
				if link.Calls(syncservice.MethodApplyV2) == 0 {
					t.Fatal("the lane acked without an apply.v2")
				}
				held, want := checkpointIDs(b.Catalog(), "host-a"), checkpointIDs(a.Catalog(), "host-a")
				slices.Sort(held)
				slices.Sort(want)
				if !slices.Equal(held, want) || !slices.Contains(held, second[0].ID) {
					t.Fatalf("host-b holds %v after the auto-resume, want host-a's %v including %s captured while paused", held, want, second[0].ID)
				}
			})
		}
	}
}

func TestPolicyReceiverRefusesMidBatch(t *testing.T) {
	origin := NewOrigin(t, "acme/app", map[string]string{"main.go": "package main\n"})
	mesh := NewMesh(t, NewClock(Now()))
	a := mesh.Add("host-a", origin)
	b := mesh.Add("host-b", origin)
	a.SetNetwork(Cellular)

	src := a.Checkout("acme/app")
	a.WriteFile(filepath.Join(src, "blob.bin"), incompressible(4<<20), 0o644)
	sess := a.WriteSession(SessionSpec{
		Cwd: src, Branch: "main",
		Turns: []Turn{{Human: true, Text: "vendor the blob"}, {Text: "Added blob.bin."}},
	})
	cp := checkpoint(t, a, sess.ID)[0]

	lanes := mesh.Lanes()
	lanes.Await("host-a", "host-b", "local pause", func(s delivery.PeerStatus) bool { return s.PauseReason == delivery.PauseLocalCellular })

	link := mesh.Link("host-a", "host-b")
	var puts, probesAtFlip atomic.Int64
	atFlip := make(chan delivery.PeerStatus, 1)
	link.Intercept(func(req *rpc.Request) error {
		if req.Method != artifact.MethodBatchPut || puts.Add(1) != 2 {
			return nil
		}
		probesAtFlip.Store(int64(link.Calls(artifact.MethodNetStatus)))
		atFlip <- lanes.Lane("host-a", "host-b")
		b.SetNetwork(Cellular)
		return nil
	})
	a.SetNetwork(Unmetered)

	refused := lanes.Await("host-a", "host-b", "receiver refusal", func(s delivery.PeerStatus) bool {
		return s.State == delivery.StatePaused && s.PauseReason != delivery.PauseLocalCellular
	})
	var flip delivery.PeerStatus
	select {
	case flip = <-atFlip:
	default:
		t.Fatalf("the lane paused (%s) before a second batch.put; puts = %d", refused.PauseReason, puts.Load())
	}
	if refused.PauseReason != delivery.PausePeerCellular {
		t.Fatalf("pause reason = %q, want %q", refused.PauseReason, delivery.PausePeerCellular)
	}
	if flip.PeerNetwork == nil || !flip.PeerNetwork.Unrestricted() {
		t.Fatalf("sender's cached peer state at the flip = %+v, want unrestricted", flip.PeerNetwork)
	}
	if got := int64(link.Calls(artifact.MethodNetStatus)); got != probesAtFlip.Load() {
		t.Fatalf("net.status ran %d times by the pause, %d at the flip: the sender reprobed instead of the receiver refusing", got, probesAtFlip.Load())
	}
	if got := puts.Load(); got != 2 {
		t.Fatalf("batch.put ran %d times, want exactly the refused second part after one accepted", got)
	}
	for _, m := range []string{artifact.MethodBatchCommit, syncservice.MethodApplyV2} {
		if n := link.Calls(m); n != 0 {
			t.Fatalf("%s ran %d times across a mid-batch refusal", m, n)
		}
	}
	delta := refused.Progress.WireBytesSent - flip.Progress.WireBytesSent
	if delta <= 0 || delta > artifact.PartSize || delta > refused.Progress.InFlightLimit {
		t.Fatalf("wire bytes after the flip = %d (%d → %d), want the one refused part within (0, %d]",
			delta, flip.Progress.WireBytesSent, refused.Progress.WireBytesSent, min(int64(artifact.PartSize), refused.Progress.InFlightLimit))
	}

	link.Intercept(nil)
	b.SetNetwork(Unmetered)
	lanes.Await("host-a", "host-b", "auto-resume", func(s delivery.PeerStatus) bool {
		return s.State == delivery.StateIdle && s.AckedChangeID != ""
	})
	if held := checkpointIDs(b.Catalog(), "host-a"); !slices.Contains(held, cp.ID) {
		t.Fatalf("host-b holds %v after the resume, want %s", held, cp.ID)
	}
}

func TestExpiryWithoutResurrection(t *testing.T) {
	tests := []struct {
		name      string
		tombstone bool
	}{
		{"tombstone delivered", true},
		{"source gone, fence only", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			origin := NewOrigin(t, "acme/app", map[string]string{"main.go": "package main\n"})
			clock := NewClock(Now())
			mesh := NewMesh(t, clock)
			a := mesh.Add("host-a", origin)
			b := mesh.Add("host-b", origin)
			c := mesh.AddAt("host-c", NewClock(clock.Now()), origin)

			src := a.Checkout("acme/app")
			a.WriteFile(filepath.Join(src, "main.go"), "package main\n\nfunc main() {}\n", 0o644)
			sess := a.WriteSession(SessionSpec{
				Cwd: src, Branch: "main",
				Turns: []Turn{{Human: true, Text: "add a main func"}, {Text: "Added func main."}},
			})
			cp := checkpoint(t, a, sess.ID)[0]
			if !cp.ExpiresAt.Equal(cp.SourceActivityAt.Add(catalog.ExpiryWindow)) {
				t.Fatalf("checkpoint expires %s, want source activity %s + 7d", cp.ExpiresAt, cp.SourceActivityAt)
			}
			stale := block(t, a.Catalog(), "host-a")
			for _, to := range []string{"host-b", "host-c"} {
				requireAcked(t, deliver(t, mesh, "host-a", to))
			}
			if held := checkpointIDs(c.Catalog(), "host-a"); !slices.Contains(held, cp.ID) {
				t.Fatalf("host-c holds %v, want %s to relay", held, cp.ID)
			}

			clock.Advance(cp.ExpiresAt.Sub(clock.Now()) + time.Minute)
			reconcile(t, a)
			own := block(t, a.Catalog(), "host-a")
			if len(own.Worktrees) != 0 || own.Revision <= stale.Revision {
				t.Fatalf("host-a's block after expiry = rev %d worktrees %+v, want none past rev %d", own.Revision, own.Worktrees, stale.Revision)
			}
			if len(own.Tombstones) != 1 || own.Tombstones[0].ID != stale.Worktrees[0].ID {
				t.Fatalf("host-a's tombstones = %+v, want one for %s", own.Tombstones, stale.Worktrees[0].ID)
			}

			if tt.tombstone {
				requireAcked(t, deliver(t, mesh, "host-a", "host-b"))
			} else {
				a.Offline()
				if held := checkpointIDs(b.Catalog(), "host-a"); len(held) != 0 {
					t.Fatalf("host-b still shows %v past their expiry", held)
				}
			}
			reconcile(t, b)
			requireExpiredOnB(t, b, tt.tombstone, own, stale)

			d := deliver(t, mesh, "host-c", "host-b")
			requireAcked(t, d)
			relayed, err := catalog.Decode(d.Change.Payload)
			if err != nil {
				t.Fatalf("decode host-c's change: %v", err)
			}
			old := block(t, catalog.Snapshot{Origins: relayed.Origins}, "host-a")
			if old.Revision != stale.Revision || len(old.Worktrees) != 1 || len(old.Worktrees[0].Checkpoints) != 1 {
				t.Fatalf("host-c relayed host-a's block %+v, want the stale rev %d with its one checkpoint", old, stale.Revision)
			}
			if got := old.Worktrees[0].Checkpoints[0]; got.ID != cp.ID || !got.ExpiresAt.Equal(cp.ExpiresAt) || !got.SourceActivityAt.Equal(cp.SourceActivityAt) {
				t.Fatalf("relayed checkpoint %s expires %s (activity %s), want %s unrenewed at %s", got.ID, got.ExpiresAt, got.SourceActivityAt, cp.ID, cp.ExpiresAt)
			}
			requireExpiredOnB(t, b, tt.tombstone, own, stale)
			reconcile(t, b)
			requireExpiredOnB(t, b, tt.tombstone, own, stale)
		})
	}
}

func requireExpiredOnB(t *testing.T, b *Host, tombstone bool, own, stale catalog.Origin) {
	t.Helper()
	if held := checkpointIDs(b.Catalog(), "host-a"); len(held) != 0 {
		t.Fatalf("host-b resurrected %v", held)
	}
	i := slices.IndexFunc(b.Catalog().Origins, func(o catalog.Origin) bool { return o.Origin == "host-a" })
	if tombstone {
		if i < 0 {
			t.Fatal("host-b dropped host-a's tombstoned block")
		}
		got := b.Catalog().Origins[i]
		if got.Revision != own.Revision || len(got.Tombstones) != 1 || !got.Tombstones[0].ExpiresAt.Equal(own.Tombstones[0].ExpiresAt) {
			t.Fatalf("host-b holds host-a's block %+v, want rev %d with tombstone %+v", got, own.Revision, own.Tombstones)
		}
	} else if i >= 0 {
		t.Fatalf("host-b still holds host-a's lapsed block %+v", b.Catalog().Origins[i])
	}
	want := stale.Revision
	if tombstone {
		want = own.Revision
	}
	if got := fenceOf(t, b, "host-a"); got != want {
		t.Fatalf("host-b fences host-a at %d, want %d", got, want)
	}
}

func checkpoint(t *testing.T, h *Host, sessionID string) []catalog.Checkpoint {
	t.Helper()
	before := checkpointIDs(h.Catalog(), h.Name)
	attempts := h.Kick(sessionID)
	if len(attempts) != 1 || attempts[0].Outcome != scheduler.OutcomeCaptured {
		t.Fatalf("%s: kick = %+v, want one captured attempt", h.Name, attempts)
	}
	var fresh []catalog.Checkpoint
	for _, w := range block(t, h.Catalog(), h.Name).Worktrees {
		for _, cp := range w.Checkpoints {
			if !slices.Contains(before, cp.ID) {
				fresh = append(fresh, cp)
			}
		}
	}
	if len(fresh) == 0 {
		t.Fatalf("%s: the kick recorded no new checkpoint", h.Name)
	}
	return fresh
}

func block(t *testing.T, snap catalog.Snapshot, origin string) catalog.Origin {
	t.Helper()
	i := slices.IndexFunc(snap.Origins, func(o catalog.Origin) bool { return o.Origin == origin })
	if i < 0 {
		t.Fatalf("no block for %s among %d origins", origin, len(snap.Origins))
	}
	return snap.Origins[i]
}

func checkpointIDs(snap catalog.Snapshot, origin string) []string {
	var ids []string
	for _, o := range snap.Origins {
		if o.Origin != origin {
			continue
		}
		for _, w := range o.Worktrees {
			for _, cp := range w.Checkpoints {
				ids = append(ids, cp.ID)
			}
		}
	}
	return ids
}

func deliver(t *testing.T, mesh *Mesh, from, to string) Delivery {
	t.Helper()
	d, err := mesh.Deliver(t.Context(), from, to)
	if err != nil {
		t.Fatalf("deliver %s→%s: %v", from, to, err)
	}
	return d
}

func requireAcked(t *testing.T, d Delivery) {
	t.Helper()
	if d.Result.Partial || d.Result.AckedRevision != d.Change.SourceRevision {
		t.Fatalf("apply = %+v, want a full ACK of %s", d.Result, d.Change.SourceRevision)
	}
}

func reconcile(t *testing.T, h *Host) {
	t.Helper()
	if _, err := syncservice.NewClient(transport{host: h}).Reconcile(t.Context(), ""); err != nil {
		t.Fatalf("%s: reconcile: %v", h.Name, err)
	}
}

func fenceOf(t *testing.T, h *Host, origin string) uint64 {
	t.Helper()
	raw, err := os.ReadFile(h.Layout.CatalogPath)
	if err != nil {
		t.Fatal(err)
	}
	var st struct {
		Fences []struct {
			Origin   string `json:"origin"`
			Revision uint64 `json:"revision"`
		} `json:"fences"`
	}
	if err := json.Unmarshal(raw, &st); err != nil {
		t.Fatalf("%s: decode catalog state: %v", h.Name, err)
	}
	for _, f := range st.Fences {
		if f.Origin == origin {
			return f.Revision
		}
	}
	t.Fatalf("%s has no fence for %s in %+v", h.Name, origin, st.Fences)
	return 0
}

func requireNoBulk(t *testing.T, link *Link) {
	t.Helper()
	for _, m := range bulkMethods {
		if n := link.Calls(m); n != 0 {
			t.Fatalf("%s ran %d times on a paused lane", m, n)
		}
	}
}

func incompressible(size int) string {
	buf := make([]byte, size)
	_, _ = rand.NewChaCha8([32]byte{'c', 'c', '-', 's', 'y', 'n', 'c'}).Read(buf)
	return string(buf)
}
