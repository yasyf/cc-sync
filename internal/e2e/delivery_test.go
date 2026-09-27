//go:build e2e

package e2e

import (
	"encoding/base64"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/cc-sync/internal/catalog"
	"github.com/yasyf/cc-sync/internal/scheduler"
	"github.com/yasyf/synckit/artifact"
	"github.com/yasyf/synckit/delivery"
	"github.com/yasyf/synckit/rpc"
	"github.com/yasyf/synckit/syncservice"
)

const (
	hostA = "host-a"
	hostB = "host-b"
	hostC = "host-c"
)

var errDropped = errors.New("e2e: part dropped in transit")

type workspace struct {
	mesh *Mesh
	src  string
	sess *Session
}

func newWorkspace(t *testing.T, hosts ...string) workspace {
	t.Helper()
	origin := NewOrigin(t, "acme/app", map[string]string{"README.md": "hello\n", "main.go": "package main\n"})
	mesh := NewMesh(t, NewClock(Now()))
	for _, name := range hosts {
		mesh.Add(name, origin)
	}
	a := mesh.Host(hostA)
	src := a.Checkout("acme/app")
	a.WriteFile(filepath.Join(src, "main.go"), "package main\n\nfunc main() {}\n", 0o644)
	sess := a.WriteSession(SessionSpec{
		Cwd: src, Branch: "main",
		Turns:       []Turn{{Human: true, Text: "add a main func"}, {Text: "Added func main."}},
		ToolResults: 1,
	})
	return workspace{mesh: mesh, src: src, sess: sess}
}

func captured(t *testing.T, h *Host, sess *Session) {
	t.Helper()
	attempts := h.Kick(sess.ID)
	if len(attempts) != 1 || attempts[0].Outcome != scheduler.OutcomeCaptured {
		t.Fatalf("%s kick = %+v, want one captured attempt", h.Name, attempts)
	}
}

func ownCheckpoints(t *testing.T, h *Host) []catalog.Checkpoint {
	t.Helper()
	block, ok := originBlock(h.Catalog(), h.Name)
	if !ok {
		t.Fatalf("%s has no own catalog block", h.Name)
	}
	var cps []catalog.Checkpoint
	for _, wt := range block.Worktrees {
		cps = append(cps, wt.Checkpoints...)
	}
	if len(cps) == 0 {
		t.Fatalf("%s's own block holds no checkpoints", h.Name)
	}
	return cps
}

func originBlock(snap catalog.Snapshot, origin string) (catalog.Origin, bool) {
	i := slices.IndexFunc(snap.Origins, func(o catalog.Origin) bool { return o.Origin == origin })
	if i < 0 {
		return catalog.Origin{}, false
	}
	return snap.Origins[i], true
}

func readyOn(h *Host, origin string, want catalog.Checkpoint) bool {
	snap := h.Catalog()
	block, ok := originBlock(snap, origin)
	if !ok {
		return false
	}
	for _, wt := range block.Worktrees {
		for _, cp := range wt.Checkpoints {
			if cp.ID == want.ID {
				return snap.ReadinessOf(origin, cp).Ready
			}
		}
	}
	return false
}

func changeID(t *testing.T, req *rpc.Request) string {
	t.Helper()
	id, ok := req.Params["change_id"].(string)
	if !ok || id == "" {
		t.Errorf("%s params carry change_id %v", req.Method, req.Params["change_id"])
	}
	return id
}

func digests(refs []artifact.Ref) []artifact.Digest {
	out := make([]artifact.Digest, len(refs))
	for i, r := range refs {
		out[i] = r.Digest
	}
	slices.Sort(out)
	return out
}

func TestDeliveryFaultWithholdsAckUntilCleanRetry(t *testing.T) {
	faults := []struct {
		name  string
		fault func(t *testing.T, hits *atomic.Int64) func(*rpc.Request) error
	}{
		{"missing parts", func(_ *testing.T, hits *atomic.Int64) func(*rpc.Request) error {
			return func(req *rpc.Request) error {
				if req.Method != artifact.MethodBatchPut {
					return nil
				}
				hits.Add(1)
				return errDropped
			}
		}},
		{"corrupt part", func(t *testing.T, hits *atomic.Int64) func(*rpc.Request) error {
			return func(req *rpc.Request) error {
				if req.Method != artifact.MethodBatchPut {
					return nil
				}
				data, ok := req.Params["data"].(string)
				if !ok {
					t.Errorf("batch put data is %T, want base64 string", req.Params["data"])
					return errDropped
				}
				raw, err := base64.StdEncoding.DecodeString(data)
				if err != nil || len(raw) == 0 {
					t.Errorf("batch put data %q does not decode: %v", data, err)
					return errDropped
				}
				raw[len(raw)/2] ^= 0xff
				req.Params["data"] = base64.StdEncoding.EncodeToString(raw)
				hits.Add(1)
				return nil
			}
		}},
		{"interrupted after first part", func(_ *testing.T, hits *atomic.Int64) func(*rpc.Request) error {
			var crossed atomic.Bool
			return func(req *rpc.Request) error {
				if crossed.Load() {
					hits.Add(1)
					return ErrLinkDown
				}
				if req.Method == artifact.MethodBatchPut {
					crossed.Store(true)
				}
				return nil
			}
		}},
	}
	for _, tc := range faults {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorkspace(t, hostA, hostB)
			a, b := w.mesh.Host(hostA), w.mesh.Host(hostB)
			captured(t, a, w.sess)
			cps := ownCheckpoints(t, a)
			link := w.mesh.Link(hostA, hostB)
			var hits atomic.Int64
			link.Intercept(tc.fault(t, &hits))

			lanes := w.mesh.Lanes(LanesConfig{Senders: []string{hostA}, Receivers: []string{hostB}, Retry: 100 * time.Millisecond})
			held := lanes.Await(hostA, hostB, "backoff after the fault", func(s delivery.PeerStatus) bool {
				return s.State == delivery.StateBackoff && hits.Load() > 0
			})
			if held.Pending == nil || held.AckedChangeID != "" || held.Acked != syncservice.NewRevision(0) {
				t.Fatalf("after %s: status %+v, want a retained pending and no ACK", tc.name, held)
			}
			if got := a.PendingChange(held.Pending.ChangeID); got.ChangeID != held.Pending.ChangeID || len(got.Artifacts) == 0 {
				t.Fatalf("pending envelope on disk = %+v, want change %s with roots", got, held.Pending.ChangeID)
			}
			if r, ok := b.Receipts()[hostA]; ok {
				t.Fatalf("%s recorded receipt %+v for %s after %s", hostB, r, hostA, tc.name)
			}
			for _, cp := range cps {
				if readyOn(b, hostA, cp) {
					t.Fatalf("checkpoint %s is Ready on %s after %s", cp.ID, hostB, tc.name)
				}
			}

			link.Intercept(nil)
			done := lanes.WaitIdle(hostA, hostB)
			if done.Pending != nil || done.AckedChangeID != held.Pending.ChangeID || done.Acked != held.Pending.SourceRevision {
				t.Fatalf("after clean retry: status %+v, want ACK of retained change %s at %s", done, held.Pending.ChangeID, held.Pending.SourceRevision)
			}
			if r := b.Receipts()[hostA]; r.ChangeID != held.Pending.ChangeID {
				t.Fatalf("%s receipt for %s = %+v, want change %s", hostB, hostA, r, held.Pending.ChangeID)
			}
			for _, cp := range cps {
				if !readyOn(b, hostA, cp) {
					t.Fatalf("checkpoint %s is not Ready on %s after the clean retry", cp.ID, hostB)
				}
			}
		})
	}
}

func TestDeliveryReplayIsIdempotent(t *testing.T) {
	w := newWorkspace(t, hostA, hostB)
	a, b := w.mesh.Host(hostA), w.mesh.Host(hostB)
	captured(t, a, w.sess)

	var mu sync.Mutex
	applies := map[string]map[string]any{}
	w.mesh.Link(hostA, hostB).Intercept(func(req *rpc.Request) error {
		if req.Method == syncservice.MethodApplyV2 {
			mu.Lock()
			applies[changeID(t, req)] = maps.Clone(req.Params)
			mu.Unlock()
		}
		return nil
	})
	lanes := w.mesh.Lanes(LanesConfig{Senders: []string{hostA}, Receivers: []string{hostB}, Retry: time.Second})
	acked := lanes.WaitIdle(hostA, hostB)
	lanes.Close()
	if acked.AckedChangeID == "" || acked.Pending != nil {
		t.Fatalf("first delivery: status %+v, want an ACK", acked)
	}
	mu.Lock()
	params, ok := applies[acked.AckedChangeID]
	mu.Unlock()
	if !ok {
		t.Fatalf("no apply of acked change %s crossed the link", acked.AckedChangeID)
	}

	before, err := os.ReadFile(b.Layout.CatalogPath)
	if err != nil {
		t.Fatal(err)
	}
	receipt := b.Receipts()[hostA]
	if receipt.ChangeID != acked.AckedChangeID {
		t.Fatalf("receipt = %+v, want change %s", receipt, acked.AckedChangeID)
	}
	block, _ := originBlock(b.Catalog(), hostA)
	for i := range 2 {
		var res syncservice.ApplyResult
		if err := b.Call(t.Context(), syncservice.MethodApplyV2, params, &res); err != nil {
			t.Fatalf("replay %d: %v", i, err)
		}
		if res.Partial || res.NeedSnapshot || res.AckedRevision != acked.Acked {
			t.Fatalf("replay %d = %+v, want the same full ACK at %s", i, res, acked.Acked)
		}
	}
	after, err := os.ReadFile(b.Layout.CatalogPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(before) {
		t.Fatalf("replay rewrote %s's catalog:\nbefore %s\nafter  %s", hostB, before, after)
	}
	if got := b.Receipts()[hostA]; got != receipt {
		t.Fatalf("receipt after replay = %+v, want %+v", got, receipt)
	}
	if got, _ := originBlock(b.Catalog(), hostA); !reflect.DeepEqual(got, block) {
		t.Fatalf("block after replay = %+v, want %+v", got, block)
	}
}

func TestDeliveryRelaysOriginBlockVerbatim(t *testing.T) {
	w := newWorkspace(t, hostA, hostB, hostC)
	a, b, c := w.mesh.Host(hostA), w.mesh.Host(hostB), w.mesh.Host(hostC)
	w.mesh.Link(hostA, hostC).Cut()
	captured(t, a, w.sess)

	first := w.mesh.Lanes(LanesConfig{Senders: []string{hostA}, Receivers: []string{hostB}, Retry: time.Second})
	if s := first.WaitIdle(hostA, hostB); s.AckedChangeID == "" || s.Pending != nil {
		t.Fatalf("%s -> %s: %+v, want an ACK", hostA, hostB, s)
	}
	first.Close()
	w.mesh.Link(hostA, hostB).Cut()
	own, ok := originBlock(a.Catalog(), hostA)
	if !ok {
		t.Fatalf("%s has no own block", hostA)
	}
	if _, ok := originBlock(c.Catalog(), hostA); ok {
		t.Fatalf("%s holds %s's block before any relay", hostC, hostA)
	}

	a.Offline()
	if err := os.RemoveAll(a.Root); err != nil {
		t.Fatal(err)
	}

	second := w.mesh.Lanes(LanesConfig{Senders: []string{hostB}, Receivers: []string{hostC}, Retry: time.Second})
	if s := second.WaitIdle(hostB, hostC); s.AckedChangeID == "" || s.Pending != nil {
		t.Fatalf("%s -> %s: %+v, want an ACK", hostB, hostC, s)
	}
	for _, h := range []*Host{b, c} {
		got, ok := originBlock(h.Catalog(), hostA)
		if !ok || !reflect.DeepEqual(got, own) {
			t.Fatalf("%s's copy of %s's block = %+v, want verbatim %+v", h.Name, hostA, got, own)
		}
	}
	for _, method := range []string{artifact.MethodBatchPut, syncservice.MethodApplyV2} {
		if n := w.mesh.Link(hostA, hostC).Calls(method); n != 0 {
			t.Fatalf("%d %s calls went %s -> %s directly", n, method, hostA, hostC)
		}
	}

	res := c.Pickup(hostA + ":" + w.sess.ID)
	got, err := os.ReadFile(filepath.Join(res.Checkout.Path, "main.go"))
	if err != nil || string(got) != "package main\n\nfunc main() {}\n" {
		t.Fatalf("%s restored main.go = %q, %v", hostC, got, err)
	}
}

func TestDeliveryOfflineSupersedeKeepsLatestOnly(t *testing.T) {
	w := newWorkspace(t, hostA, hostB)
	a, b := w.mesh.Host(hostA), w.mesh.Host(hostB)
	b.Offline()

	lanes := w.mesh.Lanes(LanesConfig{Senders: []string{hostA}, Receivers: []string{hostB}, Retry: time.Hour})
	start := lanes.WaitIdle(hostA, hostB)
	base := uint64(0)
	prev := ""
	if start.Pending != nil {
		base, prev = start.Pending.Superseded+1, start.Pending.ChangeID
	}
	staged := make([]syncservice.ChangeEnvelope, 0, 5)
	for i := range 5 {
		w.mesh.Clock.Advance(time.Minute)
		a.AppendTurns(w.sess, Turn{Human: true, Text: fmt.Sprintf("step %d", i)}, Turn{Text: fmt.Sprintf("did step %d", i)})
		captured(t, a, w.sess)
		lanes.Kick(hostA, hostB)
		s := lanes.Await(hostA, hostB, fmt.Sprintf("checkpoint %d staged", i), func(s delivery.PeerStatus) bool {
			return s.Pending != nil && s.Pending.ChangeID != prev && s.State == delivery.StatePaused
		})
		if s.PauseReason != delivery.PausePeerUnreachable {
			t.Fatalf("checkpoint %d: pause %q, want %q", i, s.PauseReason, delivery.PausePeerUnreachable)
		}
		if s.Pending.Superseded != base+uint64(i) {
			t.Fatalf("checkpoint %d: superseded = %d, want %d", i, s.Pending.Superseded, base+uint64(i))
		}
		prev = s.Pending.ChangeID
		staged = append(staged, a.PendingChange(prev))
	}

	latest := staged[len(staged)-1]
	held := lanes.Status(hostA, hostB)
	if held.Pending == nil || held.Pending.ChangeID != latest.ChangeID || held.AckedChangeID != "" {
		t.Fatalf("while offline: %+v, want only pending %s", held, latest.ChangeID)
	}
	if files := a.PendingFiles(); !slices.Equal(files, []string{latest.ChangeID}) {
		t.Fatalf("pending envelopes on disk = %v, want only %s", files, latest.ChangeID)
	}
	if pins := digests(a.Pins("synckit.delivery/" + hostB)); !slices.Equal(pins, digests(latest.Artifacts)) {
		t.Fatalf("delivery pins = %v, want exactly the latest roots %v", pins, digests(latest.Artifacts))
	}

	var mu sync.Mutex
	var applied []string
	w.mesh.Link(hostA, hostB).Intercept(func(req *rpc.Request) error {
		if req.Method == syncservice.MethodApplyV2 {
			mu.Lock()
			applied = append(applied, changeID(t, req))
			mu.Unlock()
		}
		return nil
	})
	b.Online()
	a.SetNetwork(Unmetered)
	lanes.Kick(hostA, hostB)
	done := lanes.WaitIdle(hostA, hostB)
	if done.Pending != nil || done.AckedChangeID != latest.ChangeID || done.Acked != latest.SourceRevision {
		t.Fatalf("after %s returned: %+v, want ACK of latest %s", hostB, done, latest.ChangeID)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(applied) == 0 || slices.ContainsFunc(applied, func(id string) bool { return id != latest.ChangeID }) {
		t.Fatalf("applies to %s = %v, want only latest %s", hostB, applied, latest.ChangeID)
	}
	if r := b.Receipts()[hostA]; r.ChangeID != latest.ChangeID {
		t.Fatalf("%s receipt = %+v, want latest %s", hostB, r, latest.ChangeID)
	}
}

func TestDeliveryStaleAckCannotClearNewerPending(t *testing.T) {
	w := newWorkspace(t, hostA, hostB)
	a, b := w.mesh.Host(hostA), w.mesh.Host(hostB)
	captured(t, a, w.sess)

	entered := make(chan string, 1)
	release := make(chan struct{})
	var once sync.Once
	w.mesh.Link(hostA, hostB).Intercept(func(req *rpc.Request) error {
		if req.Method == syncservice.MethodApplyV2 {
			once.Do(func() {
				entered <- changeID(t, req)
				select {
				case <-release:
				case <-t.Context().Done():
				}
			})
		}
		return nil
	})
	old := w.mesh.Lanes(LanesConfig{Senders: []string{hostA}, Receivers: []string{hostB}, Retry: time.Hour})
	var stale string
	select {
	case stale = <-entered:
	case <-time.After(laneWait):
		t.Fatal("the first delivery never reached apply")
	}

	w.mesh.Clock.Advance(time.Minute)
	a.AppendTurns(w.sess, Turn{Human: true, Text: "one more"}, Turn{Text: "done"})
	captured(t, a, w.sess)
	detached := &Link{calls: map[string]int{}}
	detached.Cut()
	reloaded := w.mesh.Lanes(LanesConfig{
		Senders: []string{hostA}, Receivers: []string{hostB}, Retry: time.Hour,
		Link: func(string, string) *Link { return detached },
	})
	fresh := reloaded.Await(hostA, hostB, "newer change staged", func(s delivery.PeerStatus) bool {
		return s.Pending != nil && s.Pending.ChangeID != stale && s.State == delivery.StatePaused
	})
	if fresh.Pending.Superseded != 1 {
		t.Fatalf("restaged pending superseded = %d, want 1", fresh.Pending.Superseded)
	}

	close(release)
	refused := old.Await(hostA, hostB, "stale ACK refused", func(s delivery.PeerStatus) bool {
		return s.State == delivery.StateBackoff && strings.Contains(s.LastError, "pending change identity changed")
	})
	if !strings.Contains(refused.LastError, stale) {
		t.Fatalf("refusal %q does not name the stale change %s", refused.LastError, stale)
	}
	old.Close()
	if r := b.Receipts()[hostA]; r.ChangeID != stale {
		t.Fatalf("%s receipt = %+v, want the stale change %s it acknowledged", hostB, r, stale)
	}
	kept := reloaded.Status(hostA, hostB)
	if kept.Pending == nil || kept.Pending.ChangeID != fresh.Pending.ChangeID || kept.AckedChangeID != "" || kept.Acked != syncservice.NewRevision(0) {
		t.Fatalf("after the stale ACK: %+v, want pending %s untouched and nothing acked", kept, fresh.Pending.ChangeID)
	}

	detached.Heal()
	a.SetNetwork(Unmetered)
	reloaded.Kick(hostA, hostB)
	done := reloaded.WaitIdle(hostA, hostB)
	if done.Pending != nil || done.AckedChangeID != fresh.Pending.ChangeID {
		t.Fatalf("after heal: %+v, want ACK of %s", done, fresh.Pending.ChangeID)
	}
}
