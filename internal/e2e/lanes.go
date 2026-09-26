//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-sync/internal/consumer"
	"github.com/yasyf/cc-sync/internal/resident"
	"github.com/yasyf/synckit/artifact"
	"github.com/yasyf/synckit/daemon"
	"github.com/yasyf/synckit/delivery"
	"github.com/yasyf/synckit/manifest"
	"github.com/yasyf/synckit/syncservice"
)

const (
	laneWait = 60 * time.Second
	lanePoll = 5 * time.Millisecond
)

// LanesConfig picks the mesh hosts one Lanes run covers.
type LanesConfig struct {
	// Senders run the cc-sync service: each gets a lane to every other listed
	// host. Nil makes every host of the mesh a sender.
	Senders []string
	// Receivers are peers only and run no lanes of their own.
	Receivers []string
	// Retry is the harness RetryInterval: a paused lane's recheck and a failed
	// lane's backoff.
	Retry time.Duration
	// MaxWait is the harness ArtifactMaxWait: how long a kicked lane coalesces
	// further kicks before it runs (synckitd: 10 s).
	MaxWait time.Duration
	// Link routes from's calls to to; nil routes over Mesh.Link.
	Link func(from, to string) *Link
}

// Lanes is synckit's real delivery scheduler (daemon.Harness) over mesh
// hosts. Each sender's lanes read its own resident and dial every peer's
// resident over a Link, keeping delivery-v2 state in the host's DeliveryDir,
// so two Lanes over one host share its delivery store the way an old and a
// reloaded synckitd would.
type Lanes struct {
	t       *testing.T
	harness *daemon.Harness
	closed  bool
}

// Lanes starts the scheduler for cfg; like synckitd after a reload, every
// lane runs once at start.
func (m *Mesh) Lanes(cfg LanesConfig) *Lanes {
	m.t.Helper()
	link := cfg.Link
	if link == nil {
		link = m.Link
	}
	senders := cfg.Senders
	if senders == nil {
		m.mu.Lock()
		senders = slices.Clone(m.order)
		m.mu.Unlock()
	}
	hosts := make([]daemon.HarnessHost, 0, len(senders)+len(cfg.Receivers))
	for _, name := range slices.Concat(senders, cfg.Receivers) {
		h := m.Host(name)
		if err := os.MkdirAll(h.DeliveryDir(), 0o750); err != nil {
			m.t.Fatal(err)
		}
		host := daemon.HarnessHost{Name: name, StateDir: h.DeliveryDir(), Monitor: h.Net}
		if slices.Contains(senders, name) {
			host.Services = map[string]syncservice.Transport{consumer.ServiceID: transport{host: h}}
		}
		hosts = append(hosts, host)
	}
	harness, err := daemon.NewHarness(m.t.Context(), daemon.HarnessConfig{
		Hosts:     hosts,
		Manifests: []manifest.Manifest{resident.Manifest()},
		Links: func(from, to, _ string) syncservice.Transport {
			return transport{host: m.Host(to), link: link(from, to)}
		},
		ArtifactMaxWait: cfg.MaxWait,
		RetryInterval:   cfg.Retry,
	})
	if err != nil {
		m.t.Fatalf("start delivery lanes: %v", err)
	}
	l := &Lanes{t: m.t, harness: harness}
	m.t.Cleanup(l.Close)
	return l
}

// Kick marks from's lane to to dirty, as a local catalog change does; an
// empty to kicks every lane of from.
func (l *Lanes) Kick(from, to string) {
	l.t.Helper()
	if err := l.harness.Kick(consumer.ServiceID, from, to); err != nil {
		l.t.Fatalf("kick %s -> %s: %v", from, to, err)
	}
}

// Status reads host's lane to peer.
func (l *Lanes) Status(host, peer string) delivery.PeerStatus {
	l.t.Helper()
	statuses, err := l.harness.Status(host, consumer.ServiceID)
	if err != nil {
		l.t.Fatalf("status of %s: %v", host, err)
	}
	i := slices.IndexFunc(statuses, func(s delivery.PeerStatus) bool { return s.Peer == peer })
	if i < 0 {
		l.t.Fatalf("%s has no lane to %s: %+v", host, peer, statuses)
	}
	return statuses[i]
}

// WaitIdle waits until host's lane to peer rests idle or paused.
func (l *Lanes) WaitIdle(host, peer string) delivery.PeerStatus {
	l.t.Helper()
	ctx, cancel := context.WithTimeout(l.t.Context(), laneWait)
	defer cancel()
	status, err := l.harness.WaitIdle(ctx, host, consumer.ServiceID, peer)
	if err != nil {
		l.t.Fatalf("%v; last status %+v", err, status)
	}
	return status
}

// Await polls host's lane to peer until cond holds.
func (l *Lanes) Await(host, peer, what string, cond func(delivery.PeerStatus) bool) delivery.PeerStatus {
	l.t.Helper()
	deadline := time.Now().Add(laneWait)
	for {
		status := l.Status(host, peer)
		if cond(status) {
			return status
		}
		if time.Now().After(deadline) {
			l.t.Fatalf("%s -> %s never reached %s; last status %+v", host, peer, what, status)
		}
		time.Sleep(lanePoll)
	}
}

// Close stops every lane and waits for in-flight attempts to unwind.
func (l *Lanes) Close() {
	if l.closed {
		return
	}
	l.closed = true
	if err := l.harness.Close(); err != nil {
		l.t.Errorf("close delivery lanes: %v", err)
	}
}

// DeliveryDir is the host's synckitd delivery-v2 state dir.
func (h *Host) DeliveryDir() string { return filepath.Join(h.MeshDir, "delivery") }

// PendingFiles lists the staged change envelopes the host's delivery store
// holds on disk, by change ID.
func (h *Host) PendingFiles() []string {
	h.t.Helper()
	entries, err := os.ReadDir(filepath.Join(h.DeliveryDir(), "delivery-v2", "pending"))
	if err != nil {
		h.t.Fatalf("%s: list pending changes: %v", h.Name, err)
	}
	ids := make([]string, 0, len(entries))
	for _, e := range entries {
		if id, ok := strings.CutSuffix(e.Name(), ".json"); ok {
			ids = append(ids, id)
		}
	}
	return ids
}

// PendingChange reads the staged change envelope changeID.
func (h *Host) PendingChange(changeID string) syncservice.ChangeEnvelope {
	h.t.Helper()
	raw, err := os.ReadFile(filepath.Join(h.DeliveryDir(), "delivery-v2", "pending", changeID+".json"))
	if err != nil {
		h.t.Fatalf("%s: read pending change %s: %v", h.Name, changeID, err)
	}
	var change syncservice.ChangeEnvelope
	if err := json.Unmarshal(raw, &change); err != nil {
		h.t.Fatalf("%s: decode pending change %s: %v", h.Name, changeID, err)
	}
	return change
}

// Pins reads the roots the host's artifact store pins for owner.
func (h *Host) Pins(owner string) []artifact.Ref {
	h.t.Helper()
	sets, err := h.Store().Pins(context.Background())
	if err != nil {
		h.t.Fatalf("%s: read pins: %v", h.Name, err)
	}
	for _, set := range sets {
		if set.Owner == owner {
			return set.Roots
		}
	}
	return nil
}

// Receipts reads the catalog's persisted apply receipts by origin; a host
// that never applied a change has none.
func (h *Host) Receipts() map[string]syncservice.Receipt {
	h.t.Helper()
	raw, err := os.ReadFile(h.Layout.CatalogPath)
	if errors.Is(err, os.ErrNotExist) {
		return map[string]syncservice.Receipt{}
	}
	if err != nil {
		h.t.Fatalf("%s: read catalog: %v", h.Name, err)
	}
	var st struct {
		Receipts []syncservice.Receipt `json:"receipts"`
	}
	if err := json.Unmarshal(raw, &st); err != nil {
		h.t.Fatalf("%s: decode catalog receipts: %v", h.Name, err)
	}
	out := make(map[string]syncservice.Receipt, len(st.Receipts))
	for _, r := range st.Receipts {
		out[r.Origin] = r
	}
	return out
}
