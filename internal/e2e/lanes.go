//go:build e2e

package e2e

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/cc-sync/internal/consumer"
	"github.com/yasyf/cc-sync/internal/resident"
	"github.com/yasyf/synckit/daemon"
	"github.com/yasyf/synckit/delivery"
	"github.com/yasyf/synckit/manifest"
	"github.com/yasyf/synckit/syncservice"
)

// LaneRetry is the pause recheck and backoff interval of every Lanes
// scheduler.
const LaneRetry = 50 * time.Millisecond

// Lanes runs synckitd's real delivery scheduler for every host of a mesh:
// a cc-sync lane from each host to every other, gated on each host's own
// Network, over the mesh's Links.
type Lanes struct {
	*daemon.Harness

	t *testing.T
}

// Lanes starts delivery lanes between every host added so far. Like
// synckitd after a reload, every lane runs once at start; nothing kicks a
// lane after that unless the scenario calls Kick.
func (m *Mesh) Lanes() *Lanes {
	m.t.Helper()
	m.mu.Lock()
	hosts := make([]daemon.HarnessHost, 0, len(m.order))
	for _, name := range m.order {
		h := m.hosts[name]
		hosts = append(hosts, daemon.HarnessHost{
			Name:     name,
			StateDir: filepath.Join(h.MeshDir, "delivery"),
			Monitor:  h.Net,
			Services: map[string]syncservice.Transport{consumer.ServiceID: transport{host: h}},
		})
	}
	m.mu.Unlock()
	harness, err := daemon.NewHarness(context.WithoutCancel(m.t.Context()), daemon.HarnessConfig{
		Hosts:     hosts,
		Manifests: []manifest.Manifest{resident.Manifest()},
		Links: func(from, to, _ string) syncservice.Transport {
			return transport{host: m.Host(to), link: m.Link(from, to)}
		},
		RetryInterval: LaneRetry,
	})
	if err != nil {
		m.t.Fatalf("start delivery lanes: %v", err)
	}
	m.t.Cleanup(func() {
		if err := harness.Close(); err != nil {
			m.t.Errorf("close delivery lanes: %v", err)
		}
	})
	return &Lanes{Harness: harness, t: m.t}
}

// Lane returns the live status of host's cc-sync lane to peer.
func (l *Lanes) Lane(host, peer string) delivery.PeerStatus {
	l.t.Helper()
	statuses, err := l.Status(host, consumer.ServiceID)
	if err != nil {
		l.t.Fatalf("%s: delivery status: %v", host, err)
	}
	for _, s := range statuses {
		if s.Peer == peer {
			return s
		}
	}
	l.t.Fatalf("%s has no lane to %s in %+v", host, peer, statuses)
	return delivery.PeerStatus{}
}

// Await waits until host's lane to peer has settled into a status ok
// accepts, failing the test after 60 s.
func (l *Lanes) Await(host, peer, what string, ok func(delivery.PeerStatus) bool) delivery.PeerStatus {
	l.t.Helper()
	ctx, cancel := context.WithTimeout(l.t.Context(), 60*time.Second)
	defer cancel()
	for {
		status, err := l.WaitIdle(ctx, host, consumer.ServiceID, peer)
		if err != nil {
			l.t.Fatalf("%s→%s: await %s: %v; last status %+v", host, peer, what, err, status)
		}
		if ok(status) {
			return status
		}
		select {
		case <-ctx.Done():
			l.t.Fatalf("%s→%s: await %s: %v; last status %+v", host, peer, what, ctx.Err(), status)
		case <-time.After(LaneRetry / 5):
		}
	}
}
