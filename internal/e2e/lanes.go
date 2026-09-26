//go:build e2e

package e2e

import (
	"context"
	"path/filepath"
	"time"

	"github.com/yasyf/synckit/daemon"
	"github.com/yasyf/synckit/manifest"
	"github.com/yasyf/synckit/syncservice"

	"github.com/yasyf/cc-sync/internal/consumer"
	"github.com/yasyf/cc-sync/internal/resident"
)

// Lanes runs synckitd's delivery scheduler (daemon.Harness) over every host
// of the mesh: each host runs cc-sync's manifest with its resident as the
// local consumer and reaches each peer over Link(from, to). maxWait is the
// artifact coalescing window (synckitd: 10 s); retry paces paused and failed
// lanes.
func (m *Mesh) Lanes(maxWait, retry time.Duration) *daemon.Harness {
	m.t.Helper()
	m.mu.Lock()
	hosts := make([]daemon.HarnessHost, 0, len(m.order))
	for _, name := range m.order {
		h := m.hosts[name]
		hosts = append(hosts, daemon.HarnessHost{
			Name:     name,
			StateDir: filepath.Join(h.Root, "delivery"),
			Monitor:  h.Net,
			Services: map[string]syncservice.Transport{consumer.ServiceID: transport{host: h}},
		})
	}
	m.mu.Unlock()
	lanes, err := daemon.NewHarness(context.Background(), daemon.HarnessConfig{
		Hosts:     hosts,
		Manifests: []manifest.Manifest{resident.Manifest()},
		Links: func(from, to, _ string) syncservice.Transport {
			return transport{host: m.Host(to), link: m.Link(from, to)}
		},
		ArtifactMaxWait: maxWait,
		RetryInterval:   retry,
	})
	if err != nil {
		m.t.Fatalf("start delivery lanes: %v", err)
	}
	m.t.Cleanup(func() {
		if err := lanes.Close(); err != nil {
			m.t.Errorf("close delivery lanes: %v", err)
		}
	})
	return lanes
}
