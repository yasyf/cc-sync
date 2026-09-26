//go:build e2e

package e2e

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/yasyf/cc-sync/internal/consumer"
	"github.com/yasyf/synckit/artifact"
	"github.com/yasyf/synckit/rpc"
	"github.com/yasyf/synckit/syncservice"
)

// ErrLinkDown is what a cut link returns for every call.
var ErrLinkDown = errors.New("e2e: link down")

// Mesh is a set of hosts sharing one clock, with one directed Link per
// ordered host pair.
//
// TODO: switch Deliver to synckit's daemon.NewHarness once it lands; this
// pump drives the same public v2 calls (export, closure, have, batch,
// apply) as the daemon deliverer but has no scheduler, coalescing, or
// supersede.
type Mesh struct {
	Clock *Clock

	t     *testing.T
	mu    sync.Mutex
	hosts map[string]*Host
	order []string
	links map[[2]string]*Link
}

// NewMesh returns an empty mesh whose hosts share clock.
func NewMesh(t *testing.T, clock *Clock) *Mesh {
	return &Mesh{Clock: clock, t: t, hosts: map[string]*Host{}, links: map[[2]string]*Link{}}
}

// Add boots a host on the mesh clock with clones of origins and makes every
// host a peer of every other.
func (m *Mesh) Add(name string, origins ...*Origin) *Host {
	m.t.Helper()
	return m.AddAt(name, m.Clock, origins...)
}

// AddAt is Add with the host reading its own clock, as a machine whose clock
// lags the rest of the mesh does.
func (m *Mesh) AddAt(name string, clock *Clock, origins ...*Origin) *Host {
	m.t.Helper()
	h := NewHost(m.t, name, clock, origins...)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hosts[name] = h
	m.order = append(m.order, name)
	for _, n := range m.order {
		m.hosts[n].setPeers(slices.DeleteFunc(slices.Clone(m.order), func(p string) bool { return p == n }))
	}
	return h
}

// Host returns the host named name.
func (m *Mesh) Host(name string) *Host {
	m.mu.Lock()
	defer m.mu.Unlock()
	h, ok := m.hosts[name]
	if !ok {
		m.t.Fatalf("no host %q", name)
	}
	return h
}

// Link returns the directed link carrying from's calls to to.
func (m *Mesh) Link(from, to string) *Link {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := [2]string{from, to}
	if m.links[key] == nil {
		m.links[key] = &Link{calls: map[string]int{}}
	}
	return m.links[key]
}

// Delivery is one Deliver run.
type Delivery struct {
	Change  syncservice.ChangeEnvelope
	Result  syncservice.ApplyResult
	Objects int
	Bytes   int64
}

// Deliver exports from's catalog change and transfers its missing closure
// to to over Link(from, to), then applies it there.
func (m *Mesh) Deliver(ctx context.Context, from, to string) (Delivery, error) {
	src, dst := m.Host(from), m.Host(to)
	local := syncservice.NewClient(transport{host: src})
	peer := syncservice.NewClient(transport{host: dst, link: m.Link(from, to)})
	change, err := local.ExportV2(ctx, syncservice.ExportRequest{
		ServiceID: consumer.ServiceID, SchemaFingerprint: consumer.Fingerprint, SinceRevision: syncservice.NewRevision(0),
	})
	if err != nil {
		return Delivery{}, fmt.Errorf("export from %s: %w", from, err)
	}
	if change, err = syncservice.BindDelivery(change, from); err != nil {
		return Delivery{}, fmt.Errorf("bind delivery from %s: %w", from, err)
	}
	d := Delivery{Change: change}
	if len(change.Artifacts) > 0 {
		if err := transfer(ctx, local, peer, src, change.Artifacts, &d); err != nil {
			return d, err
		}
	}
	if d.Result, err = peer.ApplyV2(ctx, change); err != nil {
		return d, fmt.Errorf("apply on %s: %w", to, err)
	}
	return d, nil
}

func transfer(ctx context.Context, local, peer *syncservice.Client, src *Host, roots []artifact.Ref, d *Delivery) error {
	after := 0
	for {
		page, err := local.ArtifactClosure(ctx, artifact.ClosureParams{Roots: roots, After: after, Limit: artifact.MaxClosurePage})
		if err != nil {
			return fmt.Errorf("closure: %w", err)
		}
		digests := make([]artifact.Digest, len(page.Objects))
		for i, o := range page.Objects {
			digests[i] = o.Digest
		}
		absent := map[artifact.Digest]bool{}
		if len(digests) > 0 {
			missing, err := peer.ArtifactHave(ctx, digests)
			if err != nil {
				return fmt.Errorf("have: %w", err)
			}
			for _, dg := range missing {
				absent[dg] = true
			}
		}
		var batch []artifact.ObjectEntry
		var raw int64
		for _, o := range page.Objects {
			if !absent[o.Digest] {
				continue
			}
			if len(batch) == artifact.MaxBatchObjects || raw+o.Size > artifact.MaxBatchRaw {
				if err := send(ctx, local, peer, src, batch, d); err != nil {
					return err
				}
				batch, raw = nil, 0
			}
			batch = append(batch, o)
			raw += o.Size
		}
		if len(batch) > 0 {
			if err := send(ctx, local, peer, src, batch, d); err != nil {
				return err
			}
		}
		if page.Done {
			return nil
		}
		after = page.Next
	}
}

func send(ctx context.Context, local, peer *syncservice.Client, src *Host, objects []artifact.ObjectEntry, d *Delivery) error {
	batch, err := local.BatchBuild(ctx, objects)
	if err != nil {
		return fmt.Errorf("build batch: %w", err)
	}
	state, _ := src.Net.Current()
	begin, err := peer.BatchBegin(ctx, batch, state)
	if err != nil {
		return fmt.Errorf("begin batch: %w", err)
	}
	if begin.Paused != nil {
		return begin.Paused
	}
	for index := range batch.Parts {
		if slices.Contains(begin.HaveParts, index) {
			continue
		}
		data, err := local.BatchRead(ctx, batch.ID, index)
		if err != nil {
			return fmt.Errorf("read part %d: %w", index, err)
		}
		put, err := peer.BatchPut(ctx, batch.ID, index, data, state)
		if err != nil {
			return fmt.Errorf("put part %d: %w", index, err)
		}
		if put.Paused != nil {
			return put.Paused
		}
	}
	report, err := peer.BatchCommit(ctx, batch.ID)
	if err != nil {
		return fmt.Errorf("commit batch: %w", err)
	}
	d.Objects += report.Stored
	d.Bytes += report.Bytes
	return local.BatchDrop(ctx, batch.ID)
}

// Link is one directed host-to-host path. Cut fails every call; Intercept
// sees every request first and may rewrite it (corrupt a part) or fail it
// (drop or interrupt).
type Link struct {
	down      atomic.Bool
	mu        sync.Mutex
	intercept func(*rpc.Request) error
	calls     map[string]int
}

// Cut takes the link down.
func (l *Link) Cut() { l.down.Store(true) }

// Heal brings the link back up.
func (l *Link) Heal() { l.down.Store(false) }

// Intercept installs f; nil removes it.
func (l *Link) Intercept(f func(*rpc.Request) error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.intercept = f
}

// Calls counts the requests of method that crossed the link.
func (l *Link) Calls(method string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.calls[method]
}

func (l *Link) pass(req *rpc.Request) error {
	if l.down.Load() {
		return ErrLinkDown
	}
	l.mu.Lock()
	f := l.intercept
	l.calls[req.Method]++
	l.mu.Unlock()
	if f == nil {
		return nil
	}
	return f(req)
}

type transport struct {
	host *Host
	link *Link
}

func (t transport) Do(ctx context.Context, req *rpc.Request) (*syncservice.Response, error) {
	if t.link != nil {
		if err := t.link.pass(req); err != nil {
			return nil, err
		}
	}
	d, err := t.host.Dispatcher()
	if err != nil {
		return nil, err
	}
	resp := d.Dispatch(ctx, req)
	return &syncservice.Response{OK: resp.OK, Result: resp.Result, Error: resp.Error}, nil
}

func (transport) Close() error { return nil }
