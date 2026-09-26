//go:build e2e

// Package e2e boots simulated cc-sync hosts inside one test process. Each
// Host runs the real resident pipeline (artifact store, catalog, reposync
// worktree store, capture, scheduler, consumer) over directories isolated
// under t.TempDir(), and a Mesh carries artifact deliveries between hosts
// over links a scenario can cut or fault.
package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/reposync/registry"
	"github.com/yasyf/reposync/worktree"

	"github.com/yasyf/cc-sync/internal/capture"
	"github.com/yasyf/cc-sync/internal/catalog"
	"github.com/yasyf/cc-sync/internal/claudenative"
	"github.com/yasyf/cc-sync/internal/codesnap"
	"github.com/yasyf/cc-sync/internal/config"
	"github.com/yasyf/cc-sync/internal/inventory"
	"github.com/yasyf/cc-sync/internal/orcabridge"
	"github.com/yasyf/cc-sync/internal/resident"
	"github.com/yasyf/cc-sync/internal/scheduler"
	"github.com/yasyf/synckit/artifact"
	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/netpolicy"
	"github.com/yasyf/synckit/rpc"
	"github.com/yasyf/synckit/syncservice"
)

var (
	// Unmetered is a connected, unrestricted network.
	Unmetered = netpolicy.State{Status: netpolicy.StatusConnected}
	// Cellular is a connected cellular network that blocks bulk transfer.
	Cellular = netpolicy.State{Status: netpolicy.StatusConnected, Cellular: true, Expensive: true}
	// Disconnected has no network path.
	Disconnected = netpolicy.State{Status: netpolicy.StatusDisconnected}
)

// Clock is a settable time source shared by every host of a scenario.
type Clock struct {
	mu  sync.Mutex
	now time.Time
}

// NewClock starts a Clock at start.
func NewClock(start time.Time) *Clock {
	return &Clock{now: start}
}

// Now returns the clock's current time.
func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves the clock forward by d.
func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// Network is a host's fake netpolicy.Monitor.
type Network struct {
	mu      sync.Mutex
	state   netpolicy.State
	changed chan struct{}
}

// NewNetwork starts a Network in state.
func NewNetwork(state netpolicy.State) *Network {
	return &Network{state: state, changed: make(chan struct{})}
}

// Current returns the state observed now and a channel closed at the next
// Set.
func (n *Network) Current() (netpolicy.State, <-chan struct{}) {
	n.mu.Lock()
	defer n.mu.Unlock()
	state := n.state
	state.ObservedAt = time.Now()
	return state, n.changed
}

// Set switches the network to state and wakes every watcher.
func (n *Network) Set(state netpolicy.State) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.state = state
	close(n.changed)
	n.changed = make(chan struct{})
}

// Close does nothing; the Network outlives any one resident boot.
func (*Network) Close() error { return nil }

// Processes is a host's fake process table; empty means no Claude is live.
type Processes struct {
	mu    sync.Mutex
	procs []claudenative.Process
}

// Set replaces the process table.
func (p *Processes) Set(procs ...claudenative.Process) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.procs = procs
}

// Processes returns the current table.
func (p *Processes) Processes(context.Context) ([]claudenative.Process, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]claudenative.Process(nil), p.procs...), nil
}

// Host is one simulated machine. Every directory lives under Root:
// Layout is the cc-sync config dir (CC_SYNC_CONFIG_DIR), Claude the
// canonical Claude layout under Home/.claude with its own scratch TmpRoot,
// MeshDir the synckit mesh registry, Artifacts the artifact store root,
// Registry the reposync registry of Clones, and Layout.CheckoutRoot the
// recovery checkout root. Captures instruments the scheduler's attempts
// across every boot.
type Host struct {
	Name      string
	Root      string
	Home      string
	Layout    config.Layout
	Claude    claudenative.Layout
	MeshDir   string
	Artifacts string
	Registry  registry.Registry
	Clones    map[string]string
	Clock     *Clock
	Net       *Network
	Procs     *Processes
	Orca      *FakeOrca
	Captures  *CaptureLog

	t       *testing.T
	peers   []string
	offline atomic.Bool

	mu         sync.Mutex
	dispatcher *rpc.Dispatcher
	resident   *resident.Resident
	store      *artifact.Store
	execs      []ExecCall
}

// NewHost builds a host named name with a clone of every origin registered
// with reposync, then boots its resident.
func NewHost(t *testing.T, name string, clock *Clock, origins ...*Origin) *Host {
	t.Helper()
	root := physical(t, t.TempDir())
	home := filepath.Join(root, "home")
	h := &Host{
		Name:      name,
		Root:      root,
		Home:      home,
		Layout:    config.At(filepath.Join(root, "cc-sync"), filepath.Join(root, "checkouts")),
		Claude:    claudenative.Layout{ConfigDir: filepath.Join(home, ".claude"), TmpRoot: filepath.Join(root, "tmp"), UID: os.Getuid()},
		MeshDir:   filepath.Join(root, "synckit"),
		Artifacts: filepath.Join(root, "artifacts"),
		Registry:  registry.Registry{DefaultLocation: filepath.Join(root, "code")},
		Clones:    map[string]string{},
		Clock:     clock,
		Net:       NewNetwork(Unmetered),
		Procs:     &Processes{},
		Captures:  newCaptureLog(),
		t:         t,
	}
	for _, dir := range []string{h.Claude.ConfigDir, h.Claude.TmpRoot, h.MeshDir, h.Registry.DefaultLocation, h.Layout.CheckoutRoot} {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			t.Fatal(err)
		}
	}
	for _, o := range origins {
		clone := filepath.Join(h.Registry.DefaultLocation, o.Relpath)
		gitRun(t, "", "clone", "-q", o.URL, clone)
		h.Clones[o.Relpath] = clone
		h.Registry.Repos = append(h.Registry.Repos, registry.Repo{Relpath: o.Relpath, Path: clone, Origin: o.URL, Trunk: "main"})
	}
	h.Orca = newFakeOrca(t, filepath.Join(root, "orca"))
	h.setPeers(nil)
	h.Boot()
	t.Cleanup(h.shutdown)
	return h
}

// Boot starts the host's resident: resident.Prepare over the concrete
// artifact store, the reposync worktree store, the capture job, the
// inventory, and the consumer registered on a fresh dispatcher.
func (h *Host) Boot() {
	h.t.Helper()
	d := rpc.NewDispatcher()
	r, err := resident.Prepare(context.Background(), d, resident.Deps[*artifact.Store]{
		Layout:         h.Layout,
		Self:           h.Name,
		Now:            h.Clock.Now,
		OpenArtifacts:  func() (*artifact.Store, error) { return artifact.Open(h.Artifacts) },
		Monitor:        func() (netpolicy.Monitor, error) { return h.Net, nil },
		Register:       syncservice.RegisterArtifactConsumer,
		Pipeline:       h.pipeline,
		VerifyInterval: time.Hour,
	}, func(err error) { h.t.Errorf("%s: resident stopped: %v", h.Name, err) })
	if err != nil {
		h.t.Fatalf("%s: prepare resident: %v", h.Name, err)
	}
	h.mu.Lock()
	h.dispatcher, h.resident = d, r
	h.mu.Unlock()
	h.offline.Store(false)
}

func (h *Host) pipeline(c resident.Capture[*artifact.Store]) (resident.Pipeline, error) {
	orca, err := h.orcaClient()
	if err != nil {
		return resident.Pipeline{}, err
	}
	inv, err := inventory.New(inventory.Config{
		Layout:     h.Claude,
		Processes:  h.Procs,
		Worktrees:  h.worktrees,
		Orca:       orca,
		CursorPath: filepath.Join(c.Layout.Dir, "scan-cursor.json"),
		Now:        h.Clock.Now,
	})
	if err != nil {
		return resident.Pipeline{}, err
	}
	job := capture.New(capture.Config{
		Self:      h.Name,
		Layout:    h.Claude,
		Home:      h.Home,
		Store:     c.Artifacts,
		Code:      c.Code,
		Stamper:   capture.StampFunc(worktree.Stamp),
		Orca:      orca,
		Catalog:   c.Catalog,
		Publisher: c.Publisher,
		Targets:   inv,
		CodeIndex: c.Layout.CodeIndex,
		StateDir:  filepath.Join(c.Layout.Dir, "capture"),
		Tiers:     c.Config.Capture.Scheduler(),
		Now:       h.Clock.Now,
	})
	h.mu.Lock()
	h.store = c.Artifacts
	h.mu.Unlock()
	verifier := codeVerifier{code: c.Code, reg: h.Registry, open: func() (codesnap.Reader, func() error, error) {
		return c.Artifacts, func() error { return nil }, nil
	}}
	return resident.Pipeline{
		Inventory: inv,
		Stamper:   loggedStamper{log: h.Captures, next: job},
		Capturer:  loggedCapturer{log: h.Captures, next: job},
		Verifier:  verifier,
	}, nil
}

func (h *Host) worktrees(ctx context.Context) ([]worktree.Worktree, error) {
	wts, _, err := worktree.Discover(ctx, h.Registry)
	return wts, err
}

func (h *Host) orcaClient() (*orcabridge.Client, error) {
	return orcabridge.New(orcabridge.Options{Binary: h.Orca.Binary})
}

// Offline drains and closes the resident and makes the host unreachable,
// as when the machine is shut down.
func (h *Host) Offline() {
	h.t.Helper()
	h.offline.Store(true)
	h.shutdown()
}

// Online reboots the resident over the host's surviving directories.
func (h *Host) Online() {
	h.t.Helper()
	h.Boot()
}

// IsOffline reports whether the host is down.
func (h *Host) IsOffline() bool { return h.offline.Load() }

// SetNetwork switches the host's network state.
func (h *Host) SetNetwork(state netpolicy.State) { h.Net.Set(state) }

// Store is the resident's open artifact store.
func (h *Host) Store() *artifact.Store {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.store
}

// Checkout is the host's reposync-registered clone of relpath.
func (h *Host) Checkout(relpath string) string {
	h.t.Helper()
	dir, ok := h.Clones[relpath]
	if !ok {
		h.t.Fatalf("%s has no clone of %s", h.Name, relpath)
	}
	return dir
}

// Catalog loads the host's checkpoint catalog.
func (h *Host) Catalog() catalog.Snapshot {
	h.t.Helper()
	snap, err := catalog.New(h.Layout.CatalogPath, h.Name, h.Clock.Now).Load()
	if err != nil {
		h.t.Fatalf("%s: load catalog: %v", h.Name, err)
	}
	return snap
}

// Kick captures the worktrees holding sessionIDs now (all when empty)
// through ccsync.kick.v1.
func (h *Host) Kick(sessionIDs ...string) []scheduler.Attempt {
	h.t.Helper()
	var reply resident.KickReply
	if err := h.Call(context.Background(), resident.MethodKick, resident.KickRequest{SessionIDs: sessionIDs}, &reply); err != nil {
		h.t.Fatalf("%s: kick: %v", h.Name, err)
	}
	return reply.Attempts
}

// Status reads ccsync.status.v1.
func (h *Host) Status() resident.StatusReply {
	h.t.Helper()
	var reply resident.StatusReply
	if err := h.Call(context.Background(), resident.MethodStatus, struct{}{}, &reply); err != nil {
		h.t.Fatalf("%s: status: %v", h.Name, err)
	}
	return reply
}

// Call dispatches method with params to the host's resident, decoding the
// result into out when out is non-nil.
func (h *Host) Call(ctx context.Context, method string, params, out any) error {
	d, err := h.Dispatcher()
	if err != nil {
		return err
	}
	raw, err := json.Marshal(params)
	if err != nil {
		return err
	}
	var p map[string]any
	if err := json.Unmarshal(raw, &p); err != nil {
		return err
	}
	resp := d.Dispatch(ctx, &rpc.Request{Method: method, Params: p})
	if !resp.OK {
		return fmt.Errorf("%s %s: %s", h.Name, method, resp.Error)
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(resp.Result, out)
}

// Dispatcher is the running resident's rpc dispatcher; it fails while the
// host is offline.
func (h *Host) Dispatcher() (*rpc.Dispatcher, error) {
	if h.offline.Load() {
		return nil, fmt.Errorf("%s is offline", h.Name)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.dispatcher, nil
}

func (h *Host) setPeers(peers []string) {
	h.peers = peers
	raw, err := json.Marshal(hostregistry.Registry{Self: h.Name, Hosts: peers})
	if err != nil {
		h.t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(h.MeshDir, "registry.json"), raw, 0o600); err != nil {
		h.t.Fatal(err)
	}
}

func (h *Host) shutdown() {
	h.mu.Lock()
	r := h.resident
	h.resident, h.store = nil, nil
	h.mu.Unlock()
	if r == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := errors.Join(r.Drain(ctx), r.Close(ctx)); err != nil {
		h.t.Errorf("%s: shut down resident: %v", h.Name, err)
	}
}

func physical(t *testing.T, dir string) string {
	t.Helper()
	resolved, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	return resolved
}
