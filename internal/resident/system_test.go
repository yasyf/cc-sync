package resident

import (
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-sync/internal/config"
	"github.com/yasyf/cc-sync/internal/consumer"
	"github.com/yasyf/cc-sync/internal/pickup"
	"github.com/yasyf/synckit/artifact"
	"github.com/yasyf/synckit/codec"
	"github.com/yasyf/synckit/netpolicy"
	"github.com/yasyf/synckit/rpc"
	"github.com/yasyf/synckit/syncservice"
)

func TestPrepareWithArtifactStore(t *testing.T) {
	dir := t.TempDir()
	store, err := artifact.Open(filepath.Join(dir, "artifacts"))
	if err != nil {
		t.Fatal(err)
	}
	monitor := newFakeMonitor(unrestricted)
	expirer := &fakeExpirer{}
	d := rpc.NewDispatcher()
	var captured Capture[*artifact.Store]
	deps := Deps[*artifact.Store]{
		Layout:        config.At(filepath.Join(dir, "config"), filepath.Join(dir, "checkouts")),
		Self:          "me@host",
		Now:           time.Now,
		OpenArtifacts: func() (*artifact.Store, error) { return store, nil },
		Monitor:       func() (netpolicy.Monitor, error) { return monitor, nil },
		Register:      syncservice.RegisterArtifactConsumer,
		Pipeline: func(c Capture[*artifact.Store]) (Pipeline, error) {
			captured = c
			return Pipeline{Inventory: &fakeInventory{}, Stamper: &fakeStamper{}, Capturer: fakeCapturer{}, Verifier: fakeVerifier{}, Expirer: expirer}, nil
		},
		ExpireInterval: 10 * time.Millisecond,
	}
	stops := make(chan error, 1)
	r, err := Prepare(t.Context(), d, deps, func(err error) { stops <- err })
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if captured.Artifacts != store || captured.Self != "me@host" || captured.Monitor != monitor {
		t.Errorf("pipeline capture store/self/monitor = %p/%q/%p, want %p/%q/%p", captured.Artifacts, captured.Self, captured.Monitor, store, "me@host", monitor)
	}

	resp := d.Dispatch(t.Context(), &rpc.Request{Method: syncservice.MethodCapabilities})
	if !resp.OK || !strings.Contains(string(resp.Result), consumer.ServiceID) {
		t.Errorf("capabilities = %+v, want the registered cc-sync consumer", resp)
	}

	blob, err := store.Put(t.Context(), strings.NewReader("checkpoint bytes"), "test.blob")
	if err != nil {
		t.Fatal(err)
	}
	ref, err := store.PutGroup(t.Context(), "test.group", []artifact.Ref{blob})
	if err != nil {
		t.Fatal(err)
	}
	owner := pickup.PinOwnerPrefix + "op1"
	data, err := json.Marshal(PinRequest{Owner: owner, Roots: []artifact.Ref{ref}, TTL: codec.Duration(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	var params map[string]any
	if err := json.Unmarshal(data, &params); err != nil {
		t.Fatal(err)
	}
	if resp := d.Dispatch(t.Context(), &rpc.Request{Method: MethodPin, Params: params}); !resp.OK {
		t.Fatalf("pin: %s", resp.Error)
	}
	sets, err := store.Pins(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(sets) != 1 || sets[0].Owner != owner || !reflect.DeepEqual(sets[0].Roots, []artifact.Ref{ref}) {
		t.Errorf("store pins = %+v, want %s pinning %v", sets, owner, ref)
	}

	deadline := time.Now().Add(10 * time.Second)
	for expirer.n.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if n := expirer.n.Load(); n < 2 {
		t.Errorf("ExpirePins calls = %d, want the periodic loop to repeat", n)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := r.Drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if err := r.Close(ctx); err != nil || !monitor.closed {
		t.Fatalf("Close = %v, monitor closed = %v; want nil, true", err, monitor.closed)
	}
	reopened, err := artifact.Open(filepath.Join(dir, "artifacts"))
	if err != nil {
		t.Fatalf("reopen after Close = %v, want the store lock released", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-stops:
		t.Errorf("stop(%v) called on a clean drain", err)
	default:
	}
}
