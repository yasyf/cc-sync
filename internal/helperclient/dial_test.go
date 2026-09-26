package helperclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/daemonkit"

	"github.com/yasyf/cc-sync/internal/consumer"
	"github.com/yasyf/cc-sync/internal/resident"
	"github.com/yasyf/cc-sync/internal/scheduler"
	"github.com/yasyf/cc-sync/internal/service"
	"github.com/yasyf/synckit/artifact"
	"github.com/yasyf/synckit/helperruntime"
	"github.com/yasyf/synckit/rpc"
)

type nopProduct struct{}

func (nopProduct) Drain(context.Context) error { return nil }
func (nopProduct) Close(context.Context) error { return nil }

func decode(params map[string]any, v any) error {
	data, err := json.Marshal(params)
	if err != nil {
		return err
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

func TestDialRoundTrip(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "ccs")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("DAEMONKIT_HOME", home)

	client, err := Dial()
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = client.Close() }()
	if _, err := client.Status(t.Context()); !errors.Is(err, service.ErrUnavailable) {
		t.Fatalf("Status with no helper = %v, want ErrUnavailable", err)
	}

	root := artifact.Ref{Digest: artifact.Sum([]byte("root")), Kind: artifact.KindManifest, Size: 42}
	var mu sync.Mutex
	var pinned []resident.PinRequest
	d := rpc.NewDispatcher()
	d.Register(resident.MethodStatus, func(context.Context, map[string]any) (any, error) {
		return resident.StatusReply{Build: "cc-sync 1.2.3", Scheduler: scheduler.Status{Workers: 2}}, nil
	})
	d.Register(resident.MethodKick, func(_ context.Context, params map[string]any) (any, error) {
		var req resident.KickRequest
		if err := decode(params, &req); err != nil {
			return nil, err
		}
		return resident.KickReply{Attempts: []scheduler.Attempt{{WorktreeID: "wt-" + req.SessionIDs[0], Result: scheduler.Result{Outcome: scheduler.OutcomeCaptured}}}}, nil
	})
	d.Register(resident.MethodPin, func(_ context.Context, params map[string]any) (any, error) {
		var req resident.PinRequest
		if err := decode(params, &req); err != nil {
			return nil, err
		}
		if req.Owner == "outsider" {
			return nil, errors.New("resident: pin owner outside the pickup contract")
		}
		mu.Lock()
		defer mu.Unlock()
		pinned = append(pinned, req)
		return nil, nil
	})
	runtime, err := helperruntime.New(helperruntime.Config{
		App:        helperruntime.App{Name: consumer.ServiceID},
		Dispatcher: d,
		Prepare:    func(daemonkit.Ctx) (helperruntime.Product, error) { return nopProduct{}, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- runtime.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-served:
			if err != nil {
				t.Errorf("helper run: %v", err)
			}
		case <-time.After(45 * time.Second):
			t.Error("helper did not stop")
		}
	})

	var status service.HelperStatus
	deadline := time.Now().Add(30 * time.Second)
	for {
		status, err = client.Status(t.Context())
		if err == nil || !errors.Is(err, service.ErrUnavailable) || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if want := (service.HelperStatus{Build: "cc-sync 1.2.3", Scheduler: scheduler.Status{Workers: 2}}); !reflect.DeepEqual(status, want) {
		t.Errorf("Status = %+v, want %+v", status, want)
	}

	attempts, err := client.Kick(t.Context(), []string{"s1"})
	if err != nil {
		t.Fatalf("Kick: %v", err)
	}
	if want := []scheduler.Attempt{{WorktreeID: "wt-s1", Result: scheduler.Result{Outcome: scheduler.OutcomeCaptured}}}; !reflect.DeepEqual(attempts, want) {
		t.Errorf("Kick = %+v, want %+v", attempts, want)
	}

	if err := client.Pin(t.Context(), "cc-sync/pickup/op1", []artifact.Ref{root}, time.Hour); err != nil {
		t.Fatalf("Pin: %v", err)
	}
	mu.Lock()
	got := slices.Clone(pinned)
	mu.Unlock()
	if len(got) != 1 || got[0].Owner != "cc-sync/pickup/op1" || !reflect.DeepEqual(got[0].Roots, []artifact.Ref{root}) {
		t.Errorf("pinned = %+v, want cc-sync/pickup/op1 pinning %v", got, root)
	}
	if err := client.Pin(t.Context(), "outsider", []artifact.Ref{root}, time.Hour); err == nil || errors.Is(err, service.ErrUnavailable) {
		t.Errorf("Pin under a foreign owner = %v, want the helper's refusal", err)
	}
}
