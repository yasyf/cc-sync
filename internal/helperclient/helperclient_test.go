package helperclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/yasyf/cc-sync/internal/resident"
	"github.com/yasyf/cc-sync/internal/scheduler"
	"github.com/yasyf/cc-sync/internal/service"
	"github.com/yasyf/synckit/artifact"
	"github.com/yasyf/synckit/codec"
	"github.com/yasyf/synckit/rpc"
)

type wireCaller struct {
	d      *rpc.Dispatcher
	closed bool
}

func (w *wireCaller) Call(ctx context.Context, req *rpc.Request) (*rpc.Response, error) {
	data, err := rpc.EncodeRequest(req)
	if err != nil {
		return nil, err
	}
	decoded, err := rpc.DecodeRequest(data)
	if err != nil {
		return nil, err
	}
	out, err := rpc.EncodeResponse(w.d.Dispatch(ctx, decoded))
	if err != nil {
		return nil, err
	}
	return rpc.DecodeResponse(out)
}

func (w *wireCaller) Close() error {
	w.closed = true
	return nil
}

type absentCaller struct{}

func (absentCaller) Call(context.Context, *rpc.Request) (*rpc.Response, error) {
	return nil, &rpc.TransportError{Undispatched: true, Err: errors.New("dial helper: no such file or directory")}
}

func (absentCaller) Close() error { return nil }

func strict[T any](t *testing.T, params map[string]any) T {
	t.Helper()
	data, err := json.Marshal(params)
	if err != nil {
		t.Fatal(err)
	}
	var v T
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&v); err != nil {
		t.Fatalf("decode %T: %v", v, err)
	}
	return v
}

func TestClientCalls(t *testing.T) {
	at := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	root := artifact.Ref{Digest: artifact.Sum([]byte("root")), Kind: artifact.KindManifest, Size: 42}
	var pins []resident.PinRequest
	var kicks []resident.KickRequest
	d := rpc.NewDispatcher()
	d.Register(resident.MethodStatus, func(context.Context, map[string]any) (any, error) {
		return resident.StatusReply{Build: "cc-sync 1.2.3", Scheduler: scheduler.Status{Workers: 2, LastRoundAt: at}}, nil
	})
	d.Register(resident.MethodKick, func(_ context.Context, params map[string]any) (any, error) {
		kicks = append(kicks, strict[resident.KickRequest](t, params))
		return resident.KickReply{Attempts: []scheduler.Attempt{{WorktreeID: "wt-1", At: at}}}, nil
	})
	d.Register(resident.MethodPin, func(_ context.Context, params map[string]any) (any, error) {
		req := strict[resident.PinRequest](t, params)
		pins = append(pins, req)
		if req.Owner == "outsider" {
			return nil, errors.New("resident: pin owner outside the pickup contract")
		}
		return nil, nil
	})
	caller := &wireCaller{d: d}
	c := New(caller)
	ctx := t.Context()

	status, err := c.Status(ctx)
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if want := (service.HelperStatus{Build: "cc-sync 1.2.3", Scheduler: scheduler.Status{Workers: 2, LastRoundAt: at}}); !reflect.DeepEqual(status, want) {
		t.Errorf("Status = %+v, want %+v", status, want)
	}

	attempts, err := c.Kick(ctx, []string{"s-1"})
	if err != nil {
		t.Fatalf("Kick: %v", err)
	}
	if want := []scheduler.Attempt{{WorktreeID: "wt-1", At: at}}; !reflect.DeepEqual(attempts, want) {
		t.Errorf("Kick = %+v, want %+v", attempts, want)
	}
	if want := []resident.KickRequest{{SessionIDs: []string{"s-1"}}}; !reflect.DeepEqual(kicks, want) {
		t.Errorf("kick params = %+v, want %+v", kicks, want)
	}

	if err := c.Pin(ctx, "cc-sync/pickup/op-1", []artifact.Ref{root}, time.Hour); err != nil {
		t.Fatalf("Pin: %v", err)
	}
	if err := c.Pin(ctx, "cc-sync/pickup/op-1", nil, 0); err != nil {
		t.Fatalf("unpin: %v", err)
	}
	wantPins := []resident.PinRequest{
		{Owner: "cc-sync/pickup/op-1", Roots: []artifact.Ref{root}, TTL: codec.Duration(time.Hour)},
		{Owner: "cc-sync/pickup/op-1"},
	}
	if !reflect.DeepEqual(pins, wantPins) {
		t.Errorf("pin params = %+v, want %+v", pins, wantPins)
	}

	err = c.Pin(ctx, "outsider", []artifact.Ref{root}, time.Hour)
	if err == nil || errors.Is(err, service.ErrUnavailable) {
		t.Fatalf("refused Pin = %v, want a non-unavailable error", err)
	}
	if want := "ccsync.pin.v1: resident: pin owner outside the pickup contract"; err.Error() != want {
		t.Errorf("refused Pin = %q, want %q", err, want)
	}

	if err := c.Close(); err != nil || !caller.closed {
		t.Errorf("Close = %v, closed = %v", err, caller.closed)
	}
}

func TestClientHelperAbsent(t *testing.T) {
	c := New(absentCaller{})
	tests := []struct {
		name string
		call func() error
	}{
		{"status", func() error { _, err := c.Status(t.Context()); return err }},
		{"kick", func() error { _, err := c.Kick(t.Context(), nil); return err }},
		{"pin", func() error { return c.Pin(t.Context(), "cc-sync/pickup/op", nil, 0) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.call(); !errors.Is(err, service.ErrUnavailable) {
				t.Errorf("err = %v, want ErrUnavailable", err)
			}
		})
	}
}
