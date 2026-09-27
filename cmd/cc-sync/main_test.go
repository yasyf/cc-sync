package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"testing"

	"github.com/yasyf/cc-sync/internal/cli"
	"github.com/yasyf/cc-sync/internal/service"
	"github.com/yasyf/synckit/delivery"
	"github.com/yasyf/synckit/rpc"
)

func TestSynckitDeliveriesClassifiesSynckitdFailures(t *testing.T) {
	unknown := fmt.Errorf("%s: %w", delivery.MethodStatus, rpc.ReplyError(`unknown method "delivery.status"`))
	notRunning := fmt.Errorf("%s: %w", delivery.MethodStatus, &rpc.TransportError{Undispatched: true, Err: errors.New("connection refused")})
	cancelled := fmt.Errorf("%s: %w", delivery.MethodStatus, &rpc.TransportError{Undispatched: true, Err: fmt.Errorf("wire: dial: %w", context.Canceled)})
	dispatched := fmt.Errorf("%s: %w", delivery.MethodStatus, &rpc.TransportError{Err: errors.New("connection reset")})
	boom := fmt.Errorf("%s: %w", delivery.MethodStatus, rpc.ReplyError("boom"))
	prefixed := fmt.Errorf("%s: %w", delivery.MethodStatus, rpc.ReplyError("unknown method 'x'"))
	statuses := []delivery.PeerStatus{{ServiceID: "cc-sync", Peer: "peer-a"}}
	tests := []struct {
		name        string
		statuses    []delivery.PeerStatus
		err         error
		want        []delivery.PeerStatus
		unavailable bool
		tooOld      bool
		msg         string
	}{
		{name: "answered", statuses: statuses, want: statuses},
		{
			name: "predates delivery.status", err: unknown, unavailable: true, tooOld: true,
			msg: `synckitd too old; upgrade synckit: delivery.status: unknown method "delivery.status"`,
		},
		{
			name: "not running", err: notRunning, unavailable: true,
			msg: "unavailable: synckitd is not running: delivery.status: rpc transport (undispatched): connection refused",
		},
		{name: "caller cancelled before dispatch", err: cancelled, msg: "delivery.status: rpc transport (undispatched): wire: dial: context canceled"},
		{name: "transport after dispatch", err: dispatched, msg: "delivery.status: rpc transport: connection reset"},
		{name: "handler error", err: boom, msg: "delivery.status: boom"},
		{name: "handler error sharing the frozen prefix", err: prefixed, msg: "delivery.status: unknown method 'x'"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := synckitDeliveries{status: func(_ context.Context, serviceID string) ([]delivery.PeerStatus, error) {
				if serviceID != "cc-sync" {
					t.Fatalf("status serviceID = %q, want cc-sync", serviceID)
				}
				return tt.statuses, tt.err
			}}
			got, err := d.Status(context.Background(), "cc-sync")
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("Status() = %v, want %v", got, tt.want)
			}
			if tt.err == nil {
				if err != nil {
					t.Fatalf("Status() error = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, tt.err) {
				t.Fatalf("Status() error = %v, want it to wrap %v", err, tt.err)
			}
			if got := errors.Is(err, service.ErrUnavailable); got != tt.unavailable {
				t.Fatalf("errors.Is(%v, ErrUnavailable) = %t, want %t", err, got, tt.unavailable)
			}
			if got := errors.Is(err, service.ErrSynckitdTooOld); got != tt.tooOld {
				t.Fatalf("errors.Is(%v, ErrSynckitdTooOld) = %t, want %t", err, got, tt.tooOld)
			}
			if err.Error() != tt.msg {
				t.Fatalf("Status() error = %q, want %q", err, tt.msg)
			}
		})
	}
}

func TestSynckitDeliveriesReportsCallerCancellation(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "ccs")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("DAEMONKIT_HOME", home)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = synckitDeliveries{status: delivery.Status}.Status(ctx, "cc-sync")
	if errors.Is(err, service.ErrUnavailable) {
		t.Fatalf("Status() with a cancelled context = %v, want no ErrUnavailable", err)
	}
	if code := cli.Classify(err); code != cli.CodeCancelled {
		t.Fatalf("Status() with a cancelled context = %v classified %q, want %q", err, code, cli.CodeCancelled)
	}
}

func TestSynckitdMissingIsUnavailable(t *testing.T) {
	notFound := fmt.Errorf("synckitd register m.json: %w", &exec.Error{Name: "synckitd", Err: exec.ErrNotFound})
	failed := errors.New("synckitd install: exit status 1: boom")
	tests := []struct {
		name        string
		err         error
		unavailable bool
		msg         string
	}{
		{
			name: "not on PATH", err: notFound, unavailable: true,
			msg: `unavailable: synckitd is not installed: synckitd register m.json: exec: "synckitd": executable file not found in $PATH`,
		},
		{name: "ran and failed", err: failed, msg: "synckitd install: exit status 1: boom"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := synckitdMissing(tt.err)
			if !errors.Is(err, tt.err) {
				t.Fatalf("synckitdMissing() = %v, want it to wrap %v", err, tt.err)
			}
			if got := errors.Is(err, service.ErrUnavailable); got != tt.unavailable {
				t.Fatalf("errors.Is(%v, ErrUnavailable) = %t, want %t", err, got, tt.unavailable)
			}
			if err.Error() != tt.msg {
				t.Fatalf("synckitdMissing() = %q, want %q", err, tt.msg)
			}
		})
	}
}
