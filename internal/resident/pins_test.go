package resident

import (
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-sync/internal/consumer"
	"github.com/yasyf/cc-sync/internal/pickup"
	"github.com/yasyf/synckit/artifact"
	"github.com/yasyf/synckit/codec"
)

func ref(b string) artifact.Ref {
	return artifact.Ref{Digest: artifact.Sum([]byte(b)), Kind: artifact.KindManifest, Size: int64(len(b))}
}

func (h *harness) pin(t *testing.T, req PinRequest) (Pin, string) {
	t.Helper()
	raw, errText := h.call(t, MethodPin, req)
	if errText != "" {
		return Pin{}, errText
	}
	var got Pin
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	return got, ""
}

func (h *harness) reconcile(t *testing.T) {
	t.Helper()
	if _, err := h.registered.Reconcile(t.Context(), consumer.WatchItemID); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
}

func TestPinExpiresAfterTTL(t *testing.T) {
	h := newHarness(t)
	const owner = pickup.PinOwnerPrefix + "op1"
	roots := []artifact.Ref{ref("a"), ref("b")}
	t0 := h.clock.Now()

	got, errText := h.pin(t, PinRequest{Owner: owner, Roots: roots})
	if errText != "" {
		t.Fatalf("pin: %s", errText)
	}
	if want := (Pin{Owner: owner, Roots: 2, ExpiresAt: t0.Add(pickup.PinTTL)}); got != want {
		t.Fatalf("pin = %+v, want %+v", got, want)
	}
	if !reflect.DeepEqual(h.store.pinned(owner), roots) {
		t.Fatalf("store pins[%s] = %v, want %v", owner, h.store.pinned(owner), roots)
	}

	h.clock.advance(pickup.PinTTL - time.Second)
	h.reconcile(t)
	if !reflect.DeepEqual(h.store.pinned(owner), roots) {
		t.Fatalf("pins swept before expiry: %v", h.store.pinned(owner))
	}

	h.clock.advance(time.Second)
	h.reconcile(t)
	if pinned := h.store.pinned(owner); pinned != nil {
		t.Fatalf("store pins[%s] = %v after expiry, want none", owner, pinned)
	}
	ops := h.store.ops()
	unpin := slices.Index(ops, "unpin "+owner)
	if unpin < 0 || !slices.Contains(ops[unpin:], "gc") {
		t.Errorf("store ops = %v, want the expired unpin before the reconcile GC", ops)
	}
	listed, err := (&pins{path: h.layout.PinsPath, store: h.store, now: h.clock.Now}).List()
	if err != nil || len(listed) != 0 {
		t.Errorf("ledger after sweep = %v, %v; want empty", listed, err)
	}
}

func TestPinRenewalAndRelease(t *testing.T) {
	h := newHarness(t)
	const owner = pickup.PinOwnerPrefix + "op1"
	t0 := h.clock.Now()
	if _, errText := h.pin(t, PinRequest{Owner: owner, Roots: []artifact.Ref{ref("a")}, TTL: codec.Duration(time.Hour)}); errText != "" {
		t.Fatal(errText)
	}
	h.clock.advance(30 * time.Minute)
	got, errText := h.pin(t, PinRequest{Owner: owner, Roots: []artifact.Ref{ref("a")}, TTL: codec.Duration(time.Hour)})
	if errText != "" {
		t.Fatal(errText)
	}
	if want := t0.Add(90 * time.Minute); !got.ExpiresAt.Equal(want) {
		t.Errorf("renewed expiry = %v, want %v", got.ExpiresAt, want)
	}

	reopened := &pins{path: h.layout.PinsPath, store: h.store, now: h.clock.Now}
	listed, err := reopened.List()
	if err != nil || len(listed) != 1 || listed[0] != got {
		t.Fatalf("reopened ledger = %v, %v; want [%+v]", listed, err, got)
	}

	released, errText := h.pin(t, PinRequest{Owner: owner})
	if errText != "" {
		t.Fatal(errText)
	}
	if released != (Pin{Owner: owner}) || h.store.pinned(owner) != nil {
		t.Errorf("release = %+v, store pins = %v; want a bare owner and no pins", released, h.store.pinned(owner))
	}
	if listed, _ := reopened.List(); len(listed) != 0 {
		t.Errorf("ledger after release = %v, want empty", listed)
	}
}

func TestPinRefusals(t *testing.T) {
	tests := []struct {
		name   string
		params map[string]any
		want   string
	}{
		{name: "catalog owner", params: map[string]any{"owner": consumer.PinOwner, "roots": []artifact.Ref{ref("a")}}, want: ErrPin.Error()},
		{name: "bare prefix", params: map[string]any{"owner": pickup.PinOwnerPrefix, "roots": []artifact.Ref{ref("a")}}, want: ErrPin.Error()},
		{name: "ttl past a day", params: map[string]any{"owner": pickup.PinOwnerPrefix + "op", "roots": []artifact.Ref{ref("a")}, "ttl": "25h"}, want: ErrPin.Error()},
		{name: "negative ttl", params: map[string]any{"owner": pickup.PinOwnerPrefix + "op", "roots": []artifact.Ref{ref("a")}, "ttl": "-1s"}, want: ErrPin.Error()},
		{name: "invalid root", params: map[string]any{"owner": pickup.PinOwnerPrefix + "op", "roots": []artifact.Ref{{Digest: "zz", Kind: artifact.KindBlob}}}, want: ErrPin.Error()},
		{name: "unknown field", params: map[string]any{"owner": pickup.PinOwnerPrefix + "op", "expires": "1h"}, want: "unknown field"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t)
			if _, errText := h.call(t, MethodPin, tt.params); !strings.Contains(errText, tt.want) {
				t.Errorf("pin error = %q, want %q", errText, tt.want)
			}
			if ops := h.store.ops(); len(ops) != 0 {
				t.Errorf("refused pin touched the store: %v", ops)
			}
		})
	}
}
