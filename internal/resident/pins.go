package resident

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/yasyf/cc-sync/internal/pickup"
	"github.com/yasyf/daemonkit/durable"
	"github.com/yasyf/synckit/artifact"
)

const pinsIdentity = "cc-sync-pins-v1"

// ErrPin refuses a ccsync.pin.v1 request outside the pickup contract.
var ErrPin = errors.New("resident: invalid pin request")

// Pin is one live pickup pin set and when Reconcile sweeps it.
type Pin struct {
	Owner     string    `json:"owner"`
	Roots     int       `json:"roots"`
	ExpiresAt time.Time `json:"expires_at"`
}

type pinLedger struct {
	Identity string `json:"identity"`
	Pins     []Pin  `json:"pins"`
}

type pinStore interface {
	SetPins(ctx context.Context, owner string, roots []artifact.Ref) error
}

type pins struct {
	mu    sync.Mutex
	path  string
	store pinStore
	now   func() time.Time
}

func (p *pins) Set(ctx context.Context, owner string, roots []artifact.Ref, ttl time.Duration) (Pin, error) {
	if !strings.HasPrefix(owner, pickup.PinOwnerPrefix) || len(owner) == len(pickup.PinOwnerPrefix) {
		return Pin{}, fmt.Errorf("%w: owner %q is not under %s", ErrPin, owner, pickup.PinOwnerPrefix)
	}
	if ttl < 0 || ttl > pickup.PinTTL {
		return Pin{}, fmt.Errorf("%w: ttl %s outside (0, %s]", ErrPin, ttl, pickup.PinTTL)
	}
	if err := artifact.ValidateRoots(roots); err != nil {
		return Pin{}, fmt.Errorf("%w: %w", ErrPin, err)
	}
	if ttl == 0 {
		ttl = pickup.PinTTL
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	ledger, err := p.read()
	if err != nil {
		return Pin{}, err
	}
	ledger.Pins = slices.DeleteFunc(ledger.Pins, func(pin Pin) bool { return pin.Owner == owner })
	if len(roots) == 0 {
		if err := p.store.SetPins(ctx, owner, nil); err != nil {
			return Pin{}, fmt.Errorf("resident: unpin %s: %w", owner, err)
		}
		return Pin{Owner: owner}, p.write(ledger)
	}
	pin := Pin{Owner: owner, Roots: len(roots), ExpiresAt: p.now().Add(ttl).UTC()}
	ledger.Pins = append(ledger.Pins, pin)
	slices.SortFunc(ledger.Pins, func(a, b Pin) int { return strings.Compare(a.Owner, b.Owner) })
	if err := p.write(ledger); err != nil {
		return Pin{}, err
	}
	if err := p.store.SetPins(ctx, owner, roots); err != nil {
		return Pin{}, fmt.Errorf("resident: pin %s: %w", owner, err)
	}
	return pin, nil
}

func (p *pins) Sweep(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	ledger, err := p.read()
	if err != nil {
		return err
	}
	now := p.now()
	live := ledger.Pins[:0]
	for _, pin := range ledger.Pins {
		if now.Before(pin.ExpiresAt) {
			live = append(live, pin)
			continue
		}
		if err := p.store.SetPins(ctx, pin.Owner, nil); err != nil {
			return fmt.Errorf("resident: sweep pin %s: %w", pin.Owner, err)
		}
	}
	if len(live) == len(ledger.Pins) {
		return nil
	}
	ledger.Pins = live
	return p.write(ledger)
}

func (p *pins) List() ([]Pin, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	ledger, err := p.read()
	return ledger.Pins, err
}

func (p *pins) read() (pinLedger, error) {
	data, err := os.ReadFile(p.path)
	if errors.Is(err, os.ErrNotExist) {
		return pinLedger{Identity: pinsIdentity, Pins: []Pin{}}, nil
	}
	if err != nil {
		return pinLedger{}, fmt.Errorf("resident: read pins: %w", err)
	}
	var ledger pinLedger
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&ledger); err != nil {
		return pinLedger{}, fmt.Errorf("resident: decode pins %s: %w", p.path, err)
	}
	if ledger.Identity != pinsIdentity {
		return pinLedger{}, fmt.Errorf("resident: pins %s has identity %q, want %q", p.path, ledger.Identity, pinsIdentity)
	}
	return ledger, nil
}

func (p *pins) write(ledger pinLedger) error {
	data, err := json.Marshal(ledger)
	if err != nil {
		return fmt.Errorf("resident: encode pins: %w", err)
	}
	if err := durable.WriteFile(p.path, data, 0o600); err != nil {
		return fmt.Errorf("resident: write pins: %w", err)
	}
	return nil
}
