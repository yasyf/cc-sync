package catalog

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/yasyf/daemonkit/durable"
)

// StampFile is the generation file inside the stamp directory, the only
// directory synckit watches for cc-sync.
const StampFile = "generation"

// StampWindow is the minimum spacing between two stamp bumps.
const StampWindow = 10 * time.Second

// Publisher bumps the stamp when the exported catalog digest changes, at
// most once per StampWindow. The stamp holds the generation and the digest
// it announced, so an unchanged catalog never changes the stamp.
type Publisher struct {
	catalog *Store
	dir     string
	kick    chan struct{}
}

// NewPublisher returns a publisher for catalog's stamp in dir.
func NewPublisher(catalog *Store, dir string) *Publisher {
	return &Publisher{catalog: catalog, dir: dir, kick: make(chan struct{}, 1)}
}

// Ensure writes the first stamp when none exists.
func (p *Publisher) Ensure() error {
	if err := os.MkdirAll(p.dir, 0o700); err != nil {
		return fmt.Errorf("catalog: stamp dir: %w", err)
	}
	if _, _, err := p.read(); !errors.Is(err, os.ErrNotExist) {
		return err
	}
	digest, err := p.catalog.Digest()
	if err != nil {
		return err
	}
	return p.write(1, digest)
}

// Publish asks Run to announce the current catalog; it never blocks.
func (p *Publisher) Publish(context.Context) error {
	select {
	case p.kick <- struct{}{}:
	default:
	}
	return nil
}

// Run serves Publish requests until ctx ends, coalescing every request that
// arrives within StampWindow of the last bump into one more check.
func (p *Publisher) Run(ctx context.Context) error {
	generation, announced, err := p.read()
	if err != nil {
		return err
	}
	var last time.Time
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-p.kick:
		}
		if wait := time.Until(last.Add(StampWindow)); wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
		digest, err := p.catalog.Digest()
		if err != nil {
			return err
		}
		if digest == announced {
			continue
		}
		if err := p.write(generation+1, digest); err != nil {
			return err
		}
		generation, announced, last = generation+1, digest, time.Now()
	}
}

func (p *Publisher) path() string { return filepath.Join(p.dir, StampFile) }

func (p *Publisher) read() (uint64, string, error) {
	data, err := os.ReadFile(p.path())
	if err != nil {
		return 0, "", err
	}
	var generation uint64
	var digest string
	if _, err := fmt.Sscanf(string(data), "%d %64s\n", &generation, &digest); err != nil {
		return 0, "", fmt.Errorf("%w: stamp %q: %w", ErrInvalid, data, err)
	}
	return generation, digest, nil
}

func (p *Publisher) write(generation uint64, digest string) error {
	return durable.WriteFile(p.path(), fmt.Appendf(nil, "%d %s\n", generation, digest), 0o600)
}
