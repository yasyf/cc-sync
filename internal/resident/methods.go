package resident

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/yasyf/cc-sync/internal/catalog"
	"github.com/yasyf/cc-sync/internal/config"
	"github.com/yasyf/cc-sync/internal/scheduler"
	"github.com/yasyf/cc-sync/internal/version"
	"github.com/yasyf/synckit/artifact"
	"github.com/yasyf/synckit/codec"
	"github.com/yasyf/synckit/netpolicy"
	"github.com/yasyf/synckit/rpc"
)

// Helper method names served on the cc-sync helper socket.
const (
	MethodStatus = "ccsync.status.v1"
	MethodKick   = "ccsync.kick.v1"
	MethodPin    = "ccsync.pin.v1"
)

// StatusReply is the ccsync.status.v1 result.
type StatusReply struct {
	Build     string           `json:"build"`
	Scheduler scheduler.Status `json:"scheduler"`
	Catalog   CatalogStatus    `json:"catalog"`
	Capture   config.Tiers     `json:"capture"`
	Network   netpolicy.State  `json:"network"`
	Pins      []Pin            `json:"pins"`
}

// CatalogStatus summarizes the catalog this host holds.
type CatalogStatus struct {
	Self    string         `json:"self"`
	Origins []OriginStatus `json:"origins"`
}

// OriginStatus counts one origin block's worktrees and checkpoints, and how
// many of those checkpoints are ready on this host.
type OriginStatus struct {
	Origin      string `json:"origin"`
	Revision    uint64 `json:"revision"`
	Worktrees   int    `json:"worktrees"`
	Checkpoints int    `json:"checkpoints"`
	Ready       int    `json:"ready"`
}

// KickRequest is the ccsync.kick.v1 params; no session ids kicks every unit.
type KickRequest struct {
	SessionIDs []string `json:"session_ids"`
}

// KickReply is the ccsync.kick.v1 result: one attempt per kicked unit.
type KickReply struct {
	Attempts []scheduler.Attempt `json:"attempts"`
}

// PinRequest is the ccsync.pin.v1 params. Empty roots release the owner's
// pins; a zero TTL means pickup.PinTTL.
type PinRequest struct {
	Owner string         `json:"owner"`
	Roots []artifact.Ref `json:"roots"`
	TTL   codec.Duration `json:"ttl"`
}

type methods struct {
	scheduler *scheduler.Scheduler
	catalog   *catalog.Store
	config    config.Config
	monitor   netpolicy.Monitor
	pins      *pins
}

func register(d *rpc.Dispatcher, m methods) {
	d.Register(MethodStatus, func(context.Context, map[string]any) (any, error) {
		return m.status()
	})
	d.Register(MethodKick, func(ctx context.Context, params map[string]any) (any, error) {
		var req KickRequest
		if err := decode(params, &req); err != nil {
			return nil, err
		}
		attempts, err := m.scheduler.Kick(ctx, req.SessionIDs...)
		if err != nil {
			return nil, err
		}
		return KickReply{Attempts: attempts}, nil
	})
	d.Register(MethodPin, func(ctx context.Context, params map[string]any) (any, error) {
		var req PinRequest
		if err := decode(params, &req); err != nil {
			return nil, err
		}
		return m.pins.Set(ctx, req.Owner, req.Roots, time.Duration(req.TTL))
	})
}

func (m methods) status() (StatusReply, error) {
	snapshot, err := m.catalog.Load()
	if err != nil {
		return StatusReply{}, err
	}
	pins, err := m.pins.List()
	if err != nil {
		return StatusReply{}, err
	}
	network, _ := m.monitor.Current()
	origins := make([]OriginStatus, 0, len(snapshot.Origins))
	for _, o := range snapshot.Origins {
		status := OriginStatus{Origin: o.Origin, Revision: o.Revision, Worktrees: len(o.Worktrees)}
		for _, wt := range o.Worktrees {
			for _, cp := range wt.Checkpoints {
				status.Checkpoints++
				if snapshot.ReadinessOf(o.Origin, cp).Ready {
					status.Ready++
				}
			}
		}
		origins = append(origins, status)
	}
	return StatusReply{
		Build:     version.String(),
		Scheduler: m.scheduler.Status(),
		Catalog:   CatalogStatus{Self: snapshot.Self, Origins: origins},
		Capture:   m.config.Capture,
		Network:   network,
		Pins:      pins,
	}, nil
}

func decode(params map[string]any, v any) error {
	data, err := json.Marshal(params)
	if err != nil {
		return fmt.Errorf("resident: encode params: %w", err)
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("resident: decode params: %w", err)
	}
	return nil
}
