// Package helperclient calls the resident cc-sync helper's ccsync.* methods
// over its daemonkit business lane. Every failure to reach the helper wraps
// service.ErrUnavailable.
package helperclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/yasyf/cc-sync/internal/consumer"
	"github.com/yasyf/cc-sync/internal/resident"
	"github.com/yasyf/cc-sync/internal/scheduler"
	"github.com/yasyf/cc-sync/internal/service"
	"github.com/yasyf/daemonkit"
	"github.com/yasyf/synckit/artifact"
	"github.com/yasyf/synckit/codec"
	"github.com/yasyf/synckit/helperruntime"
	"github.com/yasyf/synckit/rpc"
)

// Caller sends requests to the helper; *rpc.Client is the production one.
type Caller interface {
	Call(ctx context.Context, req *rpc.Request) (*rpc.Response, error)
	Close() error
}

// Client is the helper's ccsync.status.v1, ccsync.kick.v1, and ccsync.pin.v1
// surface.
type Client struct {
	caller Caller
}

// Dial returns a Client over the resident helper's socket; it performs no I/O,
// so an absent helper surfaces on the first call.
func Dial() (*Client, error) {
	spec, err := helperruntime.Spec(consumer.ServiceID, daemonkit.Program{}, 0)
	if err != nil {
		return nil, fmt.Errorf("helperclient: helper spec: %w", err)
	}
	d, err := daemonkit.Open(spec)
	if err != nil {
		return nil, fmt.Errorf("helperclient: open helper: %w", err)
	}
	return New(rpc.NewClient(rpc.ClientConfig{
		Open: func(context.Context) (*daemonkit.Business, error) { return d.Business(), nil },
	})), nil
}

// New returns a Client that sends every call through caller.
func New(caller Caller) *Client {
	return &Client{caller: caller}
}

// Close retires the client's lane to the helper.
func (c *Client) Close() error {
	return c.caller.Close()
}

// Status reports the helper's build and capture scheduler.
func (c *Client) Status(ctx context.Context) (service.HelperStatus, error) {
	var reply struct {
		Build     string           `json:"build"`
		Scheduler scheduler.Status `json:"scheduler"`
	}
	if err := c.call(ctx, resident.MethodStatus, struct{}{}, &reply); err != nil {
		return service.HelperStatus{}, err
	}
	return service.HelperStatus{Build: reply.Build, Scheduler: reply.Scheduler}, nil
}

// Kick runs a capture now for the units holding sessionIDs, or every unit.
func (c *Client) Kick(ctx context.Context, sessionIDs []string) ([]scheduler.Attempt, error) {
	var reply resident.KickReply
	if err := c.call(ctx, resident.MethodKick, resident.KickRequest{SessionIDs: sessionIDs}, &reply); err != nil {
		return nil, err
	}
	return reply.Attempts, nil
}

// Pin pins roots for owner in the helper's artifact store for ttl; empty
// roots release every pin owner holds.
func (c *Client) Pin(ctx context.Context, owner string, roots []artifact.Ref, ttl time.Duration) error {
	return c.call(ctx, resident.MethodPin, resident.PinRequest{Owner: owner, Roots: roots, TTL: codec.Duration(ttl)}, nil)
}

func (c *Client) call(ctx context.Context, method string, params, reply any) error {
	data, err := json.Marshal(params)
	if err != nil {
		return fmt.Errorf("%s: encode params: %w", method, err)
	}
	var p map[string]any
	if err := json.Unmarshal(data, &p); err != nil {
		return fmt.Errorf("%s: encode params: %w", method, err)
	}
	resp, err := c.caller.Call(ctx, &rpc.Request{Method: method, Params: p})
	if te := (*rpc.TransportError)(nil); errors.As(err, &te) {
		return fmt.Errorf("%w: %s: %w", service.ErrUnavailable, method, err)
	}
	if err != nil {
		return fmt.Errorf("%s: %w", method, err)
	}
	if !resp.OK {
		return fmt.Errorf("%s: %s", method, resp.Error)
	}
	if reply == nil {
		return nil
	}
	if err := json.Unmarshal(resp.Result, reply); err != nil {
		return fmt.Errorf("%s: decode result: %w", method, err)
	}
	return nil
}
