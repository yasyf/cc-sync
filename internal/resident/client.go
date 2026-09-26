package resident

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/yasyf/cc-sync/internal/consumer"
	"github.com/yasyf/cc-sync/internal/scheduler"
	"github.com/yasyf/synckit/artifact"
	"github.com/yasyf/synckit/codec"
	"github.com/yasyf/synckit/rpc"
	"github.com/yasyf/synckit/syncservice"
)

// ErrNotRunning reports that no resident helper accepted the call.
var ErrNotRunning = errors.New("resident: helper is not running")

// Client calls the ccsync.* methods on this user's resident helper over its
// helperruntime socket.
type Client struct {
	rpc *syncservice.Client
}

// Dial returns a Client for the resident helper; the socket is opened on
// the first call.
func Dial() *Client {
	return &Client{rpc: syncservice.NewClient(syncservice.Resident(consumer.ServiceID))}
}

// Close releases the helper connection.
func (c *Client) Close() error {
	return c.rpc.Close()
}

// Status calls ccsync.status.v1.
func (c *Client) Status(ctx context.Context) (StatusReply, error) {
	var reply StatusReply
	err := c.call(ctx, MethodStatus, struct{}{}, &reply)
	return reply, err
}

// Kick calls ccsync.kick.v1; no session ids kicks every unit.
func (c *Client) Kick(ctx context.Context, sessionIDs []string) ([]scheduler.Attempt, error) {
	var reply KickReply
	err := c.call(ctx, MethodKick, KickRequest{SessionIDs: sessionIDs}, &reply)
	return reply.Attempts, err
}

// Pin calls ccsync.pin.v1, holding roots under owner for ttl; empty roots
// release the owner's pins.
func (c *Client) Pin(ctx context.Context, owner string, roots []artifact.Ref, ttl time.Duration) error {
	return c.call(ctx, MethodPin, PinRequest{Owner: owner, Roots: roots, TTL: codec.Duration(ttl)}, nil)
}

func (c *Client) call(ctx context.Context, method string, req, out any) error {
	data, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("resident: encode %s params: %w", method, err)
	}
	var params map[string]any
	if err := json.Unmarshal(data, &params); err != nil {
		return fmt.Errorf("resident: encode %s params: %w", method, err)
	}
	err = c.rpc.Call(ctx, method, params, out)
	var transport *rpc.TransportError
	if errors.As(err, &transport) && transport.Undispatched {
		return fmt.Errorf("%w: %w", ErrNotRunning, err)
	}
	return err
}
