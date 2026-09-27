package service

import (
	"fmt"
	"slices"
	"time"

	"github.com/yasyf/cc-sync/internal/cli"
	"github.com/yasyf/synckit/delivery"
	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/netpolicy"
	"github.com/yasyf/synckit/syncservice"
)

type pauseCause struct {
	reason   cli.PauseReason
	endpoint cli.Endpoint
}

var pauseCauses = map[delivery.PauseReason]pauseCause{
	delivery.PauseLocalDisconnected:          {cli.PauseDisconnected, cli.EndpointLocal},
	delivery.PauseLocalUnknown:               {cli.PauseUnknownNetwork, cli.EndpointLocal},
	delivery.PauseLocalCellular:              {cli.PauseCellular, cli.EndpointLocal},
	delivery.PauseLocalExpensive:             {cli.PauseExpensive, cli.EndpointLocal},
	delivery.PauseLocalConstrained:           {cli.PauseConstrained, cli.EndpointLocal},
	delivery.PauseLocalManualMetered:         {cli.PauseManualMetered, cli.EndpointLocal},
	delivery.PauseLocalRestrictedMidTransfer: {cli.PauseRestrictedMidTransfer, cli.EndpointLocal},
	delivery.PausePeerDisconnected:           {cli.PauseDisconnected, cli.EndpointPeer},
	delivery.PausePeerUnknown:                {cli.PauseUnknownNetwork, cli.EndpointPeer},
	delivery.PausePeerCellular:               {cli.PauseCellular, cli.EndpointPeer},
	delivery.PausePeerExpensive:              {cli.PauseExpensive, cli.EndpointPeer},
	delivery.PausePeerConstrained:            {cli.PauseConstrained, cli.EndpointPeer},
	delivery.PausePeerManualMetered:          {cli.PauseManualMetered, cli.EndpointPeer},
	delivery.PausePeerRestrictedMidTransfer:  {cli.PauseRestrictedMidTransfer, cli.EndpointPeer},
	delivery.PausePeerUnreachable:            {cli.PausePeerOffline, cli.EndpointPeer},
	delivery.PausePeerIncompatible:           {cli.PauseIncompatible, cli.EndpointPeer},
}

func pauseFor(reason delivery.PauseReason, since time.Time) (*cli.Pause, error) {
	cause, ok := pauseCauses[reason]
	if !ok {
		return nil, fmt.Errorf("unknown delivery pause reason %q", reason)
	}
	return &cli.Pause{Reason: cause.reason, Endpoint: cause.endpoint, Since: cli.At(since)}, nil
}

func peerPause(ps delivery.PeerStatus) (*cli.Pause, error) {
	if ps.State != delivery.StatePaused {
		return nil, nil
	}
	return pauseFor(ps.PauseReason, ps.PauseSince)
}

func reachable(ps delivery.PeerStatus) bool {
	return ps.PeerNetwork != nil && (ps.State != delivery.StatePaused || ps.PauseReason != delivery.PausePeerUnreachable)
}

func lastSeen(ps delivery.PeerStatus) *cli.Time {
	seen := ps.AckedAt
	if ps.PeerNetwork != nil && ps.PeerNetwork.ObservedAt.After(seen) {
		seen = ps.PeerNetwork.ObservedAt
	}
	return optionalTime(seen)
}

func host(id string) cli.Host {
	return cli.Host{HostID: id, HostName: hostregistry.HostNode(id)}
}

func network(s netpolicy.State) cli.Network {
	return cli.Network{
		Status:        cli.NetworkStatus(s.Status),
		Expensive:     s.Expensive,
		Constrained:   s.Constrained,
		Cellular:      s.Cellular,
		ManualMetered: s.ManualMetered,
	}
}

func peers(reg *hostregistry.Registry, statuses []delivery.PeerStatus) (cli.Array[cli.Peer], error) {
	byPeer := make(map[string]delivery.PeerStatus, len(statuses))
	ids := slices.Clone(reg.Hosts)
	for _, ps := range statuses {
		byPeer[ps.Peer] = ps
		ids = append(ids, ps.Peer)
	}
	slices.Sort(ids)
	out := cli.Array[cli.Peer]{}
	for _, id := range slices.Compact(ids) {
		if id == reg.Self {
			continue
		}
		p := cli.Peer{Host: host(id)}
		ps, ok := byPeer[id]
		if !ok {
			out = append(out, p)
			continue
		}
		p.Reachable, p.LastSeenAt = reachable(ps), lastSeen(ps)
		var err error
		if p.AckedRevision, err = revision(ps.Acked); err != nil {
			return nil, fmt.Errorf("peer %s acked: %w", id, err)
		}
		if ps.Pending != nil {
			if p.PendingRevision, err = revision(ps.Pending.SourceRevision); err != nil {
				return nil, fmt.Errorf("peer %s pending: %w", id, err)
			}
			p.PendingSince = cli.AtPtr(ps.Pending.StagedAt)
		}
		if p.Pause, err = peerPause(ps); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, nil
}

func revision(r syncservice.Revision) (*uint64, error) {
	if r == "" {
		return nil, nil
	}
	n, err := r.Uint64()
	if err != nil || n == 0 {
		return nil, err
	}
	return &n, nil
}
