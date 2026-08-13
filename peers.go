package main

// peers.go — peer identity, reachable addresses, and explicit connection control. This is both diagnostic
// infrastructure (controlled benchmarks dial a known peer directly) and a real feature: a node can be pointed at a
// known, reachable seed by multiaddr so downloads go peer-to-peer directly instead of via the DHT/relay lottery.
// Transport-agnostic — any multiaddr libp2p can dial works; nothing here is network-specific.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	peer "github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

// connsDump returns one line per live libp2p connection: direction, whether it is RELAYED (a /p2p-circuit addr — the
// throttled fallback), the transport, and the remote multiaddr. The download-speed diagnostic: if the connection to a
// seeder is relayed, throughput is capped by the public relay (a few KB/s); a direct QUIC/TCP conn runs at link speed.
func (n *node) connsDump() string {
	if n.host == nil {
		return "offline"
	}
	conns := n.host.Network().Conns()
	var b strings.Builder
	fmt.Fprintf(&b, "%d connection(s):\n", len(conns))
	for _, c := range conns {
		ra := c.RemoteMultiaddr()
		relayed := strings.Contains(ra.String(), "p2p-circuit")
		kind := "DIRECT"
		if relayed {
			kind = "RELAYED"
		}
		transport := "tcp"
		if _, err := ra.ValueForProtocol(ma.P_QUIC_V1); err == nil {
			transport = "quic"
		} else if _, err := ra.ValueForProtocol(ma.P_WS); err == nil {
			transport = "ws"
		}
		fmt.Fprintf(&b, "  %-7s %-5s %-3s %s peer=%s\n", kind, transport, c.Stat().Direction.String(), ra, c.RemotePeer())
	}
	return b.String()
}

// peerID is this node's libp2p peer ID ("" if offline).
func (n *node) peerID() string {
	if n.host == nil {
		return ""
	}
	return n.host.ID().String()
}

// listenAddrs returns the node's dialable multiaddrs, each with the /p2p/<id> suffix so another node can dial it
// straight off (e.g. /ip4/.../udp/4001/quic-v1/p2p/12D3Koo...). Loopback is dropped.
func (n *node) listenAddrs() []string {
	if n.host == nil {
		return nil
	}
	pid := n.host.ID()
	var out []string
	for _, a := range n.host.Addrs() {
		s := a.String()
		if isLoopbackAddr(s) {
			continue
		}
		out = append(out, s+"/p2p/"+pid.String())
	}
	return out
}

func isLoopbackAddr(s string) bool {
	return len(s) >= 12 && (s[:12] == "/ip4/127.0.0" || s[:8] == "/ip6/::1")
}

// connect dials a peer at a full /p2p/ multiaddr and holds the connection (libp2p keeps it; bitswap then serves over
// it directly). Returns once connected or on error/timeout.
func (n *node) connect(maddr string) error {
	if n.host == nil {
		return errors.New("node offline")
	}
	a, err := ma.NewMultiaddr(maddr)
	if err != nil {
		return err
	}
	ai, err := peer.AddrInfoFromP2pAddr(a)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(n.ctx, 30*time.Second)
	defer cancel()
	return n.host.Connect(ctx, *ai)
}

// swarmPeerCount is the number of peers currently connected (distinct from PeerCount, which the UI already uses —
// kept separate so callers can pick the semantics they want; both currently return the connected-peer count).
func (n *node) swarmPeerCount() int {
	if n.host == nil {
		return 0
	}
	return len(n.host.Network().Peers())
}
