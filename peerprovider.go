package main

// peerprovider.go — "the peer ID is everything", applied to CONTENT ROUTING.
//
// On a hostile network the DHT is dead (measured on a peer's machine: router 0 returned 0 providers, 72 times,
// each a full 10s timeout) and the only content router that answers is the delegated HTTPS indexer — which returns
// exactly ONE provider, Pinata, a stock-cap seeder that crawls the tail of a large file and can rate-limit us to a
// halt. Yet that same machine holds a LIVE, authenticated libp2p connection to its peer (our PC seeder) via the
// peer link — a peer we KNOW has the content, that provider discovery will never surface.
//
// So we make our peers providers for EVERY cid by construction. peerFinder yields all accepted peers from
// every FindProvidersAsync call; bitswap then dials them and asks them for blocks directly (want-block), not merely
// via the periodic broadcast. A peer that lacks a given block answers DONT_HAVE and is simply not used for it —
// harmless. A peer that has it becomes a full-speed, DHT-independent source. This is what makes a download work on
// a network where the only *discoverable* provider is a throttling third party.
//
// Peer identity is free here: a peer's peer ID embeds its Ed25519 public key, so libp2p authenticates the
// connection against the exact key we paired — a hostile router cannot inject a fake "peer provider".

import (
	"context"
	network "github.com/libp2p/go-libp2p/core/network"
	"sync"
	"time"

	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p/core/peer"
	routing "github.com/libp2p/go-libp2p/core/routing"
)

// peerFinder is a routing.ContentDiscovery that returns this node's accepted peers as providers for any CID.
type peerFinder struct{ n *node }

// peerProviderPeers is the pure decision: the accepted-peer peer IDs to offer as providers, self excluded and
// malformed IDs dropped. Separated from the channel/host glue so it can be unit-tested without a live libp2p host.
func peerProviderPeers(s *socialState, self peer.ID) []peer.ID {
	if s == nil {
		return nil
	}
	var out []peer.ID
	for _, pidStr := range s.acceptedPeers() {
		pid, err := peer.Decode(pidStr)
		if err != nil || pid == self { // never offer ourselves; skip an unparseable id rather than error
			continue
		}
		out = append(out, pid)
	}
	return out
}

// maxPeerProviders caps how many peers we offer per lookup. boxo's ProviderQueryManager stops querying the
// OTHER routers (DHT, delegated) once it has enough CONNECTED providers — so yielding a large peer list could
// crowd out the real seeder. We offer only ALREADY-CONNECTED peers (warmPeers does the connecting), capped,
// so peers supplement provider discovery instead of starving it.
const maxPeerProviders = 6

func (ff peerFinder) FindProvidersAsync(ctx context.Context, _ cid.Cid, _ int) <-chan peer.AddrInfo {
	out := make(chan peer.AddrInfo)
	safeGo("peerFinder.provide", func() {
		defer close(out)
		if ff.n.host == nil {
			return
		}
		net := ff.n.host.Network()
		sent := 0
		for _, pid := range peerProviderPeers(ff.n.social, ff.n.host.ID()) {
			if sent >= maxPeerProviders {
				return
			}
			// ONLY peers we have a DIRECT (non-relayed) connection to. bitswap won't bulk-transfer over a Limited
			// (relayed) conn, so offering a relay-only peer as a provider just burns a PQM dial and can count
			// toward its "enough providers" cutoff before the real seeder answers. warmPeers + DCUtR upgrade a
			// relayed peer to direct in the background; once direct it shows up here.
			if net.Connectedness(pid) != network.Connected {
				continue
			}
			select {
			case out <- peer.AddrInfo{ID: pid, Addrs: ff.n.host.Peerstore().Addrs(pid)}:
				sent++
			case <-ctx.Done():
				return
			}
		}
	})
	return out
}

// warmPeers proactively connects to every accepted peer so bitswap has a live, bulk-capable path to them before
// the session even asks. Provider-independent (we already know who our peers are) and deduped against concurrent
// warms. A peer already connected is a cheap no-op; one reachable only via relay keeps its DCUtR upgrade running.
// peerWarmAt rate-limits warmPeers per peer: a dial+FindPeer costs a DHT round trip, and a fetch retry loop
// calls warmPeers every attempt — without this, an OFFLINE peer would be dialed every couple of seconds
// forever (the "dial storm" a dead-DHT network turns into wasted work). Skip a peer warmed within the last
// peerWarmEvery; an ALREADY-CONNECTED peer is skipped outright (nothing to do).
var peerWarmAt sync.Map // peerID string → time.Time of last warm attempt

const peerWarmEvery = 60 * time.Second

func (n *node) warmPeers() {
	if n.dht == nil || n.host == nil || n.social == nil {
		return
	}
	self := n.host.ID()
	for _, pidStr := range n.social.acceptedPeers() {
		pid, err := peer.Decode(pidStr)
		if err != nil || pid == self {
			continue
		}
		// Already connected → nothing to do (peerFinder will offer it).
		if c := n.host.Network().Connectedness(pid); c == network.Connected || c == network.Limited {
			continue
		}
		if last, ok := peerWarmAt.Load(pidStr); ok {
			if t, _ := last.(time.Time); time.Since(t) < peerWarmEvery {
				continue // warmed recently — don't storm an offline peer
			}
		}
		key := "peer:" + pidStr
		if _, busy := warmInflight.LoadOrStore(key, struct{}{}); busy {
			continue
		}
		peerWarmAt.Store(pidStr, time.Now())
		p := pid
		safeGo("node.warmPeers", func() {
			defer warmInflight.Delete(key)
			ctx, cancel := context.WithTimeout(n.ctx, 40*time.Second)
			defer cancel()
			n.freshenAndConnect(ctx, peer.AddrInfo{ID: p, Addrs: n.host.Peerstore().Addrs(p)})
		})
	}
}

// peerConnected reports whether an accepted peer is connected DIRECTLY (the peers peerFinder offers) — the
// provider search then gives it the search delay before walking the DHT (combinedFinder.hold).
func (n *node) peerConnected() bool {
	if n.host == nil {
		return false
	}
	for _, pid := range peerProviderPeers(n.social, n.host.ID()) {
		if n.host.Network().Connectedness(pid) == network.Connected {
			return true
		}
	}
	return false
}

var _ routing.ContentDiscovery = peerFinder{}
