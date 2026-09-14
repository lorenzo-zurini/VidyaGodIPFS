package main

// friendprovider.go — "the friend ID is everything", applied to CONTENT ROUTING.
//
// On a hostile network the DHT is dead (measured on a friend's machine: router 0 returned 0 providers, 72 times,
// each a full 10s timeout) and the only content router that answers is the delegated HTTPS indexer — which returns
// exactly ONE provider, Pinata, a stock-cap seeder that crawls the tail of a large file and can rate-limit us to a
// halt. Yet that same machine holds a LIVE, authenticated libp2p connection to its friend (our PC seeder) via the
// friend link — a peer we KNOW has the content, that provider discovery will never surface.
//
// So we make our friends providers for EVERY cid by construction. friendFinder yields all accepted friends from
// every FindProvidersAsync call; bitswap then dials them and asks them for blocks directly (want-block), not merely
// via the periodic broadcast. A friend that lacks a given block answers DONT_HAVE and is simply not used for it —
// harmless. A friend that has it becomes a full-speed, DHT-independent source. This is what makes a download work on
// a network where the only *discoverable* provider is a throttling third party.
//
// Peer identity is free here: a friend's peer ID embeds its Ed25519 public key, so libp2p authenticates the
// connection against the exact key we befriended — a hostile router cannot inject a fake "friend provider".

import (
	"context"
	network "github.com/libp2p/go-libp2p/core/network"
	"sync"
	"time"

	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p/core/peer"
	routing "github.com/libp2p/go-libp2p/core/routing"
)

// friendFinder is a routing.ContentDiscovery that returns this node's accepted friends as providers for any CID.
type friendFinder struct{ n *node }

// friendProviderPeers is the pure decision: the accepted-friend peer IDs to offer as providers, self excluded and
// malformed IDs dropped. Separated from the channel/host glue so it can be unit-tested without a live libp2p host.
func friendProviderPeers(s *socialState, self peer.ID) []peer.ID {
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

// maxFriendProviders caps how many friends we offer per lookup. boxo's ProviderQueryManager stops querying the
// OTHER routers (DHT, delegated) once it has enough CONNECTED providers — so yielding a large friend list could
// crowd out the real seeder. We offer only ALREADY-CONNECTED friends (warmFriends does the connecting), capped,
// so friends supplement provider discovery instead of starving it.
const maxFriendProviders = 6

func (ff friendFinder) FindProvidersAsync(ctx context.Context, _ cid.Cid, _ int) <-chan peer.AddrInfo {
	out := make(chan peer.AddrInfo)
	safeGo("friendFinder.provide", func() {
		defer close(out)
		if ff.n.host == nil {
			return
		}
		net := ff.n.host.Network()
		sent := 0
		for _, pid := range friendProviderPeers(ff.n.social, ff.n.host.ID()) {
			if sent >= maxFriendProviders {
				return
			}
			// ONLY friends we have a DIRECT (non-relayed) connection to. bitswap won't bulk-transfer over a Limited
			// (relayed) conn, so offering a relay-only friend as a provider just burns a PQM dial and can count
			// toward its "enough providers" cutoff before the real seeder answers. warmFriends + DCUtR upgrade a
			// relayed friend to direct in the background; once direct it shows up here.
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

// warmFriends proactively connects to every accepted friend so bitswap has a live, bulk-capable path to them before
// the session even asks. Provider-independent (we already know who our friends are) and deduped against concurrent
// warms. A friend already connected is a cheap no-op; one reachable only via relay keeps its DCUtR upgrade running.
// friendWarmAt rate-limits warmFriends per peer: a dial+FindPeer costs a DHT round trip, and a fetch retry loop
// calls warmFriends every attempt — without this, an OFFLINE friend would be dialed every couple of seconds
// forever (the "dial storm" a dead-DHT network turns into wasted work). Skip a friend warmed within the last
// friendWarmEvery; an ALREADY-CONNECTED friend is skipped outright (nothing to do).
var friendWarmAt sync.Map // peerID string → time.Time of last warm attempt

const friendWarmEvery = 60 * time.Second

func (n *node) warmFriends() {
	if n.dht == nil || n.host == nil || n.social == nil {
		return
	}
	self := n.host.ID()
	for _, pidStr := range n.social.acceptedPeers() {
		pid, err := peer.Decode(pidStr)
		if err != nil || pid == self {
			continue
		}
		// Already connected → nothing to do (friendFinder will offer it).
		if c := n.host.Network().Connectedness(pid); c == network.Connected || c == network.Limited {
			continue
		}
		if last, ok := friendWarmAt.Load(pidStr); ok {
			if t, _ := last.(time.Time); time.Since(t) < friendWarmEvery {
				continue // warmed recently — don't storm an offline friend
			}
		}
		key := "friend:" + pidStr
		if _, busy := warmInflight.LoadOrStore(key, struct{}{}); busy {
			continue
		}
		friendWarmAt.Store(pidStr, time.Now())
		p := pid
		safeGo("node.warmFriends", func() {
			defer warmInflight.Delete(key)
			ctx, cancel := context.WithTimeout(n.ctx, 40*time.Second)
			defer cancel()
			n.freshenAndConnect(ctx, peer.AddrInfo{ID: p, Addrs: n.host.Peerstore().Addrs(p)})
		})
	}
}

var _ routing.ContentDiscovery = friendFinder{}
