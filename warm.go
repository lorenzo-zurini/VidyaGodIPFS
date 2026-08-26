package main

// warm.go — provider address freshening. The failure mode observed on hostile NAT-to-NAT fetches: the client holds a
// STALE cached address for a content provider (an old relay-circuit reservation from a previous seeder run, or a
// provider record that predates the seeder's current relays). libp2p dials the stale addr, it fails, and bitswap only
// re-discovers after its RebroadcastDelay — often past the fetch deadline. The transfer "fails" even though the
// provider is up and reachable on a CURRENT address.
//
// warmProviders fixes this proactively and universally: at fetch start it asks the DHT who currently provides the CID,
// then for each provider does a LIVE FindPeer (a fresh routing walk, not the peerstore cache) to learn its current
// addresses, refreshes the peerstore, and dials. On a healthy fetch whose cached addr already works this is a cheap
// redundant connect to an already-connected peer; on a cold/stale cache it is what makes the fetch succeed. Runs in a
// bounded goroutine in parallel with bitswap, so it never delays a fetch that is already progressing.

import (
	"context"
	"fmt"
	"os"
	"time"

	cid "github.com/ipfs/go-cid"
	network "github.com/libp2p/go-libp2p/core/network"
	peer "github.com/libp2p/go-libp2p/core/peer"
)

// warmProviderCount bounds how many providers we actively freshen — enough to find a reachable one without fanning out.
const warmProviderCount = 6

// warmProviders runs a live provider+address refresh for c and connects to reachable providers. Non-blocking: launches
// a bounded goroutine and returns. Safe to call on every fetch; no-op if the node is offline or the DHT isn't wired.
func (n *node) warmProviders(c cid.Cid) {
	if n.dht == nil || n.host == nil {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(n.ctx, 40*time.Second)
		defer cancel()
		self := n.host.ID()
		provs := n.dht.FindProvidersAsync(ctx, c, warmProviderCount)
		for pi := range provs {
			if pi.ID == self || pi.ID == "" {
				continue
			}
			n.freshenAndConnect(ctx, pi)
		}
	}()
}

// freshenAndConnect learns a peer's CURRENT addresses via a live DHT walk (bypassing possibly-stale peerstore entries)
// and dials it, so a subsequent bitswap block request rides a working connection. Relay-circuit addresses connect
// first; DCUtR then upgrades to a direct hole-punched path transparently.
func (n *node) freshenAndConnect(ctx context.Context, pi peer.AddrInfo) {
	// If we already have a live connection to this provider, nothing to do — bitswap will use it.
	if n.host.Network().Connectedness(pi.ID) == network.Connected {
		return
	}
	// Live routing walk for current addresses. This is the key step: peers close to pi.ID return the addresses they
	// have most recently seen it on (relays it currently holds reservations with, its current public ip:port), which
	// supersedes whatever stale relay addr our peerstore cached from a previous run.
	fresh, err := n.dht.FindPeer(ctx, pi.ID)
	if err == nil && len(fresh.Addrs) > 0 {
		pi = fresh
	} else if len(pi.Addrs) == 0 {
		return // no cached addrs and the walk found none — nothing to dial
	}
	// Refresh the peerstore with the current addresses at a short TTL, so a stale entry can't out-live a good one.
	n.host.Peerstore().AddAddrs(pi.ID, pi.Addrs, 10*time.Minute)
	cctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	if err := n.host.Connect(cctx, pi); err != nil {
		fdbg("warm: connect to provider %s failed: %v", shortPeer(pi.ID.String()), err)
		return
	}
	if benchObserve() {
		fmt.Fprintf(os.Stderr, "[warm] connected fresh to provider %s (%d addr)\n", shortPeer(pi.ID.String()), len(pi.Addrs))
	}
}
