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
	"sync"
	"time"

	cid "github.com/ipfs/go-cid"
	network "github.com/libp2p/go-libp2p/core/network"
	peer "github.com/libp2p/go-libp2p/core/peer"
)

// warmProviderCount bounds how many providers we actively freshen — enough to find a reachable one without fanning out.
const warmProviderCount = 6

// warmInflight dedupes concurrent warms of the same CID: retry loops call warmProviders every attempt (backoff can be
// as low as 2s) while a walk runs up to 40s — without this, walks for one CID would pile up.
var warmInflight sync.Map

// warmProviders runs a live provider+address refresh for c and connects to reachable providers. Non-blocking: launches
// a bounded goroutine and returns. Safe to call on every fetch; no-op if the node is offline or the DHT isn't wired,
// or when a walk for this CID is already in flight.
func (n *node) warmProviders(c cid.Cid) {
	if n.dht == nil || n.host == nil {
		return
	}
	if _, busy := warmInflight.LoadOrStore(c.String(), struct{}{}); busy {
		return
	}
	safeGo("node.warmProviders", func() {
		defer warmInflight.Delete(c.String())
		ctx, cancel := context.WithTimeout(n.ctx, 40*time.Second)
		defer cancel()
		self := n.host.ID()
		t0 := time.Now()
		provs := n.dht.FindProvidersAsync(ctx, c, warmProviderCount)
		first := true
		for pi := range provs {
			if pi.ID == self || pi.ID == "" {
				continue
			}
			if first {
				fdbg("warm: first provider %s after %s (walk)", shortPeer(pi.ID.String()), time.Since(t0).Round(time.Millisecond))
				first = false
			}
			safeGo("node.freshenAndConnect", func() { n.freshenAndConnect(ctx, pi) }) // one goroutine per provider — never serialize behind a slow FindPeer
		}
	})
}

// freshenAndConnect learns a peer's CURRENT addresses via a live DHT walk (bypassing possibly-stale peerstore entries)
// and dials it, so a subsequent bitswap block request rides a working connection. Relay-circuit addresses connect
// first; DCUtR then upgrades to a direct hole-punched path transparently.
func (n *node) freshenAndConnect(ctx context.Context, pi peer.AddrInfo) {
	// If we already have a live connection to this provider, nothing to do — bitswap will use it.
	if n.host.Network().Connectedness(pi.ID) == network.Connected {
		return
	}
	// FAST PATH: FindProvidersAsync already handed us the provider's advertised addresses (from its provider record).
	// Dial them immediately — the common case where the record is current — concurrently with bitswap's own dial;
	// whichever connects first wins, the other no-ops.
	if len(pi.Addrs) > 0 {
		n.host.Peerstore().AddAddrs(pi.ID, pi.Addrs, 10*time.Minute)
		safeGo("node.dialWarm", func() { n.dialWarm(ctx, peer.AddrInfo{ID: pi.ID, Addrs: pi.Addrs}, "record") })
	}
	// REFRESH PATH (bounded): a live FindPeer walk gets CURRENT addresses when the record is stale (an old relay
	// reservation). On a hostile DHT this walk can take tens of seconds — longer than the whole fetch — so it MUST be
	// bounded and off the critical path. Cap it hard; if it returns newer addrs and we're still not connected, dial those.
	fctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	tf := time.Now()
	fresh, err := n.dht.FindPeer(fctx, pi.ID)
	fdbg("warm: FindPeer %s → %d addr in %s (err=%v)", shortPeer(pi.ID.String()), len(fresh.Addrs), time.Since(tf).Round(time.Millisecond), err)
	if err == nil && len(fresh.Addrs) > 0 && n.host.Network().Connectedness(pi.ID) != network.Connected {
		n.host.Peerstore().AddAddrs(fresh.ID, fresh.Addrs, 10*time.Minute)
		n.dialWarm(ctx, fresh, "findpeer")
	}
}

// dialWarm connects to pi with a bounded timeout, logging the outcome (VG_FETCH_DEBUG) + a bench line (VG_BENCH_OBSERVE).
func (n *node) dialWarm(ctx context.Context, pi peer.AddrInfo, via string) {
	cctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	td := time.Now()
	if err := n.host.Connect(cctx, pi); err != nil {
		fdbg("warm: connect(%s) to %s failed after %s: %v", via, shortPeer(pi.ID.String()), time.Since(td).Round(time.Millisecond), err)
		return
	}
	fdbg("warm: connected(%s) %s in %s", via, shortPeer(pi.ID.String()), time.Since(td).Round(time.Millisecond))
	if benchObserve() {
		fmt.Fprintf(os.Stderr, "[warm] connected via %s to provider %s (%d addr)\n", via, shortPeer(pi.ID.String()), len(pi.Addrs))
	}
}
