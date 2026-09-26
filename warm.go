package main

// warm.go — provider address freshening. The failure mode observed on hostile NAT-to-NAT fetches: the client holds a
// STALE cached address for a content provider (an old relay-circuit reservation from a previous seeder run, or a
// provider record that predates the seeder's current relays). libp2p dials the stale addr, it fails, and bitswap only
// re-discovers after its RebroadcastDelay — often past the fetch deadline. The transfer "fails" even though the
// provider is up and reachable on a CURRENT address.
//
// warmForFetch fixes this: it asks the DHT who currently provides the CID, then for each provider does a LIVE FindPeer
// (a fresh routing walk, not the peerstore cache) to learn its current addresses, refreshes the peerstore, and dials.
//
// It is part of the fetch's JOB — the fetch holds one slot of the one network queue (netq.go) and this runs inside it,
// on the attempt's context, ending with it. And it follows bitswap's own rule (boxo's ProviderSearchDelay): the peers
// we are connected to are asked first; only an attempt that has received nothing new after providerSearchDelay walks,
// one walk at a time. (Started unconditionally at every fetch start on the node's lifetime, a receive of a few hundred
// small blocks from a connected friend ran ~90 overlapping walks and took the household router down.)

import (
	"context"
	"fmt"
	swarm "github.com/libp2p/go-libp2p/p2p/net/swarm"
	"os"
	"sync"
	"time"

	cid "github.com/ipfs/go-cid"
	network "github.com/libp2p/go-libp2p/core/network"
	peer "github.com/libp2p/go-libp2p/core/peer"
)

// warmProviderCount bounds how many providers we actively freshen — enough to find a reachable one without fanning out.
const warmProviderCount = 6

// providerSearchDelay: how long a fetch relies on the peers it is already connected to before it walks the DHT —
// boxo bitswap's default ProviderSearchDelay, the same rule for our own walks and the finder. A var for tests.
var providerSearchDelay = time.Second

// warmInflight dedupes concurrent warms of the same CID: retry loops call warmProviders every attempt (backoff can be
// as low as 2s) while a walk runs up to 40s — without this, walks for one CID would pile up.
var warmInflight sync.Map

// warmForFetch runs the fetch attempt's provider refresh (see above) for c — and, withSeed, then for the seed-level
// roots (fresh content may have no DHT record yet; whoever seeds our sources has it) — only if the attempt has not
// moved (moved() false; nil = never moved) within providerSearchDelay. One goroutine, walks in sequence, all on ctx.
func (n *node) warmForFetch(ctx context.Context, c cid.Cid, moved func() bool, withSeed bool) {
	if n.dht == nil || n.host == nil {
		return
	}
	safeGo("node.warmForFetch", func() {
		if !walkNeeded(ctx, moved) {
			netStats.warmSkipped.Add(1)
			return
		}
		targets := []cid.Cid{c}
		if withSeed {
			n.seedMu.RLock()
			targets = append(targets, n.seedColls...)
			n.seedMu.RUnlock()
		}
		for _, t := range targets {
			if ctx.Err() != nil {
				return
			}
			n.warmWalk(ctx, t)
		}
	})
}

// warmWalk walks the DHT for c's providers and connects to them; returns when the walk ends. Skipped when a walk for
// c is already running (another attempt's).
func (n *node) warmWalk(ctx context.Context, c cid.Cid) {
	if _, busy := warmInflight.LoadOrStore(c.String(), struct{}{}); busy {
		return
	}
	defer warmInflight.Delete(c.String())
	netStats.warmWalks.Add(1)
	ctx, cancel := context.WithTimeout(ctx, 40*time.Second)
	defer cancel()
	self := n.host.ID()
	t0 := time.Now()
	provs := n.dht.FindProvidersAsync(ctx, c, warmProviderCount)
	first := true
	var wg sync.WaitGroup
	for pi := range provs {
		if pi.ID == self || pi.ID == "" {
			continue
		}
		n.noteWantedProvider(pi.ID)
		if first {
			fdbg("warm: first provider %s after %s (walk)", shortPeer(pi.ID.String()), time.Since(t0).Round(time.Millisecond))
			first = false
		}
		wg.Add(1)
		safeGo("node.freshenAndConnect", func() { defer wg.Done(); n.freshenAndConnect(ctx, pi) }) // never serialize behind a slow FindPeer
	}
	wg.Wait()
}

// walkNeeded waits providerSearchDelay and reports whether the attempt still needs a walk: false once ctx ended (the
// fetch is over) or moved() says the fetch received something new in the meantime.
func walkNeeded(ctx context.Context, moved func() bool) bool {
	t := time.NewTimer(providerSearchDelay)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
	}
	return ctx.Err() == nil && (moved == nil || !moved())
}

// freshenAndConnect learns a peer's CURRENT addresses via a live DHT walk (bypassing possibly-stale peerstore entries)
// and dials it, so a subsequent bitswap block request rides a working connection. Relay-circuit addresses connect
// first; DCUtR then upgrades to a direct hole-punched path transparently.
func (n *node) freshenAndConnect(ctx context.Context, pi peer.AddrInfo) {
	// If we already have a live connection to this provider, nothing to do — bitswap will use it. Limited
	// (relayed) counts: bitswap opts into limited conns and DCUtR upgrades in the background; dialing "again"
	// through the swarm just short-circuits on the existing conn anyway, so the walk below would be pure waste.
	if c := n.host.Network().Connectedness(pi.ID); c == network.Connected || c == network.Limited {
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
	netStats.findPeers.Add(1)
	fresh, err := n.dht.FindPeer(fctx, pi.ID)
	fdbg("warm: FindPeer %s → %d addr in %s (err=%v)", shortPeer(pi.ID.String()), len(fresh.Addrs), time.Since(tf).Round(time.Millisecond), err)
	if c := n.host.Network().Connectedness(pi.ID); err == nil && len(fresh.Addrs) > 0 && c != network.Connected && c != network.Limited {
		n.host.Peerstore().AddAddrs(fresh.ID, fresh.Addrs, 10*time.Minute)
		n.dialWarm(ctx, fresh, "findpeer")
	}
}

// clearDialBackoff lifts libp2p's per-peer dial backoff. The swarm backs a peer off after a failed dial (seconds,
// growing per failure), so a provider that refused ONE transient dial is silently not re-dialed by bitswap or by us
// for the next attempt(s): the connect-probe against Pinata's wss peer measured two dead 30 s attempts after a single
// failed dial, then success once the backoff lapsed. Every fetch attempt is a deliberate retry — give the provider a
// fresh dial. Scoped to the peers we are about to warm; no other dial policy changes. No-op on non-swarm networks
// (mocknet in tests).
func (n *node) clearDialBackoff(pid peer.ID) {
	if n.host == nil {
		return
	}
	if sw, ok := n.host.Network().(*swarm.Swarm); ok {
		sw.Backoff().Clear(pid)
	}
}

// dialWarm connects to pi with a bounded timeout, logging the outcome (VG_FETCH_DEBUG) + a bench line (VG_BENCH_OBSERVE).
func (n *node) dialWarm(ctx context.Context, pi peer.AddrInfo, via string) {
	cctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	n.clearDialBackoff(pi.ID) // a retry must actually re-dial (see clearDialBackoff)
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
