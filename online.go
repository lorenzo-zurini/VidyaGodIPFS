package main

// online.go — bring the node onto the public IPFS network (M3): a libp2p host + Kademlia DHT + online bitswap +
// a reprovider that announces our pinned content to the DHT. Best-effort: if anything here fails (e.g. no network),
// the node keeps working offline.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	bitswap "github.com/ipfs/boxo/bitswap"
	bsnet "github.com/ipfs/boxo/bitswap/network/bsnet"
	blockservice "github.com/ipfs/boxo/blockservice"
	merkledag "github.com/ipfs/boxo/ipld/merkledag"
	provider "github.com/ipfs/boxo/provider"
	routinghttp "github.com/ipfs/boxo/routing/http/client"
	routinghttpcr "github.com/ipfs/boxo/routing/http/contentrouter"
	cid "github.com/ipfs/go-cid"
	libp2p "github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	crypto "github.com/libp2p/go-libp2p/core/crypto"
	host "github.com/libp2p/go-libp2p/core/host"
	metrics "github.com/libp2p/go-libp2p/core/metrics"
	peer "github.com/libp2p/go-libp2p/core/peer"
	routing "github.com/libp2p/go-libp2p/core/routing"
	mdns "github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	connmgr "github.com/libp2p/go-libp2p/p2p/net/connmgr"
	swarm "github.com/libp2p/go-libp2p/p2p/net/swarm"
)

// loadOrCreateIdentity persists a stable Ed25519 peer key under the repo so the peer ID is consistent across runs.
func loadOrCreateIdentity(repoPath string) (crypto.PrivKey, error) {
	p := filepath.Join(repoPath, "identity.key")
	if b, err := os.ReadFile(p); err == nil {
		return crypto.UnmarshalPrivateKey(b)
	}
	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		return nil, err
	}
	if b, err := crypto.MarshalPrivateKey(priv); err == nil {
		_ = os.WriteFile(p, b, 0o600)
	}
	return priv, nil
}

// combinedFinder fans a provider lookup out to several content routers in parallel and merges the results, so bitswap
// consults the fast delegated HTTP indexer alongside the (slow, cold) Amino DHT and uses whichever answers first.
type combinedFinder struct{ routers []routing.ContentDiscovery }

func (cf combinedFinder) FindProvidersAsync(ctx context.Context, c cid.Cid, count int) <-chan peer.AddrInfo {
	out := make(chan peer.AddrInfo)
	var wg sync.WaitGroup
	for i, r := range cf.routers {
		if r == nil {
			continue
		}
		wg.Add(1)
		safeGo("finder.fanout", func() {
			idx, rr := i, r
			defer wg.Done()
			t0 := time.Now()
			n := 0
			for ai := range rr.FindProvidersAsync(ctx, c, count) {
				n++
				if n == 1 {
					fmt.Fprintf(os.Stderr, "[finder] router %d: first provider in %s\n", idx, time.Since(t0))
				}
				select {
				case out <- ai:
				case <-ctx.Done():
					return
				}
			}
			fmt.Fprintf(os.Stderr, "[finder] router %d: %d providers total in %s\n", idx, n, time.Since(t0))
		})
	}
	safeGo("finder.close", func() { wg.Wait(); close(out) })
	return out
}

// goOnline builds the network stack and swaps the node's block/DAG services from the offline exchange to online
// bitswap. Called synchronously from openNode (fast: host+DHT+bitswap construction); the slow DHT bootstrap +
// peer connection runs in a background goroutine.
func (n *node) goOnline() error {
	priv, err := loadOrCreateIdentity(n.repoPath)
	if err != nil {
		return err
	}

	// Maximum connectivity: hold a large peer set (default trims at ~192 — too low to fan out to many providers) and
	// remove resource-manager caps (the default limits per-peer streams, which throttles parallel multi-provider
	// fetch). connmgr bounds total connections (so FDs stay sane) while rcmgr stays unbounded underneath.
	cm, cmErr := connmgr.NewConnManager(400, 900, connmgr.WithGracePeriod(20*time.Second))
	if cmErr != nil {
		return cmErr
	}
	rm, rmErr := rcmgr.NewResourceManager(rcmgr.NewFixedLimiter(rcmgr.InfiniteLimits))
	if rmErr != nil {
		return rmErr
	}
	bwc := metrics.NewBandwidthCounter() // global up/down byte counters + rolling rates for every stream
	libp2pOpts := []libp2p.Option{
		libp2p.Identity(priv),
		libp2p.BandwidthReporter(bwc),
		// Listen on every default transport so we can dial — and be reached by — the widest set of peers (TCP, QUIC,
		// WebSocket, WebTransport). More transports = more usable providers when content has many hosts.
		libp2p.ListenAddrStrings(
			"/ip4/0.0.0.0/tcp/0",
			"/ip4/0.0.0.0/udp/0/quic-v1",
			"/ip4/0.0.0.0/udp/0/quic-v1/webtransport",
			"/ip4/0.0.0.0/tcp/0/ws",
			"/ip6/::/tcp/0",
			"/ip6/::/udp/0/quic-v1",
			"/ip6/::/udp/0/quic-v1/webtransport",
			"/ip6/::/tcp/0/ws",
		),
		libp2p.ConnectionManager(cm),
		libp2p.ResourceManager(rm),
		libp2p.NATPortMap(),
		libp2p.EnableNATService(),
		libp2p.EnableHolePunching(),
		// AutoRelay: when this node is behind NAT (unreachable directly), reserve slots on relays and advertise
		// relay addresses so peers on OTHER networks can still reach it (hole-punching is coordinated via the relay).
		// Candidates come from the DHT routing table (those that support circuit-relay-v2 get used). Same-LAN peers
		// don't need this — that's mDNS.
		libp2p.EnableAutoRelayWithPeerSource(n.relayPeerSource),
	}
	// Resolve /dnsaddr + /dns dials over DoH (see doh.go) so bootstrap peers AND DNS-addressed providers (Pinata is
	// /dnsaddr/bitswap.pinata.cloud) are dialable on DNS-filtered / captive networks. Best-effort: skip if it can't build.
	if mr, merr := dohMultiaddrResolver(); merr == nil {
		libp2pOpts = append(libp2pOpts, libp2p.MultiaddrResolver(swarm.ResolverFromMaDNS{Resolver: mr}))
	}
	// Benchmark control (bench.go): VG_BENCH_NO_TUNNEL forces open-internet paths by gating out the WireGuard/ZeroTier
	// subnets this machine pair shares, so a measured throughput can't secretly be riding the tunnel. No-op otherwise.
	if gater := newBenchGater(); gater != nil {
		libp2pOpts = append(libp2pOpts, libp2p.ConnectionGater(gater))
	}
	h, err := libp2p.New(libp2pOpts...)
	if err != nil {
		return err
	}

	kad, err := dht.New(n.ctx, h, dht.Mode(dht.ModeAuto),
		dht.BootstrapPeers(dht.GetDefaultBootstrapPeerAddrInfos()...))
	if err != nil {
		_ = h.Close()
		return err
	}

	// Online bitswap. Provider discovery was the ENTIRE download bottleneck: a cold Amino-DHT walk took ~14 s to find
	// who holds a CID, while the transfer itself runs near link speed once a provider is known. So consult a delegated
	// HTTP router (the public delegated-ipfs.dev indexer — IPNI + a warm DHT) IN PARALLEL with our own DHT; the HTTP
	// indexer answers in well under a second. Falls back to DHT-only if the client can't be built.
	bsn := bsnet.NewFromIpfsHost(h)
	var finder routing.ContentDiscovery = kad
	// DoH-resolving HTTP client so the indexer host (delegated-ipfs.dev) also resolves through a DNS filter.
	if hc, herr := routinghttp.New("https://delegated-ipfs.dev", routinghttp.WithHTTPClient(dohHTTPClient(newDoHResolver()))); herr == nil {
		finder = combinedFinder{routers: []routing.ContentDiscovery{kad, routinghttpcr.NewContentRoutingClient(hc)}}
	}
	n.upSeen = make(map[string]int64)
	// Concurrent-UPLOAD tuning. boxo's server defaults cap a SINGLE peer to 1 MiB of outstanding (in-flight) block
	// bytes (~4× 256 KiB) and 8 send workers. When one downloader fetches several files at once (e.g. our 3-way
	// concurrent download), all its requests hit the seeder as ONE peer, so that 1 MiB window is monopolized by the
	// first file and the others starve → they trip the fetch stall watchdog and serialize. Widen the per-peer window
	// to 128 MiB and double the task workers so the seeder interleaves many files to a single peer at link speed.
	bswap := bitswap.New(n.ctx, bsn, finder, n.fstore,
		bitswap.WithTracer(upTracer{n}), // per-CID upload tracking
		bitswap.MaxOutstandingBytesPerPeer(128<<20),
		bitswap.TaskWorkerCount(16),
		bitswap.EngineTaskWorkerCount(16),
		// The engine TRUNCATES each peer's queued wantlist at 1024 entries by default, SILENTLY dropping the rest —
		// a >256MB single-file fetch (or a few concurrent ones) overflows it, and the client limps through the tail
		// on periodic rebroadcasts: measured ON LOOPBACK as 177 MB/s for the first 1024 blocks, then 0.3-3 MB/s for
		// the rest — the GUI's "pulsing" download speed. Raise it so our seeders pipeline entire game layers; the
		// client ALSO windows its wants (fetch.go wantChunk) to stay under THIRD-PARTY seeders' default cap.
		bitswap.MaxQueuedWantlistEntriesPerPeer(1<<16),
		// A bitswap session broadcasts its wants once, then only RE-REQUESTS unfulfilled ones after RebroadcastDelay
		// (default 60s). So the TAIL of a large fetch (last blocks not served in the first pass) sits idle for up to a
		// minute — looking "stalled — waiting for peers" — until the rebroadcast (or our 20s watchdog tears the
		// session down and a fresh one re-asks). Cut it to 10s so a session self-heals its tail well before the
		// watchdog fires, which also un-starves files queued behind another on a slow link (concurrent downloads).
		bitswap.RebroadcastDelay(10*time.Second))

	n.host = h
	n.dht = kad
	n.exchange = bswap
	n.bwc = bwc
	fmt.Fprintf(os.Stderr, "[node] peerID=%s\n", h.ID())
	for _, a := range h.Addrs() {
		fmt.Fprintf(os.Stderr, "[node] listen=%s/p2p/%s\n", a, h.ID())
	}
	// Re-log addresses once AutoNAT/UPnP/relay have settled — these are what the node ACTUALLY advertises to the
	// public network (a public ip4/ip6 = directly reachable; only /p2p-circuit = relay-only → slow downloads).
	safeGo("node.addrLogger", func() {
		time.Sleep(45 * time.Second)
		for _, a := range h.Addrs() {
			fmt.Fprintf(os.Stderr, "[node] advertised=%s\n", a)
		}
	})
	// WriteThrough(true): see node.go — addNoCopy must create filestore refs even when bitswap already cached blocks.
	n.bserv = blockservice.New(n.fstore, bswap, blockservice.WriteThrough(true))
	n.dserv = merkledag.NewDAGService(n.bserv)

	// Reprovider: periodically announce our pinned roots to the DHT so peers can find what we seed.
	if prov, perr := provider.New(n.ds,
		provider.Online(kad),
		provider.KeyProvider(n.reprovideKeys),
		provider.ReproviderInterval(22*time.Hour),
	); perr == nil {
		n.provider = prov
	}

	// Local-network discovery (mDNS): same-LAN nodes find + connect to each other directly. Essential because the
	// public DHT does NOT advertise private LAN addresses, so two boxes on one network can't discover each other
	// through it. Once connected, bitswap serves blocks directly between them (no DHT provider record needed).
	if svc := mdns.NewMdnsService(h, "", &mdnsNotifee{h: h, ctx: n.ctx}); svc != nil {
		if err := svc.Start(); err == nil {
			n.mdns = svc
		}
	}

	// Friends / multiplayer social layer: register the /vidyagod/friend protocol on the host and start presence.
	// Uses the DHT for peer routing (FindPeer) and the same libp2p auth that secures bitswap. Best-effort.
	if n.social != nil {
		n.friend = newFriendService(n.ctx, h, kad, n.social, emitFriendEvent)
		n.friend.start()
		n.friend.startPresence(45 * time.Second)
		// ALWAYS-ON link maintainer (overlaylink.go): heartbeat + auto-reconnect + datagram-path proving per accepted
		// friend for the app's lifetime — the LAN's links are warm BEFORE any game launches, and the UI gets
		// live per-friend link state. Membership is re-read from the address book every beat.
		n.linkm = newLinkMaintainer(n.ctx, h, kad, func() map[peer.ID]string {
			out := map[peer.ID]string{}
			for _, c := range n.social.list() {
				if c.State != stAccepted {
					continue
				}
				pid, err := peer.Decode(c.PeerID)
				if err != nil {
					continue
				}
				nick := c.Nick
				if nick == "" {
					nick = shortPeer(c.PeerID)
				}
				out[pid] = nick
			}
			return out
		})
		safeGo("linkm.run", n.linkm.run)
		// Virtual LAN of friends: there is NO session/host. Each friend's vIP is a pure function of its peer ID
		// (friendlan.go), so membership + the overlay routing table are derived from the accepted-friends set on
		// demand. The overlay datapath registers the /vidyagod/overlay handler now; a TUN is attached at game launch.
		n.overlay = newOverlayService(n.ctx, h, kad, n.linkm)
		n.overlay.start()
	}

	n.online = true
	safeGo("node.bootstrap", n.bootstrap)
	safeGo("node.refreshPinnedSet", func() { n.refreshPinnedSet(n.ctx) }) // keep the pinned-root set warm for the upload tracer
	n.startBenchObserver()                                                // bench.go: periodic path/bandwidth ground-truth log when VG_BENCH_OBSERVE is set

	// Announce EVERYTHING we seed to the DHT shortly after startup (once the routing table is warm), instead of waiting
	// up to ReproviderInterval (22h) for the first scheduled sweep. Without this, a just-launched seeder's content —
	// and anything added while it was down — stays undiscoverable for up to 22h, so a pin-by-CID (Pinata) or a peer
	// just "searches" forever. A short-lived process's one-shot provide barely propagates (immature routing table);
	// this robust batch sweep runs from the long-lived node once it has peers.
	safeGo("node.startupReprovide", func() {
		select {
		case <-time.After(45 * time.Second): // let bootstrap + DHT routing settle so the provides actually stick
		case <-n.ctx.Done():
			return
		}
		// FALLBACK only: if the app hasn't driven the level-ordered 3-pass seed announce (seedannounce.go), do a plain
		// boxo reprovide of all pinned roots. When the app calls setSeedLevels (GUI / long-lived seeder) that runs
		// instead, giving ordered + tracked announcing; the boxo 22h reprovider remains the long-term backstop either way.
		n.seedMu.RLock()
		started := n.seedStarted
		n.seedMu.RUnlock()
		if !started && n.provider != nil {
			if err := n.provider.Reprovide(n.ctx); err != nil {
				fmt.Fprintf(os.Stderr, "[node] startup reprovide failed: %v\n", err)
			} else {
				fmt.Fprintf(os.Stderr, "[node] startup reprovide: announced seeded content to the DHT\n")
			}
		}
	})
	return nil
}

// announce eagerly publishes a freshly added/fetched CID to the DHT so peers AND pinning services (Pinata) can find
// it within seconds, instead of waiting up to ReproviderInterval (22h) for the next reprovide sweep. Without this,
// newly-added content has ZERO providers on the public routing layer — a pin-by-CID just "searches" forever because
// there is nothing to discover. Best-effort + async: a DHT provide walks to the ~20 closest peers (a few seconds).
func (n *node) announce(c cid.Cid) {
	if n.provider == nil {
		return // offline node / provider not wired
	}
	// Freshly added/downloaded content is being seeded + announced NOW — mark it so the GUI shows "seeding" immediately
	// instead of "queued for seeding" (which is for pins awaiting the next 3-pass sweep after a restart).
	n.seedMu.Lock()
	if n.seedDone != nil {
		n.seedDone[c.String()] = struct{}{}
	}
	n.seedMu.Unlock()
	safeGo("node.announceProvide", func() {
		if err := n.provider.Provide(n.ctx, c, true); err != nil {
			fmt.Fprintf(os.Stderr, "[node] provide %s failed: %v\n", c, err)
		}
	})
}

// relayPeerSource feeds AutoRelay with candidate relays from the DHT routing table — public, well-connected peers;
// those that support circuit-relay-v2 get used. Called by AutoRelay at runtime (n.dht/n.host are set by then).
func (n *node) relayPeerSource(ctx context.Context, num int) <-chan peer.AddrInfo {
	out := make(chan peer.AddrInfo)
	safeGo("node.relayPeerSource", func() {
		defer close(out)
		if n.dht == nil || n.host == nil {
			return
		}
		sent := 0
		for _, p := range n.dht.RoutingTable().ListPeers() {
			ai := n.host.Peerstore().PeerInfo(p)
			if len(ai.Addrs) == 0 {
				continue
			}
			select {
			case out <- ai:
				sent++
			case <-ctx.Done():
				return
			}
			if sent >= num {
				return
			}
		}
	})
	return out
}

// mdnsNotifee connects to peers discovered on the local network.
type mdnsNotifee struct {
	h   host.Host
	ctx context.Context
}

func (m *mdnsNotifee) HandlePeerFound(pi peer.AddrInfo) {
	// Keep the discovered same-LAN addrs for an hour (Connect alone records them with a temp TTL of minutes):
	// the link maintainer's force-direct upgrade dials need them to still be there when a relayed/unproven link
	// nudges an upgrade — this is what turns "two PCs on the same LAN" into a direct local connection instead of
	// a hairpin punch.
	m.h.Peerstore().AddAddrs(pi.ID, pi.Addrs, time.Hour)
	ctx, cancel := context.WithTimeout(m.ctx, 15*time.Second)
	defer cancel()
	_ = m.h.Connect(ctx, pi)
}

// reprovideKeys streams the recursively-pinned roots for the reprovider to announce.
func (n *node) reprovideKeys(ctx context.Context) (<-chan cid.Cid, error) {
	ch := make(chan cid.Cid)
	safeGo("node.reprovideKeys", func() {
		defer close(ch)
		for sp := range n.pinner.RecursiveKeys(ctx, false) {
			if sp.Err != nil {
				return
			}
			select {
			case ch <- sp.Pin.Key:
			case <-ctx.Done():
				return
			}
		}
	})
	return ch, nil
}

// bootstrap connects to the public bootstrap peers then bootstraps the DHT routing table.
func (n *node) bootstrap() {
	var wg sync.WaitGroup
	for _, pi := range dht.GetDefaultBootstrapPeerAddrInfos() {
		wg.Add(1)
		safeGo("node.bootstrapDial", func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(n.ctx, 30*time.Second)
			defer cancel()
			_ = n.host.Connect(ctx, pi)
		})
	}
	wg.Wait()
	_ = n.dht.Bootstrap(n.ctx)
}

// peerCount is the number of currently-connected swarm peers.
func (n *node) peerCount() int {
	if n.host == nil {
		return 0
	}
	return len(n.host.Network().Peers())
}

// bandwidth returns the current global receive/send rates in bytes per second (0,0 when offline). Backed by the
// libp2p BandwidthReporter — a rolling rate over all streams, i.e. the whole node's aggregate down/up throughput.
func (n *node) bandwidth() (rateIn float64, rateOut float64) {
	if n.bwc == nil {
		return 0, 0
	}
	s := n.bwc.GetBandwidthTotals()
	return s.RateIn, s.RateOut
}

// providerCount counts distinct peers announcing a CID via the DHT, bounded by timeoutMs. -1 if offline.
func (n *node) providerCount(c cid.Cid, timeoutMs int) int {
	if n.dht == nil {
		return -1
	}
	ctx, cancel := context.WithTimeout(n.ctx, time.Duration(timeoutMs)*time.Millisecond)
	defer cancel()
	seen := map[peer.ID]bool{}
	for pi := range n.dht.FindProvidersAsync(ctx, c, 0) {
		seen[pi.ID] = true
	}
	return len(seen)
}
