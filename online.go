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
	peer "github.com/libp2p/go-libp2p/core/peer"
	routing "github.com/libp2p/go-libp2p/core/routing"
	mdns "github.com/libp2p/go-libp2p/p2p/discovery/mdns"
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
		go func(idx int, rr routing.ContentDiscovery) {
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
		}(i, r)
	}
	go func() { wg.Wait(); close(out) }()
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

	h, err := libp2p.New(
		libp2p.Identity(priv),
		libp2p.ListenAddrStrings(
			"/ip4/0.0.0.0/tcp/0",
			"/ip4/0.0.0.0/udp/0/quic-v1",
			"/ip6/::/tcp/0",
			"/ip6/::/udp/0/quic-v1",
		),
		libp2p.NATPortMap(),
		libp2p.EnableNATService(),
		libp2p.EnableHolePunching(),
		// AutoRelay: when this node is behind NAT (unreachable directly), reserve slots on relays and advertise
		// relay addresses so peers on OTHER networks can still reach it (hole-punching is coordinated via the relay).
		// Candidates come from the DHT routing table (those that support circuit-relay-v2 get used). Same-LAN peers
		// don't need this — that's mDNS.
		libp2p.EnableAutoRelayWithPeerSource(n.relayPeerSource),
	)
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
	if hc, herr := routinghttp.New("https://delegated-ipfs.dev"); herr == nil {
		finder = combinedFinder{routers: []routing.ContentDiscovery{kad, routinghttpcr.NewContentRoutingClient(hc)}}
	}
	bswap := bitswap.New(n.ctx, bsn, finder, n.fstore)

	n.host = h
	n.dht = kad
	n.exchange = bswap
	fmt.Fprintf(os.Stderr, "[node] peerID=%s\n", h.ID())
	for _, a := range h.Addrs() {
		fmt.Fprintf(os.Stderr, "[node] listen=%s/p2p/%s\n", a, h.ID())
	}
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

	n.online = true
	go n.bootstrap()
	return nil
}

// relayPeerSource feeds AutoRelay with candidate relays from the DHT routing table — public, well-connected peers;
// those that support circuit-relay-v2 get used. Called by AutoRelay at runtime (n.dht/n.host are set by then).
func (n *node) relayPeerSource(ctx context.Context, num int) <-chan peer.AddrInfo {
	out := make(chan peer.AddrInfo)
	go func() {
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
	}()
	return out
}

// mdnsNotifee connects to peers discovered on the local network.
type mdnsNotifee struct {
	h   host.Host
	ctx context.Context
}

func (m *mdnsNotifee) HandlePeerFound(pi peer.AddrInfo) {
	ctx, cancel := context.WithTimeout(m.ctx, 15*time.Second)
	defer cancel()
	_ = m.h.Connect(ctx, pi)
}

// reprovideKeys streams the recursively-pinned roots for the reprovider to announce.
func (n *node) reprovideKeys(ctx context.Context) (<-chan cid.Cid, error) {
	ch := make(chan cid.Cid)
	go func() {
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
	}()
	return ch, nil
}

// bootstrap connects to the public bootstrap peers then bootstraps the DHT routing table.
func (n *node) bootstrap() {
	var wg sync.WaitGroup
	for _, pi := range dht.GetDefaultBootstrapPeerAddrInfos() {
		wg.Add(1)
		go func(pi peer.AddrInfo) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(n.ctx, 30*time.Second)
			defer cancel()
			_ = n.host.Connect(ctx, pi)
		}(pi)
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
