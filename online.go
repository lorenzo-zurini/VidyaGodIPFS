package main

// online.go — bring the node onto the public IPFS network (M3): a libp2p host + Kademlia DHT + online bitswap +
// a reprovider that announces our pinned content to the DHT. Best-effort: if anything here fails (e.g. no network),
// the node keeps working offline.

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"time"

	bitswap "github.com/ipfs/boxo/bitswap"
	bsnet "github.com/ipfs/boxo/bitswap/network/bsnet"
	blockservice "github.com/ipfs/boxo/blockservice"
	merkledag "github.com/ipfs/boxo/ipld/merkledag"
	provider "github.com/ipfs/boxo/provider"
	cid "github.com/ipfs/go-cid"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	libp2p "github.com/libp2p/go-libp2p"
	crypto "github.com/libp2p/go-libp2p/core/crypto"
	peer "github.com/libp2p/go-libp2p/core/peer"
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

	// Online bitswap, using the DHT to find providers; swap the DAG service over to it.
	bsn := bsnet.NewFromIpfsHost(h)
	bswap := bitswap.New(n.ctx, bsn, kad, n.fstore)

	n.host = h
	n.dht = kad
	n.exchange = bswap
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

	n.online = true
	go n.bootstrap()
	return nil
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
