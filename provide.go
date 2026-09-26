package main

// provide.go — making what we hold findable on the DHT, through the ONE network queue (netq.go).
//
// A provider record lives 48 h on the DHT and points at our peer ID, not our ports: it survives a restart. So nothing
// needs re-announcing when the node starts — only keys never announced, and records coming due. The old path re-walked
// the DHT for every pinned CID (~20 900 on the seeder) on every start AND every 3 hours, 12 walks at a time plus the
// boxo provider's own workers, outside any bound: one walk per CID, each dozens of new flows through the home router.
//
// The sweeping provider (go-libp2p-kad-dht/provider, Kubo's current provide system) keeps the provided keys and the
// schedule in our datastore across restarts, reprovides every 22 h, and does it by keyspace REGION — one walk to a
// region's closest peers carries the records of every key that falls in it, so ~20 k keys cost a few hundred walks a
// day. Its walks and record sends go through netq at background priority (queuedRouter / queuedSender): announcing
// uses the network only when no fetch wants the slot.

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	cid "github.com/ipfs/go-cid"
	dstore "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	dhtprov "github.com/libp2p/go-libp2p-kad-dht/provider"
	network "github.com/libp2p/go-libp2p/core/network"
	peer "github.com/libp2p/go-libp2p/core/peer"
	mh "github.com/multiformats/go-multihash"
)

// provideReprovideInterval matches the Amino DHT: records live 48 h, renewed well inside that.
const provideReprovideInterval = 22 * time.Hour

// queuedRouter: the sweeping provider's DHT walks, each one background slot of netq for its duration.
type queuedRouter struct{ kad dhtprov.KadClosestPeersRouter }

func (r queuedRouter) GetClosestPeers(ctx context.Context, key string) ([]peer.ID, error) {
	release, ok := netq.acquire(ctx, false)
	if !ok {
		return nil, ctx.Err()
	}
	defer release()
	netStats.provideWalks.Add(1)
	return r.kad.GetClosestPeers(ctx, key)
}

// queuedSender: the sweeping provider's record sends. It sends one record per key, in sequence, over one connection
// per peer; what opens a new flow through the router is the FIRST send to a peer we are not connected to (the dial),
// so exactly those take a background slot of netq. The rest ride the open connection at full speed.
type queuedSender struct {
	inner     pb.MessageSender
	connected func(peer.ID) bool
}

func (s queuedSender) gate(ctx context.Context, p peer.ID) (release func(), err error) {
	if s.connected != nil && s.connected(p) {
		return func() {}, nil
	}
	release, ok := netq.acquire(ctx, false)
	if !ok {
		return nil, ctx.Err()
	}
	netStats.provideDials.Add(1)
	return release, nil
}

func (s queuedSender) SendRequest(ctx context.Context, p peer.ID, m *pb.Message) (*pb.Message, error) {
	release, err := s.gate(ctx, p)
	if err != nil {
		return nil, err
	}
	defer release()
	netStats.provides.Add(1)
	return s.inner.SendRequest(ctx, p, m)
}

func (s queuedSender) SendMessage(ctx context.Context, p peer.ID, m *pb.Message) error {
	release, err := s.gate(ctx, p)
	if err != nil {
		return err
	}
	defer release()
	netStats.provides.Add(1)
	return s.inner.SendMessage(ctx, p, m)
}

// newSweepingProvider wires the provider onto our DHT, its walks and sends queued, its keys + schedule persisted
// under /provide in the node datastore (resumed across restarts).
func newSweepingProvider(ctx context.Context, kad *dht.IpfsDHT, ds dstore.Batching) (*dhtprov.SweepingProvider, error) {
	return dhtprov.New(
		dhtprov.WithHost(kad.Host()),
		dhtprov.WithReplicationFactor(kad.BucketSize()),
		dhtprov.WithSelfAddrs(kad.FilteredAddrs),
		dhtprov.WithRouter(queuedRouter{kad}),
		dhtprov.WithMessageSender(queuedSender{inner: kad.MessageSender(), connected: func(p peer.ID) bool {
			return kad.Host().Network().Connectedness(p) == network.Connected
		}}),
		dhtprov.WithAddLocalRecord(func(h mh.Multihash) error {
			return kad.Provide(ctx, cid.NewCidV1(cid.Raw, h), false)
		}),
		dhtprov.WithDatastore(namespace.Wrap(ds, dstore.NewKey("/provide"))),
		dhtprov.WithResumeCycle(true),
		dhtprov.WithReprovideInterval(provideReprovideInterval),
	)
}

// provideState: keys handed to us before the provider could take them (offline, not yet bootstrapped), retried.
type provideState struct {
	mu      sync.Mutex
	pending map[string]mh.Multihash
	started bool
}

// startProviding hands keys to the sweeping provider: announced once if never announced, then reprovided on its
// schedule; a key it already holds costs nothing. Keys it cannot take yet (offline) are kept and retried.
func (n *node) startProviding(cids ...cid.Cid) {
	if len(cids) == 0 {
		return
	}
	n.seedMu.Lock()
	for _, c := range cids {
		if n.seedDone != nil {
			n.seedDone[c.String()] = struct{}{}
		}
	}
	n.seedMu.Unlock()
	n.provideSt.mu.Lock()
	if n.provideSt.pending == nil {
		n.provideSt.pending = map[string]mh.Multihash{}
	}
	for _, c := range cids {
		n.provideSt.pending[string(c.Hash())] = c.Hash()
	}
	start := !n.provideSt.started
	n.provideSt.started = true
	n.provideSt.mu.Unlock()
	n.flushProvides()
	if start {
		safeGo("node.provideRetry", func() { n.provideRetryLoop() })
	}
}

// flushProvides offers the pending keys to the provider; they stay pending if it refuses (offline).
func (n *node) flushProvides() {
	sp := n.provider
	if sp == nil {
		return
	}
	n.provideSt.mu.Lock()
	keys := make([]mh.Multihash, 0, len(n.provideSt.pending))
	for _, k := range n.provideSt.pending {
		keys = append(keys, k)
	}
	n.provideSt.mu.Unlock()
	if len(keys) == 0 {
		return
	}
	if err := sp.StartProviding(false, keys...); err != nil {
		return // offline / not bootstrapped yet — the retry loop offers them again
	}
	n.provideSt.mu.Lock()
	for _, k := range keys {
		delete(n.provideSt.pending, string(k))
	}
	n.provideSt.mu.Unlock()
}

func (n *node) provideRetryLoop() {
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-t.C:
			n.flushProvides()
		}
	}
}

// stopProviding: a key we no longer hold is dropped from the schedule (never reprovided again).
func (n *node) stopProviding(cids ...cid.Cid) {
	n.provideSt.mu.Lock()
	for _, c := range cids {
		delete(n.provideSt.pending, string(c.Hash()))
	}
	n.provideSt.mu.Unlock()
	if sp := n.provider; sp != nil {
		keys := make([]mh.Multihash, 0, len(cids))
		for _, c := range cids {
			keys = append(keys, c.Hash())
		}
		if err := sp.StopProviding(keys...); err != nil {
			fmt.Fprintf(os.Stderr, "[provide] stop providing %d key(s): %v\n", len(keys), err)
		}
	}
}
