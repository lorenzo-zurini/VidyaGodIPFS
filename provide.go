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
	dsq "github.com/ipfs/go-datastore/query"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	dhtprov "github.com/libp2p/go-libp2p-kad-dht/provider"
	stats "github.com/libp2p/go-libp2p-kad-dht/provider/stats"
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

// keyProvider: what the node uses of the sweeping provider (an interface: a test stands in for the DHT).
type keyProvider interface {
	StartProviding(force bool, keys ...mh.Multihash) error
	StopProviding(keys ...mh.Multihash) error
	Stats(ctx context.Context) (stats.Stats, error)
	Close() error
}

// provideState: keys the provider cannot take yet (it is offline: not yet bootstrapped, or disconnected past its
// offline delay), kept and handed over once it is online.
type provideState struct {
	mu        sync.Mutex
	pending   map[string]pendingKey
	started   bool
	ready     bool      // the provider was online at checkedAt
	checkedAt time.Time // when ready was last read (Stats counts the keystore: not on every call)
}

// pendingKey: a key to hand over; force when the provider may have filed it already without announcing it.
type pendingKey struct {
	h     mh.Multihash
	force bool
}

// refileNS: keys the provider may have filed without announcing, kept across restarts until announced (force).
var refileNS = dstore.NewKey("/vg/provide-refile")

// providerReadyTTL: how long a readiness reading stands. A var so tests can make every call re-read it.
var providerReadyTTL = 5 * time.Second

// providerReady reports whether the provider would queue keys now. StartProviding never says: offline it files a key
// in its keystore and returns nil WITHOUT queueing it — and a later hand-over of that key is a no-op (no longer new),
// so it waited for its region's next reprovide, up to 22 h (a CLI --publish-cid printed a CID nobody could find).
// Offline is exactly "no network prefix measured yet", which Stats reports as a negative average prefix length.
func (n *node) providerReady(sp keyProvider) bool {
	n.provideSt.mu.Lock()
	if !n.provideSt.checkedAt.IsZero() && time.Since(n.provideSt.checkedAt) < providerReadyTTL {
		r := n.provideSt.ready
		n.provideSt.mu.Unlock()
		return r
	}
	n.provideSt.mu.Unlock()
	return n.providerReadyNow(sp)
}

// providerReadyNow reads readiness afresh (and stores the reading for providerReady).
func (n *node) providerReadyNow(sp keyProvider) bool {
	st, err := sp.Stats(n.ctx)
	ready := err == nil && !st.Closed && st.Schedule.AvgPrefixLength >= 0
	n.provideSt.mu.Lock()
	n.provideSt.ready, n.provideSt.checkedAt = ready, time.Now()
	n.provideSt.mu.Unlock()
	return ready
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
	keys := make([]pendingKey, 0, len(cids))
	for _, c := range cids {
		keys = append(keys, pendingKey{h: c.Hash()})
	}
	n.offerProvides(keys)
}

// offerProvides adds keys to the pending set (a force mark sticks), hands them over if it can, and starts the retry
// loop once.
func (n *node) offerProvides(keys []pendingKey) {
	n.provideSt.mu.Lock()
	if n.provideSt.pending == nil {
		n.provideSt.pending = map[string]pendingKey{}
	}
	for _, k := range keys {
		k.force = k.force || n.provideSt.pending[string(k.h)].force
		n.provideSt.pending[string(k.h)] = k
	}
	start := !n.provideSt.started
	n.provideSt.started = true
	n.provideSt.mu.Unlock()
	n.flushProvides()
	if start {
		safeGo("node.provideRetry", func() { n.provideRetryLoop() })
	}
}

// restoreRefiles offers again, forced, the keys a previous run's provider may have filed without announcing.
func (n *node) restoreRefiles() {
	if n.ds == nil {
		return
	}
	res, err := n.ds.Query(n.ctx, dsq.Query{Prefix: refileNS.String(), KeysOnly: true})
	if err != nil {
		return
	}
	var keys []pendingKey
	for r := range res.Next() {
		if r.Error != nil {
			break
		}
		if h, err := mh.FromB58String(dstore.RawKey(r.Key).BaseNamespace()); err == nil {
			keys = append(keys, pendingKey{h: h, force: true})
		}
	}
	_ = res.Close()
	if len(keys) > 0 {
		n.offerProvides(keys)
	}
}

// setRefiled records (or clears) keys the provider may have filed without announcing, across restarts.
func (n *node) setRefiled(keys []mh.Multihash, filed bool) {
	if n.ds == nil || len(keys) == 0 {
		return
	}
	b, err := n.ds.Batch(n.ctx)
	if err != nil {
		return
	}
	for _, h := range keys {
		k := refileNS.ChildString(h.B58String())
		if filed {
			_ = b.Put(n.ctx, k, nil)
		} else {
			_ = b.Delete(n.ctx, k)
		}
	}
	_ = b.Commit(n.ctx)
}

// flushProvides hands the pending keys to the provider once it is online; until then they stay pending.
func (n *node) flushProvides() {
	sp := n.provider
	if sp == nil {
		return
	}
	n.provideSt.mu.Lock()
	var fresh, forced []mh.Multihash
	for _, k := range n.provideSt.pending {
		if k.force {
			forced = append(forced, k.h)
		} else {
			fresh = append(fresh, k.h)
		}
	}
	n.provideSt.mu.Unlock()
	if len(fresh)+len(forced) == 0 || !n.providerReady(sp) {
		return // nothing to hand over, or the provider would only file them — the retry loop offers them again
	}
	if len(fresh) > 0 && sp.StartProviding(false, fresh...) != nil {
		return // closed
	}
	if len(forced) > 0 && sp.StartProviding(true, forced...) != nil {
		return
	}
	all := append(fresh, forced...)
	// The readiness reading may be seconds old: a provider that went offline since files the keys WITHOUT announcing
	// them (and says nothing), and a filed key is "not new" ever after — it waited for its region's reprovide, up to
	// 22 h. Read it afresh: offline now, they stay ours, to be handed over again FORCED, remembered across restarts.
	if !n.providerReadyNow(sp) {
		n.provideSt.mu.Lock()
		for _, h := range all {
			n.provideSt.pending[string(h)] = pendingKey{h: h, force: true}
		}
		n.provideSt.mu.Unlock()
		n.setRefiled(all, true)
		return
	}
	n.provideSt.mu.Lock()
	for _, h := range all {
		delete(n.provideSt.pending, string(h))
	}
	n.provideSt.mu.Unlock()
	n.setRefiled(forced, false)
}

func (n *node) provideRetryLoop() {
	t := time.NewTicker(5 * time.Second) // cheap: nothing happens while nothing is pending
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
		n.setRefiled(keys, false)
		if err := sp.StopProviding(keys...); err != nil {
			fmt.Fprintf(os.Stderr, "[provide] stop providing %d key(s): %v\n", len(keys), err)
		}
	}
}
