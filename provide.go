package main

// provide.go — making what we hold findable on the DHT, through the ONE network queue (netq.go).
//
// A provider record lives 48 h on the DHT and points at our peer ID, not our ports: it survives a restart. So nothing
// needs re-announcing when the node starts — only keys never announced, and records coming due. The old path re-walked
// the DHT for every pinned CID (~20 900 on the seeder) on every start AND every 3 hours, 12 walks at a time plus the
// boxo provider's own workers, outside any bound: one walk per CID, each dozens of new flows through the home router.
//
// The sweeping provider (go-libp2p-kad-dht/provider, Kubo's current provide system) keeps the provided keys (in a
// keystore of ours: its default is an in-memory map) and the schedule in our datastore across restarts, reprovides every 22 h, and does it by keyspace REGION — one walk to a
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
	keystore "github.com/libp2p/go-libp2p-kad-dht/provider/keystore"
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
// onOnline / onOffline: told when it comes online and when it goes Offline (its provide queue is then dropped).
func newSweepingProvider(ctx context.Context, kad *dht.IpfsDHT, ds dstore.Batching, onOnline, onOffline func()) (*sweepingProvider, error) {
	keys, err := keystore.NewKeystore(namespace.Wrap(ds, provideKeystoreNS))
	if err != nil {
		return nil, err
	}
	sp, err := dhtprov.New(
		dhtprov.WithKeystore(keys),
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
		dhtprov.WithConnectivityCallbacks(onOnline, nil, onOffline),
	)
	if err != nil {
		_ = keys.Close()
		return nil, err
	}
	return &sweepingProvider{SweepingProvider: sp, keys: keys}, nil
}

// provideKeystoreNS: the provided keys, beside the provider's own records (/provide). The library's default keystore
// is an in-memory map: every start found it empty, so every key handed over was "new" and the whole seed set (~9 k
// keys) was announced again on every start — a just-published root queued behind all of it (by region, never in our
// order), the queue never drained within a session, and nothing handed over was ever confirmed (a pin root unfindable
// to Pinata for hours).
var provideKeystoreNS = dstore.NewKey("/provide-keystore")

// sweepingProvider: the provider and its keystore, closed together (the provider closes only a keystore it made).
type sweepingProvider struct {
	*dhtprov.SweepingProvider
	keys keystore.Keystore
}

func (p *sweepingProvider) Close() error {
	err := p.SweepingProvider.Close()
	if kerr := p.keys.Close(); err == nil {
		err = kerr
	}
	return err
}

// StoredKeys: every key the keystore holds (an empty prefix matches all).
func (p *sweepingProvider) StoredKeys(ctx context.Context) ([]mh.Multihash, error) {
	return p.keys.Get(ctx, "")
}

// keyLister: a provider whose keystore outlives the process lists what it holds.
type keyLister interface {
	StoredKeys(ctx context.Context) ([]mh.Multihash, error)
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
	mu          sync.Mutex
	pending     map[string]pendingKey
	unconfirmed map[string]handedKey    // handed over, not yet known announced (confirmUnconfirmed)
	handed      map[string]struct{}     // every key the app handed over in this run (startProviding)
	leftover    map[string]mh.Multihash // the keystore as this run found it (listed once): an earlier run's keys
	listed      bool                    // the keystore was listed (by the run's first provider)
	seeded      bool                    // the app's full list was handed over (runSeedAnnounce)
	pruneWith   keyProvider             // the current provider, for dropLeftovers (n.provider is written unlocked)
	wentOffline bool                    // the provider went Offline (its queue dropped) and is not back yet
	lostBefore  time.Time               // when it came back from Offline: what was handed over before may be lost
	started     bool
	ready       bool      // the provider was online at checkedAt
	checkedAt   time.Time // when ready was last read (Stats counts the keystore: not on every call)
}

// handedKey: a key handed over, and when — the provider going Offline after that dropped it from the queue.
type handedKey struct {
	h  mh.Multihash
	at time.Time
}

// pendingKey: a key to hand over; force when the provider may have filed it already without announcing it.
type pendingKey struct {
	h     mh.Multihash
	force bool
}

// The provider files every key it is handed in its keystore at once, but announces it only from its provide queue —
// and that queue is dropped whole when the provider goes offline (and with the process). A filed key is "not new"
// ever after — across restarts too, the keystore being ours — so handed over again it is ignored, and waited for its
// region's reprovide, up to 22 h. So a handed-over key stays UNCONFIRMED until the provider's queues have drained
// while online, and it did not go Offline since the key was handed over; an offline reading, or an Offline spell since
// (between two readings), first sends it back, to be handed over again FORCED. refileNS keeps the unconfirmed across restarts (written BEFORE the hand-over:
// a crash between the two must not leave a filed key nobody re-offers), announcedNS the keys known announced (a startup
// re-offer of those is a no-op, never tracked: a blip then must not force a full re-announce).
var (
	refileNS    = dstore.NewKey("/vg/provide-refile")
	announcedNS = dstore.NewKey("/vg/provide-announced")
)

// providerReadyTTL: how long a readiness reading stands. A var so tests can make every call re-read it.
var providerReadyTTL = 5 * time.Second

// providerReady reports whether the provider would queue keys now. StartProviding never says: offline it files a key
// in its keystore and returns nil WITHOUT queueing it — and a later hand-over of that key is a no-op (no longer new),
// so it waited for its region's next reprovide, up to 22 h (a CLI --publish-cid printed a CID nobody could find).
// Offline is exactly "no network prefix measured yet", which Stats reports as a negative average prefix length; and a
// provider not connected right now (the prefix it measured persists across restarts) queues keys it cannot send yet,
// handed over again once the change of connectivity is seen (confirmUnconfirmed) — so ready is both.
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
	return n.noteReadiness(err == nil && !st.Closed && online(st))
}

func online(st stats.Stats) bool {
	return st.Connectivity.Status == "online" && st.Schedule.AvgPrefixLength >= 0
}

// noteOffline / noteOnline: the provider's own connectivity callbacks. Only going Offline (disconnected past its
// offline delay) drops the provide queue — a short disconnection keeps it — so only that makes what was handed over
// before suspect: from the moment it is back.
func (n *node) noteOffline() {
	n.provideSt.mu.Lock()
	n.provideSt.wentOffline = true
	n.provideSt.mu.Unlock()
}

func (n *node) noteOnline() {
	n.provideSt.mu.Lock()
	if n.provideSt.wentOffline {
		n.provideSt.wentOffline, n.provideSt.lostBefore = false, time.Now()
	}
	n.provideSt.mu.Unlock()
}

// dropProvider: the provider is closed (a failed goOnline attempt); the leftovers wait for the next one.
func (n *node) dropProvider() {
	n.provideSt.mu.Lock()
	n.provideSt.pruneWith = nil
	n.provideSt.mu.Unlock()
}

func (n *node) noteReadiness(ready bool) bool {
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
	n.provideSt.mu.Lock()
	if n.provideSt.handed == nil {
		n.provideSt.handed = map[string]struct{}{}
	}
	for _, c := range cids {
		keys = append(keys, pendingKey{h: c.Hash()})
		n.provideSt.handed[string(c.Hash())] = struct{}{} // wanted by this run: never a leftover
	}
	n.provideSt.mu.Unlock()
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

// useProvider installs the node's provider, lists once per run what its keystore kept from earlier runs, and offers
// again, forced, what a previous run left unconfirmed. Only the run's first provider lists: a later one (goOnline
// retried) holds what this run handed the first one too.
func (n *node) useProvider(sp keyProvider) {
	n.provideSt.mu.Lock()
	list := !n.provideSt.listed
	n.provideSt.listed = true
	n.provideSt.pruneWith = sp
	n.provideSt.mu.Unlock()
	if kl, ok := sp.(keyLister); ok && list {
		if keys, err := kl.StoredKeys(n.ctx); err != nil {
			fmt.Fprintf(os.Stderr, "[provide] keystore not listed, nothing an earlier run left is dropped this run: %v\n", err)
		} else {
			left := make(map[string]mh.Multihash, len(keys))
			for _, h := range keys {
				left[string(h)] = h
			}
			n.provideSt.mu.Lock()
			n.provideSt.leftover = left
			n.provideSt.mu.Unlock()
		}
	}
	n.provider = sp
	n.restoreRefiles()
	n.dropLeftovers() // the app's list may have come before the provider did
}

// dropLeftovers stops providing what an earlier run's keystore holds and this run has not handed over — once, when
// both the app's first full hand-over (runSeedAnnounce: what it holds, it listed) and the keystore listing are in,
// whichever comes second. A key the app hands over while they are being stopped is offered again after: stopped, it is
// new to the keystore, so announced. Returns how many it stopped.
func (n *node) dropLeftovers() int {
	n.provideSt.mu.Lock()
	sp := n.provideSt.pruneWith
	if !n.provideSt.seeded || sp == nil || n.provideSt.leftover == nil {
		n.provideSt.mu.Unlock()
		return 0
	}
	var stale []mh.Multihash
	for k, h := range n.provideSt.leftover {
		if _, wanted := n.provideSt.handed[k]; !wanted {
			stale = append(stale, h)
			delete(n.provideSt.pending, k) // an earlier run's refile, offered again before the list was in
			delete(n.provideSt.unconfirmed, k)
		}
	}
	n.provideSt.leftover = nil
	n.provideSt.mu.Unlock()
	if len(stale) == 0 {
		return 0
	}
	n.setMarks(refileNS, stale, false)
	n.setMarks(announcedNS, stale, false)
	if err := sp.StopProviding(stale...); err != nil {
		fmt.Fprintf(os.Stderr, "[provide] stop providing %d key(s) an earlier run left: %v\n", len(stale), err)
	}
	var again []pendingKey
	n.provideSt.mu.Lock()
	for _, h := range stale {
		if _, wanted := n.provideSt.handed[string(h)]; wanted {
			again = append(again, pendingKey{h: h, force: true})
		}
	}
	n.provideSt.mu.Unlock()
	fmt.Fprintf(os.Stderr, "[provide] %d key(s) an earlier run filed and this one does not hold: no longer provided\n", len(stale)-len(again))
	if len(again) > 0 {
		n.offerProvides(again)
	}
	return len(stale) - len(again)
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

// setMarks puts (or deletes) keys' records under ns, across restarts.
func (n *node) setMarks(ns dstore.Key, keys []mh.Multihash, on bool) {
	if n.ds == nil || len(keys) == 0 {
		return
	}
	b, err := n.ds.Batch(n.ctx)
	if err == nil {
		for _, h := range keys {
			k := ns.ChildString(h.B58String())
			if on {
				err = b.Put(n.ctx, k, nil)
			} else {
				err = b.Delete(n.ctx, k)
			}
			if err != nil {
				break
			}
		}
	}
	if err == nil {
		err = b.Commit(n.ctx)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "[provide] %s: %d record(s) not written: %v\n", ns, len(keys), err)
	}
}

// announced: the key is known announced by an earlier hand-over (announcedNS).
func (n *node) announced(h mh.Multihash) bool {
	if n.ds == nil {
		return false
	}
	has, err := n.ds.Has(n.ctx, announcedNS.ChildString(h.B58String()))
	return err == nil && has
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
	// Tracked — unconfirmed until announced (see refileNS) — except a fresh key an earlier run announced (a no-op
	// re-offer). Its refile record goes first: the provider files the key durably on hand-over.
	var track []mh.Multihash
	for _, h := range fresh {
		if !n.announced(h) {
			track = append(track, h)
		}
	}
	track = append(track, forced...)
	n.setMarks(refileNS, track, true)
	at := time.Now()
	if len(fresh) > 0 && sp.StartProviding(false, fresh...) != nil {
		return // closed: the records stay, the next run offers them forced
	}
	if len(forced) > 0 && sp.StartProviding(true, forced...) != nil {
		return
	}
	// A key stopped meanwhile (no longer pending) is not tracked, and its record goes again (stopProviding may have
	// cleared it before ours was written).
	var gone []mh.Multihash
	n.provideSt.mu.Lock()
	if n.provideSt.unconfirmed == nil {
		n.provideSt.unconfirmed = map[string]handedKey{}
	}
	for _, h := range track {
		if _, ok := n.provideSt.pending[string(h)]; ok {
			n.provideSt.unconfirmed[string(h)] = handedKey{h: h, at: at}
		} else if _, other := n.provideSt.unconfirmed[string(h)]; !other {
			gone = append(gone, h) // not taken by a flush running beside this one either
		}
	}
	for _, h := range append(fresh, forced...) {
		delete(n.provideSt.pending, string(h))
	}
	n.provideSt.mu.Unlock()
	n.setMarks(refileNS, gone, false)
}

// confirmUnconfirmed settles the handed-over keys: announced once the provider's queues have drained while it is not
// offline (disconnected keeps the queue), and it did not go Offline since they were handed over; sent back (forced) if
// it is offline — its queue, with them in it, is gone — or went Offline since, between two readings.
func (n *node) confirmUnconfirmed() {
	sp := n.provider
	if sp == nil {
		return
	}
	n.provideSt.mu.Lock()
	snap := make([]handedKey, 0, len(n.provideSt.unconfirmed)) // read BEFORE the stats: a key handed over after
	for _, k := range n.provideSt.unconfirmed {                // them may still be queued
		snap = append(snap, k)
	}
	n.provideSt.mu.Unlock()
	if len(snap) == 0 {
		return
	}
	st, err := sp.Stats(n.ctx)
	if err != nil || st.Closed {
		return
	}
	n.noteReadiness(online(st))
	kept := st.Schedule.AvgPrefixLength >= 0 // not Offline: the queue stands
	drained := st.Queues.PendingKeyProvides == 0 && st.Queues.PendingRegionProvides == 0 &&
		st.Operations.Ongoing.KeyProvides == 0 && st.Operations.Ongoing.RegionProvides == 0
	if kept && !drained {
		return
	}
	var settled []mh.Multihash
	n.provideSt.mu.Lock()
	lost := n.provideSt.lostBefore
	for _, k := range snap {
		if _, ok := n.provideSt.unconfirmed[string(k.h)]; !ok {
			continue // stopped meanwhile
		}
		delete(n.provideSt.unconfirmed, string(k.h))
		if kept && k.at.After(lost) {
			settled = append(settled, k.h)
		} else {
			n.provideSt.pending[string(k.h)] = pendingKey{h: k.h, force: true} // its refile record stays
		}
	}
	n.provideSt.mu.Unlock()
	n.setMarks(announcedNS, settled, true)
	n.setMarks(refileNS, settled, false)
}

func (n *node) provideRetryLoop() {
	t := time.NewTicker(5 * time.Second) // cheap: nothing happens while nothing is pending
	defer t.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-t.C:
			n.confirmUnconfirmed()
			n.flushProvides()
		}
	}
}

// stopProviding: a key we no longer hold is dropped from the schedule (never reprovided again).
func (n *node) stopProviding(cids ...cid.Cid) {
	keys := make([]mh.Multihash, 0, len(cids))
	n.provideSt.mu.Lock()
	for _, c := range cids {
		delete(n.provideSt.pending, string(c.Hash()))
		delete(n.provideSt.unconfirmed, string(c.Hash()))
		keys = append(keys, c.Hash())
	}
	n.provideSt.mu.Unlock()
	n.setMarks(refileNS, keys, false) // online or not: a next run must not announce what we stopped holding
	n.setMarks(announcedNS, keys, false)
	if sp := n.provider; sp != nil {
		if err := sp.StopProviding(keys...); err != nil {
			fmt.Fprintf(os.Stderr, "[provide] stop providing %d key(s): %v\n", len(keys), err)
		}
	}
}
