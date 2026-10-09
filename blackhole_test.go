package main

// blackhole_test.go — a provider that says it has the content and never sends it (a public pinning node that
// advertises a file and does not answer: the replication's Dino Crisis stall) must not strand the wants a session
// happens to route to it while a peer that DOES serve is connected.

import (
	"context"
	"io"
	"testing"
	"time"

	"crypto/rand"
	bitswap "github.com/ipfs/boxo/bitswap"
	bsmsg "github.com/ipfs/boxo/bitswap/message"
	pb "github.com/ipfs/boxo/bitswap/message/pb"
	bsnetwork "github.com/ipfs/boxo/bitswap/network"
	bsnet "github.com/ipfs/boxo/bitswap/network/bsnet"
	blockservice "github.com/ipfs/boxo/blockservice"
	blockstore "github.com/ipfs/boxo/blockstore"
	offline "github.com/ipfs/boxo/exchange/offline"
	merkledag "github.com/ipfs/boxo/ipld/merkledag"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	datastore "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	network "github.com/libp2p/go-libp2p/core/network"
	peer "github.com/libp2p/go-libp2p/core/peer"
	protocol "github.com/libp2p/go-libp2p/core/protocol"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"sync"
	"sync/atomic"
)

// fixedFinder names the same providers for every CID.
type fixedFinder []peer.AddrInfo

func (f fixedFinder) FindProvidersAsync(ctx context.Context, _ cid.Cid, _ int) <-chan peer.AddrInfo {
	ch := make(chan peer.AddrInfo, len(f))
	for _, p := range f {
		ch <- p
	}
	close(ch)
	return ch
}

type blackholeRig struct {
	sess   *blockservice.Session
	leaves []cid.Cid
	stop   func()
}

// Kinds of other provider.
const (
	providerSilent    = iota // speaks bitswap, never answers
	providerNoBitswap        // does not speak bitswap
	providerPromiser         // answers HAVE for everything the seeder has, never delivers (bitswap.pinata.cloud)
)

// promiseStore says it has what inner has, and never hands a block over.
type promiseStore struct{ blockstore.Blockstore }

func (s promiseStore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// hiccupStore hands nothing over until its deadline: a seeder busy elsewhere (a disk flush, a compaction).
type hiccupStore struct {
	blockstore.Blockstore
	until time.Time
}

func (s hiccupStore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	select {
	case <-time.After(time.Until(s.until)):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return s.Blockstore.Get(ctx, c)
}

// setupBlackhole: seeder A, provider H of the given kind, client B whose finder names H first for every CID; B's
// network is wrapped in the quarantine when quarantine is set. A hands nothing over for its first hiccup.
func setupBlackhole(t *testing.T, ctx context.Context, payloadSize int, kind int, quarantine bool, hiccup time.Duration) blackholeRig {
	t.Helper()
	mn, err := mocknet.FullMeshLinked(3)
	if err != nil {
		t.Fatal(err)
	}
	mn.SetLinkDefaults(mocknet.LinkOptions{Latency: 5 * time.Millisecond})
	hosts := mn.Hosts()
	hostA, hostH, hostB := hosts[0], hosts[1], hosts[2]

	bstoreA := blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore()))
	dservA := merkledag.NewDAGService(blockservice.New(bstoreA, offline.Exchange(bstoreA)))
	payload := make([]byte, payloadSize)
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}
	_, leaves := buildLeafDAG(t, dservA, payload)
	var serveA blockstore.Blockstore = bstoreA
	if hiccup > 0 {
		serveA = hiccupStore{bstoreA, time.Now().Add(hiccup)}
	}
	bswapA := bitswap.New(ctx, bsnet.NewFromIpfsHost(hostA), nilFinder{}, serveA, bitswapOptions(nil)...) // the seeder as configured

	var bswapH *bitswap.Bitswap
	switch kind {
	case providerSilent: // it accepts every stream and reads every message, and never answers
		for _, p := range []protocol.ID{bsnet.ProtocolBitswap, bsnet.ProtocolBitswapOneOne, bsnet.ProtocolBitswapOneZero, bsnet.ProtocolBitswapNoVers} {
			hostH.SetStreamHandler(p, func(s network.Stream) { _, _ = io.Copy(io.Discard, s) })
		}
	case providerPromiser:
		bswapH = bitswap.New(ctx, bsnet.NewFromIpfsHost(hostH), nilFinder{}, promiseStore{bstoreA}, bitswapOptions(nil)...)
	}

	bstoreB := blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore()))
	finder := fixedFinder{{ID: hostH.ID()}, {ID: hostA.ID()}}
	var netB bsnetwork.BitSwapNetwork = bsnet.NewFromIpfsHost(hostB)
	if quarantine {
		q := newQuarantineNet(netB)
		go q.run(ctx)
		netB = q
	}
	bswapB := bitswap.New(ctx, netB, finder, bstoreB, bitswapOptions(nil)...)
	if err := mn.ConnectAllButSelf(); err != nil {
		t.Fatal(err)
	}
	// A busy seeder: its first answer comes after the session's first idle tick (1 s), so the session asks the finder
	// and takes on the other provider — as the replication's receiver, busy with other files, did. A seeder that
	// hiccups answers first instead: the want-blocks go to it, and the other provider's HAVEs come after.
	slowA, slowB := hostA.ID(), hostB.ID()
	if hiccup > 0 {
		slowA = hostH.ID()
	}
	for _, l := range mn.LinksBetweenPeers(slowA, slowB) {
		l.SetOptions(mocknet.LinkOptions{Latency: 600 * time.Millisecond})
	}
	sess := blockservice.NewSession(ctx, blockservice.New(bstoreB, bswapB))
	return blackholeRig{sess: sess, leaves: leaves, stop: func() {
		bswapB.Close()
		if bswapH != nil {
			bswapH.Close()
		}
		bswapA.Close()
		_ = mn.Close()
	}}
}

// Every leaf arrives from the seeder, inside the fetch stall watchdog, although the finder names another provider
// first for every CID — one that never answers, or one that does not speak bitswap at all (both seen on the
// replication: a public pinning node over wss, and indexer hits). The session routes some want-blocks there; the
// DONT_HAVE timeout (silent) or the unresponsive-peer disconnect (no bitswap) must hand them to the seeder. Teeth:
// bitswap.SetSimulateDontHavesOnTimeout(false) in bitswapOptions and the silent provider strands its wants.
func TestAProviderThatNeverAnswersDoesNotStrandWants(t *testing.T) {
	for _, kind := range []int{providerSilent, providerNoBitswap} {
		t.Run(map[int]string{providerSilent: "silent", providerNoBitswap: "no-bitswap"}[kind], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			rig := setupBlackhole(t, ctx, 32<<20, kind, false, 0) // 128 leaves
			defer rig.stop()
			fctx, fcancel := context.WithTimeout(ctx, stallTimeout)
			defer fcancel()
			got := map[cid.Cid]bool{}
			for b := range rollingGetBlocks(fctx, rig.sess, rig.leaves, 32) {
				got[b.Cid()] = true
			}
			if len(got) != len(rig.leaves) {
				t.Fatalf("%d of %d leaves arrived within %s (the rest stranded on the other provider)", len(got), len(rig.leaves), stallTimeout)
			}
		})
	}
}

// A provider that answers HAVE for every block and never delivers (bitswap.pinata.cloud on the replication) strands
// the wants a session gives it: its HAVE outranks the busy seeder's, the want-block goes to it, the DONT_HAVE timeout
// does not undo its HAVE, and the want-block sent again is dropped as already sent — no new timeout. The quarantine
// takes it out of the client's rotation once it has sat on want-blocks for quarantineAfter, and boxo re-routes every
// want it held to the seeder. Teeth: build the client without the quarantine (quarantine=false) and leaves strand.
func TestAProviderThatPromisesAndNeverDeliversIsQuarantined(t *testing.T) {
	a, c, tk := quarantineAfter, quarantineCool, quarantineTick
	quarantineAfter, quarantineCool, quarantineTick = 2*time.Second, time.Minute, 200*time.Millisecond
	defer func() { quarantineAfter, quarantineCool, quarantineTick = a, c, tk }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rig := setupBlackhole(t, ctx, 32<<20, providerPromiser, true, 0)
	defer rig.stop()
	fctx, fcancel := context.WithTimeout(ctx, stallTimeout)
	defer fcancel()
	got := map[cid.Cid]bool{}
	for b := range rollingGetBlocks(fctx, rig.sess, rig.leaves, 32) {
		got[b.Cid()] = true
	}
	if len(got) != len(rig.leaves) {
		t.Fatalf("%d of %d leaves arrived within %s (the rest stranded on the provider that never delivers)", len(got), len(rig.leaves), stallTimeout)
	}
}

// An honest seeder that hiccups — nothing for longer than quarantineAfter — while the promiser offers everything too
// is taken out, and must not be starved for it: the blocks it then delivers bring it back and reach the client. The
// whole file lands well inside the stall watchdog. Teeth: drop an out peer's blocks (they are lost, and the promiser —
// offered only by a peer that is out — holds the rest until the seeder's cooldown ends, past the watchdog).
func TestAnHonestSeederThatHiccupsIsNotStarved(t *testing.T) {
	a, c, tk := quarantineAfter, quarantineCool, quarantineTick
	quarantineAfter, quarantineCool, quarantineTick = 2*time.Second, time.Minute, 200*time.Millisecond
	defer func() { quarantineAfter, quarantineCool, quarantineTick = a, c, tk }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rig := setupBlackhole(t, ctx, 16<<20, providerPromiser, true, 5*time.Second)
	defer rig.stop()
	fctx, fcancel := context.WithTimeout(ctx, stallTimeout)
	defer fcancel()
	start := time.Now()
	got := map[cid.Cid]bool{}
	for b := range rollingGetBlocks(fctx, rig.sess, rig.leaves, 32) {
		got[b.Cid()] = true
	}
	t.Logf("%d of %d leaves in %s", len(got), len(rig.leaves), time.Since(start).Round(100*time.Millisecond))
	if len(got) != len(rig.leaves) {
		t.Fatalf("%d of %d leaves arrived within %s (the seeder was starved after its hiccup)", len(got), len(rig.leaves), stallTimeout)
	}
}

// pinataRecv answers HAVE to every want-have at once and never a want-block (bitswap.pinata.cloud, as seen).
type pinataRecv struct {
	net bsnetwork.BitSwapNetwork
	wb  atomic.Int64
}

func (r *pinataRecv) ReceiveMessage(_ context.Context, p peer.ID, m bsmsg.BitSwapMessage) {
	out := bsmsg.New(false)
	for _, e := range m.Wantlist() {
		switch {
		case e.Cancel:
		case e.WantType == pb.Message_Wantlist_Have:
			out.AddHave(e.Cid)
		default:
			r.wb.Add(1)
		}
	}
	if len(out.Haves()) > 0 {
		go func() { _ = r.net.SendMessage(context.Background(), p, out) }()
	}
}
func (r *pinataRecv) ReceiveError(error)       {}
func (r *pinataRecv) PeerConnected(peer.ID)    {}
func (r *pinataRecv) PeerDisconnected(peer.ID) {}

// Replication 6's stalls: a promiser that answers every want-have with a HAVE at once, while a big fetch keeps it
// answering, must still go out for the want-blocks it sits on — a small fetch started beside the big one lands long
// before the big one does, not after it (its want-blocks parked on the promiser for as long as the big fetch ran, and
// in production the stall watchdog tore it down). Teeth: count a message without a block as progress.
func TestAPromiserKeptBusyAnsweringStillGoesOut(t *testing.T) {
	a, c, tk := quarantineAfter, quarantineCool, quarantineTick
	quarantineAfter, quarantineCool, quarantineTick = 2*time.Second, time.Minute, 200*time.Millisecond
	defer func() { quarantineAfter, quarantineCool, quarantineTick = a, c, tk }()
	defer func(m int) { maxPerFetch = m }(maxPerFetch)
	maxPerFetch = 32
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mn, err := mocknet.FullMeshLinked(3)
	if err != nil {
		t.Fatal(err)
	}
	defer mn.Close()
	mn.SetLinkDefaults(mocknet.LinkOptions{Latency: 5 * time.Millisecond})
	hosts := mn.Hosts()
	hostA, hostH, hostB := hosts[0], hosts[1], hosts[2]
	bstoreA := blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore()))
	dservA := merkledag.NewDAGService(blockservice.New(bstoreA, offline.Exchange(bstoreA)))
	leaves := func(n int) []cid.Cid {
		b := make([]byte, n)
		if _, err := rand.Read(b); err != nil {
			t.Fatal(err)
		}
		_, l := buildLeafDAG(t, dservA, b)
		return l
	}
	small, big := leaves(8<<20), leaves(64<<20)
	bswapA := bitswap.New(ctx, bsnet.NewFromIpfsHost(hostA), nilFinder{}, bstoreA, bitswapOptions(nil)...)
	defer bswapA.Close()
	netH := bsnet.NewFromIpfsHost(hostH)
	pinata := &pinataRecv{net: netH}
	netH.Start(pinata)
	defer netH.Stop()
	bstoreB := blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore()))
	q := newQuarantineNet(bsnet.NewFromIpfsHost(hostB))
	go q.run(ctx)
	bswapB := bitswap.New(ctx, q, fixedFinder{{ID: hostH.ID()}, {ID: hostA.ID()}}, bstoreB, bitswapOptions(nil)...)
	defer bswapB.Close()
	if err := mn.ConnectAllButSelf(); err != nil {
		t.Fatal(err)
	}
	for _, l := range mn.LinksBetweenPeers(hostA.ID(), hostB.ID()) {
		l.SetOptions(mocknet.LinkOptions{Latency: 5 * time.Millisecond, Bandwidth: 4 << 20}) // the big fetch takes ~16 s
	}
	bsvc := blockservice.New(bstoreB, bswapB)
	start := time.Now()
	type result struct {
		name string
		got  int
		at   time.Duration
	}
	done := make(chan result, 2)
	fetch := func(name string, l []cid.Cid) {
		fctx, fc := context.WithTimeout(ctx, 60*time.Second)
		defer fc()
		n := 0
		for range rollingGetBlocks(fctx, blockservice.NewSession(ctx, bsvc), l, 32) {
			n++
		}
		done <- result{name, n, time.Since(start)}
	}
	go fetch("big", big)
	time.Sleep(300 * time.Millisecond)
	go fetch("small", small)
	first, second := <-done, <-done
	t.Logf("%s %d in %s, %s %d in %s; the promiser was sent %d want-block(s)", first.name, first.got, first.at.Round(100*time.Millisecond),
		second.name, second.got, second.at.Round(100*time.Millisecond), pinata.wb.Load())
	if first.name != "small" || first.got != len(small) {
		t.Fatalf("the small fetch was not the first to land whole (%s first, %d leaves): its want-blocks sat on the promiser while the big fetch ran", first.name, first.got)
	}
	if second.got != len(big) {
		t.Fatalf("the big fetch got %d of %d leaves", second.got, len(big))
	}
}

// trickleRecv answers HAVE to every want-have and hands over one wanted block every `every` (a pinning node that
// delivers now and then: bitswap.pinata.cloud on replication 7, 51 blocks in 26 minutes).
type trickleRecv struct {
	net   bsnetwork.BitSwapNetwork
	store blockstore.Blockstore
	every time.Duration
	wb    atomic.Int64 // want-blocks received
	mu    sync.Mutex
	queue []struct {
		p peer.ID
		c cid.Cid
	}
}

func (r *trickleRecv) ReceiveMessage(_ context.Context, p peer.ID, m bsmsg.BitSwapMessage) {
	out := bsmsg.New(false)
	r.mu.Lock()
	for _, e := range m.Wantlist() {
		switch {
		case e.Cancel:
		case e.WantType == pb.Message_Wantlist_Have:
			out.AddHave(e.Cid)
		default:
			r.wb.Add(1)
			r.queue = append(r.queue, struct {
				p peer.ID
				c cid.Cid
			}{p, e.Cid})
		}
	}
	r.mu.Unlock()
	if len(out.Haves()) > 0 {
		go func() { _ = r.net.SendMessage(context.Background(), p, out) }()
	}
}
func (r *trickleRecv) run(ctx context.Context) {
	t := time.NewTicker(r.every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		r.mu.Lock()
		if len(r.queue) == 0 {
			r.mu.Unlock()
			continue
		}
		w := r.queue[0]
		r.queue = r.queue[1:]
		r.mu.Unlock()
		if b, err := r.store.Get(ctx, w.c); err == nil {
			m := bsmsg.New(false)
			m.AddBlock(b)
			_ = r.net.SendMessage(ctx, w.p, m)
		}
	}
}
func (r *trickleRecv) ReceiveError(error)       {}
func (r *trickleRecv) PeerConnected(peer.ID)    {}
func (r *trickleRecv) PeerDisconnected(peer.ID) {}

// Replication 7's crawl: file after file (a fresh session each), with promisers that drop and redial their connection
// and a trickler that hands over a block now and then. Each is taken out for sitting on want-blocks — and must STAY
// out longer each time: its record survives its connection, and a trickle neither clears it nor wins it new wants.
// Before, every redial and every trickled block reset it to the first cooldown, and each new file's session handed
// it wants again. Judged by each peer's record, not by want-block counts (those scale with the machine's speed: a slow
// CI runner handed out more in the second half and failed a ratio that held locally): over a run of 30 s+ (16 files at
// the seeder's 300 ms latency), a cooldown that keeps doubling reaches a 4th offence (2+4+8 s out, plus the holds); one
// reset by a redial or a trickled block stays at 1-2. Teeth: bring the peer fully back (or clear its record) on any
// block. (Offences kept per connection instead of per peer are caught by TestQuarantineRemembersAcrossConnections.)
func TestPromisersThatChurnAndTrickleAreKeptOut(t *testing.T) {
	a, c, m, tk, mh := quarantineAfter, quarantineCool, quarantineMaxCool, quarantineTick, quarantineMaxHold
	// Production's shape at a third of its scale: floor 3 s → 1 s, maximum hold 10 s → 3 s; the trickler hands over a
	// block less often than the maximum hold (pinata: ~one per 30 s), so it earns no pace of its own.
	quarantineAfter, quarantineCool, quarantineMaxCool, quarantineTick, quarantineMaxHold = time.Second, 2*time.Second, time.Minute, 100*time.Millisecond, 3*time.Second
	defer func() {
		quarantineAfter, quarantineCool, quarantineMaxCool, quarantineTick, quarantineMaxHold = a, c, m, tk, mh
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const promisers = 3
	mn, err := mocknet.FullMeshLinked(3 + promisers)
	if err != nil {
		t.Fatal(err)
	}
	defer mn.Close()
	mn.SetLinkDefaults(mocknet.LinkOptions{Latency: 5 * time.Millisecond})
	hosts := mn.Hosts()
	hostA, hostT, hostB, hostP := hosts[0], hosts[1], hosts[2], hosts[3:]
	bstoreA := blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore()))
	dservA := merkledag.NewDAGService(blockservice.New(bstoreA, offline.Exchange(bstoreA)))
	var files [][]cid.Cid
	for i := 0; i < 16; i++ {
		b := make([]byte, 4<<20)
		if _, err := rand.Read(b); err != nil {
			t.Fatal(err)
		}
		_, l := buildLeafDAG(t, dservA, b)
		files = append(files, l)
	}
	bswapA := bitswap.New(ctx, bsnet.NewFromIpfsHost(hostA), nilFinder{}, bstoreA, bitswapOptions(nil)...)
	defer bswapA.Close()
	netT := bsnet.NewFromIpfsHost(hostT)
	tr := &trickleRecv{net: netT, store: bstoreA, every: 5 * time.Second}
	netT.Start(tr)
	defer netT.Stop()
	go tr.run(ctx)
	finder := fixedFinder{{ID: hostT.ID()}}
	var pins []*pinataRecv
	for _, h := range hostP {
		n := bsnet.NewFromIpfsHost(h)
		pr := &pinataRecv{net: n}
		pins = append(pins, pr)
		n.Start(pr)
		defer n.Stop()
		finder = append(finder, peer.AddrInfo{ID: h.ID()})
	}
	finder = append(finder, peer.AddrInfo{ID: hostA.ID()})
	bstoreB := blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore()))
	q := newQuarantineNet(bsnet.NewFromIpfsHost(hostB))
	qDone := make(chan struct{})
	go func() { q.run(ctx); close(qDone) }()
	bswapB := bitswap.New(ctx, q, finder, bstoreB, bitswapOptions(nil)...)
	defer bswapB.Close()
	if err := mn.ConnectAllButSelf(); err != nil {
		t.Fatal(err)
	}
	for _, l := range mn.LinksBetweenPeers(hostA.ID(), hostB.ID()) { // a busy seeder: its HAVEs come last
		l.SetOptions(mocknet.LinkOptions{Latency: 300 * time.Millisecond})
	}
	go func() { // the promisers drop and redial, as public nodes do
		tick := time.NewTicker(3 * time.Second)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
			for _, h := range hostP {
				_ = mn.DisconnectPeers(hostB.ID(), h.ID())
				_, _ = mn.ConnectPeers(hostB.ID(), h.ID())
			}
		}
	}()
	bsvc := blockservice.New(bstoreB, bswapB)
	start := time.Now()
	var took []time.Duration
	given := func() int64 { // want-blocks the peers that never pay have been given
		n := tr.wb.Load()
		for _, pr := range pins {
			n += pr.wb.Load()
		}
		return n
	}
	var half int64
	for i, l := range files {
		if i == len(files)/2 {
			half = given()
		}
		t0 := time.Now()
		fctx, fc := context.WithTimeout(ctx, stallTimeout)
		n := 0
		for range rollingGetBlocks(fctx, blockservice.NewSession(ctx, bsvc), l, 32) {
			n++
		}
		fc()
		if n != len(l) {
			t.Fatalf("file %d: %d of %d leaves within %s", i, n, len(l), stallTimeout)
		}
		took = append(took, time.Since(t0).Round(10*time.Millisecond))
	}
	firstHalf, secondHalf := half, given()-half
	t.Logf("per file: %v; total %s; want-blocks to the promisers and the trickler: %d in the first half, %d in the second",
		took, time.Since(start).Round(100*time.Millisecond), firstHalf, secondHalf)
	// The quarantine loop stops before the records are read, and before the deferred restore of its timing globals.
	cancel()
	<-qDone
	q.mu.Lock()
	defer q.mu.Unlock()
	never := []peer.ID{hostT.ID()}
	for _, h := range hostP {
		never = append(never, h.ID())
	}
	for i, id := range never {
		strikes := 0
		if r := q.records[id]; r != nil {
			strikes = r.strikes
		}
		if strikes < 4 {
			who := "the trickler"
			if i > 0 {
				who = "promiser " + string(rune('0'+i))
			}
			t.Errorf("%s ended on offence %d, want 4 or more: its cooldown stopped doubling (its record was reset)", who, strikes)
		}
	}
}
