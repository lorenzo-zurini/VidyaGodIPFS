package main

// bitswaptwonode_test.go — a REAL two-node bitswap test of the rolling want-window (rollingGetBlocks), the exact
// production code path, over a libp2p mocknet wire. This is the test the earlier "offline, single local block"
// fixtures could not be: node A seeds a genuinely multi-leaf file; node B fetches every leaf from A across the
// wire via rollingGetBlocks; we assert all bytes arrive, the global token budget is returned, and — under a
// deliberately TINY budget far below the leaf count — the transfer still COMPLETES (the window recycles tokens
// as blocks land; a broken per-block release would wedge and hit the test timeout).

import (
	"bytes"
	"context"
	"crypto/rand"
	"sync"
	"testing"
	"time"

	bitswap "github.com/ipfs/boxo/bitswap"
	bsmsg "github.com/ipfs/boxo/bitswap/message"
	bsnet "github.com/ipfs/boxo/bitswap/network/bsnet"
	blockservice "github.com/ipfs/boxo/blockservice"
	blockstore "github.com/ipfs/boxo/blockstore"
	chunker "github.com/ipfs/boxo/chunker"
	offline "github.com/ipfs/boxo/exchange/offline"
	merkledag "github.com/ipfs/boxo/ipld/merkledag"
	balanced "github.com/ipfs/boxo/ipld/unixfs/importer/balanced"
	uih "github.com/ipfs/boxo/ipld/unixfs/importer/helpers"
	cid "github.com/ipfs/go-cid"
	datastore "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	ipld "github.com/ipfs/go-ipld-format"
	peer "github.com/libp2p/go-libp2p/core/peer"
	routing "github.com/libp2p/go-libp2p/core/routing"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
)

// nilFinder is a provider router that finds nobody — the session must reach the seeder purely via its broadcast
// to the directly-connected peer, which is exactly the "friend/seeder we're already connected to" path.
type nilFinder struct{}

func (nilFinder) FindProvidersAsync(context.Context, cid.Cid, int) <-chan peer.AddrInfo {
	ch := make(chan peer.AddrInfo)
	close(ch)
	return ch
}

var _ routing.ContentDiscovery = nilFinder{}

// buildLeafDAG imports b into dserv (backed by bstore) and returns the root plus every leaf CID in order.
func buildLeafDAG(t *testing.T, dserv ipld.DAGService, b []byte) (cid.Cid, []cid.Cid) {
	t.Helper()
	dbp := uih.DagBuilderParams{
		Maxlinks:   uih.DefaultLinksPerBlock,
		RawLeaves:  true,
		CidBuilder: merkledag.V0CidPrefix(),
		Dagserv:    dserv,
	}
	db, err := dbp.New(chunker.NewSizeSplitter(bytes.NewReader(b), chunkSize))
	if err != nil {
		t.Fatal(err)
	}
	root, err := balanced.Layout(db)
	if err != nil {
		t.Fatal(err)
	}
	// Collect raw leaves in DFS order.
	seen := cid.NewSet()
	var leaves []cid.Cid
	var walk func(nd ipld.Node)
	walk = func(nd ipld.Node) {
		links := nd.Links()
		if len(links) == 0 {
			if nd.Cid().Prefix().Codec == cid.Raw && seen.Visit(nd.Cid()) {
				leaves = append(leaves, nd.Cid())
			}
			return
		}
		for _, l := range links {
			child, err := l.GetNode(context.Background(), dserv)
			if err != nil {
				// a raw-leaf link resolves to a raw node with no links
				if seen.Visit(l.Cid) {
					leaves = append(leaves, l.Cid)
				}
				continue
			}
			walk(child)
		}
	}
	walk(root)
	return root.Cid(), leaves
}

type twoNode struct {
	bswapA  *bitswap.Bitswap
	bstoreA blockstore.Blockstore
	wire    *wireLedger // what the seeder holds as wanted by B
	hostB   peerIDer
	bswapB  *bitswap.Bitswap
	bstoreB blockstore.Blockstore
	sess    *blockservice.Session
	leaves  []cid.Cid
}

// wireLedger is what the SEEDER holds as wanted by B, exactly: a want entry adds its CID, a cancel removes it, and so
// does the seeder answering it (the block, or a HAVE / DONT_HAVE). It is recorded by a tracer on the seeder's
// bitswap, message by message — no sampling, so a short-lived window cannot slip between two looks.
type wireLedger struct {
	mu   sync.Mutex
	open map[cid.Cid]struct{}
	max  int
}

func newWireLedger() *wireLedger { return &wireLedger{open: map[cid.Cid]struct{}{}} }

func (w *wireLedger) MessageReceived(_ peer.ID, m bsmsg.BitSwapMessage) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if m.Full() {
		clear(w.open)
	}
	for _, e := range m.Wantlist() {
		if e.Cancel {
			delete(w.open, e.Cid)
		} else {
			w.open[e.Cid] = struct{}{}
		}
	}
	if len(w.open) > w.max {
		w.max = len(w.open)
	}
}

func (w *wireLedger) MessageSent(_ peer.ID, m bsmsg.BitSwapMessage) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, b := range m.Blocks() {
		delete(w.open, b.Cid())
	}
	for _, c := range m.Haves() {
		delete(w.open, c)
	}
	for _, c := range m.DontHaves() {
		delete(w.open, c)
	}
}

func (w *wireLedger) peak() int { w.mu.Lock(); defer w.mu.Unlock(); return w.max }

// peerIDer is the tiny slice of host.Host the tests use (ID()), kept local to avoid importing host just for a type.
type peerIDer interface{ ID() peer.ID }

// setupTwoNode wires A (seeder holding a multi-leaf random file) and B (client session) on a latency-mocknet.
func setupTwoNode(t *testing.T, ctx context.Context, payloadSize int) twoNode {
	t.Helper()
	mn, err := mocknet.FullMeshLinked(2)
	if err != nil {
		t.Fatal(err)
	}
	mn.SetLinkDefaults(mocknet.LinkOptions{Latency: 8 * time.Millisecond}) // so outstanding wants are observable
	hosts := mn.Hosts()
	hostA, hostB := hosts[0], hosts[1]

	bstoreA := blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore()))
	dservA := merkledag.NewDAGService(blockservice.New(bstoreA, offline.Exchange(bstoreA)))
	payload := make([]byte, payloadSize)
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}
	_, leaves := buildLeafDAG(t, dservA, payload)
	if len(leaves) < 20 {
		t.Fatalf("fixture only produced %d leaves — too few to exercise a rolling window", len(leaves))
	}
	wire := newWireLedger()
	bswapA := bitswap.New(ctx, bsnet.NewFromIpfsHost(hostA), nilFinder{}, bstoreA, bitswap.WithTracer(wire))
	t.Cleanup(func() { bswapA.Close() })

	bstoreB := blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore()))
	bswapB := bitswap.New(ctx, bsnet.NewFromIpfsHost(hostB), nilFinder{}, bstoreB)
	t.Cleanup(func() { bswapB.Close() })
	if err := mn.ConnectAllButSelf(); err != nil {
		t.Fatal(err)
	}
	sess := blockservice.NewSession(ctx, blockservice.New(bstoreB, bswapB))
	return twoNode{bswapA: bswapA, bstoreA: bstoreA, wire: wire, hostB: hostB, bswapB: bswapB, bstoreB: bstoreB, sess: sess, leaves: leaves}
}

// Cancelling a two-node transfer mid-flight must return EVERY want-token (the delivered ones per block, and the
// stragglers via each fan's channel-close release) — so a later fetch has the full budget. Teeth: delete the
// fan's ctx-cancel / channel-close remainder release and the budget is not restored here.
func TestTwoNodeCancelReleasesAllTokens(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h := setupTwoNode(t, ctx, 8<<20)

	saved := globalWantPool
	globalWantPool = newWantPool(8)
	defer func() { globalWantPool = saved }()

	fctx, fcancel := context.WithCancel(ctx)
	defer fcancel()
	ch := rollingGetBlocks(fctx, h.sess, h.leaves, 4)
	got := 0
	for range ch {
		got++
		if got == 5 { // cancel mid-transfer, with fans in flight holding tokens
			fcancel()
		}
	}
	// channel has closed (producer + fans all exited). Every token must be back.
	if globalWantPool.available() != 8 {
		t.Fatalf("after mid-transfer cancel the token budget was not fully restored: available=%d want 8 (fan remainder-release leak)", globalWantPool.available())
	}
}

func TestTwoNodeRollingGetBlocksOverTheWire(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h := setupTwoNode(t, ctx, 8<<20) // 8 MiB → ~32 random (non-dedup) leaves
	sess, leaves, bstoreA := h.sess, h.leaves, h.bstoreA

	// TIGHT global budget, far below the leaf count: proves the window RECYCLES tokens over the wire (a broken
	// per-block release would leave the producer stuck at the budget and the fetch would wedge → timeout).
	saved := globalWantPool
	globalWantPool = newWantPool(8)
	defer func() { globalWantPool = saved }()

	// WIRE CAP: what the SEEDER holds as wanted by B at once (h.wire, exact). The whole point of the token pool is
	// that the combined outstanding wants to a peer stay under the budget; assert it on the actual wire, not just in
	// our own accounting. The seeder must never hold more than the budget.
	got := map[cid.Cid][]byte{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for blk := range rollingGetBlocks(ctx, sess, leaves, 4) {
			got[blk.Cid()] = blk.RawData()
		}
	}()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatalf("two-node fetch wedged: got %d/%d leaves under an 8-token budget (rolling window not recycling)", len(got), len(leaves))
	}
	mw := h.wire.peak()
	t.Logf("max outstanding wants the seeder saw from B: %d (budget 8, refill 4)", mw)
	if mw > 8 {
		t.Errorf("seeder saw %d outstanding wants from B — the token budget is not what the wire sees (cap breached)", mw)
	}
	if mw < 2 {
		t.Errorf("seeder saw only %d outstanding want(s) — the window is not pipelining (behaving like a serial window of 1)", mw)
	}

	if len(got) != len(leaves) {
		t.Fatalf("received %d/%d leaves over the wire", len(got), len(leaves))
	}
	// Every leaf's bytes must match what A held.
	for _, lc := range leaves {
		want, err := bstoreA.Get(ctx, lc)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got[lc], want.RawData()) {
			t.Fatalf("leaf %s bytes differ over the wire", lc)
		}
	}
	// The whole 8-token budget must be back (no leak / no over-release after a real multi-batch wire transfer).
	if globalWantPool.available() != 8 {
		t.Errorf("token budget not restored after two-node fetch: available=%d want 8", globalWantPool.available())
	}
}

// The per-fetch cap must bound ONE fetch's outstanding wants even when the global budget is large — so a single
// download (especially a trickling one holding stragglers) can't monopolise the whole budget. Give the pool
// plenty (100) but cap a fetch at 4: the seeder must never hold more than 4 wants from B. Teeth: make
// maxPerFetch huge and the wire shows the full ~pool window instead.
func TestPerFetchCapBoundsASingleFetch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h := setupTwoNode(t, ctx, 8<<20)

	savedPool := globalWantPool
	globalWantPool = newWantPool(100) // ample global budget
	savedCap := maxPerFetch
	maxPerFetch = 4 // but this one fetch may hold at most 4
	defer func() { globalWantPool = savedPool; maxPerFetch = savedCap }()

	n := 0
	for range rollingGetBlocks(ctx, h.sess, h.leaves, 4) {
		n++
	}
	if n != len(h.leaves) {
		t.Fatalf("received %d/%d leaves", n, len(h.leaves))
	}
	mw := h.wire.peak()
	t.Logf("max outstanding with maxPerFetch=4: %d", mw)
	if mw > 4 {
		t.Errorf("per-fetch cap breached: seeder saw %d wants from B with maxPerFetch=4", mw)
	}
}

// A write-through's leaves go to the file, so fetching them must not store them: the blockservice's per-block
// blockstore Put (then our delete, then leveldb's compactions) throttled a replication to the database. leafGetter
// delivers every leaf over the wire with B's blockstore left without a single copy, and serves a leaf B already
// holds from B itself. Teeth: Put the received blocks into g.local in leafGetter (what a blockservice session does)
// and B's blockstore holds them.
func TestLeafGetterDeliversWithoutStoring(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h := setupTwoNode(t, ctx, 8<<20)

	// One leaf is already local on B: it must come from B's blockstore, not the wire.
	have, err := h.bstoreA.Get(ctx, h.leaves[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := h.bstoreB.Put(ctx, have); err != nil {
		t.Fatal(err)
	}

	g := leafGetter{local: h.bstoreB, sess: h.bswapB.NewSession(ctx)}
	got := map[cid.Cid][]byte{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for blk := range rollingGetBlocks(ctx, g, h.leaves, 4) {
			got[blk.Cid()] = blk.RawData()
		}
	}()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatalf("fetch did not finish: %d/%d leaves", len(got), len(h.leaves))
	}
	if len(got) != len(h.leaves) {
		t.Fatalf("received %d/%d leaves", len(got), len(h.leaves))
	}
	for _, lc := range h.leaves {
		want, err := h.bstoreA.Get(ctx, lc)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got[lc], want.RawData()) {
			t.Fatalf("leaf %s bytes differ", lc)
		}
	}
	for _, lc := range h.leaves[1:] {
		if ok, _ := h.bstoreB.Has(ctx, lc); ok {
			t.Fatalf("leaf %s was stored on the receiving side — a fetched leaf must only reach the file", lc)
		}
	}
}
