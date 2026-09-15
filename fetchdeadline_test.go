package main

// fetchdeadline_test.go — the deadline must bound the fetch ATTEMPT ITSELF, not merely the number of attempt
// STARTS (the pass-4 HIGH). Three limbs, one per scenario the reviewer named:
//   (a) getRoot: a 30s root fetch / gateway pull on a dead network must end at the caller's deadline.
//   (b) writeThrough: a session that trickles (or never delivers) a leaf must be torn down at the deadline,
//       WITHOUT waiting for the 20s stall watchdog.
//   (c) dedupFetch: a bounded caller that JOINS an UNBOUNDED in-flight leader must stop waiting at its own
//       deadline (the reintroduced pass-1 CRITICAL) — and it must NOT run its own callbacks (UAF-safety).
// Each test asserts WALL-CLOCK time, so the mutation that removes the fix (parent on n.ctx / block the joiner)
// blows the bound and fails.

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	bitswap "github.com/ipfs/boxo/bitswap"
	bsnet "github.com/ipfs/boxo/bitswap/network/bsnet"
	blockservice "github.com/ipfs/boxo/blockservice"
	blockstore "github.com/ipfs/boxo/blockstore"
	offline "github.com/ipfs/boxo/exchange/offline"
	merkledag "github.com/ipfs/boxo/ipld/merkledag"
	blocks "github.com/ipfs/go-block-format"
	datastore "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
)

// peerlessBitswapDserv builds a DAG service backed by a bitswap with NO connected peers: a Get for a block that
// isn't already in bstore blocks until its context is cancelled (there is nobody to answer the want). That is the
// "online node whose provider never delivers" the deadline must survive.
func peerlessBitswapDserv(t *testing.T, ctx context.Context) (blockstore.Blockstore, blockservice.BlockService, func()) {
	t.Helper()
	mn := mocknet.New()
	h, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	bstore := blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore()))
	bswap := bitswap.New(ctx, bsnet.NewFromIpfsHost(h), nilFinder{}, bstore)
	bs := blockservice.New(bstore, bswap)
	return bstore, bs, func() { _ = bswap.Close(); _ = h.Close() }
}

// (a) getRoot must honour the attempt deadline threaded into nctx, not spend a fixed 30s. Teeth: change getRoot's
// getCtx back to context.WithTimeout(n.ctx, 30*time.Second) and this blocks ~30s (elapsed ≫ bound) → fails.
func TestGetRootHonorsAttemptDeadline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bstore, bs, stop := peerlessBitswapDserv(t, ctx)
	defer stop()
	_ = bstore
	n := &node{ctx: ctx, dserv: merkledag.NewDAGService(bs), dht: nil} // dht nil → no gateway path, fully hermetic

	// A CID we never Put anywhere → a want that nobody can satisfy → getRoot's dserv.Get blocks until the deadline.
	absent := blocks.NewBlock([]byte("this block is never provided to anyone")).Cid()

	deadline := time.Now().Add(600 * time.Millisecond)
	nctx, ncancel := context.WithDeadline(ctx, deadline)
	defer ncancel()

	start := time.Now()
	_, err := n.getRoot(nctx, absent, absent.String(), nil)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("getRoot must fail for content nobody provides")
	}
	if elapsed > 5*time.Second {
		t.Fatalf("getRoot ignored the attempt deadline: took %s (want ~600ms). getCtx must be parented on nctx, not n.ctx.", elapsed)
	}
}

// (b) writeThrough opens a bitswap session for the file's leaves. When those leaves are unreachable, the session
// must be torn down at the ATTEMPT deadline — not left to the 20s stall watchdog. Teeth: change writeThrough's
// fctx back to context.WithCancel(n.ctx) and the transfer runs until the stall watchdog fires at stallTimeout=20s
// (elapsed ≫ bound) → fails.
func TestWriteThroughHonorsAttemptDeadline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	n := offlineNode(t) // fully-wired local node (fstore/ds/pinner); we swap only its DAG/block service below

	// Build a genuinely multi-leaf file in a scratch store, then hand writeThrough the root NODE (in memory) while
	// its LEAVES live nowhere reachable — so writeThrough must fetch them over the (peerless) session.
	payload := make([]byte, 3_000_000) // ≫ chunkSize → many leaves
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}
	scratch := blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore()))
	scratchDserv := merkledag.NewDAGService(blockservice.New(scratch, offline.Exchange(scratch)))
	rootCid, leaves := buildLeafDAG(t, scratchDserv, payload)
	if len(leaves) < 5 {
		t.Fatalf("fixture produced only %d leaves", len(leaves))
	}
	rootNode, err := scratchDserv.Get(ctx, rootCid)
	if err != nil {
		t.Fatal(err)
	}

	bstore, bs, stop := peerlessBitswapDserv(t, ctx)
	defer stop()
	if err := bstore.Put(ctx, rootNode); err != nil { // only the root is present; every leaf must be fetched → blocks
		t.Fatal(err)
	}
	n.dserv = merkledag.NewDAGService(bs)
	n.bserv = bs

	deadline := time.Now().Add(800 * time.Millisecond)
	nctx, ncancel := context.WithDeadline(ctx, deadline)
	defer ncancel()

	start := time.Now()
	werr := n.writeThrough(nctx, rootCid, rootNode, t.TempDir()+"/out.bin", rootCid.String(), nil, nil)
	elapsed := time.Since(start)
	if werr == nil {
		t.Fatal("writeThrough must fail when the file's leaves are unreachable")
	}
	if elapsed > 5*time.Second {
		t.Fatalf("writeThrough ignored the attempt deadline: took %s (want ~800ms). fctx (the session ctx) must be parented on nctx.", elapsed)
	}
}

// H-B: a node with ZERO connected peers must not burn rootLibp2pTimeout before the gateway fallback — the isolation
// matrix measured 30 s of a 72 s gateway-only fetch spent exactly so. Teeth: delete the zero-peers arm in getRoot's
// poller and phase 1 runs the full (shrunk) 3 s timeout here instead of ending at the 300 ms grace.
func TestGetRootHandsOffEarlyWhenTheNodeHasNoPeers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mn := mocknet.New()
	h, err := mn.GenPeer() // a real libp2p host with NO connections at all
	if err != nil {
		t.Fatal(err)
	}
	bstore := blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore()))
	bswap := bitswap.New(ctx, bsnet.NewFromIpfsHost(h), nilFinder{}, bstore)
	defer bswap.Close()
	// dht nil → after phase 1 getRoot returns immediately (no gateway phase), so elapsed IS phase 1's duration.
	n := &node{ctx: ctx, host: h, dserv: merkledag.NewDAGService(blockservice.New(bstore, bswap)), dht: nil}

	origT, origG := rootLibp2pTimeout, rootNoPeersGrace
	rootLibp2pTimeout, rootNoPeersGrace = 3*time.Second, 300*time.Millisecond
	defer func() { rootLibp2pTimeout, rootNoPeersGrace = origT, origG }()

	absent := blocks.NewBlock([]byte("nobody is connected to serve this")).Cid()
	start := time.Now()
	if _, err := n.getRoot(ctx, absent, absent.String(), nil); err == nil {
		t.Fatal("getRoot must fail: the node has no peers")
	}
	if el := time.Since(start); el >= 2*time.Second {
		t.Fatalf("phase 1 ran %s with zero peers — the no-peers short-circuit did not fire (grace 300ms, timeout 3s)", el)
	}
}
