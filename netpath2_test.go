package main

// Network-path hardening, second batch (isolation-matrix run-4 + the laptop's Pinata-only field report):
//   * a BOUNDED root fetch splits its budget so the gateway gets a share (covers: "did not complete before its deadline");
//   * writeThrough resumes MISSING LEAVES over the gateway when bitswap stalls (a partial DAG is no longer stuck for good);
//   * gateway bytes count on the global speedometer;
//   * one attempt's progress bar never runs backwards across the gateway→materialize handoff.

import (
	"bytes"
	"context"
	"crypto/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	bitswap "github.com/ipfs/boxo/bitswap"
	bsnet "github.com/ipfs/boxo/bitswap/network/bsnet"
	blockservice "github.com/ipfs/boxo/blockservice"
	blockstore "github.com/ipfs/boxo/blockstore"
	offline "github.com/ipfs/boxo/exchange/offline"
	merkledag "github.com/ipfs/boxo/ipld/merkledag"
	cid "github.com/ipfs/go-cid"
	datastore "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	ipld "github.com/ipfs/go-ipld-format"
	carblockstore "github.com/ipld/go-car/v2/blockstore"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/metrics"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
)

// peerlessBitswapOver is peerlessBitswapDserv over a GIVEN blockstore: a bitswap session with nobody to ask, whose
// local hits (and the gateway's CAR imports) land in the node's own store so finalize/pin see them.
func peerlessBitswapOver(t *testing.T, ctx context.Context, bstore blockstore.Blockstore) (blockservice.BlockService, func()) {
	t.Helper()
	mn := mocknet.New()
	h, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	bswap := bitswap.New(ctx, bsnet.NewFromIpfsHost(h), nilFinder{}, bstore)
	bs := blockservice.New(bstore, bswap)
	return bs, func() { _ = bswap.Close(); _ = h.Close() }
}

// carOfStore serialises every block of a scratch store into a CARv1 rooted at root — what a trustless gateway serves.
func carOfStore(t *testing.T, ctx context.Context, st blockstore.Blockstore, root cid.Cid) []byte {
	t.Helper()
	path := t.TempDir() + "/dag.car"
	rw, err := carblockstore.OpenReadWrite(path, []cid.Cid{root}, carblockstore.WriteAsCarV1(true))
	if err != nil {
		t.Fatal(err)
	}
	keys, err := st.AllKeysChan(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for k := range keys {
		blk, err := st.Get(ctx, k)
		if err != nil {
			t.Fatal(err)
		}
		if err := rw.Put(ctx, blk); err != nil {
			t.Fatal(err)
		}
	}
	if err := rw.Finalize(); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// scratchLeafDAG builds a multi-leaf file in a throwaway store and returns its root, root node and CAR bytes.
func scratchLeafDAG(t *testing.T, ctx context.Context, size int) ([]byte, cid.Cid, ipld.Node, []byte) {
	payload, root, rootNode, carBytes, _ := scratchLeafDAGWithLeaves(t, ctx, size)
	return payload, root, rootNode, carBytes
}

func scratchLeafDAGWithLeaves(t *testing.T, ctx context.Context, size int) ([]byte, cid.Cid, ipld.Node, []byte, []cid.Cid) {
	t.Helper()
	payload := make([]byte, size)
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}
	scratch := blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore()))
	dserv := merkledag.NewDAGService(blockservice.New(scratch, offline.Exchange(scratch)))
	root, leaves := buildLeafDAG(t, dserv, payload)
	if len(leaves) < 3 {
		t.Fatalf("fixture produced only %d leaves", len(leaves))
	}
	rootNode, err := dserv.Get(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	return payload, root, rootNode, carOfStore(t, ctx, scratch, root), leaves
}

// resumeFixture wires a node whose store holds ONLY the root of a leaf DAG, over a peerless bitswap, online (dht
// set), with a fast stall and the given gateway — the shape of every leaf-resume test. Restores everything.
func resumeFixture(t *testing.T, n *node, ctx context.Context, rootNode ipld.Node, gwURL string) {
	t.Helper()
	bs, stop := peerlessBitswapOver(t, ctx, n.bstore)
	if err := n.bstore.Put(ctx, rootNode); err != nil {
		t.Fatal(err)
	}
	origDserv, origBserv, origDht := n.dserv, n.bserv, n.dht
	n.dserv, n.bserv, n.dht = merkledag.NewDAGService(bs), bs, &dht.IpfsDHT{}
	origGWs, origStall := trustlessGateways, stallTimeout
	trustlessGateways, stallTimeout = []string{gwURL}, 300*time.Millisecond
	t.Cleanup(func() { // before closeNode (LIFO)
		trustlessGateways, stallTimeout = origGWs, origStall
		n.dserv, n.bserv, n.dht = origDserv, origBserv, origDht
		stop()
	})
}

// carServer serves one CAR for every request (with a Content-Length, so the client's byte progress is determinate)
// and records the last query string it was asked with.
func carServer(carBytes []byte, lastQuery *string, mu *sync.Mutex) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		*lastQuery = r.URL.RawQuery
		mu.Unlock()
		w.Header().Set("Content-Type", "application/vnd.ipld.car")
		w.Header().Set("Content-Length", strconv.Itoa(len(carBytes)))
		_, _ = w.Write(carBytes)
	}))
}

// A cover fetch has a 30 s budget; the libp2p root phase had a 30 s timeout of its own — so on a network where the
// DHT and indexers turn up nothing the deadline fired exactly when the gateway would have started, and the cover
// "did not complete before its deadline" while the unbounded download beside it succeeded through the gateway.
// Teeth: drop the budget split in getRoot (libTimeout = rootLibp2pTimeout) → the 30 s libp2p phase eats the 2 s
// budget and the gateway runs under a dead context → error.
func TestBoundedGetRootLeavesTheGatewayAShareOfTheBudget(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bstore, bs, stop := peerlessBitswapDserv(t, ctx)
	defer stop()
	n := &node{ctx: ctx, bstore: bstore, dserv: merkledag.NewDAGService(bs), dht: &dht.IpfsDHT{}}

	pn := merkledag.NodeWithData([]byte("cover art no DHT record points at"))
	scratch := blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore()))
	if err := scratch.Put(ctx, pn); err != nil {
		t.Fatal(err)
	}
	var q string
	var mu sync.Mutex
	gw := carServer(carOfStore(t, ctx, scratch, pn.Cid()), &q, &mu)
	defer gw.Close()
	origGWs, origT := trustlessGateways, rootLibp2pTimeout
	trustlessGateways, rootLibp2pTimeout = []string{gw.URL}, 30*time.Second
	defer func() { trustlessGateways, rootLibp2pTimeout = origGWs, origT }()

	nctx, ncancel := context.WithTimeout(ctx, 2*time.Second)
	defer ncancel()
	start := time.Now()
	root, err := n.getRoot(nctx, pn.Cid(), pn.Cid().String(), nil)
	el := time.Since(start)
	if err != nil {
		t.Fatalf("a bounded getRoot must reach the gateway inside its own budget; got %v after %s", err, el)
	}
	if !root.Cid().Equals(pn.Cid()) {
		t.Fatalf("wrong root %s", root.Cid())
	}
	if el > 1900*time.Millisecond {
		t.Fatalf("took %s of a 2 s budget — libp2p was not held to half", el)
	}
}

// A DAG whose root landed but whose leaves did not (a hedge winner that died mid-stream; a CAR cut by the stall
// watchdog) used to be stuck for good on a gateway-only network: the root is local, so getRoot never re-enters the
// gateway, and the bitswap session has nobody to ask — run-4 looped "incomplete" seven times. Now a stalled session
// with leaves missing asks the gateway for the entity's bytes from the first missing leaf on.
// Teeth: gate the resume off (`&& false`) → errIncomplete; drop the entity-bytes query → the assertion on the query.
func TestWriteThroughResumesMissingLeavesOverTheGatewayWhenBitswapStalls(t *testing.T) {
	n := offlineNode(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	payload, root, rootNode, carBytes := scratchLeafDAG(t, ctx, 1_500_000)
	var q string
	var mu sync.Mutex
	gw := carServer(carBytes, &q, &mu)
	defer gw.Close()

	bs, stop := peerlessBitswapOver(t, ctx, n.bstore)
	defer stop()
	if err := n.bstore.Put(ctx, rootNode); err != nil { // the root is here; every leaf is not
		t.Fatal(err)
	}
	origDserv, origBserv, origDht := n.dserv, n.bserv, n.dht
	n.dserv, n.bserv, n.dht = merkledag.NewDAGService(bs), bs, &dht.IpfsDHT{}
	t.Cleanup(func() { n.dserv, n.bserv, n.dht = origDserv, origBserv, origDht }) // before closeNode (LIFO)
	origGWs, origStall := trustlessGateways, stallTimeout
	trustlessGateways, stallTimeout = []string{gw.URL}, 300*time.Millisecond
	defer func() { trustlessGateways, stallTimeout = origGWs, origStall }()

	dest := t.TempDir() + "/out.bin"
	start := time.Now()
	err := n.writeThrough(ctx, root, rootNode, dest, root.String(), nil, nil)
	if err != nil {
		t.Fatalf("writeThrough must complete over the gateway once bitswap stalls; got %v after %s", err, time.Since(start))
	}
	got, rerr := os.ReadFile(dest)
	if rerr != nil || !bytes.Equal(got, payload) {
		t.Fatalf("materialized bytes differ from the payload (err=%v, %d vs %d bytes)", rerr, len(got), len(payload))
	}
	mu.Lock()
	defer mu.Unlock()
	if !strings.Contains(q, "dag-scope=entity") || !strings.Contains(q, "entity-bytes=0:*") {
		t.Fatalf("the resume must ask for the entity's bytes from the first missing leaf on; query was %q", q)
	}
}

// Gateway downloads showed 0 on the global ↓ speedometer: the counter only saw libp2p streams. Teeth: drop the
// LogRecvMessage in streamCarCandidate → TotalIn stays 0.
func TestGatewayBytesCountOnTheGlobalSpeedometer(t *testing.T) {
	n := offlineNode(t)
	ctx := context.Background()
	n.bwc = metrics.NewBandwidthCounter()
	t.Cleanup(func() { n.bwc = nil })
	pn := merkledag.NodeWithData([]byte("bytes that must show on the speedometer"))
	scratch := blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore()))
	if err := scratch.Put(ctx, pn); err != nil {
		t.Fatal(err)
	}
	var q string
	var mu sync.Mutex
	gw := carServer(carOfStore(t, ctx, scratch, pn.Cid()), &q, &mu)
	defer gw.Close()
	orig := trustlessGateways
	trustlessGateways = []string{gw.URL}
	defer func() { trustlessGateways = orig }()

	if err := n.fetchViaGateway(ctx, pn.Cid(), -1, nil); err != nil {
		t.Fatal(err)
	}
	// The counter's totals are flow meters swept once a second — read until the sweep lands, bounded.
	want := int64(len(pn.RawData()))
	var got int64
	for dl := time.Now().Add(3 * time.Second); time.Now().Before(dl); time.Sleep(50 * time.Millisecond) {
		if got = n.bwc.GetBandwidthTotals().TotalIn; got == want {
			return
		}
	}
	t.Fatalf("speedometer saw %d bytes of a %d-byte gateway download", got, want)
}

// One attempt = one bar. The gateway phase reports CAR bytes over the wire (→ ~99%), then the materialize phase
// reports file bytes from what the bitmap says (0 right after a CAR import, every leaf local → a second, quick bar).
// Teeth: drop the monotoneProgress wrap in fetchToPathOnce → the recorded sequence goes 9x → 0 → fails.
func TestOneAttemptsProgressNeverRunsBackwardsAcrossTheGatewayHandoff(t *testing.T) {
	n := offlineNode(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	payload, root, _, carBytes := scratchLeafDAG(t, ctx, 1_200_000)
	var q string
	var mu sync.Mutex
	gw := carServer(carBytes, &q, &mu)
	defer gw.Close()

	bs, stop := peerlessBitswapOver(t, ctx, n.bstore) // nothing local: the root itself must come from the gateway
	defer stop()
	origDserv, origBserv, origDht := n.dserv, n.bserv, n.dht
	n.dserv, n.bserv, n.dht = merkledag.NewDAGService(bs), bs, &dht.IpfsDHT{}
	t.Cleanup(func() { n.dserv, n.bserv, n.dht = origDserv, origBserv, origDht })
	origGWs, origT := trustlessGateways, rootLibp2pTimeout
	trustlessGateways, rootLibp2pTimeout = []string{gw.URL}, 300*time.Millisecond
	defer func() { trustlessGateways, rootLibp2pTimeout = origGWs, origT }()

	var rec []float64
	var rmu sync.Mutex
	dest := t.TempDir() + "/out.bin"
	err := n.fetchToPathOnce(ctx, root.String(), dest, func(p float64) { rmu.Lock(); rec = append(rec, p); rmu.Unlock() }, nil)
	if err != nil {
		t.Fatalf("fetch must complete via the gateway: %v", err)
	}
	if got, rerr := os.ReadFile(dest); rerr != nil || !bytes.Equal(got, payload) {
		t.Fatalf("materialized bytes differ from the payload (err=%v)", rerr)
	}
	rmu.Lock()
	defer rmu.Unlock()
	if len(rec) < 2 {
		t.Fatalf("expected progress from both phases, got %v", rec)
	}
	hi := 0.0
	for i, p := range rec {
		if p > hi {
			hi = p
		}
		if i > 0 && p < rec[i-1] {
			t.Fatalf("progress ran backwards at tick %d: %v", i, rec)
		}
	}
	if hi < 90 {
		t.Fatalf("the gateway phase never reported real progress (max %.1f): %v", hi, rec)
	}
}

func TestMonotoneProgressHoldsTheHighWaterMarkAndPassesNilThrough(t *testing.T) {
	if monotoneProgress(nil) != nil {
		t.Fatal("nil must stay nil (callers test for it)")
	}
	var got []float64
	f := monotoneProgress(func(p float64) { got = append(got, p) })
	for _, p := range []float64{0, 40, 99, 0, 50, 99, 100} {
		f(p)
	}
	want := []float64{0, 40, 99, 99, 99, 99, 100}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v want %v", got, want)
		}
	}
}

// The resume must ask from the FIRST MISSING leaf, not from 0: with leaf 0 already on disk (a prior attempt's
// .part + tmp), the entity-bytes offset is one chunk in. Teeth: `from := 0` (or ignoring bits) → query says 0:*.
func TestGatewayLeafResumeAsksFromTheFirstMissingLeaf(t *testing.T) {
	n := offlineNode(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	payload, root, rootNode, carBytes, leaves := scratchLeafDAGWithLeaves(t, ctx, 1_500_000)
	var q string
	var mu sync.Mutex
	gw := carServer(carBytes, &q, &mu)
	defer gw.Close()
	resumeFixture(t, n, ctx, rootNode, gw.URL)

	// A prior attempt left leaf 0 on disk: tmp holds its bytes, the bitmap says so.
	dest := t.TempDir() + "/out.bin"
	tmp, err := os.Create(tmpPath(dest))
	if err != nil {
		t.Fatal(err)
	}
	if err := tmp.Truncate(int64(len(payload))); err != nil {
		t.Fatal(err)
	}
	if _, err := tmp.WriteAt(payload[:chunkSize], 0); err != nil {
		t.Fatal(err)
	}
	_ = tmp.Close()
	bits := newPartBits(root.String(), int64(len(payload)), len(leaves))
	bits.set(0)
	if err := savePart(dest, bits); err != nil {
		t.Fatal(err)
	}

	if err := n.writeThrough(ctx, root, rootNode, dest, root.String(), nil, nil); err != nil {
		t.Fatalf("resume must complete: %v", err)
	}
	if got, rerr := os.ReadFile(dest); rerr != nil || !bytes.Equal(got, payload) {
		t.Fatalf("materialized bytes differ from the payload (err=%v)", rerr)
	}
	mu.Lock()
	defer mu.Unlock()
	if want := "entity-bytes=" + strconv.FormatInt(chunkSize, 10) + ":*"; !strings.Contains(q, want) {
		t.Fatalf("resume must start at the first MISSING leaf (%s); query was %q", want, q)
	}
}

// A Cancel pressed while the resume streams must end it promptly and must never reach finalize. Teeth: run the
// resume under nctx instead of userCancelCtx → the stream only dies when the CAR watchdog's 5 s ticker notices
// (elapsed > 3 s); drop the post-resume isCancelled check → errIncomplete instead of "cancelled".
func TestGatewayLeafResumeHonoursUserCancel(t *testing.T) {
	n := offlineNode(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, root, rootNode, carBytes := scratchLeafDAG(t, ctx, 1_500_000)
	// A gateway that sends most of the CAR, then holds the connection open until the client goes away.
	var entered atomic.Int32
	gw := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered.Add(1)
		w.Header().Set("Content-Type", "application/vnd.ipld.car")
		_, _ = w.Write(carBytes[:len(carBytes)-100])
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-r.Context().Done()
	}))
	defer gw.Close()
	resumeFixture(t, n, ctx, rootNode, gw.URL)
	cidStr := root.String()
	clearCancel(cidStr)
	defer clearCancel(cidStr)
	safeGo("test.cancel", func() { // cancel only once the RESUME is provably streaming (the server saw its request)
		for entered.Load() == 0 && ctx.Err() == nil {
			time.Sleep(20 * time.Millisecond)
		}
		time.Sleep(100 * time.Millisecond)
		requestCancel(cidStr)
	})

	dest := t.TempDir() + "/out.bin"
	start := time.Now()
	done := make(chan error, 1)
	safeGo("test.writeThrough", func() { done <- n.writeThrough(ctx, root, rootNode, dest, cidStr, nil, nil) })
	var err error
	select {
	case err = <-done:
	case <-time.After(8 * time.Second):
		t.Fatal("writeThrough did not return after the cancel — the resume stream is not cancellable")
	}
	el := time.Since(start)
	if err == nil || err.Error() != "cancelled" {
		t.Fatalf("want the cancel surfaced as \"cancelled\", got %v", err)
	}
	if el > 3*time.Second {
		t.Fatalf("cancel took %s — the resume only died when the stall watchdog noticed, not on the cancel", el)
	}
	if _, serr := os.Stat(dest); serr == nil {
		t.Fatal("a cancelled resume must not finalize the file")
	}
}

// Hedge losers die AT COMMIT (their own contexts), not when the winner's stream ends minutes later — a loser still
// waiting on headers (the public backend's usual failure) must not ride along for the whole download. Teeth: drop
// the per-candidate cancel at commit → the dead route's request lives until fetchViaGateway returns (~1 s here).
func TestHedgeLosersAreCancelledAtCommitNotAtStreamEnd(t *testing.T) {
	n := offlineNode(t)
	ctx := context.Background()
	pn := merkledag.NodeWithData([]byte("the winner streams slowly; the loser must not wait for it"))
	scratch := blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore()))
	if err := scratch.Put(ctx, pn); err != nil {
		t.Fatal(err)
	}
	carBytes := carOfStore(t, ctx, scratch, pn.Cid())
	var deadCancelled atomic.Int64 // when the dead route's request context was cancelled (unix ns)
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done() // never answers: headers pending until the client gives up on it
		deadCancelled.Store(time.Now().UnixNano())
	}))
	defer dead.Close()
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.ipld.car")
		_, _ = w.Write(carBytes) // the whole CAR at once…
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		time.Sleep(1 * time.Second) // …but the stream stays open a while before EOF
	}))
	defer slow.Close()
	origGWs, origHedge := trustlessGateways, gatewayHedgeDelay
	trustlessGateways, gatewayHedgeDelay = []string{dead.URL, slow.URL}, 50*time.Millisecond
	defer func() { trustlessGateways, gatewayHedgeDelay = origGWs, origHedge }()

	start := time.Now()
	if err := n.fetchViaGateway(ctx, pn.Cid(), -1, nil); err != nil {
		t.Fatal(err)
	}
	end := time.Now()
	for i := 0; i < 50 && deadCancelled.Load() == 0; i++ { // the handler stores after its ctx fires — give it a beat
		time.Sleep(20 * time.Millisecond)
	}
	dc := deadCancelled.Load()
	if dc == 0 {
		t.Fatal("the dead route was never cancelled")
	}
	sinceStart := time.Duration(dc - start.UnixNano())
	if sinceStart > 600*time.Millisecond || end.Sub(start) < 900*time.Millisecond {
		t.Fatalf("loser cancelled %s after start while the winner streamed for %s — losers must die at commit", sinceStart, end.Sub(start))
	}
}

// A route that never starts answering costs at most gatewayHeaderTimeout — the bound on a probe for content no
// gateway has (there is no cheap definitive negative: tools/gwprobe.sh). Teeth: build the streaming client with a
// fixed header timeout (or a multiple of the var) → the fetch outlives the 2× guard; return any other error at the
// cap → the error-text assertion.
func TestAGatewayThatNeverAnswersCostsAtMostTheHeaderTimeout(t *testing.T) {
	n := offlineNode(t)
	ctx := context.Background()
	pn := merkledag.NodeWithData([]byte("content no gateway will ever answer for"))
	hang := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer hang.Close()
	origGWs, origHdr := trustlessGateways, gatewayHeaderTimeout
	trustlessGateways, gatewayHeaderTimeout = []string{hang.URL}, 300*time.Millisecond
	defer func() { trustlessGateways, gatewayHeaderTimeout = origGWs, origHdr }()
	start := time.Now()
	err := n.fetchViaGateway(ctx, pn.Cid(), -1, nil)
	el := time.Since(start)
	if err == nil {
		t.Fatal("a route that never answers must fail")
	}
	if el > 2*gatewayHeaderTimeout {
		t.Fatalf("the probe ran %s — a silent route must be given up on at the header timeout (%s)", el, gatewayHeaderTimeout)
	}
	// Two clocks race at the cap: the transport's per-hop header clock and the per-candidate aggregate timer.
	// Either is the header-timeout give-up; anything else (a mutant erroring early) is not.
	if !strings.Contains(err.Error(), "timeout awaiting response headers") && !strings.Contains(err.Error(), "exceeded gatewayHeaderTimeout") {
		t.Fatalf("the give-up must be the header timeout, got: %v", err)
	}
}

// The public routes are redirect CHAINS and the transport's header timeout is per HOP — so a chain of hops that
// each answer just inside the per-hop clock could multiply the wait. fetchViaGateway arms one aggregate timer per
// candidate over the whole open. Teeth: drop the AfterFunc → eight 100 ms hops then a hang runs ~0.8 s + a full
// per-hop wait, past the 2× guard.
func TestARedirectChainCannotMultiplyTheHeaderTimeout(t *testing.T) {
	n := offlineNode(t)
	pn := merkledag.NodeWithData([]byte("content at the end of a redirect chain that never ends"))
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hop, _ := strconv.Atoi(r.URL.Query().Get("hop"))
		if hop < 8 {
			time.Sleep(100 * time.Millisecond) // inside the 400 ms per-hop clock, so only the aggregate bound can end this
			http.Redirect(w, r, srv.URL+r.URL.Path+"?hop="+strconv.Itoa(hop+1), http.StatusMovedPermanently)
			return
		}
		<-r.Context().Done()
	}))
	defer srv.Close()
	origGWs, origHdr, origHedge := trustlessGateways, gatewayHeaderTimeout, gatewayHedgeDelay
	trustlessGateways, gatewayHeaderTimeout, gatewayHedgeDelay = []string{srv.URL}, 400*time.Millisecond, time.Hour
	defer func() { trustlessGateways, gatewayHeaderTimeout, gatewayHedgeDelay = origGWs, origHdr, origHedge }()
	start := time.Now()
	err := n.fetchViaGateway(context.Background(), pn.Cid(), -1, nil)
	el := time.Since(start)
	if err == nil {
		t.Fatal("the chain never serves: the fetch must fail")
	}
	if el > 2*gatewayHeaderTimeout {
		t.Fatalf("open ran %s — the redirect hops multiplied the %s header timeout instead of sharing one aggregate bound", el, gatewayHeaderTimeout)
	}
	if !strings.Contains(err.Error(), "exceeded gatewayHeaderTimeout") {
		t.Fatalf("the give-up must name the aggregate bound, got: %v", err)
	}
}

// The fetch NARRATES itself: every state change emits a phase line the UI shows verbatim — hunting, falling back,
// stalling and backing off must never look like a stuck "Downloading". Teeth (each caught): drop the attempt line
// in fetchToPathLoopUntil, the gateway-fallback line in getRoot, or the commit line in fetchViaGateway → the
// matching assertion fails.
func TestAFetchNarratesItsPhases(t *testing.T) {
	n := offlineNode(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	payload, root, _, carBytes := scratchLeafDAG(t, ctx, 600_000)
	var q string
	var mu sync.Mutex
	gw := carServer(carBytes, &q, &mu)
	defer gw.Close()

	bs, stop := peerlessBitswapOver(t, ctx, n.bstore) // nothing local, no peers: p2p must fail → gateway
	defer stop()
	origDserv, origBserv, origDht := n.dserv, n.bserv, n.dht
	n.dserv, n.bserv, n.dht = merkledag.NewDAGService(bs), bs, &dht.IpfsDHT{}
	t.Cleanup(func() { n.dserv, n.bserv, n.dht = origDserv, origBserv, origDht })
	origGWs, origT := trustlessGateways, rootLibp2pTimeout
	trustlessGateways, rootLibp2pTimeout = []string{gw.URL}, 300*time.Millisecond
	defer func() { trustlessGateways, rootLibp2pTimeout = origGWs, origT }()

	var pmu sync.Mutex
	var lines []string
	phaseHook = func(cid, text string) {
		if cid == root.String() {
			pmu.Lock()
			lines = append(lines, text)
			pmu.Unlock()
		}
	}
	defer func() { phaseHook = nil }()

	dest := t.TempDir() + "/out.bin"
	if err := n.fetchToPathOnce(ctx, root.String(), dest, nil, nil); err != nil {
		t.Fatalf("fetch must complete via the gateway: %v", err)
	}
	if got, rerr := os.ReadFile(dest); rerr != nil || !bytes.Equal(got, payload) {
		t.Fatalf("materialized bytes differ (err=%v)", rerr)
	}
	pmu.Lock()
	defer pmu.Unlock()
	joined := strings.Join(lines, "\n")
	for _, want := range []string{"locating providers", "trying HTTPS gateways", "downloading from 127.0.0.1"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("the narration must contain %q; got:\n%s", want, joined)
		}
	}
}

// The retry loop's narration: with nothing to fetch from, each attempt announces itself.
func TestTheRetryLoopNarratesItsAttempts(t *testing.T) {
	n := offlineNode(t) // fully wired (fstore/pinner), dht nil → getRoot never touches gateways, terminal fast
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bs, stop := peerlessBitswapOver(t, ctx, n.bstore)
	origDserv, origBserv := n.dserv, n.bserv
	n.dserv, n.bserv = merkledag.NewDAGService(bs), bs
	t.Cleanup(func() { n.dserv, n.bserv = origDserv, origBserv; stop() })
	pn := merkledag.NodeWithData([]byte("nobody has this"))

	var pmu sync.Mutex
	var lines []string
	phaseHook = func(cid, text string) { pmu.Lock(); lines = append(lines, text); pmu.Unlock() }
	defer func() { phaseHook = nil }()
	origT := rootLibp2pTimeout
	rootLibp2pTimeout = 200 * time.Millisecond
	defer func() { rootLibp2pTimeout = origT }()

	_ = n.fetchToPathLoopUntil(pn.Cid().String(), t.TempDir()+"/out.bin", nil, nil, time.Now().Add(1*time.Second))
	pmu.Lock()
	defer pmu.Unlock()
	joined := strings.Join(lines, "\n")
	if !strings.Contains(joined, "attempt 1 — connecting to providers") {
		t.Fatalf("attempt narration missing; got:\n%s", joined)
	}
}
