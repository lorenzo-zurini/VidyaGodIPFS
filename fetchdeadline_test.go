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
	"errors"
	"sync/atomic"
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

// (c) A bounded caller that JOINS an unbounded in-flight leader for the same dest must stop waiting at its own
// deadline (not block on the leader) AND must not run its own fetch fn (its C callback strings would be freed on
// return — running them from the leader's goroutine after that is the UAF). Teeth: make dedupFetch's joiner wait
// on fw.done only (drop the timer select) and the joiner blocks until we release the leader → the 3s guard fires.
func TestBoundedFetchAbandonsUnboundedLeader(t *testing.T) {
	n := &node{ctx: context.Background()}

	leaderStarted := make(chan struct{})
	releaseLeader := make(chan struct{})
	leaderDone := make(chan struct{})
	var leaderRuns, joinerRuns atomic.Int32

	go func() {
		defer close(leaderDone)
		n.dedupFetch("/same/dest", "cidLEADER", time.Time{}, func(dl time.Time) error {
			leaderRuns.Add(1)
			close(leaderStarted)
			<-releaseLeader // an unbounded leader that does not finish on its own
			return nil
		})
	}()
	<-leaderStarted // the leader now owns the dest; a second caller is a JOINER

	type result struct {
		err error
		ran bool
	}
	resCh := make(chan result, 1)
	go func() {
		err, ran, _ := n.dedupFetch("/same/dest", "cidJOINER", time.Now().Add(300*time.Millisecond), func(dl time.Time) error {
			joinerRuns.Add(1)
			return nil
		})
		resCh <- result{err, ran}
	}()

	select {
	case r := <-resCh:
		if r.ran {
			t.Error("the abandoning joiner must report ran=false (it waited, it did not drive the fetch)")
		}
		if r.err == nil || !errors.Is(r.err, errFetchDeadline) {
			t.Errorf("the joiner should return a deadline error, got %v", r.err)
		}
	case <-time.After(3 * time.Second):
		close(releaseLeader)
		<-leaderDone
		t.Fatal("the bounded joiner did not abandon the unbounded leader at its deadline — it blocked (the pass-1 hang)")
	}

	if got := joinerRuns.Load(); got != 0 {
		t.Errorf("the joiner ran its own fetch fn %d time(s); only the leader may run (else the joiner's freed C callbacks fire → UAF)", got)
	}

	close(releaseLeader)
	<-leaderDone
	if got := leaderRuns.Load(); got != 1 {
		t.Errorf("the leader fn should run exactly once, ran %d", got)
	}
}

// backoffWait must never sleep past the deadline: near it, the backoff is clamped to the remaining budget so the
// loop's top-of-iteration deadline check ends the fetch on time. Teeth: delete the `if d > rem { d = rem }` clamp
// in backoffWait and this sleeps the full 10s backoff (elapsed ≫ bound) → fails. Second call proves rem<=0 exits
// with the deadline error (no sleep at all).
func TestBackoffWaitClampsToRemainingBudget(t *testing.T) {
	n := &node{ctx: context.Background()}

	start := time.Now()
	exit, _ := n.backoffWait("cidX", "/no/such/dest", 10*time.Second, time.Now().Add(250*time.Millisecond))
	elapsed := time.Since(start)
	if exit {
		t.Fatal("a positive remaining budget is not itself an exit — the loop-top check ends the fetch after the clamped sleep")
	}
	if elapsed > 3*time.Second {
		t.Fatalf("backoffWait did not clamp the backoff to the remaining budget: slept %s (want ~250ms)", elapsed)
	}

	exit, err := n.backoffWait("cidX", "/no/such/dest", 2*time.Second, time.Now().Add(-time.Second))
	if !exit || err == nil || !errors.Is(err, errFetchDeadline) {
		t.Fatalf("an exhausted budget must exit immediately with a deadline error, got exit=%v err=%v", exit, err)
	}
}

// A panic in the leader's fetch fn must still unlock the dest and release joiners — else a single panic wedges
// that dest forever (every later fetch of it hangs on a done channel that never closes). Teeth: change dedupFetch's
// leader cleanup from a defer back to straight-line code after run() and this hangs at the 2s guard.
func TestDedupFetchReleasesDestOnLeaderPanic(t *testing.T) {
	n := &node{ctx: context.Background()}

	func() {
		defer func() { _ = recover() }() // the panic propagates out of dedupFetch after its defer cleans up
		n.dedupFetch("/panic/dest", "cidP", time.Time{}, func(dl time.Time) error { panic("boom") })
	}()

	ran := make(chan struct{})
	go func() {
		n.dedupFetch("/panic/dest", "cidP2", time.Time{}, func(dl time.Time) error { close(ran); return nil })
	}()
	select {
	case <-ran: // the dest was unlocked → this call became a fresh leader and ran
	case <-time.After(2 * time.Second):
		t.Fatal("dest stayed locked after a leader panic — a later fetch hung (deferred cleanup missing)")
	}
}

// LEADERSHIP HANDOFF (pass-5 HIGH): a bounded leader that gives up on ITS deadline must NOT drag down an unbounded
// waiter of the same dest — the unbounded waiter takes over and the fetch continues (resuming from .part). Teeth:
// delete the `errors.Is(lerr, errFetchDeadline) ... continue` handoff arm in dedupFetch and the unbounded joiner
// inherits the leader's deadline error → the assertion that it completed with nil fails.
func TestUnboundedJoinerTakesOverBoundedLeaderGiveUp(t *testing.T) {
	n := &node{ctx: context.Background()}
	var leaderRuns, joinerRuns atomic.Int32
	leaderStarted := make(chan struct{})

	go func() {
		n.dedupFetch("/handoff/dest", "cidBounded", time.Now().Add(150*time.Millisecond), func(dl time.Time) error {
			leaderRuns.Add(1)
			close(leaderStarted)
			<-time.After(time.Until(dl)) // run until our budget expires, then give up on the deadline
			return deadlineErr("cidBounded")
		})
	}()
	<-leaderStarted // the bounded leader now owns the dest; the next caller is a JOINER

	resCh := make(chan error, 1)
	go func() {
		err, _, _ := n.dedupFetch("/handoff/dest", "cidUnbounded", time.Time{}, func(dl time.Time) error {
			joinerRuns.Add(1)
			return nil // as the new leader the fetch continues and completes
		})
		resCh <- err
	}()

	select {
	case err := <-resCh:
		if err != nil {
			t.Fatalf("the unbounded joiner inherited the bounded leader's give-up instead of taking over: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the unbounded joiner never completed — no leadership handoff (it blocked or died on the leader's error)")
	}
	if got := leaderRuns.Load(); got != 1 {
		t.Errorf("the bounded leader should have run once, ran %d", got)
	}
	if got := joinerRuns.Load(); got != 1 {
		t.Errorf("the unbounded joiner should have taken over and run once, ran %d", got)
	}
}

// LOW-1 fix (pass 7): a LEADER that gives up on its deadline must report handedOff ONLY when a successor is queued
// to take over (so the bounded C wrapper suppresses the terminal Errored and the successor owns the row); a SOLE
// give-up must report handedOff=false so its Errored still fires (no stuck "Downloading" spinner). Teeth: force
// `lead.waiters > 0` to a constant in runFetchAsLeader and one of the two cases below flips.
func TestLeaderHandoffSuppressesTerminalEmitOnlyWithAWaiter(t *testing.T) {
	n := &node{ctx: context.Background()}

	// Case A — leader gives up WITH an unbounded successor queued → handedOff=true.
	leaderIn := make(chan struct{})
	release := make(chan struct{})
	type res struct {
		handedOff bool
	}
	lres := make(chan res, 1)
	go func() {
		_, _, ho := n.dedupFetch("/ho/withwaiter", "L", time.Now().Add(10*time.Second), func(dl time.Time) error {
			close(leaderIn)
			<-release
			return deadlineErr("L") // give up on our deadline
		})
		lres <- res{ho}
	}()
	<-leaderIn
	took := make(chan struct{})
	go func() {
		n.dedupFetch("/ho/withwaiter", "U", time.Time{}, func(dl time.Time) error { close(took); return nil })
	}()
	time.Sleep(100 * time.Millisecond) // let the successor register as a waiter before the leader gives up
	close(release)
	if got := <-lres; !got.handedOff {
		t.Error("a leader that gave up with a queued successor must report handedOff=true (successor owns the row)")
	}
	select {
	case <-took:
	case <-time.After(3 * time.Second):
		t.Fatal("the queued successor never took over")
	}

	// Case B — SOLE leader give-up, no waiter → handedOff=false (its terminal event must still fire).
	_, _, ho := n.dedupFetch("/ho/solo", "S", time.Now().Add(10*time.Second), func(dl time.Time) error {
		return deadlineErr("S")
	})
	if ho {
		t.Error("a sole leader give-up (no waiter) must report handedOff=false so its terminal Errored still fires")
	}
}

// The fw.waiters-- decrements on the abandon arms (timerC / ctx.Done) are load-bearing: if a bounded joiner
// abandons but its count leaks, a LATER sole leader give-up sees waiters>0 and wrongly suppresses its terminal
// Errored (the stuck-row case). Teeth: delete the timerC-arm `fw.waiters--` and this fails (handedOff becomes true).
func TestAbandonedJoinerDecrementsWaiters(t *testing.T) {
	n := &node{ctx: context.Background()}
	leaderIn := make(chan struct{})
	release := make(chan struct{})
	handedOff := make(chan bool, 1)
	go func() {
		_, _, ho := n.dedupFetch("/decrement/dest", "L", time.Now().Add(10*time.Second), func(dl time.Time) error {
			close(leaderIn)
			<-release
			return deadlineErr("L")
		})
		handedOff <- ho
	}()
	<-leaderIn

	// A bounded joiner that ABANDONS at its short deadline (the leader is still parked, so it can't take over).
	joinerDone := make(chan struct{})
	go func() {
		n.dedupFetch("/decrement/dest", "J", time.Now().Add(80*time.Millisecond), func(dl time.Time) error { return nil })
		close(joinerDone)
	}()
	<-joinerDone // the joiner has fully returned → its waiters-- must have run (count back to 0)

	close(release) // now the sole leader gives up; with the decrement it sees waiters==0 → handedOff=false
	if <-handedOff {
		t.Error("an abandoned joiner must decrement waiters; a leaked count makes a later sole give-up wrongly suppress its terminal event")
	}
}
