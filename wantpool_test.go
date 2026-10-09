package main

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
	cid "github.com/ipfs/go-cid"
	datastore "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
)

// The pool circulates about wantLatency of downloading: the landing rate times wantLatency, between wantFloor and its
// size; tokens parked while in use are parked as they come back; stopping gives every token back. Teeth: a fixed
// size; no floor (a quiet spell would shut the pool); no cap; keep tokens parked after adapt ends.
func TestWantPoolFollowsTheLink(t *testing.T) {
	p := newWantPool(768)
	land := func(perSec int, secs int) {
		for i := 0; i < secs; i++ {
			p.delivered.Add(int64(perSec))
			p.step(1)
		}
	}
	land(16, 10) // ~4 MB/s: 16 blocks/s × 2 s = 32
	if got := p.limit.Load(); got != int64(wantFloor) || p.available() != wantFloor {
		t.Fatalf("at 16 blocks/s the pool circulates %d (available %d), want the floor %d", got, p.available(), wantFloor)
	}
	land(100, 10) // 100 blocks/s × 2 s ≈ 200
	if got := p.limit.Load(); got < 190 || got > 210 {
		t.Fatalf("at 100 blocks/s the pool circulates %d, want ~200", got)
	}
	land(1000, 10) // a LAN: capped at the pool's size
	if got := p.limit.Load(); got != 768 || p.available() != 768 {
		t.Fatalf("at 1000 blocks/s the pool circulates %d (available %d), want all 768", got, p.available())
	}
	land(0, 10) // a quiet spell: the floor, not zero
	if got := p.limit.Load(); got != int64(wantFloor) {
		t.Fatalf("idle, the pool circulates %d, want the floor %d", got, wantFloor)
	}

	// Tokens in use are parked as they come back.
	q := newWantPool(100)
	for i := 0; i < 100; i++ {
		q.tryAcquire()
	}
	q.step(1) // rate 0 → floor, but every token is out
	if q.limit.Load() != 100 {
		t.Fatalf("with every token out the pool claims %d circulating, want 100 until they return", q.limit.Load())
	}
	for i := 0; i < 100; i++ {
		q.release()
	}
	q.step(1)
	if q.available() != wantFloor {
		t.Fatalf("returned tokens were not parked: %d available, want %d", q.available(), wantFloor)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { q.adapt(ctx, time.Hour); close(done) }()
	cancel()
	<-done
	if q.available() != 100 {
		t.Fatalf("adapt ended with %d of 100 tokens available: it kept some parked", q.available())
	}
}

// The pool is process-global and every node open starts an adapt on it, so a closing node's controller (its final
// setLimit gives every token back) runs beside the next node's. Under -race that must not race, and when both end
// every token is back. Teeth (go test -race): drop wantPool.mu.
func TestTwoControllersShareThePool(t *testing.T) {
	p := newWantPool(768)
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				p.delivered.Add(50)
				time.Sleep(100 * time.Microsecond)
			}
		}
	}()
	ctxA, cancelA := context.WithCancel(context.Background())
	ctxB, cancelB := context.WithCancel(context.Background())
	doneA, doneB := make(chan struct{}), make(chan struct{})
	go func() { p.adapt(ctxA, time.Millisecond); close(doneA) }()
	go func() { p.adapt(ctxB, time.Millisecond); close(doneB) }()
	time.Sleep(50 * time.Millisecond)
	cancelA() // the closing node's controller gives its tokens back while the other keeps sizing the pool
	<-doneA
	time.Sleep(20 * time.Millisecond)
	cancelB()
	<-doneB
	close(stop)
	if p.available() != 768 {
		t.Fatalf("both controllers ended with %d of 768 tokens available", p.available())
	}
}

// The laptop replication's stalls, on a 4 MB/s link: a small fetch started beside a big one must not queue behind the
// big one's whole budget at the provider. With the pool fixed at 768 (384 for one fetch) the big fetch had ~96 MB
// queued there — ~24 s at 4 MB/s — and the small fetch got no block for 20 s and was torn down; with the pool sized
// to the link, ~2 s is queued and the small fetch lands within seconds. Teeth: don't adapt the pool.
func TestASmallFetchIsNotQueuedBehindABigOneOnASlowLink(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	saved := globalWantPool
	globalWantPool = newWantPool(wantBudget)
	defer func() { globalWantPool = saved }()
	go globalWantPool.adapt(ctx, 200*time.Millisecond)

	mn, err := mocknet.FullMeshLinked(2)
	if err != nil {
		t.Fatal(err)
	}
	defer mn.Close()
	mn.SetLinkDefaults(mocknet.LinkOptions{Latency: 20 * time.Millisecond, Bandwidth: 4 << 20})
	hosts := mn.Hosts()
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
	big, small := leaves(64<<20), leaves(2<<20)
	bswapA := bitswap.New(ctx, bsnet.NewFromIpfsHost(hosts[0]), nilFinder{}, bstoreA, bitswapOptions(nil)...)
	defer bswapA.Close()
	bstoreB := blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore()))
	bswapB := bitswap.New(ctx, bsnet.NewFromIpfsHost(hosts[1]), nilFinder{}, bstoreB, bitswapOptions(nil)...)
	defer bswapB.Close()
	if err := mn.ConnectAllButSelf(); err != nil {
		t.Fatal(err)
	}
	for _, l := range mn.LinksBetweenPeers(hosts[0].ID(), hosts[1].ID()) { // defaults do not reach existing links
		l.SetOptions(mocknet.LinkOptions{Latency: 20 * time.Millisecond, Bandwidth: 4 << 20})
	}
	bsvc := blockservice.New(bstoreB, bswapB)
	fetch := func(l []cid.Cid, done chan<- time.Duration) {
		start := time.Now()
		n := 0
		for range rollingGetBlocks(ctx, blockservice.NewSession(ctx, bsvc), l, 32) {
			n++
		}
		if n != len(l) {
			t.Errorf("%d of %d leaves", n, len(l))
		}
		done <- time.Since(start)
	}
	bigDone, smallDone := make(chan time.Duration, 1), make(chan time.Duration, 1)
	go fetch(big, bigDone)
	time.Sleep(3 * time.Second) // the big fetch's queue at the provider has built up
	go fetch(small, smallDone)
	s := <-smallDone
	t.Logf("small fetch (%d leaves) beside a big one: %s; pool circulating %d", len(small), s.Round(100*time.Millisecond), globalWantPool.limit.Load())
	if s > 3500*time.Millisecond {
		t.Fatalf("the small fetch took %s beside the big one: it queued behind the big one's budget at the provider", s)
	}
	<-bigDone
}

// On a fast link the pool opens up: the blocks landing are counted, and their rate sizes the pool. Teeth: count no
// arrivals (the pool would sit at its floor and throttle a LAN to 32 wants in flight).
func TestTheWantPoolOpensOnAFastLink(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	saved := globalWantPool
	globalWantPool = newWantPool(wantBudget)
	defer func() { globalWantPool = saved }()
	globalWantPool.setLimit(wantFloor) // start shut, as after a quiet spell
	go globalWantPool.adapt(ctx, 20*time.Millisecond)
	h := setupTwoNode(t, ctx, 32<<20)
	peak := int64(0)
	for range rollingGetBlocks(ctx, h.sess, h.leaves, 32) {
		if l := globalWantPool.limit.Load(); l > peak {
			peak = l
		}
	}
	t.Logf("peak circulating during a fast fetch: %d", peak)
	if peak <= int64(4*wantFloor) {
		t.Fatalf("on a fast link the pool never opened past %d: arrivals are not sizing it", peak)
	}
}
