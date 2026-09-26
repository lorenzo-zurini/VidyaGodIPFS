package main

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p/core/peer"
	routing "github.com/libp2p/go-libp2p/core/routing"
)

// gateRouter yields its providers and counts how often it was asked.
type gateRouter struct {
	calls atomic.Int32
	provs []peer.AddrInfo
}

func (r *gateRouter) FindProvidersAsync(ctx context.Context, _ cid.Cid, _ int) <-chan peer.AddrInfo {
	r.calls.Add(1)
	out := make(chan peer.AddrInfo, len(r.provs))
	for _, p := range r.provs {
		out <- p
	}
	close(out)
	return out
}

func withSearchDelay(t *testing.T, d time.Duration) {
	old := providerSearchDelay
	providerSearchDelay = d
	t.Cleanup(func() { providerSearchDelay = old })
}

func drainProvs(ch <-chan peer.AddrInfo) int {
	n := 0
	for range ch {
		n++
	}
	return n
}

var gateCid = func() cid.Cid {
	c, _ := cid.Decode("bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy")
	return c
}()

// A connected friend is asked first; the DHT and the indexers wait the search delay and are never asked when the
// fetch is over meanwhile (the session ends → ctx cancelled). This is what stops a receive from a friend walking the
// DHT once per block.
func TestFinderHoldsTheNetworkBehindAConnectedFriend(t *testing.T) {
	withSearchDelay(t, 300*time.Millisecond)
	friend := &gateRouter{provs: []peer.AddrInfo{{ID: peer.ID("friend")}}}
	dht, idx := &gateRouter{}, &gateRouter{}
	cf := combinedFinder{routers: []routing.ContentDiscovery{friend, dht, idx}, hold: func() bool { return true }}

	ctx, cancel := context.WithCancel(context.Background())
	ch := cf.FindProvidersAsync(ctx, gateCid, 0)
	if got := <-ch; got.ID != "friend" {
		t.Fatalf("first provider = %q, want the friend", got.ID)
	}
	cancel() // the block arrived: the session ends before the delay
	drainProvs(ch)
	if dht.calls.Load() != 0 || idx.calls.Load() != 0 {
		t.Fatalf("DHT asked %d, indexer %d times while a friend was serving — want 0", dht.calls.Load(), idx.calls.Load())
	}

	// Still unserved after the delay (the friend lacks the block): the network is asked.
	ch = cf.FindProvidersAsync(context.Background(), gateCid, 0)
	drainProvs(ch)
	if dht.calls.Load() != 1 || idx.calls.Load() != 1 {
		t.Fatalf("after the delay: DHT asked %d, indexer %d — want 1 each", dht.calls.Load(), idx.calls.Load())
	}
}

// No connected friend: the network is asked at once, not after the delay.
func TestFinderAsksTheNetworkAtOnceWithoutAFriend(t *testing.T) {
	withSearchDelay(t, 5*time.Second)
	friend, dht := &gateRouter{}, &gateRouter{provs: []peer.AddrInfo{{ID: peer.ID("seeder")}}}
	cf := combinedFinder{routers: []routing.ContentDiscovery{friend, dht}, hold: func() bool { return false }}
	start := time.Now()
	ch := cf.FindProvidersAsync(context.Background(), gateCid, 0)
	if got := <-ch; got.ID != "seeder" {
		t.Fatalf("got %q, want the DHT's provider", got.ID)
	}
	if time.Since(start) > time.Second {
		t.Fatalf("the DHT waited %s with no friend to wait for", time.Since(start))
	}
	drainProvs(ch)
}

// A warm walk runs only for a fetch still alive and unmoved after the search delay.
func TestWalkNeededOnlyForAStalledLiveFetch(t *testing.T) {
	withSearchDelay(t, 50*time.Millisecond)
	if walkNeeded(context.Background(), func() bool { return true }) {
		t.Fatal("a fetch that moved still walked")
	}
	ended, cancel := context.WithCancel(context.Background())
	cancel()
	if walkNeeded(ended, nil) {
		t.Fatal("a finished fetch still walked")
	}
	if !walkNeeded(context.Background(), func() bool { return false }) {
		t.Fatal("a stalled fetch did not walk")
	}
	if !walkNeeded(context.Background(), nil) {
		t.Fatal("a fetch with no progress signal did not walk")
	}
	// A fetch that ends releases its waiting walk at once (it does not sit out the delay).
	providerSearchDelay = 5 * time.Second
	ctx, stop := context.WithCancel(context.Background())
	go func() { time.Sleep(20 * time.Millisecond); stop() }()
	t0 := time.Now()
	walkNeeded(ctx, nil)
	if time.Since(t0) > time.Second {
		t.Fatalf("a finished fetch held its walk for %s", time.Since(t0))
	}
}

// Moving = a report above the first one (a resume's first report is what it already had).
func TestProgressWatchMovesOnlyAboveTheFirstReport(t *testing.T) {
	var seen []float64
	cb, moved := progressWatch(func(p float64) { seen = append(seen, p) })
	if moved() {
		t.Fatal("moved before any report")
	}
	cb(40)
	cb(40)
	if moved() {
		t.Fatal("a repeat of the first report counted as progress")
	}
	cb(41)
	if !moved() {
		t.Fatal("progress above the first report not seen")
	}
	if len(seen) != 3 {
		t.Fatalf("wrapped callback saw %v", seen)
	}
	cb2, _ := progressWatch(nil) // nil callbacks are fine
	cb2(1)
}

func TestNetStatsLinePrintsChangesOnly(t *testing.T) {
	a := netSnapshot{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	if netStatsLine(a, a, 9) != "" {
		t.Fatal("printed with nothing changed")
	}
	b := a
	b[0] += 12
	b[4] += 3
	l := netStatsLine(a, b, 9)
	if !strings.Contains(l, "+12 conns, 0 inbound (9 peers)") || !strings.Contains(l, "dht +3") || !strings.Contains(l, "indexer +0") {
		t.Fatalf("line = %q", l)
	}
}
