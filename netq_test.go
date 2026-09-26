package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	cid "github.com/ipfs/go-cid"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	peer "github.com/libp2p/go-libp2p/core/peer"
)

func withQueue(t *testing.T, slots int) {
	old := netq
	netq = newNetQueue(slots)
	t.Cleanup(func() { netq = old })
}

func acquired(fg bool) (chan func(), context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	got := make(chan func(), 1)
	go func() {
		if r, ok := netq.acquire(ctx, fg); ok {
			got <- r
		}
	}()
	return got, cancel
}

func waitGot(t *testing.T, ch chan func(), what string) func() {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(2 * time.Second):
		t.Fatalf("%s: never granted", what)
		return nil
	}
}

func notGot(t *testing.T, ch chan func(), what string) {
	t.Helper()
	select {
	case <-ch:
		t.Fatalf("%s: granted past the bound", what)
	case <-time.After(100 * time.Millisecond):
	}
}

// At most `slots` jobs hold the queue; a freed slot rolls straight to the next waiter.
func TestNetQueueBoundsAndRolls(t *testing.T) {
	withQueue(t, 2)
	a, _ := netq.acquire(context.Background(), true)
	b, _ := netq.acquire(context.Background(), false)
	c, _ := acquired(true)
	notGot(t, c, "third job")
	a()
	rc := waitGot(t, c, "third job after a release")
	b()
	rc()
	if act, w := netq.load(); act != 0 || w != 0 {
		t.Fatalf("after all released: %d active, %d waiting", act, w)
	}
}

// A waiting fetch (foreground) gets the next slot before a waiting announce (background), whatever the arrival order.
func TestNetQueueGivesFetchesPriority(t *testing.T) {
	withQueue(t, 1)
	hold, _ := netq.acquire(context.Background(), true)
	bg, _ := acquired(false)
	time.Sleep(20 * time.Millisecond)
	fg, _ := acquired(true)
	time.Sleep(20 * time.Millisecond)
	hold()
	rf := waitGot(t, fg, "fetch")
	notGot(t, bg, "announce while the fetch holds the slot")
	rf()
	waitGot(t, bg, "announce once the fetch is done")()
}

// A waiter whose job is over leaves the queue (its slot is never granted to nobody); a larger size lets waiters in.
func TestNetQueueCancelAndResize(t *testing.T) {
	withQueue(t, 1)
	hold, _ := netq.acquire(context.Background(), true)
	gone, cancel := acquired(false)
	time.Sleep(20 * time.Millisecond)
	cancel()
	time.Sleep(20 * time.Millisecond)
	if _, w := netq.load(); w != 0 {
		t.Fatalf("a cancelled waiter is still queued (%d waiting)", w)
	}
	notGot(t, gone, "cancelled waiter")
	next, _ := acquired(true)
	notGot(t, next, "second job at size 1")
	netq.setSlots(2)
	waitGot(t, next, "second job after resizing to 2")()
	hold()
}

type fakeClosest struct{ calls atomic.Int32 }

func (f *fakeClosest) GetClosestPeers(context.Context, string) ([]peer.ID, error) {
	f.calls.Add(1)
	return nil, nil
}

type fakeSender struct{ calls atomic.Int32 }

func (f *fakeSender) SendRequest(context.Context, peer.ID, *pb.Message) (*pb.Message, error) {
	f.calls.Add(1)
	return nil, nil
}
func (f *fakeSender) SendMessage(context.Context, peer.ID, *pb.Message) error {
	f.calls.Add(1)
	return nil
}

// The provider's walks and record sends take the SAME queue as fetches: while fetches hold every slot, announcing
// waits; a walk whose context ends while waiting never reaches the network.
func TestProviderTrafficWaitsBehindFetches(t *testing.T) {
	withQueue(t, 1)
	fetch, _ := netq.acquire(context.Background(), true)
	fc, fs := &fakeClosest{}, &fakeSender{}
	r, s := queuedRouter{fc}, queuedSender{inner: fs}
	walked, sent := make(chan struct{}), make(chan struct{})
	go func() { _, _ = r.GetClosestPeers(context.Background(), "k"); close(walked) }()
	go func() { _ = s.SendMessage(context.Background(), "p", nil); close(sent) }()
	time.Sleep(100 * time.Millisecond)
	if fc.calls.Load() != 0 {
		t.Fatal("a provide walk reached the network while a fetch held the only slot")
	}
	if fs.calls.Load() != 0 {
		t.Fatal("a provider record went out while a fetch held the only slot")
	}
	fetch()
	for _, ch := range []chan struct{}{walked, sent} {
		select {
		case <-ch:
		case <-time.After(2 * time.Second):
			t.Fatal("the provider never got the freed slot")
		}
	}
	if fc.calls.Load() != 1 || fs.calls.Load() != 1 {
		t.Fatalf("walk %d, send %d — want 1 each", fc.calls.Load(), fs.calls.Load())
	}
	hold, _ := netq.acquire(context.Background(), true)
	defer hold()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := r.GetClosestPeers(ctx, "k"); !errors.Is(err, context.DeadlineExceeded) || fc.calls.Load() != 1 {
		t.Fatalf("a walk that gave up waiting still ran (err %v, calls %d)", err, fc.calls.Load())
	}
}

// A record to a peer we are already connected to opens no new flow: it goes out while fetches hold every slot.
func TestProviderRecordsToConnectedPeersSkipTheQueue(t *testing.T) {
	withQueue(t, 1)
	fetch, _ := netq.acquire(context.Background(), true)
	defer fetch()
	fs := &fakeSender{}
	s := queuedSender{inner: fs, connected: func(p peer.ID) bool { return p == "near" }}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.SendMessage(ctx, "near", nil); err != nil || fs.calls.Load() != 1 {
		t.Fatalf("a record to a connected peer waited for the queue (err %v, sent %d)", err, fs.calls.Load())
	}
	short, stop := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer stop()
	if err := s.SendMessage(short, "far", nil); err == nil || fs.calls.Load() != 1 {
		t.Fatalf("a record needing a new connection skipped the queue (err %v, sent %d)", err, fs.calls.Load())
	}
}

// Keys given before the provider can take them are kept (and marked seeding), and a key we stop holding leaves.
func TestStartProvidingKeepsWhatItCannotHandOverYet(t *testing.T) {
	n := &node{seedDone: map[string]struct{}{}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	n.ctx = ctx
	c1 := gateCid
	c2, _ := cid.Decode("bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku")
	n.startProviding(c1, c2)
	if len(n.provideSt.pending) != 2 || !n.seedAnnounced(c1.String()) {
		t.Fatalf("pending %d, seeding %v", len(n.provideSt.pending), n.seedAnnounced(c1.String()))
	}
	n.stopProviding(c2)
	if len(n.provideSt.pending) != 1 {
		t.Fatalf("stopped key still pending (%d)", len(n.provideSt.pending))
	}
}

// fgIdleNow reports whether waitFgIdle returns at once.
func fgIdleNow() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	return netq.waitFgIdle(ctx)
}

// Whole-datastore compaction waits for the installs: idle means no fetch holds a slot AND none is waiting for one.
// Background jobs (announcing) never count. Teeth: drop the fgActive accounting in grantLocked/releaser, or the
// markFgIdleIfLocked call in the cancelled-waiter path, and one of these states reads wrong.
func TestNetQueueForegroundIdle(t *testing.T) {
	withQueue(t, 1)
	if !fgIdleNow() {
		t.Fatal("a fresh queue must read idle")
	}

	bg, cancelBg := acquired(false)
	defer cancelBg()
	relBg := <-bg
	if !fgIdleNow() {
		t.Fatal("a background job holding the slot must not block the idle signal")
	}

	fg, cancelFg := acquired(true) // waits behind the background job
	time.Sleep(20 * time.Millisecond)
	if fgIdleNow() {
		t.Fatal("a fetch waiting for a slot must read busy")
	}
	cancelFg() // the fetch gives up while waiting
	time.Sleep(20 * time.Millisecond)
	if !fgIdleNow() {
		t.Fatal("after the only waiting fetch gave up the queue must read idle again")
	}

	fg2, cancelFg2 := acquired(true)
	defer cancelFg2()
	time.Sleep(20 * time.Millisecond)
	relBg() // the slot passes to the waiting fetch
	relFg := <-fg2
	if fgIdleNow() {
		t.Fatal("a fetch holding a slot must read busy")
	}

	done := make(chan bool, 1)
	go func() { done <- netq.waitFgIdle(context.Background()) }()
	relFg()
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("waitFgIdle returned false without its context ending")
		}
	case <-time.After(time.Second):
		t.Fatal("releasing the last fetch must wake a waiter")
	}
	_ = fg
}
