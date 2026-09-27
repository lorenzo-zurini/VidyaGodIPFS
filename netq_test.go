package main

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	cid "github.com/ipfs/go-cid"
	datastore "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	dssync "github.com/ipfs/go-datastore/sync"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	stats "github.com/libp2p/go-libp2p-kad-dht/provider/stats"
	peer "github.com/libp2p/go-libp2p/core/peer"
	mh "github.com/multiformats/go-multihash"
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

// Background work never holds the last slot: a walk keeps its slot for its whole duration, and at a closure's wave
// boundary (no fetch holding or waiting for an instant) every slot went to walks — the next fetch waited behind them.
// With one slot only, background may use it (else nothing would ever be announced). Teeth: drop the bgMax bound in
// acquire or in grantLocked and the third background job gets the slot kept for fetches.
func TestNetQueueKeepsASlotForFetches(t *testing.T) {
	withQueue(t, 3)
	b1, c1 := acquired(false)
	defer c1()
	b2, c2 := acquired(false)
	defer c2()
	r1, r2 := <-b1, <-b2
	b3, c3 := acquired(false)
	defer c3()
	time.Sleep(30 * time.Millisecond)
	select {
	case <-b3:
		t.Fatal("a third background job took the slot kept for fetches")
	default:
	}
	f, cf := acquired(true)
	defer cf()
	var rf func()
	select {
	case rf = <-f:
	case <-time.After(time.Second):
		t.Fatal("a fetch did not get the free slot at once")
	}
	rf() // the fetch's slot frees while background holds two: it stays free for the next fetch
	time.Sleep(30 * time.Millisecond)
	select {
	case <-b3:
		t.Fatal("a released fetch slot went to a third background job")
	default:
	}
	r1() // a background slot frees: the waiting background job may have it (still two background)
	select {
	case r3 := <-b3:
		r3()
	case <-time.After(time.Second):
		t.Fatal("a freed background slot did not pass to the waiting background job")
	}
	r2()

	withQueue(t, 1)
	b, cb := acquired(false)
	defer cb()
	select {
	case r := <-b:
		r()
	case <-time.After(time.Second):
		t.Fatal("with a single slot, background must still get it")
	}
}

// fakeProvider stands in for the sweeping provider: online or not, and what it was handed.
type fakeProvider struct {
	online bool
	queued int64 // keys waiting in its provide queue
	handed []mh.Multihash
	forced []mh.Multihash // … of which handed with force
	during func()         // runs inside the next Stats call (then cleared)
}

func (f *fakeProvider) StartProviding(force bool, keys ...mh.Multihash) error {
	f.handed = append(f.handed, keys...)
	if force {
		f.forced = append(f.forced, keys...)
	}
	return nil // like the real one offline: no error, whether it queued them or not
}
func (f *fakeProvider) StopProviding(...mh.Multihash) error { return nil }
func (f *fakeProvider) Stats(context.Context) (stats.Stats, error) {
	if d := f.during; d != nil {
		f.during = nil
		d()
	}
	var s stats.Stats
	s.Queues.PendingKeyProvides = f.queued
	s.Schedule.AvgPrefixLength = -1
	if f.online {
		s.Schedule.AvgPrefixLength = 7
	}
	return s, nil
}
func (f *fakeProvider) Close() error { return nil }

// An offline provider files a handed key in its keystore WITHOUT queueing it (and says nothing), and a later hand-over
// of that key is a no-op: it waited up to 22 h for its region's reprovide. So keys stay ours until the provider is
// online, then go over. Teeth: drop the providerReady gate in flushProvides and the key is handed over while offline.
func TestProvidingWaitsUntilTheProviderIsOnline(t *testing.T) {
	saved := providerReadyTTL
	providerReadyTTL = 0 // every call reads readiness afresh
	defer func() { providerReadyTTL = saved }()
	fp := &fakeProvider{}
	n := &node{seedDone: map[string]struct{}{}, provider: fp}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	n.ctx = ctx
	n.provideSt.started = true // no background retry loop in this test: the flushes below are the loop's
	n.startProviding(gateCid)
	if len(fp.handed) != 0 || len(n.provideSt.pending) != 1 {
		t.Fatalf("offline: handed %d, pending %d — the key must stay ours", len(fp.handed), len(n.provideSt.pending))
	}
	fp.online = true
	n.flushProvides()
	if len(fp.handed) != 1 || len(n.provideSt.pending) != 0 {
		t.Fatalf("online: handed %d, pending %d — the key must go over", len(fp.handed), len(n.provideSt.pending))
	}
}

// While a download runs — here a multi-wave job holding the foreground between its fetches, when no fetch holds or
// waits for a slot — background work gets one slot, not all but one: at every wave boundary the walks took the rest
// and the next wave waited behind them. Released, background may use all but one again; compaction (waitFgIdle)
// waits for the release. Teeth: ignore holds in fgBusyLocked and the second background job gets a slot mid-download.
func TestNetQueueGivesBackgroundOneSlotWhileADownloadRuns(t *testing.T) {
	withQueue(t, 3)
	release := netq.hold()
	if fgIdleNow() {
		t.Fatal("a held foreground reads as idle: compaction would run between the waves")
	}
	b1, c1 := acquired(false)
	defer c1()
	r1 := waitGot(t, b1, "the first background job")
	defer r1()
	b2, c2 := acquired(false)
	defer c2()
	time.Sleep(30 * time.Millisecond)
	select {
	case <-b2:
		t.Fatal("a second background job got a slot while a download runs")
	default:
	}
	release()
	r2 := waitGot(t, b2, "the second background job, once the download is over")
	r2()
	if !fgIdleNow() {
		t.Fatal("the released foreground still reads as busy")
	}
}

// The provider files a handed-over key at once but announces it only from its queue, and drops the queue whole when it
// goes offline (or the process ends): the key is "not new" ever after and waited up to 22 h. So a handed-over key
// stays unconfirmed until the queue drains while online; offline first, it is handed over again FORCED — by the next
// run too — and once announced it is known: a later re-offer is not tracked. Teeth: confirm without waiting for the
// queue to drain and the dropped key is taken for announced; drop useProvider's restore and the next run never
// re-offers it; hand it over again without force and the provider ignores it.
func TestAKeyDroppedFromTheProvideQueueIsHandedOverAgainForced(t *testing.T) {
	saved := providerReadyTTL
	providerReadyTTL = 0
	defer func() { providerReadyTTL = saved }()
	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fp := &fakeProvider{online: true, queued: 1}
	n := &node{seedDone: map[string]struct{}{}, ds: ds, ctx: ctx}
	n.provideSt.started = true
	n.useProvider(fp)
	n.startProviding(gateCid)
	if len(fp.handed) != 1 || len(n.provideSt.unconfirmed) != 1 {
		t.Fatalf("handed %d, unconfirmed %d — handed over, not yet announced", len(fp.handed), len(n.provideSt.unconfirmed))
	}
	n.confirmUnconfirmed() // still queued: nothing settles
	if len(n.provideSt.unconfirmed) != 1 {
		t.Fatal("a key still in the provide queue was taken for announced")
	}
	fp.online = false // the provider goes offline: its queue, with the key, is gone
	n.confirmUnconfirmed()
	if k, ok := n.provideSt.pending[string(gateCid.Hash())]; !ok || !k.force || len(n.provideSt.unconfirmed) != 0 {
		t.Fatalf("offline: pending %v unconfirmed %v — the key must come back, forced", n.provideSt.pending, n.provideSt.unconfirmed)
	}
	// The process ends here. The next run offers it again, forced, and settles it once its queue drains.
	fp2 := &fakeProvider{online: true, queued: 1}
	n2 := &node{seedDone: map[string]struct{}{}, ds: ds, ctx: ctx}
	n2.provideSt.started = true
	n2.useProvider(fp2)
	if len(fp2.forced) != 1 || !bytes.Equal(fp2.forced[0], gateCid.Hash()) {
		t.Fatalf("the next run handed over forced: %v", fp2.forced)
	}
	fp2.queued = 0
	n2.confirmUnconfirmed()
	if len(n2.provideSt.unconfirmed) != 0 || !n2.announced(gateCid.Hash()) {
		t.Fatal("announced once the queue drained: must be known announced")
	}
	if left := refiles(t, ds); len(left) != 0 {
		t.Fatalf("an announced key is still remembered as unconfirmed: %v", left)
	}
	n2.startProviding(gateCid) // a startup re-offer of an announced key: a no-op, not tracked
	if len(n2.provideSt.unconfirmed) != 0 {
		t.Fatal("a re-offer of an announced key was tracked (a blip would force a full re-announce)")
	}
}

// A key we stop holding leaves every trace — pending, unconfirmed, both records — even while offline, and an offline
// reading afterwards does not bring it back. Teeth: re-mark every snapshot key on the offline path (not only those
// still unconfirmed) and the stopped key is announced again; clear the records only with a provider and the next run
// re-announces it.
func TestAStoppedKeyIsNotBroughtBack(t *testing.T) {
	saved := providerReadyTTL
	providerReadyTTL = 0
	defer func() { providerReadyTTL = saved }()
	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fp := &fakeProvider{online: true, queued: 1}
	n := &node{seedDone: map[string]struct{}{}, ds: ds, ctx: ctx}
	n.provideSt.started = true
	n.useProvider(fp)
	n.startProviding(gateCid)
	fp.online = false
	fp.during = func() { // unpinned while the offline reading is taken, with no provider to tell
		n.provider = nil
		n.stopProviding(gateCid)
		n.provider = fp
	}
	n.confirmUnconfirmed()
	if len(n.provideSt.pending) != 0 || len(n.provideSt.unconfirmed) != 0 {
		t.Fatalf("a stopped key came back: pending %v unconfirmed %v", n.provideSt.pending, n.provideSt.unconfirmed)
	}
	if left := refiles(t, ds); len(left) != 0 {
		t.Fatalf("a stopped key is still remembered: %v", left)
	}
}

func refiles(t *testing.T, ds datastore.Datastore) []dsq.Entry {
	t.Helper()
	res, err := ds.Query(context.Background(), dsq.Query{Prefix: refileNS.String(), KeysOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	left, _ := res.Rest()
	return left
}
