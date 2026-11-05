package main

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	ipfspinner "github.com/ipfs/boxo/pinning/pinner"
	cid "github.com/ipfs/go-cid"
	datastore "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	libp2p "github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	mh "github.com/multiformats/go-multihash"
)

// The provider's keystore is what makes a key "not new": a key it holds is never announced again on hand-over, only
// reprovided on schedule. Its default keystore was an in-memory map, so every start re-announced the whole seed set
// (~9 k keys) and a just-published root waited behind all of it. Ours lives in our datastore: a second provider over
// the same datastore — the next run — holds what the first was handed. Teeth: drop WithKeystore from
// newSweepingProvider and the next run's provider holds nothing.
func TestTheProviderKeystoreOutlivesTheProcess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	k := gateCid.Hash()
	run := func() *sweepingProvider {
		h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = h.Close() })
		kad, err := dht.New(ctx, h, dht.Mode(dht.ModeServer), dht.BootstrapPeers())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = kad.Close() })
		sp, err := newSweepingProvider(ctx, kad, ds, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		return sp
	}
	sp := run()
	if err := sp.StartProviding(false, k); err != nil {
		t.Fatal(err)
	}
	if err := sp.Close(); err != nil {
		t.Fatal(err)
	}
	sp2 := run()
	defer sp2.Close()
	st, err := sp2.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if st.Schedule.Keys != 1 {
		t.Fatalf("the next run's provider holds %d key(s), want the 1 handed over before", st.Schedule.Keys)
	}
	stored, err := sp2.StoredKeys(ctx)
	if err != nil || len(stored) != 1 || string(stored[0]) != string(k) {
		t.Fatalf("stored %v (%v), want the key handed over before", stored, err)
	}
	// Stopped, it is gone for the run after too.
	if err := sp2.StopProviding(k); err != nil {
		t.Fatal(err)
	}
	_ = sp2.Close()
	sp3 := run()
	defer sp3.Close()
	if stored, err := sp3.StoredKeys(ctx); err != nil || len(stored) != 0 {
		t.Fatalf("a stopped key is still stored for the next run: %v (%v)", stored, err)
	}
}

// listingProvider is fakeProvider with a keystore that persists what it is handed (an earlier run filled it), and
// records what it is told to stop.
type listingProvider struct {
	fakeProvider
	stored  []mh.Multihash
	stopped []mh.Multihash
	onStop  func()
}

func (p *listingProvider) StoredKeys(context.Context) ([]mh.Multihash, error) {
	return append([]mh.Multihash(nil), p.stored...), nil
}
func (p *listingProvider) StartProviding(force bool, keys ...mh.Multihash) error {
	for _, k := range keys {
		have := false
		for _, s := range p.stored {
			have = have || string(s) == string(k)
		}
		if !have {
			p.stored = append(p.stored, k)
		}
	}
	return p.fakeProvider.StartProviding(force, keys...)
}
func (p *listingProvider) StopProviding(keys ...mh.Multihash) error {
	if p.onStop != nil {
		f := p.onStop
		p.onStop = nil
		f()
	}
	p.stopped = append(p.stopped, keys...)
	return nil
}

func keyCid(t *testing.T, s string) cid.Cid {
	h, err := mh.Sum([]byte(s), mh.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}
	return cid.NewCidV1(cid.Raw, h)
}

func names(t *testing.T, of map[string]string, keys []mh.Multihash) []string {
	var out []string
	for _, h := range keys {
		out = append(out, of[string(h)])
	}
	sort.Strings(out)
	return out
}

// With the keystore outliving the process, what an earlier run announced and this one no longer holds (unpinned while
// we were down, a superseded node block) would be reprovided forever. Once the app has handed over its full list, and
// the provider listed its keystore — in either order — the earlier run's keys this run did not hand over are stopped,
// and nothing handed over in this run ever is. Teeth: subtract nothing handed over before the provider; prune on every
// seed; skip the provider-second path.
func TestLeftoversOfAnEarlierRunAreStoppedOnceTheListIsIn(t *testing.T) {
	for _, providerFirst := range []bool{true, false} {
		ctx, cancel := context.WithCancel(context.Background())
		a, b, c, d := keyCid(t, "a"), keyCid(t, "b"), keyCid(t, "c"), keyCid(t, "d")
		name := map[string]string{string(a.Hash()): "a", string(b.Hash()): "b", string(c.Hash()): "c", string(d.Hash()): "d"}
		lp := &listingProvider{fakeProvider: fakeProvider{online: true}, stored: []mh.Multihash{a.Hash(), b.Hash(), c.Hash(), d.Hash()}}
		n := &node{seedDone: map[string]struct{}{}, ctx: ctx}
		n.provideSt.started = true
		seed := func() { // runSeedAnnounce's hand-over of the app's full list: a and b
			n.startProviding(a, b)
			n.provideSt.mu.Lock()
			n.provideSt.seeded = true
			n.provideSt.mu.Unlock()
			n.dropLeftovers()
		}
		saved := providerReadyTTL
		providerReadyTTL = 0
		if providerFirst {
			n.useProvider(lp)
			if len(lp.stopped) != 0 {
				t.Fatal("stopped before the app's list was in")
			}
			seed()
		} else {
			n.startProviding(c) // handed over before there was a provider: pending, not a leftover
			seed()
			n.useProvider(lp)
		}
		want := []string{"c", "d"}
		if !providerFirst {
			want = []string{"d"}
		}
		if got := names(t, name, lp.stopped); len(got) != len(want) || got[0] != want[0] || got[len(got)-1] != want[len(want)-1] {
			t.Fatalf("providerFirst=%v: stopped %v, want %v", providerFirst, got, want)
		}
		lp.stopped = nil
		n.startProviding(d) // wanted again later in the run
		seed()
		if len(lp.stopped) != 0 {
			t.Fatalf("providerFirst=%v: a later seed stopped %v — only an earlier run's keys, once", providerFirst, names(t, name, lp.stopped))
		}
		providerReadyTTL = saved
		cancel()
	}
}

// goOnline can build a second provider in one run (the first attempt failed late). The keystore it opens holds what
// this run handed the first one, none of it pending any more: none of it is a leftover. Teeth: tell this run's keys by
// "still pending" (instead of handed over in this run) and the retry stops the whole seed set.
func TestARetriedProviderStopsNothingThisRunHandedOver(t *testing.T) {
	saved := providerReadyTTL
	providerReadyTTL = 0
	defer func() { providerReadyTTL = saved }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a, b, old := keyCid(t, "a"), keyCid(t, "b"), keyCid(t, "old")
	n := &node{seedDone: map[string]struct{}{}, ctx: ctx}
	n.provideSt.started = true
	n.startProviding(a, b) // the app's list, before any provider
	n.provideSt.seeded = true
	p1 := &listingProvider{fakeProvider: fakeProvider{online: true}, stored: []mh.Multihash{old.Hash()}}
	n.useProvider(p1) // hands a and b over, stops old
	if len(p1.stopped) != 1 || string(p1.stopped[0]) != string(old.Hash()) {
		t.Fatalf("the first provider stopped %d key(s), want old", len(p1.stopped))
	}
	n.provider = nil // the attempt fails; the next one opens the same keystore
	p2 := &listingProvider{fakeProvider: fakeProvider{online: true}, stored: p1.stored}
	n.useProvider(p2)
	if len(p2.stopped) != 0 {
		t.Fatalf("the retried provider stopped %d key(s) this run handed over", len(p2.stopped))
	}
}

// A key the app hands over while an earlier run's leftovers are being stopped is wanted: handed over again after the
// stop, forced (stopped, it is new to the keystore). Teeth: drop the re-check after StopProviding.
func TestAKeyHandedOverWhileLeftoversAreStoppedIsOfferedAgain(t *testing.T) {
	saved := providerReadyTTL
	providerReadyTTL = 0
	defer func() { providerReadyTTL = saved }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	x := keyCid(t, "x")
	n := &node{seedDone: map[string]struct{}{}, ctx: ctx}
	n.provideSt.started = true
	lp := &listingProvider{fakeProvider: fakeProvider{online: true}, stored: []mh.Multihash{x.Hash()}}
	n.useProvider(lp)
	lp.onStop = func() { n.startProviding(x) } // a fetch finishes x as it is stopped
	n.provideSt.mu.Lock()
	n.provideSt.seeded = true
	n.provideSt.mu.Unlock()
	n.dropLeftovers()
	if len(lp.forced) != 1 || string(lp.forced[0]) != string(x.Hash()) {
		t.Fatalf("x was stopped and not handed over again forced: forced %d", len(lp.forced))
	}
}

// The provider files a key durably the moment it is handed over; its refile record must already be there, or a crash
// in between leaves a filed, unannounced key no run re-offers. Teeth: write the record after StartProviding.
func TestTheRefileRecordIsWrittenBeforeTheHandOver(t *testing.T) {
	saved := providerReadyTTL
	providerReadyTTL = 0
	defer func() { providerReadyTTL = saved }()
	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	n := &node{seedDone: map[string]struct{}{}, ds: ds, ctx: ctx}
	n.provideSt.started = true
	fp := &fakeProvider{online: true, queued: 1}
	held := -1
	fp.onHand = func() { held = len(refiles(t, ds)) }
	n.useProvider(fp)
	n.startProviding(gateCid)
	if held != 1 {
		t.Fatalf("refile records at hand-over: %d, want the key's", held)
	}
}

// Going Offline (disconnected past its delay) drops the provider's queue, and a spell of it can fall between two
// readings. A key handed over before it is handed over again, forced, not taken for announced; a short disconnection
// keeps the queue, and sends nothing back. Teeth: settle by "online and drained" alone; send back on a disconnected
// reading.
func TestOnlyGoingOfflineSendsAHandedOverKeyBack(t *testing.T) {
	saved := providerReadyTTL
	providerReadyTTL = 0
	defer func() { providerReadyTTL = saved }()
	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	n := &node{seedDone: map[string]struct{}{}, ds: ds, ctx: ctx}
	n.provideSt.started = true
	fp := &fakeProvider{online: true, queued: 1}
	n.useProvider(fp)
	n.startProviding(gateCid)
	fp.unconn = true // disconnected for a moment: the queue stands
	n.confirmUnconfirmed()
	if len(n.provideSt.unconfirmed) != 1 || len(fp.forced) != 0 {
		t.Fatal("a disconnected reading sent a still-queued key back")
	}
	fp.unconn = false
	time.Sleep(time.Millisecond)
	n.noteOffline() // Offline and back between two readings; the queue now reads drained
	n.noteOnline()
	fp.queued = 0
	n.confirmUnconfirmed()
	if n.announced(gateCid.Hash()) {
		t.Fatal("a key handed over before an Offline spell was taken for announced")
	}
	n.flushProvides()
	if len(fp.forced) != 1 {
		t.Fatalf("handed over again forced: %d", len(fp.forced))
	}
	n.confirmUnconfirmed()
	if !n.announced(gateCid.Hash()) {
		t.Fatal("handed over again after the spell and drained: must be announced")
	}
}

// Two flushes can hand the same pending key over at once (every startProviding flushes, and so does the tick). The one
// that finds it already taken must not clear the refile record the other relies on. Teeth: count a key the other
// flush took as stopped.
func TestTwoFlushesKeepTheRefileRecord(t *testing.T) {
	saved := providerReadyTTL
	providerReadyTTL = 0
	defer func() { providerReadyTTL = saved }()
	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	n := &node{seedDone: map[string]struct{}{}, ds: ds, ctx: ctx}
	n.provideSt.started = true
	fp := &fakeProvider{online: true, queued: 1}
	n.useProvider(fp)
	fp.onHand = func() { fp.onHand = nil; n.flushProvides() } // a second flush, inside the first one's hand-over
	n.startProviding(gateCid)
	if len(n.provideSt.unconfirmed) != 1 || len(refiles(t, ds)) != 1 {
		t.Fatalf("unconfirmed %d, refile records %d — both must stand", len(n.provideSt.unconfirmed), len(refiles(t, ds)))
	}
}

// failingPinner: a pinset that cannot be listed.
type failingPinner struct{ ipfspinner.Pinner }

func (failingPinner) RecursiveKeys(context.Context, bool) <-chan ipfspinner.StreamedPin {
	ch := make(chan ipfspinner.StreamedPin, 1)
	ch <- ipfspinner.StreamedPin{Err: errors.New("listing failed")}
	close(ch)
	return ch
}

// A seed whose pinset could not be listed handed over no content: pruning from it would stop every content key. So
// it prunes nothing, and waits for a complete list. Teeth: drop the err check in runSeedAnnounce.
func TestASeedWithoutThePinsetPrunesNothing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	content := keyCid(t, "content")
	lp := &listingProvider{fakeProvider: fakeProvider{online: true}, stored: []mh.Multihash{content.Hash()}}
	n := &node{seedDone: map[string]struct{}{}, ctx: ctx, pinner: failingPinner{}}
	n.provideSt.started = true
	n.useProvider(lp)
	n.seedColls = []cid.Cid{keyCid(t, "root")}
	n.runSeedAnnounce()
	if len(lp.stopped) != 0 {
		t.Fatalf("an incomplete list stopped %d content key(s)", len(lp.stopped))
	}
}
