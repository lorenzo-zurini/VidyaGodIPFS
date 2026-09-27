package main

import (
	"context"
	"testing"
	"time"

	bsmsg "github.com/ipfs/boxo/bitswap/message"
	pb "github.com/ipfs/boxo/bitswap/message/pb"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
)

// fakeSide records what reaches it.
type fakeSide struct {
	msgs            []bsmsg.BitSwapMessage
	from            []peer.ID
	conns, disconns []peer.ID
}

func (f *fakeSide) ReceiveMessage(_ context.Context, p peer.ID, m bsmsg.BitSwapMessage) {
	f.msgs, f.from = append(f.msgs, m), append(f.from, p)
}
func (f *fakeSide) PeerConnected(p peer.ID)    { f.conns = append(f.conns, p) }
func (f *fakeSide) PeerDisconnected(p peer.ID) { f.disconns = append(f.disconns, p) }
func (f *fakeSide) ReceiveError(error)         {}

// quarantineRig: a quarantineNet on a fake clock, its client, server and the Bitswap behind both faked.
type quarantineRig struct {
	t                   *testing.T
	q                   *quarantineNet
	r                   quarantineReceiver
	now                 time.Time
	client, server, all *fakeSide
}

func newQuarantineRig(t *testing.T) *quarantineRig {
	a, c, m := quarantineAfter, quarantineCool, quarantineMaxCool
	quarantineAfter, quarantineCool, quarantineMaxCool = 5*time.Second, 30*time.Second, 10*time.Minute
	t.Cleanup(func() { quarantineAfter, quarantineCool, quarantineMaxCool = a, c, m })
	g := &quarantineRig{t: t, now: time.Unix(1000, 0), client: &fakeSide{}, server: &fakeSide{}, all: &fakeSide{}}
	g.q = newQuarantineNet(nil)
	g.q.now = func() time.Time { return g.now }
	g.q.client, g.q.server, g.q.recv = g.client, g.server, g.all
	g.r = quarantineReceiver{g.q}
	return g
}

func (g *quarantineRig) send(p peer.ID, wt pb.Message_Wantlist_WantType, cs ...cid.Cid) {
	m := bsmsg.New(false)
	for _, c := range cs {
		m.AddEntry(c, 1, wt, true)
	}
	g.q.noteSent(p, m)
}

func (g *quarantineRig) want(p peer.ID, cs ...cid.Cid) { g.send(p, pb.Message_Wantlist_Block, cs...) }

func (g *quarantineRig) cancel(p peer.ID, c cid.Cid) {
	m := bsmsg.New(false)
	m.Cancel(c)
	g.q.noteSent(p, m)
}

func (g *quarantineRig) recv(p peer.ID, f func(bsmsg.BitSwapMessage)) {
	m := bsmsg.New(false)
	f(m)
	g.r.ReceiveMessage(context.Background(), p, m)
}

func (g *quarantineRig) have(p peer.ID, cs ...cid.Cid) {
	g.recv(p, func(m bsmsg.BitSwapMessage) {
		for _, c := range cs {
			m.AddHave(c)
		}
	})
}

// offer: we ask p whether it has cs (want-have), and it says HAVE.
func (g *quarantineRig) offer(p peer.ID, cs ...cid.Cid) {
	g.send(p, pb.Message_Wantlist_Have, cs...)
	g.have(p, cs...)
}

func (g *quarantineRig) advance(d time.Duration) {
	for end := g.now.Add(d); g.now.Before(end); {
		g.now = g.now.Add(time.Second)
		g.q.check()
	}
}

func (g *quarantineRig) toldGone(p peer.ID) bool {
	for _, d := range g.client.disconns {
		if d == p {
			return true
		}
	}
	return false
}

func blk(s string) blocks.Block { return blocks.NewBlock([]byte(s)) }

// Who is taken out, and when. Teeth, each caught below: count a HAVE as a delivery; quarantine a sole provider (no
// other peer offers what it holds); count an offer from a peer that is itself out; count an unsolicited HAVE as an
// offer; keep an offer past a Full wantlist, or past its peer going out; quarantine a peer that keeps delivering;
// keep a cancelled or answered want (or its offers) on the ledger; record wants sent to a peer while it is out (it
// would be put out again the moment it is back); stop doubling the cooldown; keep the strikes after a delivery.
func TestQuarantineRules(t *testing.T) {
	promiser, seeder, sole, other := peer.ID("promiser"), peer.ID("seeder"), peer.ID("sole"), peer.ID("other")
	x, y, z, u, w := blk("x"), blk("y"), blk("z"), blk("u"), blk("w")
	// rig: every peer connected; the promiser out at t+5s (it held x, which the seeder offers).
	rig := func(t *testing.T) *quarantineRig {
		g := newQuarantineRig(t)
		for _, p := range []peer.ID{promiser, seeder, sole, other} {
			g.r.PeerConnected(p)
		}
		g.offer(seeder, x.Cid())
		g.want(promiser, x.Cid())
		g.have(promiser, x.Cid())
		g.advance(4 * time.Second)
		if g.q.out(promiser) {
			t.Fatal("quarantined before quarantineAfter")
		}
		g.have(promiser, x.Cid()) // it keeps promising (rebroadcast want-haves)
		g.advance(1 * time.Second)
		if !g.q.out(promiser) {
			t.Fatal("a HAVE with no block is not a delivery, and the seeder offers the block: the promiser must be out")
		}
		if len(g.client.disconns) != 1 || g.client.disconns[0] != promiser {
			t.Fatalf("the client was told %v disconnected, want [promiser]", g.client.disconns)
		}
		return g
	}

	t.Run("a sole provider stays", func(t *testing.T) {
		// Taking it out would turn slow into stopped. No alternative: an unsolicited HAVE (other never was asked for
		// w), nor an offer from a peer that is out (the promiser's HAVE for u, sent as it went out, is kept from us).
		g := rig(t)
		g.have(other, w.Cid())
		g.offer(promiser, u.Cid())
		g.offer(sole, w.Cid(), u.Cid())
		g.want(sole, w.Cid(), u.Cid())
		g.advance(20 * time.Second) // the promiser stays out throughout
		if g.toldGone(sole) {
			t.Fatal("a sole provider was taken out: nobody in the rotation offers what it holds")
		}
		if _, ok := g.q.haves[w.Cid()][other]; ok {
			t.Fatal("an unsolicited HAVE was noted as an offer")
		}
	})

	t.Run("an offer ends with the peer going out", func(t *testing.T) {
		// The promiser offered u before it went out; once back, that stale offer must not take a peer out.
		g := newQuarantineRig(t)
		for _, p := range []peer.ID{promiser, seeder, sole} {
			g.r.PeerConnected(p)
		}
		g.offer(promiser, u.Cid())
		g.offer(seeder, x.Cid())
		g.want(promiser, x.Cid())
		g.advance(5 * time.Second)
		if !g.q.out(promiser) {
			t.Fatal("setup: the promiser is not out")
		}
		g.advance(30 * time.Second) // back
		g.offer(sole, u.Cid())
		g.want(sole, u.Cid())
		g.advance(10 * time.Second)
		if g.toldGone(sole) {
			t.Fatal("a peer was taken out on an offer made before the offerer went out")
		}
	})

	t.Run("a full wantlist ends the offers", func(t *testing.T) {
		g := rig(t)
		g.offer(other, w.Cid())
		full := bsmsg.New(true)
		full.AddEntry(y.Cid(), 1, pb.Message_Wantlist_Have, true)
		g.q.noteSent(other, full)
		if _, ok := g.q.haves[w.Cid()]; ok {
			t.Fatal("an offer outlived the want a Full wantlist replaced")
		}
	})

	t.Run("a delivering peer stays; answers leave the ledger", func(t *testing.T) {
		g := rig(t)
		g.offer(sole, y.Cid(), z.Cid())
		g.want(seeder, y.Cid(), z.Cid())
		for i := 0; i < 5; i++ {
			g.advance(2 * time.Second)
			g.recv(seeder, func(m bsmsg.BitSwapMessage) { m.AddBlock(blk("other" + string(rune('a'+i)))) })
		}
		if g.toldGone(seeder) { // (a delivery brings it straight back: ask the client, not out())
			t.Fatal("a peer that keeps delivering was quarantined")
		}
		g.recv(seeder, func(m bsmsg.BitSwapMessage) { m.AddBlock(y); m.AddDontHave(z.Cid()) })
		g.want(seeder, x.Cid())
		g.cancel(seeder, x.Cid())
		if n := len(g.q.peers[seeder].wanted); n != 0 {
			t.Fatalf("%d want(s) still held after a block, a DONT_HAVE and a cancel", n)
		}
		if n := len(g.q.peers[seeder].asked); n != 0 {
			t.Fatalf("%d ask(s) still open after a block, a DONT_HAVE and a cancel", n)
		}
		if _, ok := g.q.haves[y.Cid()]; ok {
			t.Fatal("a delivered block's offers are still remembered")
		}
		if _, ok := g.q.haves[x.Cid()]; ok {
			t.Fatal("a cancelled want's offers are still remembered")
		}
		g.recv(sole, func(m bsmsg.BitSwapMessage) { m.AddDontHave(z.Cid()) })
		if _, ok := g.q.haves[z.Cid()]; ok {
			t.Fatal("a peer's HAVE still counts after its DONT_HAVE")
		}
	})

	t.Run("a want sent while out is not held against it", func(t *testing.T) {
		g := rig(t)
		g.want(promiser, z.Cid()) // queued before it went out
		g.offer(seeder, z.Cid())
		g.advance(30 * time.Second) // its first cooldown (30 s) is over
		if g.q.out(promiser) {
			t.Fatal("still out after its cooldown")
		}
		if len(g.client.conns) != 1 || g.client.conns[0] != promiser {
			t.Fatalf("its cooldown over, the client was told %v connected, want [promiser]", g.client.conns)
		}
		g.advance(time.Second)
		if g.q.out(promiser) {
			t.Fatal("put out again the moment it was back, for a want sent while it was out")
		}
	})

	t.Run("the cooldown doubles, and starts over after a delivery", func(t *testing.T) {
		g := rig(t)
		g.advance(30 * time.Second)
		g.offer(seeder, x.Cid())
		g.want(promiser, x.Cid())
		g.advance(5 * time.Second)
		if l := g.q.peers[promiser]; !g.q.out(promiser) || l.until.Sub(g.now) != 60*time.Second {
			t.Fatalf("second offence: out=%v for %s, want 1m0s", g.q.out(promiser), l.until.Sub(g.now))
		}
		g.advance(60 * time.Second)
		g.recv(promiser, func(m bsmsg.BitSwapMessage) { m.AddBlock(x) })
		g.offer(seeder, y.Cid())
		g.want(promiser, y.Cid())
		g.advance(5 * time.Second)
		if l := g.q.peers[promiser]; l.until.Sub(g.now) != 30*time.Second {
			t.Fatalf("after a delivery its next cooldown is %s, want the first (30s)", l.until.Sub(g.now))
		}
	})
}

// What a quarantined peer says, and where it goes. Out, its HAVEs never reach the client (they would make it the
// session's best peer again), its wants still reach our server, and a block it delivers brings it back and reaches
// the client: nothing it sends is thrown away. A peer that left while out is not announced back, and its ledger goes
// once its cooldown ends; a peer with nothing to remember is dropped at the next check. Teeth: forward an out peer's
// HAVEs; drop its wants; drop (or hold back) its blocks; release it without telling the client; announce a departed
// peer; drop an out peer's ledger when it leaves; keep a departed peer's ledger; keep its offers.
func TestQuarantineKeepsWhatThePeerSays(t *testing.T) {
	g := newQuarantineRig(t)
	p, seeder, passer := peer.ID("promiser"), peer.ID("seeder"), peer.ID("passer")
	g.r.PeerConnected(p)
	g.r.PeerConnected(seeder)
	x, y := blk("x"), blk("y")
	g.offer(seeder, x.Cid())
	g.want(p, x.Cid())
	g.advance(5 * time.Second)
	if !g.q.out(p) {
		t.Fatal("setup: the promiser is not out")
	}
	before := len(g.all.msgs)

	g.have(p, x.Cid())
	if len(g.all.msgs) != before {
		t.Fatal("an out peer's HAVE-only message reached bitswap (the client would take it back)")
	}
	g.recv(p, func(m bsmsg.BitSwapMessage) {
		m.AddHave(x.Cid())
		m.AddDontHave(y.Cid())
		m.AddEntry(y.Cid(), 1, pb.Message_Wantlist_Block, true)
		m.Cancel(x.Cid())
	})
	if len(g.all.msgs) != before+1 {
		t.Fatal("an out peer's wants did not reach our server")
	}
	if m := g.all.msgs[before]; len(m.Wantlist()) != 2 || len(m.Haves()) != 0 || len(m.DontHaves()) != 0 {
		t.Fatalf("an out peer's message reached bitswap with %d want(s), %d HAVE(s), %d DONT_HAVE(s); want 2 (a want, a cancel), 0, 0",
			len(m.Wantlist()), len(m.Haves()), len(m.DontHaves()))
	}
	for _, e := range g.all.msgs[before].Wantlist() {
		if e.Cid == x.Cid() && !e.Cancel {
			t.Fatal("an out peer's cancel reached our server as a want")
		}
	}
	g.r.PeerConnected(p)
	if len(g.server.conns) != 1 || len(g.all.conns) != 2 {
		t.Fatalf("an out peer's connect went to the server %d time(s), to bitswap %d; want 1, 2 (the setup's)", len(g.server.conns), len(g.all.conns))
	}

	g.recv(p, func(m bsmsg.BitSwapMessage) { m.AddBlock(x); m.AddHave(y.Cid()) })
	if g.q.out(p) {
		t.Fatal("a peer that delivers a block is still out")
	}
	if len(g.client.conns) != 1 || g.client.conns[0] != p {
		t.Fatalf("the client was told %v connected, want [promiser]: its blocks must find it registered", g.client.conns)
	}
	if m := g.all.msgs[len(g.all.msgs)-1]; len(g.all.msgs) != before+2 || len(m.Blocks()) != 1 || len(m.Haves()) != 1 {
		t.Fatal("the block an out peer delivered (with the rest of its message) did not reach bitswap")
	}

	// Out again; it leaves; its strikes last until its cooldown ends, and nobody is announced.
	g.offer(seeder, y.Cid())
	g.want(p, y.Cid())
	g.advance(5 * time.Second)
	if !g.q.out(p) {
		t.Fatal("setup: the promiser is not out again")
	}
	g.r.PeerDisconnected(p)
	g.advance(time.Second)
	if _, ok := g.q.peers[p]; !ok {
		t.Fatal("a quarantined peer's ledger was dropped when it left: it would come back with a clean record")
	}
	g.advance(30 * time.Second)
	if len(g.client.conns) != 1 {
		t.Fatal("a peer that left while out was announced back to the client")
	}
	if _, ok := g.q.peers[p]; ok {
		t.Fatal("a departed peer's ledger outlived its cooldown")
	}

	// A peer with nothing to remember is dropped at the next check, and its offers at once.
	g.r.PeerConnected(passer)
	g.offer(passer, x.Cid())
	g.r.PeerDisconnected(passer)
	if _, ok := g.q.haves[x.Cid()]; ok {
		t.Fatal("a departed peer's offers are still remembered")
	}
	g.advance(time.Second)
	if _, ok := g.q.peers[passer]; ok {
		t.Fatal("a departed peer's ledger outlived its connection")
	}
}

// A tick that moves nobody takes no exclusive lock: it would wait out the slowest delivery in flight and hold every
// other peer's messages behind it, once a second. A tick that moves someone does. Teeth: take dmu exclusively on every
// tick; move a peer without it.
func TestQuarantineCheckLocksOnlyToMove(t *testing.T) {
	g := newQuarantineRig(t)
	p, seeder := peer.ID("promiser"), peer.ID("seeder")
	g.r.PeerConnected(p)
	g.r.PeerConnected(seeder)
	x := blk("x")
	g.offer(seeder, x.Cid())
	g.want(p, x.Cid())
	g.q.dmu.RLock() // a delivery in flight
	done := make(chan struct{})
	go func() { g.q.check(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		g.q.dmu.RUnlock()
		t.Fatal("a tick with nobody to move waited on a delivery in flight")
	}
	g.now = g.now.Add(5 * time.Second)
	done = make(chan struct{})
	go func() { g.q.check(); close(done) }()
	select {
	case <-done:
		g.q.dmu.RUnlock()
		t.Fatal("a peer was moved while a delivery was in flight")
	case <-time.After(200 * time.Millisecond):
	}
	g.q.dmu.RUnlock()
	<-done
	if !g.q.out(p) {
		t.Fatal("the promiser was not moved once the delivery finished")
	}
}

// The finder does not hand a quarantined peer back to the sessions (boxo adds every provider it is given). Teeth:
// ignore skip.
func TestCombinedFinderSkipsAQuarantinedProvider(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	noted := 0
	cf := combinedFinder{routers: []routing.ContentDiscovery{fakeRouter{provide: true}},
		noteProvider: func(peer.ID) { noted++ }, skip: func(peer.ID) bool { return true }}
	for range cf.FindProvidersAsync(ctx, cid.Cid{}, 10) {
		t.Fatal("a skipped provider reached the session")
	}
	if noted != 0 {
		t.Fatal("a skipped provider was noted for a dial")
	}
}
