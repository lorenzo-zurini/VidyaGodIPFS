package main

import (
	"context"
	"testing"
	"time"

	bsmsg "github.com/ipfs/boxo/bitswap/message"
	pb "github.com/ipfs/boxo/bitswap/message/pb"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/metrics"
	peer "github.com/libp2p/go-libp2p/core/peer"
	protocol "github.com/libp2p/go-libp2p/core/protocol"
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
	forgot              []peer.ID
}

func newQuarantineRig(t *testing.T) *quarantineRig {
	a, c, m := quarantineAfter, quarantineCool, quarantineMaxCool
	quarantineAfter, quarantineCool, quarantineMaxCool = 5*time.Second, 30*time.Second, 10*time.Minute
	t.Cleanup(func() { quarantineAfter, quarantineCool, quarantineMaxCool = a, c, m })
	g := &quarantineRig{t: t, now: time.Unix(1000, 0), client: &fakeSide{}, server: &fakeSide{}, all: &fakeSide{}}
	g.q = newQuarantineNet(nil)
	g.q.now = func() time.Time { return g.now }
	g.q.client, g.q.server, g.q.recv = g.client, g.server, g.all
	g.q.forget = func(p peer.ID) { g.forgot = append(g.forgot, p) }
	g.r = quarantineReceiver{g.q}
	return g
}

func (g *quarantineRig) want(p peer.ID, cs ...cid.Cid) {
	m := bsmsg.New(false)
	for _, c := range cs {
		m.AddEntry(c, 1, pb.Message_Wantlist_Block, true)
	}
	g.q.noteSent(p, m)
}

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

func (g *quarantineRig) advance(d time.Duration) {
	for end := g.now.Add(d); g.now.Before(end); {
		g.now = g.now.Add(time.Second)
		g.q.check()
	}
}

func blk(s string) blocks.Block { return blocks.NewBlock([]byte(s)) }

// Who is taken out, and when. Teeth, each caught below: count a HAVE as a delivery; quarantine a sole provider (no
// other peer offers what it holds); count an offer from a peer that is itself out; ignore bitswap bytes as progress;
// quarantine a peer that keeps delivering; keep a cancelled or answered want on the ledger; record wants sent to a
// peer while it is out (it would be put out again the moment it is back); stop doubling the cooldown; keep the
// strikes after a delivery.
func TestQuarantineRules(t *testing.T) {
	promiser, seeder, sole, slow := peer.ID("promiser"), peer.ID("seeder"), peer.ID("sole"), peer.ID("slow")
	x, y, z, u, w := blk("x"), blk("y"), blk("z"), blk("u"), blk("w")
	// rig: every peer connected; the promiser out at t+5s (it held x, which the seeder offers).
	rig := func(t *testing.T) *quarantineRig {
		g := newQuarantineRig(t)
		for _, p := range []peer.ID{promiser, seeder, sole, slow} {
			g.r.PeerConnected(p)
		}
		g.have(promiser, x.Cid(), u.Cid())
		g.have(seeder, x.Cid())
		g.want(promiser, x.Cid())
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
		// Taking it out would turn slow into stopped. An offer from a peer that is itself out (the promiser HAVE'd u
		// before it went out) is no alternative.
		g := rig(t)
		g.have(sole, w.Cid(), u.Cid())
		g.want(sole, w.Cid(), u.Cid())
		g.advance(20 * time.Second) // the promiser stays out throughout
		if g.q.out(sole) {
			t.Fatal("a sole provider was taken out: nobody in the rotation offers what it holds")
		}
	})

	t.Run("bytes are progress", func(t *testing.T) {
		// Bytes arriving on bitswap: a block still crossing a slow link.
		g := rig(t)
		bytes := map[peer.ID]int64{}
		g.q.bytes = func(p peer.ID) int64 { return bytes[p] }
		g.have(seeder, y.Cid())
		g.want(slow, y.Cid())
		for i := 0; i < 6; i++ {
			bytes[slow] += 64 << 10
			g.advance(2 * time.Second)
		}
		if g.q.out(slow) {
			t.Fatal("a peer whose block is still arriving was taken out")
		}
		g.advance(5 * time.Second) // nothing more arrives
		if !g.q.out(slow) {
			t.Fatal("no bytes for quarantineAfter while the seeder offers the block: the slow peer must be out")
		}
	})

	t.Run("a delivering peer stays; answers leave the ledger", func(t *testing.T) {
		g := rig(t)
		g.have(sole, y.Cid(), z.Cid())
		g.want(seeder, y.Cid(), z.Cid())
		for i := 0; i < 5; i++ {
			g.advance(2 * time.Second)
			g.recv(seeder, func(m bsmsg.BitSwapMessage) { m.AddBlock(blk("other" + string(rune('a'+i)))) })
		}
		for _, p := range g.client.disconns {
			if p == seeder { // (a delivery brings it straight back: ask the client, not out())
				t.Fatal("a peer that keeps delivering was quarantined")
			}
		}
		g.recv(seeder, func(m bsmsg.BitSwapMessage) { m.AddBlock(y); m.AddDontHave(z.Cid()) })
		g.want(seeder, x.Cid())
		g.cancel(seeder, x.Cid())
		if n := len(g.q.peers[seeder].wanted); n != 0 {
			t.Fatalf("%d want(s) still held after a block, a DONT_HAVE and a cancel", n)
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
		g.have(seeder, z.Cid())
		g.advance(30 * time.Second) // its first cooldown (30 s) is over
		if g.q.out(promiser) {
			t.Fatal("still out after its cooldown")
		}
		g.advance(time.Second)
		if g.q.out(promiser) {
			t.Fatal("put out again the moment it was back, for a want sent while it was out")
		}
	})

	t.Run("the cooldown doubles, and starts over after a delivery", func(t *testing.T) {
		g := rig(t)
		g.advance(30 * time.Second)
		g.want(promiser, x.Cid())
		g.advance(5 * time.Second)
		if l := g.q.peers[promiser]; !g.q.out(promiser) || l.until.Sub(g.now) != 60*time.Second {
			t.Fatalf("second offence: out=%v for %s, want 1m0s", g.q.out(promiser), l.until.Sub(g.now))
		}
		g.advance(60 * time.Second)
		g.recv(promiser, func(m bsmsg.BitSwapMessage) { m.AddBlock(x) })
		g.want(promiser, y.Cid())
		g.have(seeder, y.Cid())
		g.advance(5 * time.Second)
		if l := g.q.peers[promiser]; l.until.Sub(g.now) != 30*time.Second {
			t.Fatalf("after a delivery its next cooldown is %s, want the first (30s)", l.until.Sub(g.now))
		}
	})
}

// What a quarantined peer says, and where it goes. Out, its HAVEs never reach the client (they would make it the
// session's best peer again), its wants still reach our server, and a block it delivers brings it back and reaches
// the client: nothing it sends is thrown away. A peer that left while out is not announced back; a peer with nothing
// to remember is forgotten when it leaves. Teeth: forward an out peer's HAVEs; drop its wants; drop (or hold back)
// its blocks; release it without telling the client; announce a departed peer; keep a clean ledger; skip forget.
func TestQuarantineKeepsWhatThePeerSays(t *testing.T) {
	g := newQuarantineRig(t)
	p, seeder, passer := peer.ID("promiser"), peer.ID("seeder"), peer.ID("passer")
	g.r.PeerConnected(p)
	g.r.PeerConnected(seeder)
	x, y := blk("x"), blk("y")
	g.have(seeder, x.Cid())
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

	// Out again; it leaves; its cooldown ends: nobody to announce, but its strikes are remembered.
	g.have(seeder, y.Cid())
	g.want(p, y.Cid())
	g.advance(5 * time.Second)
	if !g.q.out(p) {
		t.Fatal("setup: the promiser is not out again")
	}
	g.r.PeerDisconnected(p)
	if _, ok := g.q.peers[p]; !ok {
		t.Fatal("a quarantined peer's ledger was dropped when it left: it would come back with a clean record")
	}
	g.advance(31 * time.Second)
	if len(g.client.conns) != 1 {
		t.Fatal("a peer that left while out was announced back to the client")
	}

	// A peer with nothing to remember is forgotten when it leaves, and so are its offers.
	g.r.PeerConnected(passer)
	g.have(passer, x.Cid())
	g.r.PeerDisconnected(passer)
	if _, ok := g.q.peers[passer]; ok {
		t.Fatal("a clean peer's ledger outlived its connection")
	}
	if len(g.forgot) != 1 || g.forgot[0] != passer {
		t.Fatalf("forgot %v, want [passer] (its byte count would outlive it)", g.forgot)
	}
	if _, ok := g.q.haves[x.Cid()]; ok {
		t.Fatal("a departed peer's offers are still remembered")
	}
}

type recordingReporter struct {
	*metrics.BandwidthCounter
	in int64
}

func (r *recordingReporter) LogRecvMessageStream(size int64, proto protocol.ID, p peer.ID) {
	r.in += size
	r.BandwidthCounter.LogRecvMessageStream(size, proto, p)
}

// Only bitswap streams count: a peer's DHT chatter is not progress on our want-blocks. Teeth: count every protocol;
// forget nothing on drop; stop passing the counts on to the node's bandwidth counter.
func TestBitswapBytesCountsOnlyBitswapStreams(t *testing.T) {
	inner := &recordingReporter{BandwidthCounter: metrics.NewBandwidthCounter()}
	b := newBitswapBytes(inner)
	p := peer.ID("p")
	b.LogRecvMessageStream(1000, "/ipfs/bitswap/1.2.0", p)
	b.LogRecvMessageStream(24, "/ipfs/bitswap", p)
	b.LogRecvMessageStream(5000, "/ipfs/kad/1.0.0", p)
	if got := b.of(p); got != 1024 {
		t.Fatalf("counted %d bitswap bytes, want 1024", got)
	}
	if inner.in != 6024 {
		t.Fatalf("the node's bandwidth counter saw %d bytes, want all 6024", inner.in)
	}
	b.drop(p)
	if got := b.of(p); got != 0 {
		t.Fatalf("%d bytes still counted after drop", got)
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
