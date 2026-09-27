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
)

// The rules, on a fake clock. Teeth, each caught below: count a HAVE as a delivery; quarantine a peer that is still
// delivering; keep a cancelled want on the ledger; stop doubling the cooldown; keep the strikes after a delivery.
func TestQuarantineRules(t *testing.T) {
	a, c, m := quarantineAfter, quarantineCool, quarantineMaxCool
	quarantineAfter, quarantineCool, quarantineMaxCool = 5*time.Second, 30*time.Second, 10*time.Minute
	defer func() { quarantineAfter, quarantineCool, quarantineMaxCool = a, c, m }()
	now := time.Unix(1000, 0)
	q := newQuarantineNet(nil)
	q.now = func() time.Time { return now }
	blk := func(s string) blocks.Block { return blocks.NewBlock([]byte(s)) }
	want := func(p peer.ID, cs ...cid.Cid) {
		m := bsmsg.New(false)
		for _, c := range cs {
			m.AddEntry(c, 1, pb.Message_Wantlist_Block, true)
		}
		q.noteSent(p, m)
	}
	recv := func(p peer.ID, f func(bsmsg.BitSwapMessage)) { m := bsmsg.New(false); f(m); q.noteReceived(p, m) }
	advance := func(d time.Duration) { now = now.Add(d); q.check() }

	// A peer that answers HAVE and never delivers is quarantined once it has held a want-block for quarantineAfter.
	promiser, seeder := peer.ID("promiser"), peer.ID("seeder")
	x, y, z := blk("x"), blk("y"), blk("z")
	want(promiser, x.Cid())
	recv(promiser, func(m bsmsg.BitSwapMessage) { m.AddHave(x.Cid()) })
	advance(4 * time.Second)
	if q.out(promiser) {
		t.Fatal("quarantined before quarantineAfter")
	}
	recv(promiser, func(m bsmsg.BitSwapMessage) { m.AddHave(x.Cid()) }) // it keeps promising (rebroadcast want-haves)
	advance(2 * time.Second)
	if !q.out(promiser) {
		t.Fatal("a HAVE with no block is not a delivery: the promiser must be out")
	}
	// A seeder with a want-block pending for long, but delivering other blocks meanwhile, stays in.
	want(seeder, y.Cid(), z.Cid())
	for i := 0; i < 5; i++ {
		advance(2 * time.Second)
		recv(seeder, func(m bsmsg.BitSwapMessage) { m.AddBlock(blk("other" + string(rune('a'+i)))) })
	}
	if q.out(seeder) {
		t.Fatal("a peer that keeps delivering was quarantined")
	}
	// Answered or cancelled wants leave the ledger.
	recv(seeder, func(m bsmsg.BitSwapMessage) { m.AddBlock(y); m.AddDontHave(z.Cid()) })
	cancel := bsmsg.New(false)
	cancel.Cancel(x.Cid())
	want(seeder, x.Cid())
	q.noteSent(seeder, cancel)
	if n := len(q.peers[seeder].wanted); n != 0 {
		t.Fatalf("%d want(s) still held after a block, a DONT_HAVE and a cancel", n)
	}
	// The cooldown doubles per offence, and resets once the peer delivers.
	advance(31 * time.Second) // first cooldown (30 s) over
	if q.out(promiser) {
		t.Fatal("still out after its cooldown")
	}
	want(promiser, x.Cid())
	advance(6 * time.Second)
	if l := q.peers[promiser]; !q.out(promiser) || l.until.Sub(now) != 60*time.Second {
		t.Fatalf("second offence: out=%v for %s, want 1m0s", q.out(promiser), l.until.Sub(now))
	}
	advance(61 * time.Second)
	recv(promiser, func(m bsmsg.BitSwapMessage) { m.AddBlock(x) })
	want(promiser, y.Cid())
	advance(6 * time.Second)
	if l := q.peers[promiser]; l.until.Sub(now) != 30*time.Second {
		t.Fatalf("after a delivery its next cooldown is %s, want the first (30s)", l.until.Sub(now))
	}
}

// fakeSide records what reaches it.
type fakeSide struct{ msgs, conns, disconns []peer.ID }

func (f *fakeSide) ReceiveMessage(_ context.Context, p peer.ID, _ bsmsg.BitSwapMessage) {
	f.msgs = append(f.msgs, p)
}
func (f *fakeSide) PeerConnected(p peer.ID)    { f.conns = append(f.conns, p) }
func (f *fakeSide) PeerDisconnected(p peer.ID) { f.disconns = append(f.disconns, p) }
func (f *fakeSide) ReceiveError(error)         {}

// Quarantined, a peer leaves the client (told it disconnected) and nothing it sends reaches the client — a HAVE would
// make it the session's best peer again — while the server keeps getting its messages and serving it. Its cooldown
// over, the client is told it is back. Teeth: forward a quarantined peer's messages or connects to the client, or
// never tell the client it is back.
func TestQuarantineKeepsThePeerFromTheClientOnly(t *testing.T) {
	a, c := quarantineAfter, quarantineCool
	quarantineAfter, quarantineCool = 5*time.Second, 30*time.Second
	defer func() { quarantineAfter, quarantineCool = a, c }()
	now := time.Unix(1000, 0)
	client, server, all := &fakeSide{}, &fakeSide{}, &fakeSide{}
	q := newQuarantineNet(nil)
	q.now = func() time.Time { return now }
	q.client, q.server, q.recv = client, server, all
	q.linked = func(peer.ID) bool { return true }
	r := quarantineReceiver{q}
	p := peer.ID("promiser")
	x := blocks.NewBlock([]byte("x"))
	m := bsmsg.New(false)
	m.AddEntry(x.Cid(), 1, pb.Message_Wantlist_Block, true)
	q.noteSent(p, m)
	now = now.Add(6 * time.Second)
	q.check()
	if len(client.disconns) != 1 {
		t.Fatalf("the client was told %d disconnect(s), want 1", len(client.disconns))
	}
	have := bsmsg.New(false)
	have.AddHave(x.Cid())
	r.ReceiveMessage(context.Background(), p, have)
	r.PeerConnected(p)
	if len(all.msgs) != 0 || len(all.conns) != 0 {
		t.Fatal("a quarantined peer's message or connect reached the client")
	}
	if len(server.msgs) != 1 || len(server.conns) != 1 {
		t.Fatal("the server must keep getting a quarantined peer's messages")
	}
	now = now.Add(31 * time.Second)
	q.check()
	if len(client.conns) != 1 {
		t.Fatal("its cooldown over, the client was not told the peer is back")
	}
	r.ReceiveMessage(context.Background(), p, have)
	if len(all.msgs) != 1 {
		t.Fatal("back in the rotation, its messages must reach bitswap again")
	}
}
