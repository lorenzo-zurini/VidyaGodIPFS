package main

// quarantine.go — a peer that sits on our want-blocks while another peer offers them is taken out of our download
// rotation.
//
// boxo's sessions park a want on a peer that will never answer it, for good: a peer that said HAVE keeps "having" the
// block after its DONT_HAVE timeout (blockpresencemanager never lets a DONT_HAVE override a HAVE), the session sends it
// the want-block again, the peer-want manager drops that as already sent — and no new timeout starts. A peer without
// HAVE support (bitswap 1.1) gets no timeout at all. On the replication, bitswap.pinata.cloud answered HAVE for every
// block it pins and delivered none: wants sat on it while the seeder, which had said HAVE too, was never asked, until
// our 20 s stall watchdog tore the fetch down (a 4.7 GB file then crawled in over the gateway; content only a friend
// has would just have stalled).
//
// boxo re-routes every want a peer held the moment that peer disconnects. So a peer is reported DISCONNECTED to our
// client (not closed: the connection stays, our server keeps serving it) when, all three:
//   - it has held a want-block for quarantineAfter,
//   - it delivered no block in that time,
//   - another peer, in the rotation, has said HAVE for a block it holds: there is somewhere better to ask. A sole
//     provider, however slow, is never taken out — that would turn slow into stopped.
// While out, its HAVEs and DONT_HAVEs are kept from the client (they would make it the session's best peer again) and
// our finder does not offer it; its own wants still reach our server. The moment it delivers a block it is back, and
// the block reaches the client — nothing it sends is thrown away. The cooldown doubles per offence (quarantineCool,
// up to quarantineMaxCool) and starts over once it delivers.
//
// Progress is a delivered block, not bytes: a promiser's own HAVE replies are bitswap bytes too, and counting them let
// it hold wants for as long as any other fetch kept it answering (replication 6: three stalls). An honest peer taken
// out while a slow block is still crossing loses nothing: the block brings it back, and whoever then holds its wants
// without delivering is offered around and goes out in turn.

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	bitswap "github.com/ipfs/boxo/bitswap"
	bsmsg "github.com/ipfs/boxo/bitswap/message"
	pb "github.com/ipfs/boxo/bitswap/message/pb"
	bsnetwork "github.com/ipfs/boxo/bitswap/network"
	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p/core/peer"
)

// Vars so tests can shorten them.
var (
	quarantineAfter   = 6 * time.Second  // a want-block held this long, with no block delivered meanwhile
	quarantineCool    = 30 * time.Second // the first cooldown; doubles per offence
	quarantineMaxCool = 10 * time.Minute
	quarantineTick    = time.Second
)

type peerLedger struct {
	wanted   map[cid.Cid]time.Time // want-blocks sent and not answered (block, DONT_HAVE) or cancelled
	asked    map[cid.Cid]struct{}  // every want (have or block) sent and not answered or cancelled: its HAVEs we note
	progress time.Time             // its last block (or its ledger's start)
	until    time.Time             // out of the rotation until then (zero: in it)
	strikes  int
	linked   bool // connected, as the network last told bitswap
}

// peerReceiver: what the client takes from the network.
type peerReceiver interface {
	ReceiveMessage(ctx context.Context, p peer.ID, m bsmsg.BitSwapMessage)
	PeerConnected(p peer.ID)
	PeerDisconnected(p peer.ID)
}

// quarantineNet wraps the bitswap network: it sees the want-blocks our client sends and what comes back.
type quarantineNet struct {
	bsnetwork.BitSwapNetwork
	dmu    sync.RWMutex // held shared while a message or connect event reaches bitswap, exclusively to move a peer
	mu     sync.Mutex   // peers, haves
	peers  map[peer.ID]*peerLedger
	haves  map[cid.Cid]map[peer.ID]struct{} // who said HAVE for a block we asked them for
	client peerReceiver                     // told a quarantined peer left
	server peerReceiver                     // never told: it keeps serving the peer (nil: no server)
	recv   bsnetwork.Receiver               // the Bitswap both sit behind
	now    func() time.Time
}

func newQuarantineNet(inner bsnetwork.BitSwapNetwork) *quarantineNet {
	return &quarantineNet{BitSwapNetwork: inner, peers: map[peer.ID]*peerLedger{}, haves: map[cid.Cid]map[peer.ID]struct{}{}, now: time.Now}
}

// run checks the ledgers every quarantineTick until ctx ends.
func (q *quarantineNet) run(ctx context.Context) {
	t := time.NewTicker(quarantineTick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			q.check()
		}
	}
}

func (q *quarantineNet) ledger(p peer.ID) *peerLedger {
	l := q.peers[p]
	if l == nil {
		l = &peerLedger{wanted: map[cid.Cid]time.Time{}, asked: map[cid.Cid]struct{}{}, progress: q.now()}
		q.peers[p] = l
	}
	return l
}

func (l *peerLedger) outAt(t time.Time) bool { return t.Before(l.until) }

// out: p is out of the client's rotation now (the finder skips it too).
func (q *quarantineNet) out(p peer.ID) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	l := q.peers[p]
	return l != nil && l.outAt(q.now())
}

// check puts out the peers sitting on want-blocks another peer offers, and brings back those whose cooldown ended.
// Moving a peer is exclusive with delivery (no message or connect event reaches bitswap while it moves under it) —
// but only a tick that moves someone takes that lock: an exclusive lock waits out the slowest delivery in flight and
// holds every new one behind it.
func (q *quarantineNet) check() {
	if outs, backs := q.scan(false); len(outs)+len(backs) == 0 {
		return
	}
	q.dmu.Lock()
	defer q.dmu.Unlock()
	outs, backs := q.scan(true)
	q.mu.Lock()
	client := q.client
	q.mu.Unlock()
	if client == nil {
		return
	}
	for _, p := range outs {
		client.PeerDisconnected(p) // every want it held goes to the other peers
	}
	for _, p := range backs {
		client.PeerConnected(p)
	}
}

// scan: who goes out, who comes back — moved only when apply. A ledger whose peer is gone, and not out, is dropped
// either way (nobody to tell).
func (q *quarantineNet) scan(apply bool) (outs, backs []peer.ID) {
	now := q.now()
	q.mu.Lock()
	defer q.mu.Unlock()
	for p, l := range q.peers {
		if !l.linked && !l.outAt(now) {
			delete(q.peers, p)
			continue
		}
		if !l.until.IsZero() {
			if !l.outAt(now) {
				backs = append(backs, p)
				if apply {
					l.until = time.Time{} // wanted is empty: cleared going out, and nothing is recorded while out
				}
			}
			continue
		}
		if len(l.wanted) == 0 || now.Sub(l.progress) < quarantineAfter {
			continue
		}
		held, offered := 0, false
		for c, t := range l.wanted {
			if now.Sub(t) < quarantineAfter {
				continue
			}
			held++
			if !offered {
				offered = q.offeredElsewhere(c, p)
			}
		}
		if held == 0 || !offered {
			continue
		}
		outs = append(outs, p)
		if !apply {
			continue
		}
		l.strikes++
		cool := quarantineCool << (l.strikes - 1)
		if cool > quarantineMaxCool || cool <= 0 {
			cool = quarantineMaxCool
		}
		l.until = now.Add(cool)
		fmt.Fprintf(os.Stderr, "[quarantine] %s held %d want-block(s) for %s+ and delivered nothing, and another peer offers them — out of our download rotation for %s\n",
			shortPeer(p.String()), held, quarantineAfter, cool)
		clear(l.wanted)
		q.dropAsked(p, l) // the client stops talking to it: no cancel will follow, and its offers are void
	}
	return outs, backs
}

// offeredElsewhere: a peer other than p said HAVE for c. It is in the rotation: a peer's offers go when it goes out
// or away, and are not noted while it is out. Under mu.
func (q *quarantineNet) offeredElsewhere(c cid.Cid, p peer.ID) bool {
	for o := range q.haves[c] {
		if o != p {
			return true
		}
	}
	return false
}

// forgetHave: p no longer offers c. Under mu.
func (q *quarantineNet) forgetHave(c cid.Cid, p peer.ID) {
	if who := q.haves[c]; who != nil {
		delete(who, p)
		if len(who) == 0 {
			delete(q.haves, c)
		}
	}
}

// dropAsked: every want to p is over, and so is every offer it made (it only offers what it was asked). Under mu.
func (q *quarantineNet) dropAsked(p peer.ID, l *peerLedger) {
	for c := range l.asked {
		q.forgetHave(c, p)
	}
	clear(l.asked)
}

// noteSent records the wants (and cancels) a message to p carries. A want-block that reaches a peer while it is out
// (queued before it went out) is not held against it.
func (q *quarantineNet) noteSent(p peer.ID, m bsmsg.BitSwapMessage) {
	wl := m.Wantlist()
	if len(wl) == 0 {
		return
	}
	now := q.now()
	q.mu.Lock()
	defer q.mu.Unlock()
	l := q.ledger(p)
	out := l.outAt(now)
	if m.Full() {
		clear(l.wanted)
		q.dropAsked(p, l)
	}
	for _, e := range wl {
		if e.Cancel {
			delete(l.wanted, e.Cid)
			delete(l.asked, e.Cid)
			delete(q.haves, e.Cid) // the want is over: who offered it no longer matters
			continue
		}
		l.asked[e.Cid] = struct{}{}
		if _, ok := l.wanted[e.Cid]; !ok && e.WantType == pb.Message_Wantlist_Block && !out {
			l.wanted[e.Cid] = now // a re-sent want keeps its first time
		}
	}
}

// noteReceived records what a message from p answers and offers: its blocks (progress; the want is over), its
// DONT_HAVEs, and its HAVEs for what we asked it (an unsolicited HAVE is no offer, and would never be cleared).
func (q *quarantineNet) noteReceived(p peer.ID, m bsmsg.BitSwapMessage) {
	blks, have, dh := m.Blocks(), m.Haves(), m.DontHaves()
	if len(blks) == 0 && len(have) == 0 && len(dh) == 0 {
		return
	}
	now := q.now()
	q.mu.Lock()
	defer q.mu.Unlock()
	l := q.ledger(p)
	for _, b := range blks {
		delete(l.wanted, b.Cid())
		delete(l.asked, b.Cid())
		delete(q.haves, b.Cid())
	}
	for _, c := range dh {
		delete(l.wanted, c)
		delete(l.asked, c)
		q.forgetHave(c, p)
	}
	for _, c := range have {
		if _, ok := l.asked[c]; !ok {
			continue
		}
		if q.haves[c] == nil {
			q.haves[c] = map[peer.ID]struct{}{}
		}
		q.haves[c][p] = struct{}{}
	}
	if len(blks) > 0 {
		l.progress = now
		l.strikes = 0 // it delivers: its next offence starts from the first cooldown
	}
}

// release brings p back the moment it delivers; true when it was out and is connected (the client must hear of it).
func (q *quarantineNet) release(p peer.ID) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	l := q.peers[p]
	if l == nil || !l.outAt(q.now()) {
		return false
	}
	l.until = time.Time{}
	return l.linked
}

// wantsOnly: m's wantlist alone — a quarantined peer's wants for our server, without what it tells our client.
func wantsOnly(m bsmsg.BitSwapMessage) bsmsg.BitSwapMessage {
	w := bsmsg.New(m.Full())
	for _, e := range m.Wantlist() {
		if e.Cancel {
			w.Cancel(e.Cid)
		} else {
			w.AddEntry(e.Cid, e.Priority, e.WantType, e.SendDontHave)
		}
	}
	return w
}

// ---- the network, wrapped ----

func (q *quarantineNet) Start(r ...bsnetwork.Receiver) {
	if len(r) == 1 {
		if bs, ok := r[0].(*bitswap.Bitswap); ok {
			q.mu.Lock()
			q.client, q.recv = bs.Client, r[0]
			if bs.Server != nil {
				q.server = bs.Server
			}
			q.mu.Unlock()
			q.BitSwapNetwork.Start(quarantineReceiver{q})
			return
		}
	}
	q.BitSwapNetwork.Start(r...) // not a Bitswap: nothing to take a peer out of
}

func (q *quarantineNet) NewMessageSender(ctx context.Context, p peer.ID, opts *bsnetwork.MessageSenderOpts) (bsnetwork.MessageSender, error) {
	s, err := q.BitSwapNetwork.NewMessageSender(ctx, p, opts)
	if err != nil {
		return nil, err
	}
	return quarantineSender{MessageSender: s, q: q, p: p}, nil
}

func (q *quarantineNet) SendMessage(ctx context.Context, p peer.ID, m bsmsg.BitSwapMessage) error {
	err := q.BitSwapNetwork.SendMessage(ctx, p, m)
	if err == nil {
		q.noteSent(p, m)
	}
	return err
}

type quarantineSender struct {
	bsnetwork.MessageSender
	q *quarantineNet
	p peer.ID
}

func (s quarantineSender) SendMsg(ctx context.Context, m bsmsg.BitSwapMessage) error {
	err := s.MessageSender.SendMsg(ctx, m)
	if err == nil {
		s.q.noteSent(s.p, m)
	}
	return err
}

// quarantineReceiver keeps what a quarantined peer tells the client from it — until the peer delivers.
type quarantineReceiver struct{ q *quarantineNet }

func (r quarantineReceiver) ReceiveMessage(ctx context.Context, p peer.ID, m bsmsg.BitSwapMessage) {
	r.q.dmu.RLock()
	defer r.q.dmu.RUnlock()
	if r.q.out(p) {
		if len(m.Blocks()) == 0 {
			if len(m.Wantlist()) > 0 {
				r.q.recv.ReceiveMessage(ctx, p, wantsOnly(m)) // its wants: our server (and tracer); nothing for the client
			}
			return
		}
		if r.q.release(p) { // it delivers: back in the rotation first, so its blocks find it registered
			r.q.client.PeerConnected(p)
		}
	}
	r.q.noteReceived(p, m)
	r.q.recv.ReceiveMessage(ctx, p, m)
}

func (r quarantineReceiver) ReceiveError(err error) { r.q.recv.ReceiveError(err) }

func (r quarantineReceiver) PeerConnected(p peer.ID) {
	r.q.dmu.RLock()
	defer r.q.dmu.RUnlock()
	r.q.mu.Lock()
	l := r.q.ledger(p)
	l.linked = true
	out := l.outAt(r.q.now())
	r.q.mu.Unlock()
	if out {
		if r.q.server != nil {
			r.q.server.PeerConnected(p)
		}
		return
	}
	r.q.recv.PeerConnected(p)
}

// PeerDisconnected: what it held and offered goes with it; its ledger goes at the next check unless it is out (its
// strikes then last until its cooldown ends).
func (r quarantineReceiver) PeerDisconnected(p peer.ID) {
	r.q.dmu.RLock()
	defer r.q.dmu.RUnlock()
	r.q.mu.Lock()
	if l := r.q.peers[p]; l != nil {
		l.linked = false
		clear(l.wanted)
		r.q.dropAsked(p, l)
	}
	r.q.mu.Unlock()
	r.q.recv.PeerDisconnected(p)
}
