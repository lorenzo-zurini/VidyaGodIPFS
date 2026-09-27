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
// It then OWES us the want-blocks it held. While out, its HAVEs and DONT_HAVEs are kept from the client (they would
// make it the session's best peer again) and our finder does not offer it; its own wants still reach our server.
//
// Nothing it sends is thrown away: a block it delivers while out reaches the client — after the client is told it is
// back (a session registers any peer a block comes from; one the client thinks gone would have no queue for the
// wants it is then given). Back, but on PROBATION until it has paid what it owed or its cooldown ends: its blocks
// and DONT_HAVEs reach the client, its HAVEs do not, so a session prefers any peer that does say HAVE (it can still
// pick it where nobody has: the sole-provider case). An honest peer that hiccuped pays everything at once (its server
// sends what it had queued) and is fully back, its record cleared; a peer that trickles a block now and then never
// pays, and never wins wants on the strength of that trickle. DONT_HAVEs settle what it owes but clear nothing: a
// peer that said HAVE, sat on the want-block and then said DONT_HAVE lied.
//
// Offences are remembered per peer, not per connection (public nodes churn their connections; a record lost with the
// connection gave the same promiser the first cooldown over and over — replication 7: one peer taken out twelve
// times). The cooldown doubles per offence (quarantineCool, up to quarantineMaxCool); paying in full clears the
// record, and it is forgotten quarantineForget after the last offence.
//
// How long a peer may sit silent on a want-block is its own business up to a point: three of its usual block
// intervals, between quarantineAfter (3 s — a promiser, which never delivers) and quarantineMaxHold (10 s — a friend
// on a thin uplink). Progress is a delivered block, not bytes: a promiser's own HAVE replies are bitswap bytes too, and counting them let
// it hold wants for as long as any other fetch kept it answering (replication 6: three stalls).

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
	quarantineAfter   = 3 * time.Second  // a want-block held this long, with no block delivered meanwhile: the floor —
	quarantineMaxHold = 10 * time.Second // a peer that delivers slowly gets three of its own block intervals, up to this
	quarantineCool    = 30 * time.Second // the first cooldown; doubles per offence
	quarantineMaxCool = 10 * time.Minute
	quarantineForget  = time.Hour // a peer's offences are forgotten this long after its last
	quarantineTick    = time.Second
)

type peerLedger struct {
	wanted    map[cid.Cid]time.Time // want-blocks sent and not answered (block, DONT_HAVE) or cancelled
	asked     map[cid.Cid]struct{}  // every want (have or block) sent and not answered or cancelled: its HAVEs we note
	owed      map[cid.Cid]bool      // the want-blocks it held when taken out, not yet answered (true: it had said HAVE)
	denied    bool                  // it answered with a DONT_HAVE a block it had said it had
	lastBlock time.Time             // its last delivery
	gap       time.Duration         // its usual interval between deliveries (0: none seen)
	progress  time.Time             // its last block (or its ledger's start)
	until     time.Time             // out, or on probation, until then (zero: in)
	probation bool                  // back with the client (it delivered while out), its HAVEs kept from it
	linked    bool                  // connected, as the network last told bitswap
}

// peerRecord: a peer's offences, kept across its connections.
type peerRecord struct {
	strikes int
	last    time.Time
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
	dmu     sync.RWMutex // held shared while a message or connect event reaches bitswap, exclusively to move a peer
	mu      sync.Mutex   // peers, records, haves
	peers   map[peer.ID]*peerLedger
	records map[peer.ID]*peerRecord
	haves   map[cid.Cid]map[peer.ID]struct{} // who, in the rotation, said HAVE for a block we asked them for
	client  peerReceiver                     // told a quarantined peer left
	server  peerReceiver                     // never told: it keeps serving the peer (nil: no server)
	recv    bsnetwork.Receiver               // the Bitswap both sit behind
	now     func() time.Time
}

func newQuarantineNet(inner bsnetwork.BitSwapNetwork) *quarantineNet {
	return &quarantineNet{BitSwapNetwork: inner, peers: map[peer.ID]*peerLedger{}, records: map[peer.ID]*peerRecord{},
		haves: map[cid.Cid]map[peer.ID]struct{}{}, now: time.Now}
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
		l = &peerLedger{wanted: map[cid.Cid]time.Time{}, asked: map[cid.Cid]struct{}{}, owed: map[cid.Cid]bool{}, progress: q.now()}
		q.peers[p] = l
	}
	return l
}

// hold: how long this peer may hold a want-block with nothing delivered — quarantineAfter, or three of its own block
// intervals for a peer that delivers slowly (a friend on a thin uplink serving several of us), up to
// quarantineMaxHold. A peer that never delivered gets the floor.
func (l *peerLedger) hold() time.Duration {
	d := 3 * l.gap
	if d < quarantineAfter {
		return quarantineAfter
	}
	if d > quarantineMaxHold {
		return quarantineMaxHold
	}
	return d
}

// held: out of the rotation or on probation at t.
func (l *peerLedger) held(t time.Time) bool { return t.Before(l.until) }

// outAt: out of the client's rotation at t (the client thinks it gone).
func (l *peerLedger) outAt(t time.Time) bool { return !l.probation && l.held(t) }

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
	if outs, backs, ends := q.scan(false); len(outs)+len(backs)+len(ends) == 0 {
		return
	}
	q.dmu.Lock()
	defer q.dmu.Unlock()
	outs, backs, _ := q.scan(true)
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

// scan: who goes out, who comes back from out (the client is told), whose probation ends (the client already has it)
// — moved only when apply. A ledger whose peer is gone, and not out, is dropped either way (nobody to tell); so is a
// record past quarantineForget.
func (q *quarantineNet) scan(apply bool) (outs, backs, ends []peer.ID) {
	now := q.now()
	q.mu.Lock()
	defer q.mu.Unlock()
	for p, r := range q.records {
		if l := q.peers[p]; now.Sub(r.last) > quarantineForget && (l == nil || !l.held(now)) {
			delete(q.records, p)
		}
	}
	for p, l := range q.peers {
		if !l.linked && !l.held(now) { // gone, and neither out nor on probation: nothing to keep
			delete(q.peers, p)
			continue
		}
		if !l.until.IsZero() && !l.held(now) { // its cooldown is over: back in, its record kept
			if l.probation {
				ends = append(ends, p)
			} else {
				backs = append(backs, p)
			}
			if apply {
				l.until, l.probation = time.Time{}, false // wanted is empty: cleared going out, not recorded while out
				clear(l.owed)
			}
			continue
		}
		hold := l.hold()
		if l.outAt(now) || len(l.wanted) == 0 || now.Sub(l.progress) < hold {
			continue
		}
		held, offered := 0, false
		for c, t := range l.wanted {
			if now.Sub(t) < hold {
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
		r := q.records[p]
		if r == nil {
			r = &peerRecord{}
			q.records[p] = r
		}
		r.strikes++
		r.last = now
		cool := quarantineCool << (r.strikes - 1)
		if cool > quarantineMaxCool || cool <= 0 {
			cool = quarantineMaxCool
		}
		l.until, l.probation, l.denied = now.Add(cool), false, false
		for c := range l.wanted {
			_, said := q.haves[c][p]
			l.owed[c] = said
		}
		fmt.Fprintf(os.Stderr, "[quarantine] %s held %d want-block(s) for %s+ and delivered nothing, and another peer offers them — out of our download rotation for %s (offence %d)\n",
			shortPeer(p.String()), held, hold.Round(100*time.Millisecond), cool, r.strikes)
		clear(l.wanted)
		q.dropAsked(p, l) // the client stops talking to it: no cancel will follow, and its offers are void
	}
	return outs, backs, ends
}

// offeredElsewhere: a peer other than p said HAVE for c. It is in the rotation: a peer's offers go when it goes out
// or away, and are not noted while it is out or on probation. Under mu.
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
// DONT_HAVEs, and its HAVEs for what we asked it (an unsolicited HAVE is no offer, and would never be cleared). Under mu.
func (q *quarantineNet) noteReceived(p peer.ID, l *peerLedger, m bsmsg.BitSwapMessage) {
	blks, have, dh := m.Blocks(), m.Haves(), m.DontHaves()
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
		now := q.now()
		if d := now.Sub(l.lastBlock); !l.lastBlock.IsZero() && d < quarantineMaxHold { // an idle gap is no pace
			if l.gap == 0 {
				l.gap = d
			} else {
				l.gap = (3*l.gap + d) / 4
			}
		}
		l.progress, l.lastBlock = now, now
	}
}

// admit: what of a message from p reaches bitswap (nil: nothing), and whether the client must first be told p is back.
// A held peer's blocks and DONT_HAVEs pay what it owes; paid in full, it is back and its record cleared. Out, only
// its wants reach us (for our server) — unless it delivers, which brings it back on probation. On probation, all but
// its HAVEs reach the client.
func (q *quarantineNet) admit(p peer.ID, m bsmsg.BitSwapMessage) (bsmsg.BitSwapMessage, bool) {
	now := q.now()
	q.mu.Lock()
	defer q.mu.Unlock()
	l := q.ledger(p)
	if !l.held(now) {
		q.noteReceived(p, l, m)
		return m, false
	}
	for _, b := range m.Blocks() {
		delete(l.owed, b.Cid())
	}
	for _, c := range m.DontHaves() {
		if said, ok := l.owed[c]; ok {
			delete(l.owed, c)
			l.denied = l.denied || said // an optimistic want-block (sent with no HAVE) it may lack in good faith
		}
	}
	wasOut, delivered := !l.probation, len(m.Blocks()) > 0
	if len(l.owed) == 0 { // settled: back in — and, paid in blocks (an honest peer that hiccuped), its record cleared
		l.until, l.probation = time.Time{}, false
		if !l.denied {
			delete(q.records, p)
		}
		q.noteReceived(p, l, m)
		return m, wasOut && l.linked
	}
	if wasOut && !delivered {
		if len(m.Wantlist()) == 0 {
			return nil, false
		}
		return wantsOnly(m), false // its wants: our server (and tracer); nothing for the client
	}
	announce := wasOut && l.linked
	l.probation = true
	fwd := withoutHaves(m)
	q.noteReceived(p, l, fwd)
	if len(fwd.Blocks())+len(fwd.DontHaves())+len(fwd.Wantlist()) == 0 {
		return nil, announce // its HAVEs were all it said
	}
	return fwd, announce
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

// withoutHaves: m less its HAVEs — a peer on probation, whose offers must not win it wants.
func withoutHaves(m bsmsg.BitSwapMessage) bsmsg.BitSwapMessage {
	w := wantsOnly(m)
	for _, b := range m.Blocks() {
		w.AddBlock(b)
	}
	for _, c := range m.DontHaves() {
		w.AddDontHave(c)
	}
	w.SetPendingBytes(m.PendingBytes())
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
	fwd, announce := r.q.admit(p, m)
	if announce { // back in the client's rotation first, so what it sends finds it registered
		r.q.client.PeerConnected(p)
	}
	if fwd != nil {
		r.q.recv.ReceiveMessage(ctx, p, fwd)
	}
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

// PeerDisconnected: what it held and offered goes with it; its ledger goes at the next check unless it is out or on
// probation (its record, and so its strikes, stay regardless).
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
