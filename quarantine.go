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
//   - nothing arrived from it on bitswap in that time — BYTES, not whole blocks: a block still crossing a slow link is
//     progress (bitswapBytes),
//   - another peer, in the rotation, has said HAVE for a block it holds: there is somewhere better to ask. A sole
//     provider, however slow, is never taken out — that would turn slow into stopped.
// While out, its HAVEs and DONT_HAVEs are kept from the client (they would make it the session's best peer again) and
// our finder does not offer it; its own wants still reach our server. The moment it delivers a block it is back, and
// the block reaches the client — nothing it sends is thrown away. The cooldown doubles per offence (quarantineCool,
// up to quarantineMaxCool) and starts over once it delivers.

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	bitswap "github.com/ipfs/boxo/bitswap"
	bsmsg "github.com/ipfs/boxo/bitswap/message"
	pb "github.com/ipfs/boxo/bitswap/message/pb"
	bsnetwork "github.com/ipfs/boxo/bitswap/network"
	cid "github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/metrics"
	peer "github.com/libp2p/go-libp2p/core/peer"
	protocol "github.com/libp2p/go-libp2p/core/protocol"
)

// Vars so tests can shorten them.
var (
	quarantineAfter   = 6 * time.Second  // a want-block held this long, with nothing arriving from the peer meanwhile
	quarantineCool    = 30 * time.Second // the first cooldown; doubles per offence
	quarantineMaxCool = 10 * time.Minute
	quarantineTick    = time.Second
)

type peerLedger struct {
	wanted   map[cid.Cid]time.Time // want-blocks sent and not answered (block, DONT_HAVE) or cancelled
	bytesIn  int64                 // its bitswap bytes at the last check
	progress time.Time             // the last time anything arrived from it on bitswap
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
	haves  map[cid.Cid]map[peer.ID]struct{} // who said HAVE for a block we want
	client peerReceiver                     // told a quarantined peer left
	server peerReceiver                     // never told: it keeps serving the peer (nil: no server)
	recv   bsnetwork.Receiver               // the Bitswap both sit behind
	now    func() time.Time
	bytes  func(peer.ID) int64 // its bitswap bytes so far (nil: only a block is progress)
	forget func(peer.ID)       // drops that count
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
		l = &peerLedger{wanted: map[cid.Cid]time.Time{}, progress: q.now()}
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
// Exclusive over delivery: no message or connect event reaches bitswap while a peer moves in or out under it.
func (q *quarantineNet) check() {
	q.dmu.Lock()
	defer q.dmu.Unlock()
	now := q.now()
	var outs, backs []peer.ID
	q.mu.Lock()
	for p, l := range q.peers {
		if q.bytes != nil {
			if b := q.bytes(p); b != l.bytesIn {
				l.bytesIn, l.progress = b, now
			}
		}
		if !l.until.IsZero() {
			if l.outAt(now) {
				continue
			}
			l.until = time.Time{} // wanted is empty: cleared going out, and nothing is recorded while out
			if l.linked {
				backs = append(backs, p)
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
				offered = q.offeredElsewhere(c, p, now)
			}
		}
		if held == 0 || !offered {
			continue
		}
		l.strikes++
		cool := quarantineCool << (l.strikes - 1)
		if cool > quarantineMaxCool || cool <= 0 {
			cool = quarantineMaxCool
		}
		l.until = now.Add(cool)
		fmt.Fprintf(os.Stderr, "[quarantine] %s held %d want-block(s) for %s+ with nothing arriving, and another peer offers them — out of our download rotation for %s\n",
			shortPeer(p.String()), held, quarantineAfter, cool)
		clear(l.wanted)
		outs = append(outs, p)
	}
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

// offeredElsewhere: a peer other than p, in the rotation, said HAVE for c (a peer's HAVEs go with its connection).
// Under mu.
func (q *quarantineNet) offeredElsewhere(c cid.Cid, p peer.ID, now time.Time) bool {
	for o := range q.haves[c] {
		if ol := q.peers[o]; o != p && ol != nil && !ol.outAt(now) {
			return true
		}
	}
	return false
}

// noteSent records the want-blocks (and cancels) a message to p carries. A want that reaches a peer while it is out
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
	}
	for _, e := range wl {
		switch {
		case e.Cancel:
			delete(l.wanted, e.Cid)
			delete(q.haves, e.Cid) // the want is over: who offered it no longer matters
		case e.WantType == pb.Message_Wantlist_Block && !out:
			if _, ok := l.wanted[e.Cid]; !ok {
				l.wanted[e.Cid] = now // a re-sent want keeps its first time
			}
		}
	}
}

// noteReceived records what a message from p answers and offers: its blocks (progress; the want is over), its
// DONT_HAVEs, its HAVEs.
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
		delete(q.haves, b.Cid())
	}
	for _, c := range dh {
		delete(l.wanted, c)
		if who := q.haves[c]; who != nil {
			delete(who, p)
			if len(who) == 0 {
				delete(q.haves, c)
			}
		}
	}
	for _, c := range have {
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

func (r quarantineReceiver) PeerDisconnected(p peer.ID) {
	r.q.dmu.RLock()
	defer r.q.dmu.RUnlock()
	r.q.mu.Lock()
	for c, who := range r.q.haves {
		delete(who, p)
		if len(who) == 0 {
			delete(r.q.haves, c)
		}
	}
	if l := r.q.peers[p]; l != nil {
		l.linked = false
		clear(l.wanted)
		if l.until.IsZero() && l.strikes == 0 { // nothing to remember: a peer that comes and goes costs nothing
			delete(r.q.peers, p)
			if r.q.forget != nil {
				r.q.forget(p)
			}
		}
	}
	r.q.mu.Unlock()
	r.q.recv.PeerDisconnected(p)
}

// bitswapBytes counts, per peer, the bytes read from its bitswap streams — progress a block still crossing a slow link
// already shows (the bandwidth counter's per-peer totals cover every protocol: a peer's DHT chatter is not progress).
type bitswapBytes struct {
	metrics.Reporter
	mu sync.Mutex
	in map[peer.ID]int64
}

func newBitswapBytes(inner metrics.Reporter) *bitswapBytes {
	return &bitswapBytes{Reporter: inner, in: map[peer.ID]int64{}}
}

func (b *bitswapBytes) LogRecvMessageStream(size int64, proto protocol.ID, p peer.ID) {
	if strings.HasPrefix(string(proto), "/ipfs/bitswap") {
		b.mu.Lock()
		b.in[p] += size
		b.mu.Unlock()
	}
	b.Reporter.LogRecvMessageStream(size, proto, p)
}

func (b *bitswapBytes) of(p peer.ID) int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.in[p]
}

func (b *bitswapBytes) drop(p peer.ID) {
	b.mu.Lock()
	delete(b.in, p)
	b.mu.Unlock()
}
