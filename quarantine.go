package main

// quarantine.go — a peer that holds our want-blocks and delivers nothing is taken out of our download rotation.
//
// boxo's sessions park a want on a peer that will never answer it, for good: a peer that said HAVE keeps "having" the
// block after its DONT_HAVE timeout (blockpresencemanager never lets a DONT_HAVE override a HAVE), the session sends it
// the want-block again, the peer-want manager drops that as already sent — and no new timeout starts. A peer without
// HAVE support (bitswap 1.1) gets no timeout at all. On the replication, bitswap.pinata.cloud answered HAVE for every
// block it pins and delivered none: wants sat on it while the seeder, which had said HAVE too, was never asked, until
// our 20 s stall watchdog tore the fetch down (a 4.7 GB file then crawled in over the gateway; content only a friend
// has would just have stalled). boxo does re-route every want a peer held the moment that peer disconnects — so a peer
// that holds want-blocks for quarantineAfter with no block delivered in that time is reported DISCONNECTED to our
// client (not closed: the connection stays, and our server keeps serving it), and its messages are kept from the
// client for a cooldown that doubles per offence (quarantineCool, up to quarantineMaxCool) and resets once it delivers.

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
	quarantineAfter   = 6 * time.Second  // a want-block held this long, with nothing delivered in that time
	quarantineCool    = 30 * time.Second // the first cooldown; doubles per offence
	quarantineMaxCool = 10 * time.Minute
	quarantineTick    = time.Second
)

type peerLedger struct {
	wanted    map[cid.Cid]time.Time // want-blocks sent and not answered (block, DONT_HAVE) or cancelled
	delivered time.Time             // the last block it sent us
	until     time.Time             // out of the rotation until then (zero: in it)
	strikes   int
}

// peerReceiver: what the client and the server each take from the network.
type peerReceiver interface {
	ReceiveMessage(ctx context.Context, p peer.ID, m bsmsg.BitSwapMessage)
	PeerConnected(p peer.ID)
	PeerDisconnected(p peer.ID)
}

// quarantineNet wraps the bitswap network: it sees the want-blocks our client sends and the blocks that come back.
type quarantineNet struct {
	bsnetwork.BitSwapNetwork
	mu     sync.Mutex
	peers  map[peer.ID]*peerLedger
	client peerReceiver       // told a quarantined peer left, kept from its messages
	server peerReceiver       // never told: it keeps serving the peer (nil: no server)
	recv   bsnetwork.Receiver // the Bitswap both sit behind
	now    func() time.Time
	linked func(peer.ID) bool // still connected (a peer that left while out is not announced back)
}

func newQuarantineNet(inner bsnetwork.BitSwapNetwork) *quarantineNet {
	q := &quarantineNet{BitSwapNetwork: inner, peers: map[peer.ID]*peerLedger{}, now: time.Now}
	q.linked = func(p peer.ID) bool { return inner.IsConnectedToPeer(context.Background(), p) }
	return q
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
		l = &peerLedger{wanted: map[cid.Cid]time.Time{}}
		q.peers[p] = l
	}
	return l
}

func (q *quarantineNet) out(p peer.ID) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	l := q.peers[p]
	return l != nil && q.now().Before(l.until)
}

// check quarantines the peers sitting on want-blocks and returns to the rotation those whose cooldown ended.
func (q *quarantineNet) check() {
	now := q.now()
	var outs, backs []peer.ID
	q.mu.Lock()
	for p, l := range q.peers {
		if !l.until.IsZero() {
			if now.Before(l.until) {
				continue
			}
			l.until = time.Time{}
			backs = append(backs, p)
			continue
		}
		if len(l.wanted) == 0 || now.Sub(l.delivered) < quarantineAfter {
			continue
		}
		oldest := now
		for _, t := range l.wanted {
			if t.Before(oldest) {
				oldest = t
			}
		}
		if now.Sub(oldest) < quarantineAfter {
			continue
		}
		l.strikes++
		cool := quarantineCool << (l.strikes - 1)
		if cool > quarantineMaxCool || cool <= 0 {
			cool = quarantineMaxCool
		}
		l.until = now.Add(cool)
		fmt.Fprintf(os.Stderr, "[quarantine] %s held %d want-block(s) for %s and delivered nothing — out of our download rotation for %s\n",
			shortPeer(p.String()), len(l.wanted), now.Sub(oldest).Round(time.Second), cool)
		l.wanted = map[cid.Cid]time.Time{}
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
		if q.linked(p) {
			client.PeerConnected(p)
		}
	}
}

// noteSent records the want-blocks (and cancels) a message to p carries.
func (q *quarantineNet) noteSent(p peer.ID, m bsmsg.BitSwapMessage) {
	wl := m.Wantlist()
	if len(wl) == 0 {
		return
	}
	now := q.now()
	q.mu.Lock()
	defer q.mu.Unlock()
	l := q.ledger(p)
	if m.Full() {
		clear(l.wanted)
	}
	for _, e := range wl {
		switch {
		case e.Cancel:
			delete(l.wanted, e.Cid)
		case e.WantType == pb.Message_Wantlist_Block:
			if _, ok := l.wanted[e.Cid]; !ok {
				l.wanted[e.Cid] = now // a re-sent want keeps its first time
			}
		}
	}
}

// noteReceived clears what a message from p answers: its blocks, its DONT_HAVEs. A HAVE is not a delivery.
func (q *quarantineNet) noteReceived(p peer.ID, m bsmsg.BitSwapMessage) {
	blks, dh := m.Blocks(), m.DontHaves()
	if len(blks) == 0 && len(dh) == 0 {
		return
	}
	now := q.now()
	q.mu.Lock()
	defer q.mu.Unlock()
	l := q.ledger(p)
	for _, b := range blks {
		delete(l.wanted, b.Cid())
	}
	for _, c := range dh {
		delete(l.wanted, c)
	}
	if len(blks) > 0 {
		l.delivered = now
		if l.until.IsZero() {
			l.strikes = 0 // it delivers: its next offence starts from the first cooldown
		}
	}
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
	q.noteSent(p, m)
	return q.BitSwapNetwork.SendMessage(ctx, p, m)
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

// quarantineReceiver keeps a quarantined peer's messages and connection events from the client; the server gets them.
type quarantineReceiver struct{ q *quarantineNet }

func (r quarantineReceiver) ReceiveMessage(ctx context.Context, p peer.ID, m bsmsg.BitSwapMessage) {
	if r.q.out(p) {
		if r.q.server != nil {
			r.q.server.ReceiveMessage(ctx, p, m)
		}
		return
	}
	r.q.noteReceived(p, m)
	r.q.recv.ReceiveMessage(ctx, p, m)
}

func (r quarantineReceiver) ReceiveError(err error) { r.q.recv.ReceiveError(err) }

func (r quarantineReceiver) PeerConnected(p peer.ID) {
	if r.q.out(p) {
		if r.q.server != nil {
			r.q.server.PeerConnected(p)
		}
		return
	}
	r.q.recv.PeerConnected(p)
}

func (r quarantineReceiver) PeerDisconnected(p peer.ID) {
	r.q.mu.Lock()
	if l := r.q.peers[p]; l != nil {
		clear(l.wanted) // gone: its cooldown and strikes stay (a reconnect is not a delivery)
	}
	r.q.mu.Unlock()
	r.q.recv.PeerDisconnected(p)
}
