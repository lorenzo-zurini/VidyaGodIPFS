package main

// wiretrace.go — per-peer bitswap WIRE accounting for the fetch tracer (VG_FETCH_DEBUG / VG_FETCH_RATE).
//
// The fetch path already logs what WE did (opened a session, asked for N leaves, received M blocks). What it could
// not show is what the PEER did with our wants — and that is the whole question when a large fetch strands its tail
// against a third-party seeder: did our WANT-BLOCKs go out at all, did the peer answer HAVE / DONT_HAVE / nothing,
// did our CANCELs go out when blocks arrived. A 1.06 GB file stopped dead at 3015/4037 leaves against Pinata with a
// fresh session unable to get a single further block on the same connection, while a second provider served the
// remainder instantly; nothing in the existing trace could distinguish "the peer forgot our wants" from "the peer
// keeps saying DONT_HAVE" from "we stopped asking". This tracer counts every wantlist entry and block presence per
// peer so the per-second rate line can print the deltas alongside the throughput.
//
// Zero cost when tracing is off: the tracer methods return before touching the mutex. The tracer is wired into the
// same bitswap.WithTracer hook as upTracer (uploadtracer.go), which is how bitswap exposes messages at all.

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"

	bsmsg "github.com/ipfs/boxo/bitswap/message"
	pb "github.com/ipfs/boxo/bitswap/message/pb"
	peer "github.com/libp2p/go-libp2p/core/peer"
)

// wireCounts is one peer's cumulative bitswap traffic, both directions, as seen by this node.
type wireCounts struct {
	sentWantBlock, sentWantHave, sentCancel int64 // what we asked the peer for
	recvBlocks, recvBytes                   int64 // what the peer delivered
	recvHave, recvDontHave                  int64 // what the peer said about our wants
	recvWantlist                            int64 // wants the peer asked US for (we are its seeder)
}

type wireStats struct {
	mu    sync.Mutex
	peers map[peer.ID]*wireCounts
	last  map[peer.ID]wireCounts // snapshot at the previous report, for deltas
}

var wire = &wireStats{peers: map[peer.ID]*wireCounts{}, last: map[peer.ID]wireCounts{}}

// wireTracing is the master switch: both the fetch tracer and the rate line read it.
var wireTracing = os.Getenv("VG_FETCH_DEBUG") != "" || os.Getenv("VG_FETCH_RATE") != ""

func (w *wireStats) get(p peer.ID) *wireCounts {
	c := w.peers[p]
	if c == nil {
		c = &wireCounts{}
		w.peers[p] = c
	}
	return c
}

// sent records an outgoing message: our wantlist entries to the peer.
func (w *wireStats) sent(p peer.ID, m bsmsg.BitSwapMessage) {
	if !wireTracing {
		return
	}
	wl := m.Wantlist()
	if len(wl) == 0 {
		return
	}
	w.mu.Lock()
	c := w.get(p)
	for _, e := range wl {
		switch {
		case e.Cancel:
			c.sentCancel++
		case e.WantType == pb.Message_Wantlist_Have:
			c.sentWantHave++
		default:
			c.sentWantBlock++
		}
	}
	w.mu.Unlock()
}

// received records an incoming message: blocks and presences the peer sent us, and wants it asked of us.
func (w *wireStats) received(p peer.ID, m bsmsg.BitSwapMessage) {
	if !wireTracing {
		return
	}
	blks := m.Blocks()
	haves := m.Haves()
	dont := m.DontHaves()
	wl := m.Wantlist()
	if len(blks) == 0 && len(haves) == 0 && len(dont) == 0 && len(wl) == 0 {
		return
	}
	w.mu.Lock()
	c := w.get(p)
	c.recvBlocks += int64(len(blks))
	for _, b := range blks {
		c.recvBytes += int64(len(b.RawData()))
	}
	c.recvHave += int64(len(haves))
	c.recvDontHave += int64(len(dont))
	c.recvWantlist += int64(len(wl))
	w.mu.Unlock()
}

// report prints, for every peer with any traffic since the last report, the DELTA of each counter, followed by the
// cumulative totals — one line per peer, so a stranded fetch reads as "+want-block=0 … blocks=3015" against a peer
// that has gone silent, versus "+dont-have=1022" against one that is refusing.
func (w *wireStats) report(prefix string) {
	if !wireTracing {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	ids := make([]peer.ID, 0, len(w.peers))
	for p := range w.peers {
		ids = append(ids, p)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	var lines []string
	for _, p := range ids {
		cur := *w.peers[p]
		prev := w.last[p]
		if cur == prev {
			continue // nothing happened with this peer since the last report
		}
		w.last[p] = cur
		lines = append(lines, fmt.Sprintf("%s peer=%s | +wb=%d +wh=%d +cancel=%d | +blk=%d(%.1fMB) +have=%d +dont=%d | tot wb=%d cancel=%d blk=%d have=%d dont=%d askedUs=%d",
			prefix, shortPeer(p.String()),
			cur.sentWantBlock-prev.sentWantBlock, cur.sentWantHave-prev.sentWantHave, cur.sentCancel-prev.sentCancel,
			cur.recvBlocks-prev.recvBlocks, float64(cur.recvBytes-prev.recvBytes)/1e6,
			cur.recvHave-prev.recvHave, cur.recvDontHave-prev.recvDontHave,
			cur.sentWantBlock, cur.sentCancel, cur.recvBlocks, cur.recvHave, cur.recvDontHave, cur.recvWantlist))
	}
	if len(lines) > 0 {
		fmt.Fprintln(os.Stderr, strings.Join(lines, "\n"))
	}
}
