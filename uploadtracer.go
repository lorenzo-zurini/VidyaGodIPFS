package main

// uploadtracer.go — per-CID upload ("who's pulling from us") signal for the GUI.
//
// libp2p's BandwidthCounter gives only the GLOBAL up rate. To know WHICH seeded items are being uploaded, we hook
// bitswap with a Tracer: every block we serve passes through MessageSent. We only care about blocks that are a PINNED
// ROOT (the CID a peer asks for when it starts fetching one of our seeded items) — matched against an in-memory
// pinnedSet so the hot path stays a map lookup. A root stays "active" for a window after it was last served, which
// approximates the peer then pulling that item's leaves (root-level attribution; see the plan's non-goals).

import (
	"context"
	"time"

	bsmsg "github.com/ipfs/boxo/bitswap/message"
	peer "github.com/libp2p/go-libp2p/core/peer"
)

// upTracer implements boxo/bitswap/tracer.Tracer.
type upTracer struct{ n *node }

func (upTracer) MessageReceived(peer.ID, bsmsg.BitSwapMessage) {}

func (t upTracer) MessageSent(_ peer.ID, m bsmsg.BitSwapMessage) {
	blocks := m.Blocks()
	if len(blocks) == 0 {
		return
	}
	ps, _ := t.n.pinnedSet.Load().(map[string]struct{})
	if len(ps) == 0 {
		return
	}
	now := time.Now().UnixMilli()
	t.n.upMu.Lock()
	for _, b := range blocks {
		c := b.Cid().String()
		if _, ok := ps[c]; ok {
			t.n.upSeen[c] = now
		}
	}
	t.n.upMu.Unlock()
}

// refreshPinnedSet rebuilds the pinned-root set periodically (and once up front) so MessageSent stays lock-free of the
// datastore. Cheap: RecursiveKeys streams the ~hundreds of pinned roots.
func (n *node) refreshPinnedSet(ctx context.Context) {
	build := func() {
		set := make(map[string]struct{})
		for sp := range n.pinner.RecursiveKeys(ctx, false) {
			if sp.Err != nil {
				break
			}
			set[sp.Pin.Key.String()] = struct{}{}
		}
		n.pinnedSet.Store(set)
	}
	build()
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			build()
		}
	}
}

// activeUploads returns the pinned-root CIDs served to a peer within the last windowMs, pruning older entries.
func (n *node) activeUploads(windowMs int64) []string {
	cutoff := time.Now().UnixMilli() - windowMs
	out := make([]string, 0)
	n.upMu.Lock()
	for c, ts := range n.upSeen {
		if ts >= cutoff {
			out = append(out, c)
		} else {
			delete(n.upSeen, c)
		}
	}
	n.upMu.Unlock()
	return out
}
