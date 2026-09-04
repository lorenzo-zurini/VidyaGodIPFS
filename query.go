package main

// query.go — read-only status helpers backing the IPFS tab columns.

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	filestore "github.com/ipfs/boxo/filestore"
	merkledag "github.com/ipfs/boxo/ipld/merkledag"
	unixfs "github.com/ipfs/boxo/ipld/unixfs"
	cid "github.com/ipfs/go-cid"
)

// dirSize sums the on-disk byte size under root (the node's local repo footprint — datastore + filestore index +
// intermediate blocks; leaf data lives in the referenced package files, not here).
func dirSize(root string) int64 {
	var total int64
	_ = filepath.WalkDir(root, func(_ string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if info, e := d.Info(); e == nil {
			total += info.Size()
		}
		return nil
	})
	return total
}

// cidSize returns the CumulativeSize of a CID's DAG (dag-pb root.Size() == cumulative size), -1 on error. Bounded so a
// no-provider fetch can't hang; on a hostile network where libp2p can't get the root, it falls back to an HTTPS gateway
// HEAD so the UI still shows a size + speed for gateway-served downloads.
func (n *node) cidSize(c cid.Cid) int64 {
	getCtx, cancel := context.WithTimeout(n.ctx, 15*time.Second)
	nd, err := n.dserv.Get(getCtx, c)
	cancel()
	if err == nil {
		if sz, serr := nd.Size(); serr == nil {
			return int64(sz)
		}
	}
	sctx, scancel := context.WithTimeout(n.ctx, 20*time.Second)
	defer scancel()
	return n.gatewaySize(sctx, c)
}

// cidFileSizeLocal returns the UnixFS FILE size (payload bytes) for a CID, from the local store only, or -1.
//
// Distinct from cidSizeLocal, which reports the ENCODED DAG size (payload + block/protobuf overhead). Comparing
// that against st_size would mismatch on essentially every file. This reads only the ROOT block, so it is cheap
// enough to run before every seed — the point being to notice "the file on disk is no longer the file we
// published" without reading gigabytes.
func (n *node) cidFileSizeLocal(c cid.Cid) int64 {
	ctx, cancel := context.WithTimeout(n.ctx, 5*time.Second)
	defer cancel()
	nd, err := n.localDserv.Get(ctx, c)
	if err != nil {
		return -1
	}
	// Raw leaves (a file smaller than one chunk) carry the payload directly.
	if c.Type() == cid.Raw {
		return int64(len(nd.RawData()))
	}
	pn, ok := nd.(*merkledag.ProtoNode)
	if !ok {
		return -1
	}
	fsn, err := unixfs.FSNodeFromBytes(pn.Data())
	if err != nil {
		return -1
	}
	return int64(fsn.FileSize())
}

// cidSizeLocal is cidSize restricted to the LOCAL store: no bitswap, no gateway. Returns -1 immediately when the
// root block isn't locally readable (absent, or an orphaned filestore reference). This is what periodic status
// refresh must use — the network-falling cidSize blocks up to 35s per un-readable pin, and a repo with hundreds of
// orphaned references turns the first GUI refresh into hours of "off" (the bug this fixed).
func (n *node) cidSizeLocal(c cid.Cid) int64 {
	ctx, cancel := context.WithTimeout(n.ctx, 5*time.Second) // disk read; the bound is just a safety net
	defer cancel()
	nd, err := n.localDserv.Get(ctx, c)
	if err != nil {
		return -1
	}
	if sz, serr := nd.Size(); serr == nil {
		return int64(sz)
	}
	return -1
}

// pinLs returns the recursively-pinned (seeded) CIDs (drains the streaming pinner API).
func (n *node) pinLs() ([]cid.Cid, error) {
	var out []cid.Cid
	for sp := range n.pinner.RecursiveKeys(n.ctx, false) {
		if sp.Err != nil {
			fmt.Fprintf(os.Stderr, "[pinLs] RecursiveKeys err: %v\n", sp.Err)
			return out, sp.Err
		}
		out = append(out, sp.Pin.Key)
	}
	return out, nil
}

// unpin removes a recursive pin.
func (n *node) unpin(c cid.Cid) error {
	return n.pinner.Unpin(n.ctx, c, true)
}

// counts returns (filestore references, plain-blockstore blocks) — the logical dedup view (ignores leveldb's
// deferred disk reclaim). After a fetch, leaves should be references and only the small dag-pb root/intermediate
// nodes plain blocks.
func (n *node) counts() (fsRefs, mainBlocks int) {
	if ch, err := n.fstore.FileManager().AllKeysChan(n.ctx); err == nil {
		for range ch {
			fsRefs++
		}
	}
	if ch, err := n.fstore.MainBlockstore().AllKeysChan(n.ctx); err == nil {
		for range ch {
			mainBlocks++
		}
	}
	return
}

// cidMissing reports whether a pinned CID's backing file (its filestore reference) is gone from disk — i.e. the
// content was seeded by reference but the underlying package file has since been deleted. Cheap: it finds the first
// filestore reference reachable from the CID (all of one file's leaves share the same path) and stats it, without
// reading or hash-verifying any block contents.
// hasLocal reports whether the node holds a block for c locally (a filestore reference OR a plain block) WITHOUT any
// network fetch. A reference whose backing file is gone still counts as "has" — pair with cidMissing to tell a
// healthy reference from an orphaned one (additive seeding skips only healthy-present CIDs).
func (n *node) hasLocal(c cid.Cid) bool {
	has, err := n.fstore.Has(n.ctx, c)
	return err == nil && has
}

// cidMissing reports whether content the node believes it holds (via filestore references) is actually un-serveable
// because a backing file is gone. It walks the WHOLE reference chain, not just the first leaf: a file that moved
// wholesale orphans every leaf together, but a PARTIALLY-broken reference (only some leaves' backing gone) would slip
// past a first-leaf check and then fail only when a peer requests those specific blocks — the "green locally, errors
// only while serving" bug. Each DISTINCT backing path is stat'd once (a large file's thousands of leaves share one
// path, so the walk is cheap) and the CID is "missing" if ANY backing is gone. Uses the LOCAL-only DAG service so an
// absent block never triggers a network fetch. A CID with no filestore references at all (we simply don't host it) is
// NOT missing — matches the previous firstRefPath=="" semantics.
// orphanedRefPaths scans the filestore's references and returns the DISTINCT backing paths whose file is gone. Cheap
// relative to per-CID cidMissing: one datastore scan + one stat per distinct file (fileOrder groups a file's leaves so
// each path is met contiguously). This is the SERVE-reliability probe — every path returned is content some pinned CID
// references but the node can no longer read for a requesting peer, i.e. an orphan the heal should re-point.
func (n *node) orphanedRefPaths() []string {
	next, err := filestore.ListAll(n.ctx, n.fstore, true) // fileOrder: a file's leaves are contiguous → 1 stat/file
	if err != nil || next == nil {
		return nil
	}
	var gone []string
	seen := map[string]struct{}{}
	for {
		r := next(n.ctx)
		if r == nil {
			break
		}
		p := filepath.Join("/", r.FilePath)
		if p == "/" {
			continue
		}
		if _, done := seen[p]; done {
			continue
		}
		seen[p] = struct{}{}
		if _, statErr := os.Stat(p); statErr != nil {
			gone = append(gone, p)
		}
	}
	return gone
}

// cidUnservable READS every block of c out of the local blockstore and returns the first read error, or "" if the
// whole DAG serves cleanly. This is the check cidMissing cannot do: cidMissing only os.Stat()s the backing path, so
// a reference whose file still EXISTS but whose BYTES have changed passes it while every real bitswap request fails
// with "data in file did not match ... offset N". Such a ref is worse than a missing one — we advertise the block,
// a peer asks for it, and the transfer hangs instead of failing over to another provider.
//
// Cost: reads the referenced bytes (the filestore verifies each leaf's hash against its backing range on Get), so
// this is I/O-bound and belongs in an explicit deep pass, never on a background timer.
func (n *node) cidUnservable(c cid.Cid) string {
	seen := cid.NewSet()
	var walk func(c cid.Cid) string
	walk = func(c cid.Cid) string {
		if !seen.Visit(c) {
			return ""
		}
		// Get through the filestore-backed blockstore: this is the exact path bitswap serves from, so any
		// posinfo/hash disagreement surfaces here the same way it would for a remote peer.
		if _, err := n.bstore.Get(n.ctx, c); err != nil {
			return fmt.Sprintf("%s: %v", c.String(), err)
		}
		if res := filestore.List(n.ctx, n.fstore, c); res != nil && res.FilePath != "" {
			return "" // a verified filestore leaf has no links to descend into
		}
		nd, err := n.localDserv.Get(n.ctx, c)
		if err != nil {
			return "" // not a dag-pb node we hold — nothing further to verify locally
		}
		for _, l := range nd.Links() {
			if e := walk(l.Cid); e != "" {
				return e // short-circuit on the first unservable block
			}
		}
		return ""
	}
	return walk(c)
}

// unservableRefs walks the ENTIRE filestore index and verifies each entry's bytes against its backing range,
// returning one record per BAD entry. This is broader than verifying the CIDs a manifest records: a stale entry can
// survive under a CID no node JSON references any more (a superseded delta, a re-published package), and we keep
// advertising it — so a peer asks for that block and hangs. Nothing that starts from the recorded CIDs can find it;
// only enumerating the index does. Status 12 = contents changed, 11 = backing file gone.
func (n *node) unservableRefs() []map[string]interface{} {
	next, err := filestore.VerifyAll(n.ctx, n.fstore, true) // fileOrder: a file's leaves are contiguous
	if err != nil || next == nil {
		return nil
	}
	out := []map[string]interface{}{}
	for {
		r := next(n.ctx)
		if r == nil {
			break
		}
		if r.Status == filestore.StatusOk {
			continue
		}
		out = append(out, map[string]interface{}{
			"cid":    r.Key.String(),
			"path":   filepath.Join("/", r.FilePath),
			"status": int(r.Status),
			"err":    r.ErrorMsg,
		})
	}
	return out
}

// cidServeStatus answers the only question the UI should be claiming: CAN WE DELIVER THIS? "" means yes as far
// as a cheap check can tell; otherwise a human-readable reason.
//
// "Seeded" used to mean nothing more than "a pin exists", which is why a stale reference displayed as happily
// seeded while every peer request for it hung. This checks the two conditions that cost only a stat:
//   - every backing file still EXISTS (what cidMissing does), and
//   - each backing file is still the SIZE its references cover — a file's refs tile it exactly, so a file that
//     grew or was truncated no longer matches what we published.
//
// It deliberately does NOT read file contents: proving deliverability byte-for-byte means reading everything
// (61 GB here), so that stays in the explicit verify path. A same-size edit therefore still passes this.
func (n *node) cidServeStatus(c cid.Cid) string {
	extent := map[string]uint64{} // backing path → highest offset+size any reference covers
	seen := cid.NewSet()
	var walk func(c cid.Cid) string
	walk = func(c cid.Cid) string {
		if !seen.Visit(c) {
			return ""
		}
		if res := filestore.List(n.ctx, n.fstore, c); res != nil && res.FilePath != "" {
			p := filepath.Join("/", res.FilePath)
			st, err := os.Stat(p)
			if err != nil {
				return "backing file gone: " + p
			}
			if end := res.Offset + res.Size; end > extent[p] {
				extent[p] = end
			}
			if uint64(st.Size()) < extent[p] {
				return "backing file truncated: " + p
			}
			return ""
		}
		nd, err := n.localDserv.Get(n.ctx, c)
		if err != nil {
			return ""
		}
		for _, l := range nd.Links() {
			if e := walk(l.Cid); e != "" {
				return e
			}
		}
		return ""
	}
	if e := walk(c); e != "" {
		return e
	}
	// NOTE: a file that GREW is deliberately NOT an error. The references still tile the original bytes, so every
	// block a peer asks for still reads correctly — appending to a file does not stop us serving what we
	// published. Verified: appending 4 KiB leaves the CID fully servable. Only a file that SHRANK below the
	// covered extent (handled above) actually breaks delivery. Treating "grew" as broken would have been a false
	// alarm that also destroyed a working reference during repair.
	return ""
}

func (n *node) cidMissing(c cid.Cid) bool {
	checked := map[string]bool{} // backing path -> exists (memoized across a file's many same-path leaves)
	seen := cid.NewSet()
	var walk func(c cid.Cid) bool
	walk = func(c cid.Cid) bool {
		if !seen.Visit(c) {
			return false
		}
		if res := filestore.List(n.ctx, n.fstore, c); res != nil && res.FilePath != "" {
			// A filestore leaf. List returns the path RELATIVE to the FileManager root ("/", see node.go).
			p := filepath.Join("/", res.FilePath)
			ok, done := checked[p]
			if !done {
				_, err := os.Stat(p)
				ok = err == nil
				checked[p] = ok
			}
			return !ok // backing gone → this leaf, and thus the CID, is un-serveable
		}
		// Plain block (dag-pb root/intermediate) — descend. Local-only: a block we don't hold isn't a broken ref.
		nd, err := n.localDserv.Get(n.ctx, c)
		if err != nil {
			return false
		}
		for _, l := range nd.Links() {
			if walk(l.Cid) {
				return true // short-circuit on the first broken leaf
			}
		}
		return false
	}
	return walk(c)
}
