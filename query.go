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
