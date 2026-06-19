package main

// query.go — read-only status helpers backing the IPFS tab columns.

import (
	"io/fs"
	"os"
	"path/filepath"

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

// cidSize returns the CumulativeSize of a CID's DAG (dag-pb root.Size() == cumulative size), -1 on error.
func (n *node) cidSize(c cid.Cid) int64 {
	nd, err := n.dserv.Get(n.ctx, c)
	if err != nil {
		return -1
	}
	sz, err := nd.Size()
	if err != nil {
		return -1
	}
	return int64(sz)
}

// pinLs returns the recursively-pinned (seeded) CIDs (drains the streaming pinner API).
func (n *node) pinLs() ([]cid.Cid, error) {
	var out []cid.Cid
	for sp := range n.pinner.RecursiveKeys(n.ctx, false) {
		if sp.Err != nil {
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

// cidMissing reports whether a pinned CID's backing file (its filestore reference) is gone from disk — i.e. the
// content was seeded by reference but the underlying package file has since been deleted. Cheap: it finds the first
// filestore reference reachable from the CID (all of one file's leaves share the same path) and stats it, without
// reading or hash-verifying any block contents.
func (n *node) cidMissing(c cid.Cid) bool {
	p := n.firstRefPath(c, cid.NewSet())
	if p == "" {
		return false // no filestore-referenced content reachable → nothing to be "missing"
	}
	_, err := os.Stat(p)
	return err != nil
}

func (n *node) firstRefPath(c cid.Cid, seen *cid.Set) string {
	if !seen.Visit(c) {
		return ""
	}
	if res := filestore.List(n.ctx, n.fstore, c); res != nil && res.FilePath != "" {
		// c is a filestore leaf reference. List returns the path RELATIVE to the FileManager root ("/", see
		// node.go), so join it back to an absolute path before stat-ing.
		return filepath.Join("/", res.FilePath)
	}
	// Otherwise c is a plain block (a dag-pb intermediate/root) — descend into the first child that yields a ref.
	// Use the LOCAL-only DAG service: a node we don't have locally must not trigger a network fetch here.
	nd, err := n.localDserv.Get(n.ctx, c)
	if err != nil {
		return ""
	}
	for _, l := range nd.Links() {
		if p := n.firstRefPath(l.Cid, seen); p != "" {
			return p
		}
	}
	return ""
}
