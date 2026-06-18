package main

// query.go — read-only status helpers backing the IPFS tab columns.

import (
	cid "github.com/ipfs/go-cid"
)

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
