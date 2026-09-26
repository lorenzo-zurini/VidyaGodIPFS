package main

// blocks.go — node blocks and package folders (generation 6: no IPLD codec of our own).
//
// A VidyaGod node is canonical JSON bytes (keys sorted, compact, UTF-8 — the app writes them) of at most one 256 KiB
// chunk, so the file `ipfs add` would make of it is ONE raw leaf: its CID is the CIDv1 raw sha2-256 of the bytes —
// the same CID the app computes locally (src/cid.cpp) and Kubo prints for the file. A package is a UnixFS folder of
// those files (`<cid>.json`), so a receiver fetches it like any folder and lands every node verbatim; nothing here
// knows what a node means. References inside a node are plain CID strings the app follows itself.

import (
	"context"
	"fmt"
	"sort"
	"time"

	ufsio "github.com/ipfs/boxo/ipld/unixfs/io"
	ipfspinner "github.com/ipfs/boxo/pinning/pinner"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	mh "github.com/multiformats/go-multihash"
)

// blockGetTimeout bounds a single block fetch: a well-formed CID nobody provides must fail cleanly, not hang the
// caller (the "add by CID" paste box) on the app-lifetime context.
const blockGetTimeout = 60 * time.Second

// rawCid is the CID of bytes stored as one raw leaf: CIDv1, raw codec, sha2-256.
func rawCid(b []byte) (cid.Cid, error) {
	h, err := mh.Sum(b, mh.SHA2_256, -1)
	if err != nil {
		return cid.Undef, err
	}
	return cid.NewCidV1(cid.Raw, h), nil
}

// blockPut stores bytes as one raw leaf, DIRECT-pins it and announces it now (a published node must be findable the
// moment it exists). The CID is the file CID of the bytes: at most one chunk, or it would be a chunked file instead.
func (n *node) blockPut(b []byte) (cid.Cid, error) {
	if int64(len(b)) > chunkSize {
		return cid.Undef, fmt.Errorf("block is %d bytes — over one %d-byte chunk", len(b), chunkSize)
	}
	c, err := rawCid(b)
	if err != nil {
		return cid.Undef, err
	}
	blk, err := blocks.NewBlockWithCid(b, c)
	if err != nil {
		return cid.Undef, err
	}
	if err := n.bserv.AddBlock(n.ctx, blk); err != nil {
		return cid.Undef, err
	}
	if err := n.pinner.PinWithMode(n.ctx, c, ipfspinner.Direct, ""); err != nil {
		return cid.Undef, err
	}
	if err := n.pinner.Flush(n.ctx); err != nil {
		return cid.Undef, err
	}
	n.announceNow(c)
	return c, nil
}

// blockGet returns one raw block's bytes, fetched over bitswap when it is not local. Only raw leaves: a dag-pb CID is
// a chunked file or a folder, never one node.
func (n *node) blockGet(c cid.Cid) ([]byte, error) {
	if c.Prefix().Codec != cid.Raw {
		return nil, fmt.Errorf("%s is not a single raw block (codec 0x%x)", c, c.Prefix().Codec)
	}
	ctx, cancel := context.WithTimeout(n.ctx, blockGetTimeout)
	defer cancel()
	blk, err := n.bserv.GetBlock(ctx, c)
	if err != nil {
		return nil, err
	}
	return blk.RawData(), nil
}

// makeDir builds the UnixFS folder {name: child CID} over children the node already holds (the app put them first),
// pins it recursively and announces it now. The children are linked, not copied: a folder of node files costs one
// directory block. Names are added in sorted order, so the same entries always make the same folder.
//
// The pin is RECORDED, not walked (PinWithMode, no graph fetch): every child was checked held above, and a folder over
// a package's content links gigabytes — pinner.Pin re-reads (and re-hashes, through the filestore) every block below
// on every publish, even for a folder already pinned, and through a bitswap-backed DAG service it would wait on the
// network for any block missing from a partly-held child.
func (n *node) makeDir(entries map[string]string) (cid.Cid, error) {
	d, err := ufsio.NewDirectory(n.dserv)
	if err != nil {
		return cid.Undef, err
	}
	names := make([]string, 0, len(entries))
	for name := range entries {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		c, err := cid.Decode(entries[name])
		if err != nil {
			return cid.Undef, fmt.Errorf("entry %q: %w", name, err)
		}
		if !n.hasLocal(c) {
			return cid.Undef, fmt.Errorf("entry %q: %s is not held locally — put it before linking it", name, c)
		}
		var child ipld.Node
		if child, err = n.dserv.Get(n.ctx, c); err != nil {
			return cid.Undef, fmt.Errorf("entry %q: %w", name, err)
		}
		if err := d.AddChild(n.ctx, name, child); err != nil {
			return cid.Undef, fmt.Errorf("entry %q: %w", name, err)
		}
	}
	dn, err := d.GetNode()
	if err != nil {
		return cid.Undef, err
	}
	if err := n.dserv.Add(n.ctx, dn); err != nil {
		return cid.Undef, err
	}
	if err := n.pinner.PinWithMode(n.ctx, dn.Cid(), ipfspinner.Recursive, ""); err != nil {
		return cid.Undef, err
	}
	if err := n.pinner.Flush(n.ctx); err != nil {
		return cid.Undef, err
	}
	n.announceNow(dn.Cid())
	return dn.Cid(), nil
}
