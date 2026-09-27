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
	// Stored bytes, straight into the blockstore under the filestore (which Get reads first). Through the filestore, a
	// reference to the same bytes whose file has since moved or gone "has" the block: the put stored nothing, the node
	// read as unheld, and its package folder was refused — the whole publish with it. That dead reference goes too, or
	// the block would still read as missing.
	if err := n.plain.Put(n.ctx, blk); err != nil {
		return cid.Undef, err
	}
	if n.cidMissing(c) {
		if err := n.fstore.FileManager().DeleteBlock(n.ctx, c); err != nil {
			return cid.Undef, err
		}
	}
	// Kept already (a node file landed in place is pinned recursively): pinning it Direct again is an error, not a no-op.
	if _, pinned, err := n.pinner.IsPinned(n.ctx, c); err != nil {
		return cid.Undef, err
	} else if !pinned {
		if err := n.pinner.PinWithMode(n.ctx, c, ipfspinner.Direct, ""); err != nil {
			return cid.Undef, err
		}
		if err := n.pinner.Flush(n.ctx); err != nil {
			return cid.Undef, err
		}
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
// The pin is RECORDED, not walked (PinWithMode): a folder over a package's content links gigabytes, and pinner.Pin
// re-reads (and re-hashes, through the filestore) every block below on every publish, even for a folder already
// pinned. That walk was also the only completeness check, so each child is checked whole first (heldWhole: every
// block held, every backing file present — without re-reading the bytes); a child that is not is refused.
func (n *node) makeDir(entries map[string]string) (cid.Cid, error) {
	c, _, err := n.makeWholeDir(entries, false)
	return c, err
}

// makeWholeDir is makeDir that, with leaveOut, builds the folder of the entries held whole and names the rest instead
// of refusing — so a caller choosing what to include does not check each entry first (a second walk of every DAG).
// When no entry is whole it makes no folder (cid.Undef, no error).
func (n *node) makeWholeDir(entries map[string]string, leaveOut bool) (cid.Cid, []string, error) {
	d, err := ufsio.NewDirectory(n.dserv)
	if err != nil {
		return cid.Undef, nil, err
	}
	names := make([]string, 0, len(entries))
	for name := range entries {
		names = append(names, name)
	}
	sort.Strings(names)
	var notWhole []string
	for _, name := range names {
		c, err := cid.Decode(entries[name])
		if err != nil {
			if leaveOut { // not a CID (a typo in a node's SOURCE): a gap to name, never a reason to publish nothing
				notWhole = append(notWhole, name)
				continue
			}
			return cid.Undef, nil, fmt.Errorf("entry %q: %w", name, err)
		}
		// The folder is pinned recursively WITHOUT walking it (below), so each entry must be whole here first: a pin
		// over a partly-held entry is a package "pinned" and published that no one — nor a pinning service — can get.
		// A folder this node made is whole already (each of its entries was checked as it was made): a pin folder
		// nesting a package's content folder walked all of that content again.
		if _, made := n.madeDirs.Load(c.String()); !made {
			if err := n.heldWhole(c); err != nil {
				if leaveOut {
					notWhole = append(notWhole, name)
					continue
				}
				return cid.Undef, nil, fmt.Errorf("entry %q: not held whole — %w", name, err)
			}
		}
		var child ipld.Node
		if child, err = n.localDserv.Get(n.ctx, c); err != nil { // held here or refused: never fetched to be linked
			return cid.Undef, nil, fmt.Errorf("entry %q: %w", name, err)
		}
		if err := d.AddChild(n.ctx, name, child); err != nil {
			return cid.Undef, nil, fmt.Errorf("entry %q: %w", name, err)
		}
	}
	if leaveOut && len(notWhole) > 0 && len(notWhole) == len(entries) {
		return cid.Undef, notWhole, nil
	}
	dn, err := d.GetNode()
	if err != nil {
		return cid.Undef, nil, err
	}
	if err := n.dserv.Add(n.ctx, dn); err != nil {
		return cid.Undef, nil, err
	}
	if err := n.pinner.PinWithMode(n.ctx, dn.Cid(), ipfspinner.Recursive, ""); err != nil {
		return cid.Undef, nil, err
	}
	if err := n.pinner.Flush(n.ctx); err != nil {
		return cid.Undef, nil, err
	}
	n.madeDirs.Store(dn.Cid().String(), struct{}{})
	n.announceNow(dn.Cid())
	return dn.Cid(), notWhole, nil
}
