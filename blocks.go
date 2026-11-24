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
	"sync/atomic"
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
	// Only the Recursive and Direct indexes are asked, never IsPinned: for a CID in neither, its Indirect check walks
	// the children of EVERY recursive pin, and on a library node (thousands of pins) that ran for hours or failed —
	// publish-all then left half the library out (2026-11-17). An indirect-only pin may take a Direct one beside it.
	if pinned, err := n.pinnedDirectly(c); err != nil {
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

// blockPutMany is blockPut for many nodes — a mint's every new node — in one stored batch, their pins made with the
// pinner's dirty and clean flags written once for all of them, and one announcement: a node at a time, each was ~5
// synced writes (its block, the dirty flag, its pin's record and index, the clean flag). Each pin's record and index
// are still a synced write apiece (leveldb syncs every Put). A node over one chunk refuses all of them, before
// anything is stored; a pin that fails after the store leaves the blocks stored, unannounced.
func (n *node) blockPutMany(bs [][]byte) ([]cid.Cid, error) {
	cs := make([]cid.Cid, len(bs))
	blks := make([]blocks.Block, 0, len(bs))
	for i, b := range bs {
		if int64(len(b)) > chunkSize {
			return nil, fmt.Errorf("block %d is %d bytes — over one %d-byte chunk", i, len(b), chunkSize)
		}
		c, err := rawCid(b)
		if err != nil {
			return nil, err
		}
		blk, err := blocks.NewBlockWithCid(b, c)
		if err != nil {
			return nil, err
		}
		cs[i] = c
		blks = append(blks, blk)
	}
	if err := n.plain.PutMany(n.ctx, blks); err != nil {
		return nil, err
	}
	pinned := false
	// The dirty flag set once and cleared once (Flush) for every pin below, not around each of them. One batch at a
	// time: the flag is the pinner's, and two batches saving and restoring it crossed would leave it off for good.
	n.putManyMu.Lock()
	defer n.putManyMu.Unlock()
	if a, ok := n.pinner.(interface{ SetAutosync(bool) bool }); ok {
		defer a.SetAutosync(a.SetAutosync(false))
	}
	for _, c := range cs {
		if n.cidMissing(c) { // a dead reference to the same bytes goes too (blockPut)
			if err := n.fstore.FileManager().DeleteBlock(n.ctx, c); err != nil {
				return nil, err
			}
		}
		if has, err := n.pinnedDirectly(c); err != nil {
			return nil, err
		} else if !has {
			if err := n.pinner.PinWithMode(n.ctx, c, ipfspinner.Direct, ""); err != nil {
				if pinned {
					_ = n.pinner.Flush(n.ctx) // the pins made so far, recorded clean: a dirty flag left set rebuilds the index at the next start
				}
				return nil, err
			}
			pinned = true
		}
	}
	if pinned {
		if err := n.pinner.Flush(n.ctx); err != nil {
			return nil, err
		}
	}
	n.startProviding(cs...)
	return cs, nil
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

// maxDirEntries bounds a folder listing: a peer's package folder is untrusted.
var maxDirEntries = 100000 // var: tests shrink it

// dirEntries lists a UnixFS folder — entry name → CID — from blocks this node holds, never the network: a received
// package's node list is its own folder's listing (the share entry names the folder; nothing inside repeats it).
func (n *node) dirEntries(c cid.Cid) (map[string]string, error) {
	local := n.localDserv // always offline: a folder not held is an error at once, never a fetch
	ctx, cancel := context.WithTimeout(n.ctx, 10*time.Second)
	defer cancel()
	nd, err := local.Get(ctx, c)
	if err != nil {
		return nil, err
	}
	d, err := ufsio.NewDirectoryFromNode(local, nd)
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	err = d.ForEachLink(ctx, func(l *ipld.Link) error {
		if len(out) >= maxDirEntries {
			return fmt.Errorf("folder %s has more than %d entries", c, maxDirEntries)
		}
		out[l.Name] = l.Cid.String()
		return nil
	})
	return out, err
}

// makeWholeDir is makeDir that, with leaveOut, builds the folder of the entries held whole and names the rest instead
// of refusing — so a caller choosing what to include does not check each entry first (a second walk of every DAG).
// When no entry is whole it makes no folder (cid.Undef, no error).
func (n *node) makeWholeDir(entries map[string]string, leaveOut bool) (cid.Cid, []string, error) {
	d, err := ufsio.NewDirectory(linkOnlyDserv{n.dserv})
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
		storedRaw := false
		if c.Prefix().Codec == cid.Raw {
			storedRaw, _ = n.plain.Has(n.ctx, c)
		}
		if storedRaw {
			// A minted node (stored bytes, hashed when put) is linked by its size: reading every one of them to link it
			// read a library's node files at every publish. A node file referenced in place is read, below: its file may
			// have changed since, and the read checks it.
			size, err := n.plain.GetSize(n.ctx, c)
			if err != nil {
				return cid.Undef, nil, fmt.Errorf("entry %q: %w", name, err)
			}
			child = &sizedLeaf{c: c, size: uint64(size)}
		} else if child, err = n.localDserv.Get(n.ctx, c); err != nil { // held here or refused: never fetched to be linked
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
	// Pinned already (the same folder made again: every publish remakes every package's three): a recursive pin of a
	// pinned CID is taken off and put back — ~6 synced writes and a flush, for each of a library's 2,000 folders.
	if _, has, err := n.pinner.IsPinnedWithType(n.ctx, dn.Cid(), ipfspinner.Recursive); err != nil {
		return cid.Undef, nil, err
	} else if !has {
		if err := n.pinner.PinWithMode(n.ctx, dn.Cid(), ipfspinner.Recursive, ""); err != nil {
			return cid.Undef, nil, err
		}
		if err := n.pinner.Flush(n.ctx); err != nil {
			return cid.Undef, nil, err
		}
		dirPins.Add(1)
	}
	n.madeDirs.Store(dn.Cid().String(), struct{}{})
	n.announceNow(dn.Cid())
	return dn.Cid(), notWhole, nil
}

// pinnedDirectly says whether c has a Recursive or a Direct pin of its own: two index lookups, no DAG walk.
func (n *node) pinnedDirectly(c cid.Cid) (bool, error) {
	for _, m := range []ipfspinner.Mode{ipfspinner.Recursive, ipfspinner.Direct} {
		if _, pinned, err := n.pinner.IsPinnedWithType(n.ctx, c, m); err != nil || pinned {
			return pinned, err
		}
	}
	return false, nil
}

// dirPins counts the folders makeWholeDir pinned (a test reads it: a folder made again is not pinned again).
var dirPins atomic.Int64

// sizedLeaf is a raw block linked by its CID and size alone, for a folder listing it: the link is all a folder keeps of
// an entry (ipld.MakeLink reads Cid and Size), so its bytes need not be read to make it.
type sizedLeaf struct {
	c    cid.Cid
	size uint64
}

func (l *sizedLeaf) RawData() []byte { return nil }
func (l *sizedLeaf) Cid() cid.Cid    { return l.c }
func (l *sizedLeaf) String() string  { return l.c.String() }
func (l *sizedLeaf) Loggable() map[string]interface{} {
	return map[string]interface{}{"node": l.c.String()}
}
func (l *sizedLeaf) Resolve([]string) (interface{}, []string, error) {
	return nil, nil, fmt.Errorf("a leaf has no paths")
}
func (l *sizedLeaf) Tree(string, int) []string { return nil }
func (l *sizedLeaf) ResolveLink([]string) (*ipld.Link, []string, error) {
	return nil, nil, fmt.Errorf("a leaf has no links")
}
func (l *sizedLeaf) Copy() ipld.Node     { c := *l; return &c }
func (l *sizedLeaf) Links() []*ipld.Link { return nil }
func (l *sizedLeaf) Stat() (*ipld.NodeStat, error) {
	return &ipld.NodeStat{Hash: l.c.String(), CumulativeSize: int(l.size), DataSize: int(l.size)}, nil
}
func (l *sizedLeaf) Size() (uint64, error) { return l.size, nil }

// linkOnlyDserv: the DAG service a folder is built over. A sharded folder adds each child it is given to the store (boxo
// hamt Swap): a sizedLeaf carries no bytes, and stored it would be an empty block under its CID — held whole, served
// as nothing. It is never stored; it is there already (that is why it may be linked by its size).
type linkOnlyDserv struct{ ipld.DAGService }

func (d linkOnlyDserv) Add(ctx context.Context, nd ipld.Node) error {
	if _, ok := nd.(*sizedLeaf); ok {
		return nil
	}
	return d.DAGService.Add(ctx, nd)
}

func (d linkOnlyDserv) AddMany(ctx context.Context, nds []ipld.Node) error {
	keep := make([]ipld.Node, 0, len(nds))
	for _, nd := range nds {
		if _, ok := nd.(*sizedLeaf); !ok {
			keep = append(keep, nd)
		}
	}
	return d.DAGService.AddMany(ctx, keep)
}
