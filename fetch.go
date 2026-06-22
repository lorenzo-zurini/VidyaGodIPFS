package main

// fetch.go — materialize a CID's content at a destination path and seed it from there BY REFERENCE.
//
// This is the no-duplication fetch: the DAG is read and written to `dest`, then `dest` is filestore-added so the
// only on-disk copy of the content IS the destination file. M2 validates the mechanics offline (content already in
// the local filestore); M3 swaps the offline exchange for online bitswap so blocks arrive from peers, at which point
// gcUnpinnedLeaves() reclaims the leaf blocks bitswap cached in the blockstore.

import (
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"

	blockstore "github.com/ipfs/boxo/blockstore"
	dshelp "github.com/ipfs/boxo/datastore/dshelp"
	filestore "github.com/ipfs/boxo/filestore"
	posinfo "github.com/ipfs/boxo/filestore/posinfo"
	ufsio "github.com/ipfs/boxo/ipld/unixfs/io"
	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
)

// errNotRawLeaves signals that a DAG isn't all-raw-leaves, so the write-through path can't reference it (filestore
// references require raw leaves) and the caller must fall back to the read + re-add path.
var errNotRawLeaves = errors.New("not all-raw-leaves")

// zeroChunk backs refLeaf.RawData() — FileManager.Put only reads len(RawData()), never the bytes, so a shared
// read-only buffer of the max chunk size avoids allocating per leaf.
var zeroChunk = make([]byte, chunkSize)

// refLeaf is a minimal ipld.Node carrying just a CID + a byte length. It lets us create a filestore reference for an
// already-downloaded leaf WITHOUT holding or re-reading its data (FileManager.Put needs only Cid() + len(RawData())).
type refLeaf struct {
	c    cid.Cid
	size int
}

func (r *refLeaf) Cid() cid.Cid { return r.c }
func (r *refLeaf) RawData() []byte {
	if r.size <= len(zeroChunk) {
		return zeroChunk[:r.size]
	}
	return make([]byte, r.size)
}
func (r *refLeaf) String() string                                     { return r.c.String() }
func (r *refLeaf) Loggable() map[string]interface{}                   { return nil }
func (r *refLeaf) Resolve([]string) (interface{}, []string, error)    { return nil, nil, nil }
func (r *refLeaf) Tree(string, int) []string                          { return nil }
func (r *refLeaf) ResolveLink([]string) (*ipld.Link, []string, error) { return nil, nil, nil }
func (r *refLeaf) Copy() ipld.Node                                    { return r }
func (r *refLeaf) Links() []*ipld.Link                                { return nil }
func (r *refLeaf) Stat() (*ipld.NodeStat, error)                      { return &ipld.NodeStat{}, nil }
func (r *refLeaf) Size() (uint64, error)                              { return uint64(r.size), nil }

// errMissingFiles is returned when content the node believes it has (a filestore reference) can't be read because
// the backing file was deleted. Surfaced to the UI as "Errored: missing files" rather than a cryptic open() error.
var errMissingFiles = errors.New("missing files")

// isMissingFile detects a filestore reference whose backing file is gone (deleted package content).
func isMissingFile(err error) bool {
	if err == nil {
		return false
	}
	var cre *filestore.CorruptReferenceError
	if errors.As(err, &cre) {
		return true
	}
	return strings.Contains(err.Error(), "no such file")
}

var (
	cancelMu  sync.Mutex
	cancelSet = map[string]bool{}
)

func requestCancel(c string) { cancelMu.Lock(); cancelSet[c] = true; cancelMu.Unlock() }
func clearCancel(c string)   { cancelMu.Lock(); delete(cancelSet, c); cancelMu.Unlock() }
func isCancelled(c string) bool {
	cancelMu.Lock()
	defer cancelMu.Unlock()
	return cancelSet[c]
}

// fetchToPath retrieves cidStr's file content to dest and seeds it from there. onProgress is called with 0..100
// during the transfer; onFinalize is called once the bytes are all down and the (slower) re-reference/"pinning"
// step runs — with 0..100 progress through that step on the write-through path, or -1 (indeterminate) on the
// fallback re-add path (so the UI can show "Pinning… 42%" instead of looking stuck at 100%).
func (n *node) fetchToPath(cidStr, dest string, onProgress func(pct float64), onFinalize func(pct float64)) error {
	if isCancelled(cidStr) {
		return errors.New("cancelled")
	}
	c, err := cid.Decode(cidStr)
	if err != nil {
		return err
	}
	if _, err := os.Stat(dest); err == nil {
		return nil // already present — no-op (matches the old FetchToPath semantics)
	}
	// Orphaned reference: the node "has" this CID via a filestore reference, but the backing file was deleted.
	// Surface it cleanly as "missing files" (→ "Errored: missing files" in the UI) instead of reading the gone
	// file. (cidMissing is local-only; for content we don't have it returns false, so normal fetches proceed.)
	if n.cidMissing(c) {
		return errMissingFiles
	}
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}

	root, err := n.dserv.Get(n.ctx, c)
	if err != nil {
		if isMissingFile(err) {
			return errMissingFiles // node has a filestore ref but the backing file is gone
		}
		return err
	}

	// Fast path: stream the fetched leaf blocks straight to dest AND reference them in place — no re-chunk/re-hash
	// (the old "stuck at 100%" delay). Falls back to read + re-add for DAGs that aren't all raw leaves.
	if err := n.writeThrough(c, root, dest, cidStr, onProgress, onFinalize); err == nil {
		n.scheduleCompaction() // reclaim tombstone disk from the leaves we dropped from the blockstore
		return nil
	} else if err != errNotRawLeaves {
		if isMissingFile(err) {
			return errMissingFiles
		}
		return err // cancelled / network / io
	}

	rdr, err := ufsio.NewDagReader(n.ctx, root, n.dserv)
	if err != nil {
		return err
	}
	total := int64(rdr.Size())

	tmp := dest + ".tmp"
	_ = os.RemoveAll(tmp)
	out, err := os.Create(tmp)
	if err != nil {
		return err
	}

	buf := make([]byte, 1<<20)
	var written int64
	for {
		if isCancelled(cidStr) {
			_ = out.Close()
			_ = os.Remove(tmp)
			return errors.New("cancelled")
		}
		nr, rerr := rdr.Read(buf)
		if nr > 0 {
			if _, werr := out.Write(buf[:nr]); werr != nil {
				_ = out.Close()
				_ = os.Remove(tmp)
				return werr
			}
			written += int64(nr)
			if total > 0 && onProgress != nil {
				onProgress(100.0 * float64(written) / float64(total))
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			_ = out.Close()
			_ = os.Remove(tmp)
			if isMissingFile(rerr) {
				return errMissingFiles
			}
			return rerr
		}
	}
	if err := out.Close(); err != nil {
		return err
	}

	// Publish atomically, then seed from the destination by reference (filestore) so dest IS the seed source.
	_ = os.RemoveAll(dest)
	if err := os.Rename(tmp, dest); err != nil {
		return err
	}

	// Online bitswap cached the fetched blocks in the plain blockstore. Drop that whole closure first: Filestore.Put
	// skips any block that already exists, so without clearing them addNoCopy would NOT create the filestore
	// references and the content would stay duplicated in the blockstore. After dropping, addNoCopy re-chunks dest
	// from disk and stores the leaves as references into it — leaving the destination file as the only on-disk copy.
	// All bytes are down; the remaining re-chunk/re-reference step ("pinning") can take a while for large files.
	// addNoCopy re-hashes the whole file opaquely, so we can't report granular progress here — signal indeterminate.
	if onFinalize != nil {
		onFinalize(-1)
	}
	n.dropClosure(c)
	if _, err := n.addNoCopy(dest); err != nil {
		return err
	}
	n.scheduleCompaction() // reclaim tombstone disk from the dropped bitswap-cached blocks
	return nil
}

// writeThrough materializes a CID's file at dest by streaming each fetched leaf block straight to disk in DAG order,
// then recording each leaf as a filestore reference into dest — WITHOUT re-chunking or re-hashing the file (which is
// the slow "pinning" step the old path did via addNoCopy). Only the small dag-pb root/intermediate nodes remain as
// plain blocks. Returns errNotRawLeaves if the DAG isn't all raw leaves (filestore refs require raw leaves; the
// caller then falls back to read + re-add).
func (n *node) writeThrough(root cid.Cid, rootNode ipld.Node, dest, cidStr string,
	onProgress func(pct float64), onFinalize func(pct float64)) error {
	// File size (for progress) — cheap, reads the root's UnixFS metadata.
	rdr, err := ufsio.NewDagReader(n.ctx, rootNode, n.dserv)
	if err != nil {
		return err
	}
	total := int64(rdr.Size())

	tmp := dest + ".tmp"
	_ = os.RemoveAll(tmp)
	out, err := os.Create(tmp)
	if err != nil {
		return err
	}

	type leafRef struct {
		c   cid.Cid
		off uint64
		sz  int
	}
	var leaves []leafRef
	var written int64

	var walk func(c cid.Cid, nd ipld.Node) error
	walk = func(c cid.Cid, nd ipld.Node) error {
		if isCancelled(cidStr) {
			return errors.New("cancelled")
		}
		if len(nd.Links()) == 0 { // a leaf
			if c.Prefix().Codec != cid.Raw {
				return errNotRawLeaves // dag-pb leaf carries protobuf framing, not raw file bytes — can't reference
			}
			data := nd.RawData()
			if _, werr := out.Write(data); werr != nil {
				return werr
			}
			leaves = append(leaves, leafRef{c, uint64(written), len(data)})
			written += int64(len(data))
			if total > 0 && onProgress != nil {
				onProgress(math.Min(99, 100.0*float64(written)/float64(total)))
			}
			return nil
		}
		for _, l := range nd.Links() {
			child, gerr := n.dserv.Get(n.ctx, l.Cid) // fetches via bitswap if not local
			if gerr != nil {
				return gerr
			}
			if werr := walk(l.Cid, child); werr != nil {
				return werr
			}
		}
		return nil
	}
	if err := walk(root, rootNode); err != nil {
		_ = out.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := out.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}

	_ = os.RemoveAll(dest)
	if err := os.Rename(tmp, dest); err != nil {
		return err
	}

	// Reference each leaf into dest, then drop its plain blockstore copy. Done as TWO batched datastore commits (one
	// for the filestore references, one for the blockstore deletes) instead of per-leaf Put/Delete — a large file has
	// thousands of leaves, and individual writes under the shared datastore lock made "Pinning…" take ~a minute and
	// starved concurrent GUI node queries. Batching collapses that to a couple of commits (near-instant).
	if onFinalize != nil {
		onFinalize(0)
	}
	st, err := os.Stat(dest)
	if err != nil {
		return err
	}
	fm := n.fstore.FileManager()
	fsns := make([]*posinfo.FilestoreNode, 0, len(leaves))
	for _, lf := range leaves {
		fsns = append(fsns, &posinfo.FilestoreNode{
			Node:    &refLeaf{c: lf.c, size: lf.sz},
			PosInfo: &posinfo.PosInfo{Offset: lf.off, FullPath: dest, Stat: st},
		})
	}
	if err := fm.PutMany(n.ctx, fsns); err != nil { // one batched commit of all references
		return err
	}
	// Drop the bitswap-cached leaf blocks from the plain blockstore in one batched commit (they're now referenced in
	// place, so the blockstore copies are redundant). Keys follow boxo's blockstore scheme: BlockPrefix + multihash.
	if batch, berr := n.ds.Batch(n.ctx); berr == nil {
		for _, lf := range leaves {
			_ = batch.Delete(n.ctx, blockstore.BlockPrefix.Child(dshelp.MultihashToDsKey(lf.c.Hash())))
		}
		_ = batch.Commit(n.ctx)
	}
	if onFinalize != nil {
		onFinalize(100)
	}

	// Pin the root so it's seeded + reprovided (mirrors addNoCopy in the fallback path).
	if err := n.pinner.Pin(n.ctx, rootNode, true, dest); err != nil {
		return err
	}
	return n.pinner.Flush(n.ctx)
}

// dropClosure removes a CID's entire block closure from the plain blockstore (used to clear bitswap's cached copy
// before re-adding as filestore references). Reads each node's links before deleting it. Raw leaves have no links.
func (n *node) dropClosure(root cid.Cid) {
	main := n.fstore.MainBlockstore()
	seen := cid.NewSet()
	var walk func(c cid.Cid)
	walk = func(c cid.Cid) {
		if !seen.Visit(c) {
			return
		}
		if nd, err := n.dserv.Get(n.ctx, c); err == nil {
			for _, l := range nd.Links() {
				walk(l.Cid)
			}
		}
		_ = main.DeleteBlock(n.ctx, c)
	}
	walk(root)
}

// dropRef removes a CID's entire closure from the FILESTORE (the FileManager references AND any plain blockstore
// blocks), then unpins it. Filestore.Put skips a block it already has, so an existing reference — healthy or orphaned
// (backing file deleted/moved) — must be deleted before addNoCopy can re-point it at a new backing file. Uses the
// offline DAG service so an orphaned leaf (whose backing file is gone) is simply skipped, never fetched over the
// network. Only the small dag-pb intermediate nodes need reading to enumerate leaf CIDs; the leaves themselves are
// deleted by CID without reading their (possibly gone) content.
func (n *node) dropRef(root cid.Cid) {
	seen := cid.NewSet()
	var walk func(c cid.Cid)
	walk = func(c cid.Cid) {
		if !seen.Visit(c) {
			return
		}
		if nd, err := n.localDserv.Get(n.ctx, c); err == nil {
			for _, l := range nd.Links() {
				walk(l.Cid)
			}
		}
		_ = n.fstore.DeleteBlock(n.ctx, c)
	}
	walk(root)
	_ = n.unpin(root) // drop the stale recursive pin; the re-add re-pins against the new backing file
	n.scheduleCompaction()
}
