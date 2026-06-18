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
	"os"
	"path/filepath"
	"sync"

	ufsio "github.com/ipfs/boxo/ipld/unixfs/io"
	cid "github.com/ipfs/go-cid"
)

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

// fetchToPath retrieves cidStr's file content to dest and seeds it from there. onProgress is called with 0..100.
func (n *node) fetchToPath(cidStr, dest string, onProgress func(pct float64)) error {
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
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}

	root, err := n.dserv.Get(n.ctx, c)
	if err != nil {
		return err
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
	if _, err := n.addNoCopy(dest); err != nil {
		return err
	}
	n.gcUnpinnedLeaves() // M2 offline: no-op (no blockstore leaves); M3 online: reclaim bitswap-cached leaves
	return nil
}

// gcUnpinnedLeaves reclaims leaf blocks left in the blockstore by online bitswap once they're referenced via the
// filestore. M2 is offline (reads come straight from filestore references), so there is nothing to collect yet.
func (n *node) gcUnpinnedLeaves() {
	// TODO(M3): colored-set GC over the blockstore against the pinner once bitswap is online.
}
