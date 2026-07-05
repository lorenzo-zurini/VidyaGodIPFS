package main

// fetch.go — materialize a CID's content at a destination path and seed it from there BY REFERENCE.
//
// This is the no-duplication fetch: the DAG is read and written to `dest`, then `dest` is filestore-added so the
// only on-disk copy of the content IS the destination file. M2 validates the mechanics offline (content already in
// the local filestore); M3 swaps the offline exchange for online bitswap so blocks arrive from peers, at which point
// gcUnpinnedLeaves() reclaims the leaf blocks bitswap cached in the blockstore.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	blockservice "github.com/ipfs/boxo/blockservice"
	blockstore "github.com/ipfs/boxo/blockstore"
	dshelp "github.com/ipfs/boxo/datastore/dshelp"
	files "github.com/ipfs/boxo/files"
	filestore "github.com/ipfs/boxo/filestore"
	posinfo "github.com/ipfs/boxo/filestore/posinfo"
	unixfile "github.com/ipfs/boxo/ipld/unixfs/file"
	ufsio "github.com/ipfs/boxo/ipld/unixfs/io"
	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"golang.org/x/sync/singleflight"
)

// fetchGroup de-duplicates concurrent fetches that target the SAME destination file. Two workers (e.g. a layer CID
// referenced from two nodes, an overlapping hydrate, or a startup auto-resume racing a manual download) writing the
// same dest.tmp/dest would stomp each other's rename/pin → spurious "no such file"/missing-files. Keyed by dest (a
// dest maps to exactly one CID), the second caller JOINS the first and shares its result instead of racing.
var fetchGroup singleflight.Group

// errNotRawLeaves signals that a DAG isn't all-raw-leaves, so the write-through path can't reference it (filestore
// references require raw leaves) and the caller must fall back to the read + re-add path.
var errNotRawLeaves = errors.New("not all-raw-leaves")

// dbgFetch enables verbose stderr tracing of the fetch path when VG_FETCH_DEBUG is set — used to diagnose why a
// specific CID takes the slow fallback / errors. No-op in normal operation. Every line is wall-clock timestamped
// (HH:MM:SS.mmm) so a "stuck for a minute" gap is visible directly in the log.
var dbgFetch = os.Getenv("VG_FETCH_DEBUG") != ""

func fdbg(format string, a ...interface{}) {
	if dbgFetch {
		fmt.Fprintf(os.Stderr, "[fetchdbg %s] "+format+"\n", append([]interface{}{time.Now().Format("15:04:05.000")}, a...)...)
	}
}

// shortCid trims a CID for readable logs (first 6 + last 4 of the base58/base32 string).
func shortCid(c cid.Cid) string {
	s := c.String()
	if len(s) <= 12 {
		return s
	}
	return s[:6] + ".." + s[len(s)-4:]
}

// stallTimeout: if no block arrives for this long the fetch session is torn down so the wrapper can back off and RESUME
// from the on-disk bitmap (a dead peer shouldn't hang the download). Longer than the UI's 6s "Stalled" hint.
const stallTimeout = 20 * time.Second

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

// fetchToPath retrieves cidStr's file content to dest and seeds it from there — TORRENT-STYLE: it resumes and retries
// until the file is whole or the user cancels, never failing on a transient stall. Each attempt (fetchToPathOnce →
// writeThrough) resumes only the missing leaves from the on-disk partial (dest.tmp + dest.part). A stall/drop returns
// errIncomplete → back off (growing to a cap, reset once an attempt has clearly made progress) and resume. A stale
// LOCAL reference (removed/moved content) returns errMissingFiles → clear it once and re-fetch over the network.
// fetchToPath de-duplicates by dest (see fetchGroup) then runs the resumable retry loop. Concurrent callers for the
// same dest share ONE fetch and its result — the leader drives the transfer callbacks; joiners just wait + get the
// same error, so they never race on the same tmp/dest files.
func (n *node) fetchToPath(cidStr, dest string, onProgress func(pct float64), onFinalize func(pct float64)) error {
	v, err, shared := fetchGroup.Do(dest, func() (interface{}, error) {
		return nil, n.fetchToPathLoop(cidStr, dest, onProgress, onFinalize)
	})
	_ = v
	if shared {
		fdbg("fetchToPath JOINED an in-flight fetch of the same dest (deduped) cid=%s dest=%s err=%v", cidStr, dest, err)
	}
	return err
}

func (n *node) fetchToPathLoop(cidStr, dest string, onProgress func(pct float64), onFinalize func(pct float64)) error {
	backoff := 2 * time.Second
	missTries := 0
	fdbg("fetchToPath ENTER cid=%s dest=%s", cidStr, dest)
	for attempt := 1; ; attempt++ {
		if isCancelled(cidStr) {
			fdbg("fetchToPath cancelled before attempt %d cid=%s", attempt, cidStr)
			removePartial(dest)
			return errors.New("cancelled")
		}
		start := time.Now()
		fdbg("fetchToPath attempt %d START cid=%s", attempt, cidStr)
		err := n.fetchToPathOnce(cidStr, dest, onProgress, onFinalize)
		fdbg("fetchToPath attempt %d DONE cid=%s err=%v elapsed=%s", attempt, cidStr, err, time.Since(start).Round(time.Millisecond))
		switch {
		case err == nil:
			fdbg("fetchToPath SUCCESS cid=%s after %d attempt(s)", cidStr, attempt)
			return nil
		case err == errIncomplete:
			if time.Since(start) > 5*time.Second { // the attempt fetched for a while before stalling → reset backoff
				backoff = 2 * time.Second
			} else if backoff < 30*time.Second {
				backoff *= 2
			}
			fdbg("fetchToPath incomplete → backoff %s then resume cid=%s", backoff, cidStr)
			select {
			case <-time.After(backoff):
			case <-n.ctx.Done():
				return n.ctx.Err()
			}
		case err == errMissingFiles:
			missTries++
			fdbg("fetchToPath missingFiles (try %d) → dropRef + refetch cid=%s", missTries, cidStr)
			if missTries > 2 { // shouldn't recur after dropRef; guard against a spin
				return err
			}
			if c, derr := cid.Decode(cidStr); derr == nil {
				n.dropRef(c) // clear stale refs (+ cached blocks) so the retry fetches over the network
			}
		default:
			return err // hard io/decode error, or "cancelled"
		}
	}
}

// fetchDirToPath materializes a UnixFS DIRECTORY CID (a folder of dehydrated packages) recursively to dest, writing the
// whole tree — files and subdirs — to disk. Missing blocks arrive over the network via bitswap through n.dserv (online).
// Unlike fetchToPath (a single file, no-dup filestore reference), this is a plain recursive materialize: the fetched
// tree is intentionally SMALL (node JSON + cover images, no content bytes — the folder is dehydrated), and each
// package's large per-layer content is hydrated later, on demand, by fetchToPath. Writes to a temp dir then renames
// into place so a partial/cancelled fetch never leaves a half dir at dest.
// onProgress is called with 0..100 while the tree streams in (indeterminate until the first tick); onFinalize(-1) fires
// once the bytes are down and the pin/reference step runs. Both may be nil (e.g. the offline test path).
func (n *node) fetchDirToPath(cidStr, dest string, onProgress func(pct float64), onFinalize func(pct float64)) error {
	c, err := cid.Decode(cidStr)
	if err != nil {
		return err
	}
	root, err := n.dserv.Get(n.ctx, c) // blocks here until the root block arrives — where a no-reachable-provider fetch hangs
	if err != nil {
		if isMissingFile(err) {
			return errMissingFiles
		}
		return err
	}
	fnode, err := unixfile.NewUnixfsFile(n.ctx, n.dserv, root)
	if err != nil {
		return err
	}
	defer fnode.Close()

	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}
	tmp := dest + ".tmp"
	_ = os.RemoveAll(tmp)

	// Report download progress by polling bytes-on-disk vs the folder's cumulative size while WriteTo streams the tree.
	var total int64
	if sz, serr := fnode.Size(); serr == nil {
		total = sz
	}
	stop := make(chan struct{})
	if onProgress != nil && total > 0 {
		go func() {
			t := time.NewTicker(250 * time.Millisecond)
			defer t.Stop()
			for {
				select {
				case <-stop:
					return
				case <-t.C:
					pct := 100.0 * float64(dirSize(tmp)) / float64(total)
					if pct > 99 {
						pct = 99 // leave the last 1% for the pin/finalize step
					}
					onProgress(pct)
				}
			}
		}()
	}
	werr := files.WriteTo(fnode, tmp)
	close(stop)
	if werr != nil {
		_ = os.RemoveAll(tmp)
		return werr
	}
	_ = os.RemoveAll(dest)
	if err := os.Rename(tmp, dest); err != nil {
		return err
	}
	// Pin the folder root recursively so the fetched source tree is SEEDED (reprovided to the DHT) and shows in VgPinLs
	// — mirrors addDirNoCopy. Best-effort: the files are already on disk, so a pin hiccup must not fail the fetch.
	if onFinalize != nil {
		onFinalize(-1)
	}
	if err := n.pinner.Pin(n.ctx, root, true, dest); err != nil {
		fmt.Fprintf(os.Stderr, "[fetchDir] pin %s failed: %v\n", cidStr, err)
	} else if err := n.pinner.Flush(n.ctx); err != nil {
		fmt.Fprintf(os.Stderr, "[fetchDir] pin flush %s failed: %v\n", cidStr, err)
	}
	return nil
}

// onProgress is called with 0..100 during the transfer; onFinalize is called once the bytes are all down and the
// (slower) re-reference/"pinning" step runs — with 0..100 progress through that step on the write-through path, or -1
// (indeterminate) on the fallback re-add path (so the UI can show "Pinning… 42%" instead of looking stuck at 100%).
func (n *node) fetchToPathOnce(cidStr, dest string, onProgress func(pct float64), onFinalize func(pct float64)) error {
	if isCancelled(cidStr) {
		return errors.New("cancelled")
	}
	c, err := cid.Decode(cidStr)
	if err != nil {
		return err
	}
	if st, err := os.Stat(dest); err == nil {
		fdbg("fetchToPathOnce: dest already present (%d bytes) → no-op cid=%s", st.Size(), cidStr)
		return nil // already present — no-op (matches the old FetchToPath semantics)
	}
	// Orphaned reference: the node "has" this CID via a filestore reference, but the backing file was deleted.
	// Surface it cleanly as "missing files" (→ "Errored: missing files" in the UI) instead of reading the gone
	// file. (cidMissing is local-only; for content we don't have it returns false, so normal fetches proceed.)
	if n.cidMissing(c) {
		fdbg("fetchToPathOnce: cidMissing=true (orphaned local ref) → errMissingFiles cid=%s", cidStr)
		return errMissingFiles
	}
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}

	fdbg("fetchToPathOnce: fetching root block cid=%s", cidStr)
	getStart := time.Now()
	root, err := n.dserv.Get(n.ctx, c)
	if err != nil {
		fdbg("fetchToPathOnce: root Get FAILED cid=%s err=%v (isMissingFile=%v)", cidStr, err, isMissingFile(err))
		if isMissingFile(err) {
			return errMissingFiles // node has a filestore ref but the backing file is gone
		}
		return err
	}
	fdbg("fetchToPathOnce: got root block codec=%d in %s cid=%s", root.Cid().Prefix().Codec, time.Since(getStart).Round(time.Millisecond), cidStr)

	// Fast path: stream the fetched leaf blocks straight to dest AND reference them in place — no re-chunk/re-hash
	// (the old "stuck at 100%" delay). Falls back to read + re-add for DAGs that aren't all raw leaves.
	wtErr := n.writeThrough(c, root, dest, cidStr, onProgress, onFinalize)
	fdbg("writeThrough(%s) -> %v", cidStr, wtErr)
	if err := wtErr; err == nil {
		n.scheduleCompaction() // reclaim tombstone disk from the leaves we dropped from the blockstore
		return nil
	} else if err == errIncomplete {
		return errIncomplete // stalled/dropped mid-transfer — partial kept; the wrapper backs off + resumes
	} else if err != errNotRawLeaves {
		if isMissingFile(err) {
			fdbg("writeThrough(%s) classified as MISSING FILES (isMissingFile=true): %v", cidStr, err)
			return errMissingFiles
		}
		return err // cancelled / network / io
	}
	fdbg("writeThrough(%s) fell through to SLOW read+addNoCopy fallback", cidStr)

	rdr, err := ufsio.NewDagReader(n.ctx, root, n.dserv)
	if err != nil {
		return err
	}
	total := int64(rdr.Size())
	fdbg("fallback: DagReader size=%d cid=%s — reading whole DAG over the network", total, cidStr)

	tmp := dest + ".tmp"
	_ = os.RemoveAll(tmp)
	out, err := os.Create(tmp)
	if err != nil {
		return err
	}

	buf := make([]byte, 1<<20)
	var written int64
	nextMark := int64(10)
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
				pct := 100.0 * float64(written) / float64(total)
				onProgress(pct)
				if int64(pct) >= nextMark { // log every ~10% so a stall inside the read is visible
					fdbg("fallback: read %d%% (%d/%d) cid=%s", int64(pct), written, total, cidStr)
					nextMark = int64(pct) + 10
				}
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			_ = out.Close()
			_ = os.Remove(tmp)
			fdbg("fallback: read error cid=%s err=%v (isMissingFile=%v)", cidStr, rerr, isMissingFile(rerr))
			if isMissingFile(rerr) {
				return errMissingFiles
			}
			return rerr
		}
	}
	if err := out.Close(); err != nil {
		return err
	}
	fdbg("fallback: read complete (%d bytes) → rename + addNoCopy re-hash cid=%s", written, cidStr)

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
	fdbg("fallback: dropClosure + addNoCopy re-hash START cid=%s", cidStr)
	addStart := time.Now()
	n.dropClosure(c)
	if _, err := n.addNoCopy(dest); err != nil {
		fdbg("fallback: addNoCopy FAILED cid=%s err=%v", cidStr, err)
		return err
	}
	fdbg("fallback: addNoCopy DONE in %s cid=%s", time.Since(addStart).Round(time.Millisecond), cidStr)
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
	fdbg("writeThrough ENTER cid=%s dest=%s", cidStr, dest)
	// File size (for progress + preallocation) — cheap, reads the root's UnixFS metadata.
	rdr, err := ufsio.NewDagReader(n.ctx, rootNode, n.dserv)
	if err != nil {
		return err
	}
	total := int64(rdr.Size())

	// Enumerate the file's leaves IN ORDER from the dag-pb spine BEFORE touching any tmp (so errNotRawLeaves is a clean
	// fall-through). A raw-codec link is a leaf — CID/size/offset come from the link (no fetch); only the few
	// intermediate dag-pb nodes are read. A dag-pb node with no links = a dag-pb leaf → errNotRawLeaves. A chunk can
	// repeat (dedup), so a CID maps to every offset AND every leaf-INDEX it occupies (leaf-index drives the resume bitmap).
	type leafRef struct {
		c   cid.Cid
		off uint64
		sz  int
	}
	var leaves []leafRef
	offsets := map[cid.Cid][]int64{}
	idxOf := map[cid.Cid][]int{}
	var uniq []cid.Cid
	uniqSeen := cid.NewSet()
	var off int64
	addLeaf := func(c cid.Cid, sz int) {
		idxOf[c] = append(idxOf[c], len(leaves))
		leaves = append(leaves, leafRef{c, uint64(off), sz})
		offsets[c] = append(offsets[c], off)
		off += int64(sz)
		if uniqSeen.Visit(c) {
			uniq = append(uniq, c)
		}
	}
	if root.Prefix().Codec == cid.Raw {
		addLeaf(root, len(rootNode.RawData())) // single-block file: the root IS the only leaf
	} else {
		seenN := cid.NewSet()
		var spine func(nd ipld.Node) error
		spine = func(nd ipld.Node) error {
			if len(nd.Links()) == 0 {
				fdbg("spine: dag-pb node %s has NO links -> errNotRawLeaves", nd.Cid())
				return errNotRawLeaves
			}
			for _, l := range nd.Links() {
				if l.Cid.Prefix().Codec == cid.Raw {
					addLeaf(l.Cid, int(l.Size))
				} else if seenN.Visit(l.Cid) {
					child, gerr := n.dserv.Get(n.ctx, l.Cid)
					if gerr != nil {
						return gerr
					}
					if serr := spine(child); serr != nil {
						return serr
					}
				}
			}
			return nil
		}
		if serr := spine(rootNode); serr != nil {
			return serr
		}
	}
	fdbg("enumerated %s: total(unixfs)=%d off(sum-leaf-sizes)=%d leaves=%d uniq=%d match=%v", root, total, off, len(leaves), len(uniq), total == off)

	// Open the partial: RESUME if a matching dest.tmp + dest.part exist, else start fresh (preallocated + empty bitmap).
	tmp := tmpPath(dest)
	var out *os.File
	var bits *partBits
	if pb, ok := loadPart(dest, root.String(), total, len(leaves)); ok {
		if f, oerr := os.OpenFile(tmp, os.O_RDWR, 0o644); oerr == nil {
			out, bits = f, pb // resume where the last attempt left off
			fdbg("writeThrough RESUME from partial: %d/%d leaves already on disk cid=%s", pb.nset, pb.count, cidStr)
		} else {
			fdbg("writeThrough: .part loaded but tmp open FAILED (%v) → starting fresh cid=%s", oerr, cidStr)
		}
	}
	if out == nil {
		_ = os.RemoveAll(tmp)
		f, cerr := os.Create(tmp)
		if cerr != nil {
			return cerr
		}
		if terr := f.Truncate(off); terr != nil { // preallocate so out-of-order WriteAt lands correctly
			_ = f.Close()
			_ = os.Remove(tmp)
			return terr
		}
		out, bits = f, newPartBits(root.String(), total, len(leaves))
		_ = savePart(dest, bits)
	}

	// Bytes already on disk from prior attempts → resume the progress bar from there, not 0.
	var written int64
	for i, lf := range leaves {
		if bits.get(i) {
			written += int64(lf.sz)
		}
	}
	if total > 0 && onProgress != nil {
		onProgress(math.Min(99, 100.0*float64(written)/float64(total)))
	}

	// Only re-request the leaves not yet durable on disk.
	var need []cid.Cid
	for _, c := range uniq {
		for _, i := range idxOf[c] {
			if !bits.get(i) {
				need = append(need, c)
				break
			}
		}
	}
	fdbg("writeThrough: %d/%d unique leaves still needed (written so far=%d/%d) cid=%s", len(need), len(uniq), written, total, cidStr)

	if len(need) > 0 {
		// Cancel + STALL watcher: cancel the fetch on user-cancel, OR when no block has arrived for stallTimeout (a dead
		// peer), so the wrapper can back off + resume from the bitmap instead of hanging.
		fctx, fcancel := context.WithCancel(n.ctx)
		defer fcancel()
		var lastBlk atomic.Int64
		lastBlk.Store(time.Now().UnixNano())
		var stalled atomic.Bool
		go func() {
			t := time.NewTicker(200 * time.Millisecond)
			defer t.Stop()
			for {
				select {
				case <-fctx.Done():
					return
				case <-t.C:
					if isCancelled(cidStr) {
						fdbg("writeThrough: user-cancel detected → tearing down session cid=%s", cidStr)
						fcancel()
						return
					}
					if time.Since(time.Unix(0, lastBlk.Load())) > stallTimeout {
						stalled.Store(true)
						fdbg("writeThrough: STALL — no block for >%s → tearing down session cid=%s", stallTimeout, cidStr)
						fcancel()
						return
					}
				}
			}
		}()

		// One long-lived bitswap SESSION for the want-list (keeps peers warm + pipelines to link speed). Blocks arrive
		// out of order → written straight to their offset(s); the bitmap + a periodically fsync'd sidecar make it resumable.
		fdbg("writeThrough: opening bitswap session for %d leaves cid=%s", len(need), cidStr)
		sess := blockservice.NewSession(fctx, n.bserv)
		lastSave := time.Now()
		recv := 0
		sessStart := time.Now()
		for blk := range sess.GetBlocks(fctx, need) {
			lastBlk.Store(time.Now().UnixNano())
			recv++
			data := blk.RawData()
			for _, o := range offsets[blk.Cid()] {
				if _, werr := out.WriteAt(data, o); werr != nil {
					_ = out.Close()
					fdbg("writeThrough: WriteAt IO error cid=%s err=%v", cidStr, werr)
					return werr // IO error — keep the partial for a later retry
				}
				written += int64(len(data))
				if total > 0 && onProgress != nil {
					onProgress(math.Min(99, 100.0*float64(written)/float64(total)))
				}
			}
			for _, i := range idxOf[blk.Cid()] {
				bits.set(i)
			}
			if recv%256 == 0 { // periodic heartbeat so throughput/stall is visible in the log
				fdbg("writeThrough: recv %d/%d leaves, written=%d/%d (%.1f%%) cid=%s", recv, len(need), written, total, 100.0*float64(written)/float64(total), cidStr)
			}
			if time.Since(lastSave) > 2*time.Second { // durability: fsync the data BEFORE persisting the bitmap
				_ = out.Sync()
				_ = savePart(dest, bits)
				lastSave = time.Now()
			}
		}
		fdbg("writeThrough: session ended cid=%s recv=%d/%d elapsed=%s stalled=%v cancelled=%v allSet=%v", cidStr, recv, len(need), time.Since(sessStart).Round(time.Millisecond), stalled.Load(), isCancelled(cidStr), bits.allSet())

		if isCancelled(cidStr) {
			_ = out.Close()
			removePartial(dest) // explicit cancel discards the partial
			return errors.New("cancelled")
		}
		if !bits.allSet() { // stalled / session dropped incomplete → persist progress, keep the partial, resume later
			_ = out.Sync()
			_ = savePart(dest, bits)
			_ = out.Close()
			fdbg("writeThrough: INCOMPLETE — %d/%d leaves, keeping partial, returning errIncomplete cid=%s", bits.nset, bits.count, cidStr)
			return errIncomplete
		}
	}

	// Complete: flush, publish atomically, reference every leaf into dest, drop the redundant blockstore copies, pin.
	fdbg("writeThrough: all %d leaves present → FINALIZE START (fsync+rename) cid=%s", len(leaves), cidStr)
	finStart := time.Now()
	if err := out.Sync(); err != nil {
		_ = out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	_ = os.RemoveAll(dest)
	if err := os.Rename(tmp, dest); err != nil {
		return err
	}
	fdbg("finalize: fsync+rename done in %s cid=%s", time.Since(finStart).Round(time.Millisecond), cidStr)

	// Reference each leaf into dest, then drop its plain blockstore copy — TWO batched datastore commits (one for refs,
	// one for blockstore deletes) so a large file's thousands of leaves finalize in ~a couple of commits (near-instant).
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
	fdbg("finalize %s: dest size=%d, referencing %d leaves via PutMany (validates each leaf hash vs on-disk bytes)", root, st.Size(), len(fsns))
	putStart := time.Now()
	if err := fm.PutMany(n.ctx, fsns); err != nil { // one batched commit of all references
		fdbg("PutMany(%s) FAILED after %s: %v (isMissingFile=%v)", root, time.Since(putStart).Round(time.Millisecond), err, isMissingFile(err))
		// A CorruptReferenceError here means the bytes we just wrote to dest DON'T hash to their leaf CIDs — a
		// corrupt/incomplete transfer, NOT a stale local reference. Returning errMissingFiles would send the wrapper
		// down dropRef (wrong medicine) AND leave the bad dest + partial in place, so every retry re-fails identically
		// (the "errored missing files that keeps re-downloading" symptom). Instead DISCARD the bad output and the
		// resume sidecar and report errIncomplete so the wrapper backs off and re-fetches from scratch.
		if isMissingFile(err) {
			_ = os.Remove(dest)
			removePartial(dest)
			return errIncomplete
		}
		return err
	}
	fdbg("finalize: PutMany OK in %s cid=%s", time.Since(putStart).Round(time.Millisecond), cidStr)
	// Drop the bitswap-cached leaf blocks from the plain blockstore in one batched commit (they're now referenced in
	// place, so the blockstore copies are redundant). Keys follow boxo's blockstore scheme: BlockPrefix + multihash.
	delStart := time.Now()
	if batch, berr := n.ds.Batch(n.ctx); berr == nil {
		for _, lf := range leaves {
			_ = batch.Delete(n.ctx, blockstore.BlockPrefix.Child(dshelp.MultihashToDsKey(lf.c.Hash())))
		}
		_ = batch.Commit(n.ctx)
	}
	fdbg("finalize: dropped %d blockstore copies in %s cid=%s", len(leaves), time.Since(delStart).Round(time.Millisecond), cidStr)
	if onFinalize != nil {
		onFinalize(100)
	}
	removePartial(dest) // tmp is now dest; clear the .part resume sidecar

	// Pin the root so it's seeded + reprovided (mirrors addNoCopy in the fallback path). Walks the DAG via the OFFLINE
	// pinner dagservice (captured pre-goOnline) so leaves resolve from the filestore/disk, never the network.
	fdbg("finalize: recursive PIN START cid=%s", cidStr)
	pinStart := time.Now()
	if err := n.pinner.Pin(n.ctx, rootNode, true, dest); err != nil {
		fdbg("finalize: PIN FAILED in %s cid=%s err=%v", time.Since(pinStart).Round(time.Millisecond), cidStr, err)
		return err
	}
	if err := n.pinner.Flush(n.ctx); err != nil {
		fdbg("finalize: pin Flush FAILED cid=%s err=%v", cidStr, err)
		return err
	}
	fdbg("finalize: PIN+flush done in %s → writeThrough SUCCESS cid=%s", time.Since(pinStart).Round(time.Millisecond), cidStr)
	return nil
}

// dropClosure removes a CID's block closure from the plain blockstore — used both to clear bitswap's cached copy
// before re-adding as filestore references (successful fetch), and to PURGE a cancelled download's partial cached
// blocks. Walks via the OFFLINE DAG service so absent leaves (a partial download) are skipped, never fetched over the
// network, and batches the deletes into one datastore commit (a large partial is thousands of blocks). Only touches
// the plain blockstore — a completed download's leaves are filestore references, so those are unaffected.
func (n *node) dropClosure(root cid.Cid) {
	seen := cid.NewSet()
	var cids []cid.Cid
	var walk func(c cid.Cid)
	walk = func(c cid.Cid) {
		if !seen.Visit(c) {
			return
		}
		if nd, err := n.localDserv.Get(n.ctx, c); err == nil { // only present (cached) nodes are readable offline
			for _, l := range nd.Links() {
				walk(l.Cid)
			}
		}
		cids = append(cids, c)
	}
	walk(root)
	if batch, err := n.ds.Batch(n.ctx); err == nil {
		for _, c := range cids {
			_ = batch.Delete(n.ctx, blockstore.BlockPrefix.Child(dshelp.MultihashToDsKey(c.Hash())))
		}
		_ = batch.Commit(n.ctx)
	} else {
		main := n.fstore.MainBlockstore()
		for _, c := range cids {
			_ = main.DeleteBlock(n.ctx, c)
		}
	}
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
