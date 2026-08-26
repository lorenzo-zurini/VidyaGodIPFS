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

// getRoot fetches a CID's root block over the network with a bounded timeout, and — if libp2p can't reach any provider
// in time (a hostile/captive network where the DHT finds providers but no transport connects, e.g. Pinata's ws:3000
// mangled by a proxy) — falls back to importing the whole DAG from an HTTPS trustless gateway (gateway.go) and reads
// the root from the now-local blockstore. On a normal network the root arrives fast and the gateway is never touched.
func (n *node) getRoot(c cid.Cid, cidStr string, onProgress func(pct float64)) (ipld.Node, error) {
	getCtx, cancel := context.WithTimeout(n.ctx, 30*time.Second)
	root, err := n.dserv.Get(getCtx, c)
	cancel()
	if err == nil {
		return root, nil
	}
	if isMissingFile(err) {
		return nil, errMissingFiles
	}
	fdbg("getRoot: libp2p root fetch failed (%v) → HTTPS trustless-gateway fallback cid=%s", err, cidStr)
	if gerr := n.fetchViaGateway(n.ctx, c, onProgress); gerr != nil {
		fdbg("getRoot: gateway fallback failed cid=%s: %v", cidStr, gerr)
		return nil, err // surface the original network error
	}
	return n.dserv.Get(n.ctx, c) // DAG now in the local blockstore
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

// fetchWindow bounds how many leaves are requested from bitswap at once — and thus how many received blocks are buffered
// (in the channel + the plain blockstore) before they're written to disk and dropped. It caps a fetch's resident memory
// at ~fetchWindow × chunkSize (512 × 256 KiB = 128 MiB) REGARDLESS of file size, so a multi-GB runner (e.g. GE-Proton)
// can't grow the blockstore to the whole file and OOM-thrash a small-RAM machine. Below this, memory scaled with size.
const fetchWindow = 512

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
		// Proactively freshen provider addresses (warm.go) on EVERY attempt — not just the first: a live DHT walk +
		// connect in parallel with bitswap, so a provider that restarted (new ports) or came online mid-download is
		// re-discovered instead of the retry loop spinning forever on a dead peer set. No-op when already connected.
		// A fresh CONTENT CID may have no DHT record yet (bulk content announces drain through a slow batched queue),
		// but the 3-level schema blocking-announces the COLLECTION metas first — so also warm the collection providers:
		// whoever seeds our sources has this file, and bitswap's want-broadcast reaches it once we're connected.
		if c, derr := cid.Decode(cidStr); derr == nil {
			n.warmProviders(c)
			n.warmSeedLevelProviders()
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
				tdr := time.Now()
				n.dropRef(c) // clear stale refs (+ cached blocks) so the retry fetches over the network
				fdbg("fetchToPath dropRef closure cleared in %s cid=%s", time.Since(tdr).Round(time.Millisecond), cidStr)
			}
		default:
			return err // hard io/decode error, or "cancelled"
		}
	}
}

// dirFetchAttempts bounds the retry loop of one fetchDirToPath call. Unlike file fetches (torrent-like, retry forever
// in a background worker), a source sync runs INSIDE the app's sequential sync worker — an unbounded loop there would
// wedge every source behind one dead CID. The C++ side re-runs syncSources periodically while a source is missing, so
// bounded-here + periodic-there = indefinite retry overall with bounded workers.
const dirFetchAttempts = 4

// fetchDirToPath materializes a UnixFS DIRECTORY CID with the same resilience as file fetches: each attempt is
// stall-guarded (no byte written for stallTimeout tears the attempt down instead of hanging forever), failures back
// off and retry, and every fetched block persists in the blockstore between attempts — so a retry refills instantly
// to where the last attempt died and continues from there (natural resume, no sidecar needed for a small meta tree).
func (n *node) fetchDirToPath(cidStr, dest string, onProgress func(pct float64), onFinalize func(pct float64)) error {
	clearCancel(cidStr)
	backoff := 2 * time.Second
	var err error
	for attempt := 1; ; attempt++ {
		if isCancelled(cidStr) {
			return errors.New("cancelled")
		}
		err = n.fetchDirOnce(cidStr, dest, onProgress, onFinalize)
		if err == nil || err == errMissingFiles || n.ctx.Err() != nil {
			return err
		}
		// Offline (tests / local-only materialize): the error is not transient, retrying is pure delay.
		if n.dht == nil {
			return err
		}
		if attempt >= dirFetchAttempts {
			return err
		}
		fdbg("fetchDirToPath attempt %d failed (%v) → backoff %s then retry cid=%s", attempt, err, backoff, cidStr)
		select {
		case <-time.After(backoff):
		case <-n.ctx.Done():
			return n.ctx.Err()
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

// fetchDirOnce is one attempt: root fetch (bounded, with gateway fallback), then a stall-guarded tree materialize.
// The fetched tree is intentionally SMALL (node JSON + cover images, no content bytes — the folder is dehydrated);
// each package's large per-layer content is hydrated later, on demand, by fetchToPath. Writes to a temp dir then
// renames into place so a partial/cancelled fetch never leaves a half dir at dest.
// onProgress is called with 0..100 while the tree streams in (indeterminate until the first tick); onFinalize(-1)
// fires once the bytes are down and the pin/reference step runs. Both may be nil (e.g. the offline test path).
func (n *node) fetchDirOnce(cidStr, dest string, onProgress func(pct float64), onFinalize func(pct float64)) error {
	c, err := cid.Decode(cidStr)
	if err != nil {
		return err
	}
	// A meta/collection CID is a DIRECTORY, and fetching one on a hostile network needs the same proactive provider
	// warming that single-file fetches get (warm.go): a live DHT provider walk + connect in parallel with bitswap, so a
	// NAT'd provider is holepunched before getRoot's deadline instead of relying on bitswap's slower passive connect.
	n.warmProviders(c)
	root, err := n.getRoot(c, cidStr, onProgress) // libp2p, or an HTTPS trustless-gateway CAR fallback on hostile nets
	if err != nil {
		if err == errMissingFiles || isMissingFile(err) {
			return errMissingFiles
		}
		return err
	}
	// Per-attempt context: the stall watchdog cancels it to abort a tree stream that has gone quiet, so a dead peer
	// mid-materialize surfaces as a retryable error instead of hanging the sync forever (observed: "stuck at 36%").
	actx, acancel := context.WithCancel(n.ctx)
	defer acancel()
	fnode, err := unixfile.NewUnixfsFile(actx, n.dserv, root)
	if err != nil {
		return err
	}
	defer fnode.Close()

	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}
	tmp := dest + ".tmp"
	_ = os.RemoveAll(tmp)

	// One poller drives both progress reporting (bytes-on-disk vs the folder's cumulative size) and the stall
	// watchdog (no growth for stallTimeout → cancel the attempt). Growth-based, so a slow-but-moving stream is
	// never killed; the root-fetch wait is already bounded by getRoot.
	var total int64
	if sz, serr := fnode.Size(); serr == nil {
		total = sz
	}
	stop := make(chan struct{})
	var stalled atomic.Bool
	go func() {
		t := time.NewTicker(250 * time.Millisecond)
		defer t.Stop()
		last, lastGrow := int64(-1), time.Now()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				sz := dirSize(tmp)
				if sz != last {
					last, lastGrow = sz, time.Now()
				} else if time.Since(lastGrow) > stallTimeout {
					stalled.Store(true)
					acancel()
					return
				}
				if onProgress != nil && total > 0 {
					pct := 100.0 * float64(sz) / float64(total)
					if pct > 99 {
						pct = 99 // leave the last 1% for the pin/finalize step
					}
					onProgress(pct)
				}
			}
		}
	}()
	werr := files.WriteTo(fnode, tmp)
	close(stop)
	if werr != nil {
		_ = os.RemoveAll(tmp)
		if stalled.Load() {
			return fmt.Errorf("stalled mid-transfer: %w", werr)
		}
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
	n.announce(c) // publish now so a re-hosted source tree is immediately discoverable (not after the 22h reprovide)
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
	root, err := n.getRoot(c, cidStr, onProgress)
	if err != nil {
		fdbg("fetchToPathOnce: root Get FAILED cid=%s err=%v (isMissingFile=%v)", cidStr, err, isMissingFile(err))
		if err == errMissingFiles || isMissingFile(err) {
			return errMissingFiles // node has a filestore ref but the backing file is gone
		}
		return err
	}
	fdbg("fetchToPathOnce: got root block codec=%d in %s cid=%s", root.Cid().Prefix().Codec, time.Since(getStart).Round(time.Millisecond), cidStr)
	fdbg("fetchToPathOnce: %s", n.connsDump()) // RELAYED vs DIRECT to the seeder — the throughput ceiling

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

		// One long-lived bitswap SESSION with a SINGLE continuous want-list for EVERY missing leaf — the session pipelines
		// to link speed with its own bounded in-flight window, and blocks stream in out of order. THROUGHPUT-CRITICAL: the
		// receive loop only does WriteAt (page cache — cheap, non-blocking) + a bitmap set; the expensive durability work
		// (fsync + bitmap sidecar) and memory reclaim (dropping the redundant blockstore copies) run in a BACKGROUND
		// flusher so they never stall the pipeline. The previous design fetched in windows and fsync'd on the hot path
		// every 2s, which parked the want-list during each multi-hundred-ms fsync → the download visibly PULSED. Resident
		// memory stays bounded because the flusher drops blocks as soon as ~dropBatch accumulate (independent of file
		// size — no windowing needed). Durability invariant preserved: each sync flushes the FILE (out.Sync) BEFORE
		// persisting the bitmap snapshot, so a crash never marks a leaf done whose bytes aren't on disk (it re-fetches
		// via the .part bitmap). Dropping a block without an fsync is safe — it's redundant with the file bytes, and an
		// un-synced leaf isn't in the saved bitmap, so resume re-fetches it.
		fdbg("writeThrough: opening bitswap session, %d leaves, ONE continuous want-list cid=%s", len(need), cidStr)
		sess := blockservice.NewSession(fctx, n.bserv)
		recv := 0
		sessStart := time.Now()

		const dropBatch = 256 // ~64 MiB of received blocks before the flusher reclaims them → bounded residency
		var mu sync.Mutex     // guards bits (set + snapshot) and dropQ
		var dropQ []cid.Cid
		doDrop := func() {
			mu.Lock()
			q := dropQ
			dropQ = nil
			mu.Unlock()
			if len(q) == 0 {
				return
			}
			if batch, berr := n.ds.Batch(n.ctx); berr == nil {
				for _, c := range q {
					_ = batch.Delete(n.ctx, blockstore.BlockPrefix.Child(dshelp.MultihashToDsKey(c.Hash())))
				}
				_ = batch.Commit(n.ctx)
			}
		}
		doSync := func() { // fsync the file THEN persist a bitmap snapshot — order matters for crash-consistency
			mu.Lock()
			snap := &partBits{root: bits.root, total: bits.total, count: bits.count, nset: bits.nset,
				bits: append([]byte(nil), bits.bits...)}
			mu.Unlock()
			_ = out.Sync()
			_ = savePart(dest, snap)
		}
		flushStop := make(chan struct{})
		dropSig := make(chan struct{}, 1)
		var flusher sync.WaitGroup
		flusher.Add(1)
		go func() {
			defer flusher.Done()
			t := time.NewTicker(2 * time.Second)
			defer t.Stop()
			for {
				select {
				case <-flushStop:
					return
				case <-dropSig:
					doDrop()
				case <-t.C:
					doSync()
					doDrop()
				}
			}
		}()

		for blk := range sess.GetBlocks(fctx, need) {
			lastBlk.Store(time.Now().UnixNano())
			recv++
			data := blk.RawData()
			c := blk.Cid()
			for _, o := range offsets[c] {
				if _, werr := out.WriteAt(data, o); werr != nil {
					close(flushStop)
					flusher.Wait()
					_ = out.Close()
					fdbg("writeThrough: WriteAt IO error cid=%s err=%v", cidStr, werr)
					return werr // IO error — keep the partial for a later retry
				}
				written += int64(len(data))
				if total > 0 && onProgress != nil {
					onProgress(math.Min(99, 100.0*float64(written)/float64(total)))
				}
			}
			mu.Lock()
			for _, i := range idxOf[c] {
				bits.set(i)
			}
			dropQ = append(dropQ, c)
			qlen := len(dropQ)
			mu.Unlock()
			if qlen >= dropBatch { // nudge the flusher to reclaim memory (non-blocking — it coalesces)
				select {
				case dropSig <- struct{}{}:
				default:
				}
			}
			if recv%512 == 0 {
				fdbg("writeThrough: recv %d/%d leaves, written=%d/%d (%.1f%%) cid=%s", recv, len(need), written, total, 100.0*float64(written)/float64(total), cidStr)
			}
		}
		// Session ended (all received, or torn down by stall/cancel). Stop the flusher, then do a final durability +
		// reclaim pass on the main goroutine so the returned state is fully persisted.
		close(flushStop)
		flusher.Wait()
		doSync()
		doDrop()
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
	n.announce(root) // re-announce now so a re-hosted CID is immediately discoverable (not after the 22h reprovide)
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
	// Enumerate the closure first (reading only the small dag-pb intermediates), then delete every leaf in ONE
	// datastore batch. Per-leaf fstore.DeleteBlock was O(leaves) sync writes to leveldb — ~21 ms each, so a 763-leaf
	// file stalled ~16 s. A single batched commit is ~one sync. Each leaf may live as a filestore reference
	// (/filestore/<mh>) AND/OR a plain block (/blocks/<mh>); delete both keys so neither layout survives.
	seen := cid.NewSet()
	var cids []cid.Cid
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
		cids = append(cids, c)
	}
	walk(root)
	if batch, err := n.ds.Batch(n.ctx); err == nil {
		for _, c := range cids {
			k := dshelp.MultihashToDsKey(c.Hash())
			_ = batch.Delete(n.ctx, filestore.FilestorePrefix.Child(k))
			_ = batch.Delete(n.ctx, blockstore.BlockPrefix.Child(k))
		}
		if cerr := batch.Commit(n.ctx); cerr != nil { // batch failed → fall back to the per-block path (correctness over speed)
			for _, c := range cids {
				_ = n.fstore.DeleteBlock(n.ctx, c)
			}
		}
	} else {
		for _, c := range cids {
			_ = n.fstore.DeleteBlock(n.ctx, c)
		}
	}
	_ = n.unpin(root) // drop the stale recursive pin; the re-add re-pins against the new backing file
	n.scheduleCompaction()
}
