package main

// query.go — read-only status helpers backing the IPFS tab columns.

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	filestore "github.com/ipfs/boxo/filestore"
	merkledag "github.com/ipfs/boxo/ipld/merkledag"
	unixfs "github.com/ipfs/boxo/ipld/unixfs"
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

// cidFileSizeLocal returns the UnixFS FILE size (payload bytes) for a CID, from the local store only, or -1.
//
// Distinct from cidSizeLocal, which reports the ENCODED DAG size (payload + block/protobuf overhead). Comparing
// that against st_size would mismatch on essentially every file. This reads only the ROOT block, so it is cheap
// enough to run before every seed — the point being to notice "the file on disk is no longer the file we
// published" without reading gigabytes.
func (n *node) cidFileSizeLocal(c cid.Cid) int64 {
	ctx, cancel := context.WithTimeout(n.ctx, 5*time.Second)
	defer cancel()
	nd, err := n.localDserv.Get(ctx, c)
	if err != nil {
		return -1
	}
	// Raw leaves (a file smaller than one chunk) carry the payload directly.
	if c.Type() == cid.Raw {
		return int64(len(nd.RawData()))
	}
	pn, ok := nd.(*merkledag.ProtoNode)
	if !ok {
		return -1
	}
	fsn, err := unixfs.FSNodeFromBytes(pn.Data())
	if err != nil {
		return -1
	}
	return int64(fsn.FileSize())
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

// pinLs returns every pinned CID: recursive (seeded content roots) AND direct (node blocks).
func (n *node) pinLs() ([]cid.Cid, error) {
	var out []cid.Cid
	for sp := range n.pinner.RecursiveKeys(n.ctx, false) {
		if sp.Err != nil {
			fmt.Fprintf(os.Stderr, "[pinLs] RecursiveKeys err: %v\n", sp.Err)
			return out, sp.Err
		}
		out = append(out, sp.Pin.Key)
	}
	// DIRECT pins too: dag-json node blocks (the freezer and the received-share fetch pin each node individually).
	// The IPFS tab's row set is pins PLUS in-flight transfers - leaving direct pins out made every received node
	// row VANISH the moment its transfer finished.
	for sp := range n.pinner.DirectKeys(n.ctx, false) {
		if sp.Err != nil {
			fmt.Fprintf(os.Stderr, "[pinLs] DirectKeys err: %v\n", sp.Err)
			return out, sp.Err
		}
		out = append(out, sp.Pin.Key)
	}
	return out, nil
}

// unpin removes a recursive pin.
func (n *node) unpin(c cid.Cid) error {
	n.stopProviding(c)
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

// wholeWalks counts heldWhole walks (a test reads it: publishing must not walk the same content again and again).
var wholeWalks atomic.Int64

// heldWhole: every block of c's DAG is held here and every file a block is referenced into is present — what a
// recursive pin of c claims, and what serving it needs. hasLocal answers for the top block only: a file whose leaves
// are partly missing (an interrupted fetch) or whose backing file moved still "has" its root. ONE walk: reads only the
// dag-pb interior (small); a leaf is checked by one lookup of its reference (or its stored bytes), and, referenced into
// a file, that file by one stat per distinct path (gone, or ending before the leaf) — nothing re-hashed.
func (n *node) heldWhole(c cid.Cid) error {
	return n.heldWholeWith(c, newStatMemo())
}

// statMemo: each backing file's size, -1 gone, stat'd once however many leaves (and walks, on however many workers)
// reference into it.
type statMemo struct {
	mu    sync.Mutex
	sizes map[string]*statOnce
}

type statOnce struct {
	once sync.Once
	size int64
}

// memoStats counts the backing files a statMemo stat'd (a test reads it: a file is stat'd once per check of many).
var memoStats atomic.Int64

func newStatMemo() *statMemo { return &statMemo{sizes: map[string]*statOnce{}} }

func (m *statMemo) size(p string) int64 {
	m.mu.Lock()
	e, ok := m.sizes[p]
	if !ok {
		e = &statOnce{}
		m.sizes[p] = e
	}
	m.mu.Unlock()
	e.once.Do(func() {
		memoStats.Add(1)
		e.size = -1
		if st, err := os.Stat(p); err == nil {
			e.size = st.Size()
		}
	})
	return e.size
}

// heldWholeMany: heldWhole for each of cs, on one worker per CPU, one stat per backing file across them all. Every
// start asked the library's content one file at a time: 3.6 M leaf lookups in a row, a minute and more of the store's
// reads before the first file was seeded.
func (n *node) heldWholeMany(cs []cid.Cid) []bool {
	out := make([]bool, len(cs))
	memo := newStatMemo()
	next := atomic.Int64{}
	var wg sync.WaitGroup
	workers := runtime.NumCPU()
	if workers > len(cs) {
		workers = len(cs)
	}
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1) - 1)
				if i >= len(cs) || n.ctx.Err() != nil {
					return
				}
				out[i] = n.heldWholeWith(cs[i], memo) == nil
			}
		}()
	}
	wg.Wait()
	return out
}

// heldWholeFiles: heldWhole, and the distinct files c's leaves are referenced into — the files a read of c reads.
func (n *node) heldWholeFiles(c cid.Cid) ([]string, error) {
	memo := newStatMemo()
	if err := n.heldWholeWith(c, memo); err != nil {
		return nil, err
	}
	files := make([]string, 0, len(memo.sizes))
	for p := range memo.sizes {
		files = append(files, p)
	}
	sort.Strings(files)
	return files, nil
}

func (n *node) heldWholeWith(c cid.Cid, memo *statMemo) error {
	wholeWalks.Add(1)
	seen := cid.NewSet()
	var walk func(c cid.Cid) error
	walk = func(c cid.Cid) error {
		if !seen.Visit(c) {
			return nil
		}
		if c.Prefix().Codec == cid.Raw {
			// A leaf is held as a reference into a file — one lookup answers both "held?" and "where?" — or as stored
			// bytes. Asking hasLocal first cost three lookups a leaf: ~11 M for a library of 3.6 M leaves, a minute of
			// reads at every start.
			if res := filestore.List(n.ctx, n.fstore, c); res != nil && res.FilePath != "" {
				p := filepath.Join(fileRoot, res.FilePath) // relative to the FileManager root (filestoreroot.go)
				size := memo.size(p)
				if size < 0 {
					return fmt.Errorf("%s: the file it is referenced into (%s) is gone", c, p)
				}
				// A file cut short no longer holds the bytes its references name past its end (a file that grew still does).
				if uint64(size) < res.Offset+res.Size {
					return fmt.Errorf("%s: the file it is referenced into (%s) ends before it (%d < %d)", c, p, size, res.Offset+res.Size)
				}
				return nil
			}
			if has, err := n.plain.Has(n.ctx, c); err != nil || !has {
				return fmt.Errorf("%s is not held", c)
			}
			return nil
		}
		if !n.hasLocal(c) {
			return fmt.Errorf("%s is not held", c)
		}
		nd, err := n.localDserv.Get(n.ctx, c)
		if err != nil {
			return err
		}
		for _, l := range nd.Links() {
			if err := walk(l.Cid); err != nil {
				return err
			}
		}
		return nil
	}
	return walk(c)
}

// cidMissing reports whether content the node believes it holds (via filestore references) is actually un-serveable
// because a backing file is gone. It walks the WHOLE reference chain, not just the first leaf: a file that moved
// wholesale orphans every leaf together, but a PARTIALLY-broken reference (only some leaves' backing gone) would slip
// past a first-leaf check and then fail only when a peer requests those specific blocks — the "green locally, errors
// only while serving" bug. Each DISTINCT backing path is stat'd once (a large file's thousands of leaves share one
// path, so the walk is cheap) and the CID is "missing" if ANY backing is gone. Uses the LOCAL-only DAG service so an
// absent block never triggers a network fetch. A CID with no filestore references at all (we simply don't host it) is
// NOT missing — matches the previous firstRefPath=="" semantics.
// orphanedRefPaths returns the DISTINCT backing paths of the filestore's references whose file is gone — the
// SERVE-reliability probe: every path returned is content some pinned CID references but the node can no longer read
// for a requesting peer, i.e. an orphan the heal should re-point. The distinct paths come from one scan of the
// references, kept until a reference is written or dropped (refwatch.go): asked again with none changed, it costs
// one stat per file, not a read of the whole index.
func (n *node) orphanedRefPaths() []string {
	n.refPathsMu.Lock()
	defer n.refPathsMu.Unlock()
	scanned := false
	scan := func() bool {
		n.refAdded.take() // everything written so far is in what the scan reads
		gen := n.refDropped.Load()
		paths, ok := n.refFilePaths()
		if !ok {
			n.refPathsValid = false // what it took is not in a listing: the next call reads the index again
			return false
		}
		n.refPaths, n.refPathsGen, n.refPathsValid, scanned = paths, gen, true, true
		n.refPathsWritten = false
		return true
	}
	if !n.refPathsValid || n.refDropped.Load() != n.refPathsGen {
		if !scan() {
			return nil
		}
	} else if added := n.refAdded.take(); len(added) > 0 {
		// Written since: their files join the listing, which is read again only when a reference was dropped.
		n.refPathsWritten = true
		have := make(map[string]struct{}, len(n.refPaths))
		for _, p := range n.refPaths {
			have[p] = struct{}{}
		}
		for _, p := range added {
			if _, ok := have[p]; !ok {
				n.refPaths = append(n.refPaths, p)
			}
		}
		sort.Strings(n.refPaths)
	}
	if n.refRepointed.Swap(false) {
		n.refPathsConfirmed = nil // a gone file's blocks may be named by another file now: each is looked for again
	}
	gone := n.goneOf(n.refPaths)
	// A file that looks gone, references having been written since the listing was read: one written over another's
	// (the same block in two files) can leave a file nothing names any more. Read once more before saying so — once
	// per file: a gone file a read already confirmed does not make every probe during a download read the index.
	fresh := false
	for _, p := range gone {
		if _, ok := n.refPathsConfirmed[p]; !ok {
			fresh = true
			break
		}
	}
	if fresh && n.refPathsWritten && !scanned {
		if !scan() {
			return nil
		}
		gone = n.goneOf(n.refPaths)
	}
	if scanned {
		n.refPathsConfirmed = make(map[string]struct{}, len(gone))
		for _, p := range gone {
			n.refPathsConfirmed[p] = struct{}{}
		}
	}
	return gone
}

func (n *node) goneOf(paths []string) []string {
	var gone []string
	for _, p := range paths {
		if _, statErr := os.Stat(p); statErr != nil {
			gone = append(gone, p)
		}
	}
	return gone
}

// refScans counts full scans of the references (a test reads it: an unchanged index is not read again).
var refScans atomic.Int64

// refFilePaths: every distinct file a filestore reference names, sorted — one read of the whole index.
func (n *node) refFilePaths() ([]string, bool) {
	refScans.Add(1)
	next, err := filestore.ListAll(n.ctx, n.fstore, false) // unsorted: fileOrder sorted all 3.6 M entries in memory
	if err != nil || next == nil {
		return nil, false
	}
	seen := map[string]struct{}{}
	for r := next(n.ctx); r != nil; r = next(n.ctx) {
		if r.FilePath == "" {
			continue
		}
		seen[filepath.Join(fileRoot, r.FilePath)] = struct{}{}
	}
	paths := make([]string, 0, len(seen))
	for p := range seen {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	return paths, true
}

// cidUnservable READS every block of c out of the local blockstore and returns the first read error, or "" if the
// whole DAG serves cleanly. This is the check cidMissing cannot do: cidMissing only os.Stat()s the backing path, so
// a reference whose file still EXISTS but whose BYTES have changed passes it while every real bitswap request fails
// with "data in file did not match ... offset N". Such a ref is worse than a missing one — we advertise the block,
// a peer asks for it, and the transfer hangs instead of failing over to another provider.
//
// Cost: reads the referenced bytes (the filestore verifies each leaf's hash against its backing range on Get), so
// this is I/O-bound and belongs in an explicit deep pass, never on a background timer.
func (n *node) cidUnservable(c cid.Cid) string {
	seen := cid.NewSet()
	var walk func(c cid.Cid) string
	walk = func(c cid.Cid) string {
		if !seen.Visit(c) {
			return ""
		}
		// Get through the filestore-backed blockstore: this is the exact path bitswap serves from, so any
		// posinfo/hash disagreement surfaces here the same way it would for a remote peer.
		if _, err := n.bstore.Get(n.ctx, c); err != nil {
			return fmt.Sprintf("%s: %v", c.String(), err)
		}
		if res := filestore.List(n.ctx, n.fstore, c); res != nil && res.FilePath != "" {
			return "" // a verified filestore leaf has no links to descend into
		}
		nd, err := n.localDserv.Get(n.ctx, c)
		if err != nil {
			return "" // not a dag-pb node we hold — nothing further to verify locally
		}
		for _, l := range nd.Links() {
			if e := walk(l.Cid); e != "" {
				return e // short-circuit on the first unservable block
			}
		}
		return ""
	}
	return walk(c)
}

// unservableRefs walks the ENTIRE filestore index and verifies each entry's bytes against its backing range,
// returning one record per BAD entry. This is broader than verifying the CIDs a manifest records: a stale entry can
// survive under a CID no node JSON references any more (a superseded delta, a re-published package), and we keep
// advertising it — so a peer asks for that block and hangs. Nothing that starts from the recorded CIDs can find it;
// only enumerating the index does. Status 12 = contents changed, 11 = backing file gone.
func (n *node) unservableRefs() []map[string]interface{} {
	next, err := filestore.VerifyAll(n.ctx, n.fstore, true) // fileOrder: a file's leaves are contiguous
	if err != nil || next == nil {
		return nil
	}
	out := []map[string]interface{}{}
	for {
		r := next(n.ctx)
		if r == nil {
			break
		}
		if r.Status == filestore.StatusOk {
			continue
		}
		out = append(out, map[string]interface{}{
			"cid":    r.Key.String(),
			"path":   filepath.Join("/", r.FilePath),
			"status": int(r.Status),
			"err":    r.ErrorMsg,
		})
	}
	return out
}

// cidServeStatus answers the only question the UI should be claiming: CAN WE DELIVER THIS? "" means yes as far
// as a cheap check can tell; otherwise a human-readable reason.
//
// "Seeded" used to mean nothing more than "a pin exists", which is why a stale reference displayed as happily
// seeded while every peer request for it hung. This checks the two conditions that cost only a stat:
//   - every backing file still EXISTS (what cidMissing does), and
//   - each backing file is still the SIZE its references cover — a file's refs tile it exactly, so a file that
//     grew or was truncated no longer matches what we published.
//
// It deliberately does NOT read file contents: proving deliverability byte-for-byte means reading everything
// (61 GB here), so that stays in the explicit verify path. A same-size edit therefore still passes this.
func (n *node) cidServeStatus(c cid.Cid) string {
	extent := map[string]uint64{} // backing path → highest offset+size any reference covers
	seen := cid.NewSet()
	var walk func(c cid.Cid) string
	walk = func(c cid.Cid) string {
		if !seen.Visit(c) {
			return ""
		}
		if res := filestore.List(n.ctx, n.fstore, c); res != nil && res.FilePath != "" {
			p := filepath.Join("/", res.FilePath)
			st, err := os.Stat(p)
			if err != nil {
				return "backing file gone: " + p
			}
			if end := res.Offset + res.Size; end > extent[p] {
				extent[p] = end
			}
			if uint64(st.Size()) < extent[p] {
				return "backing file truncated: " + p
			}
			return ""
		}
		nd, err := n.localDserv.Get(n.ctx, c)
		if err != nil {
			return ""
		}
		for _, l := range nd.Links() {
			if e := walk(l.Cid); e != "" {
				return e
			}
		}
		return ""
	}
	if e := walk(c); e != "" {
		return e
	}
	// NOTE: a file that GREW is deliberately NOT an error. The references still tile the original bytes, so every
	// block a peer asks for still reads correctly — appending to a file does not stop us serving what we
	// published. Verified: appending 4 KiB leaves the CID fully servable. Only a file that SHRANK below the
	// covered extent (handled above) actually breaks delivery. Treating "grew" as broken would have been a false
	// alarm that also destroyed a working reference during repair.
	return ""
}

// serveVerdict: what serving a CID's DAG here would meet — Err, the first problem (as cidServeStatus words it), and
// Gone, whether a file a leaf is referenced into is gone (cidMissing).
type serveVerdict struct {
	Err  string `json:"err"`
	Gone bool   `json:"gone"`
}

// serveWalks counts the blocks serveStatusMany walked (a test reads it: a block shared by many CIDs is walked once).
var serveWalks atomic.Int64

// serveStatusMany: cidServeStatus and cidMissing of each CID, in one pass over all of them: a subtree's verdict is
// kept, and each backing file stat'd once, for every CID that reaches it. The IPFS tab asked each pin on its own — a
// content leaf is reached from its file, its package's content folder and its pin folder, walked three times, each
// leaf stat'd every time — and then walked every row again for cidMissing.
func (n *node) serveStatusMany(cs []cid.Cid) []serveVerdict {
	memo := map[cid.Cid]serveVerdict{}
	stats := newStatMemo()
	var walk func(c cid.Cid) serveVerdict
	walk = func(c cid.Cid) serveVerdict {
		if v, ok := memo[c]; ok {
			return v
		}
		memo[c] = serveVerdict{} // a block reached again while being walked (no DAG has one): nothing more to find
		serveWalks.Add(1)
		var v serveVerdict
		if res := filestore.List(n.ctx, n.fstore, c); res != nil && res.FilePath != "" {
			p := filepath.Join("/", res.FilePath)
			size := stats.size(p)
			switch {
			case size < 0:
				v = serveVerdict{Err: "backing file gone: " + p, Gone: true}
			case uint64(size) < res.Offset+res.Size:
				v = serveVerdict{Err: "backing file truncated: " + p}
			}
			memo[c] = v
			return v
		}
		if nd, err := n.localDserv.Get(n.ctx, c); err == nil {
			for _, l := range nd.Links() {
				cv := walk(l.Cid)
				if v.Err == "" {
					v.Err = cv.Err
				}
				v.Gone = v.Gone || cv.Gone
			}
		}
		memo[c] = v
		return v
	}
	out := make([]serveVerdict, len(cs))
	for i, c := range cs {
		if n.ctx.Err() != nil { // shutting down: not checked, never "serves"
			out[i] = serveVerdict{Err: "not checked: the node is stopping"}
			continue
		}
		out[i] = walk(c)
		if n.ctx.Err() != nil { // stopped during the walk: what it found (its reads failing) is no verdict
			out[i] = serveVerdict{Err: "not checked: the node is stopping"}
		}
	}
	return out
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
