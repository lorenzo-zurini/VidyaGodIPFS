package main

// api.go — the exported C ABI consumed by VidyaGod's src/ipfswrapper.cpp via cgo.
//
// Convention: fallible calls return 0 on success / -1 on failure; results and error reasons come back through
// char** out-params allocated with C.CString (the C++ side frees them with VgFree). This mirrors the existing
// IpfsWrapper signatures (which already pass `std::string *Error`). Built with `go build -buildmode=c-shared`.

/*
#include <stdlib.h>

// Transfer lifecycle callback (M2+): kind 0=Started 1=Progress 2=Finished. err is non-NULL only on a failed Finished.
typedef void (*vg_transfer_cb)(const char* cid, int kind, double percent, int ok, const char* err);

static inline void vg_invoke_transfer(vg_transfer_cb cb,
                                      const char* cid, int kind, double percent, int ok, const char* err) {
    if (cb) cb(cid, kind, percent, ok, err);
}
*/
import "C"

import (
	"encoding/json"
	"unsafe"

	cid "github.com/ipfs/go-cid"
)

func main() {} // required for buildmode=c-shared

// ---- helpers ----

func setStr(out **C.char, s string) {
	if out != nil {
		*out = C.CString(s)
	}
}

func fail(errOut **C.char, err error) C.int {
	if err != nil {
		setStr(errOut, err.Error())
	}
	return -1
}

//export VgFree
func VgFree(p *C.char) { C.free(unsafe.Pointer(p)) }

// ---- lifecycle ----

//export VgStart
func VgStart(repoPath *C.char, errOut **C.char) C.int {
	if err := openNode(C.GoString(repoPath)); err != nil {
		return fail(errOut, err)
	}
	return 0
}

//export VgStop
func VgStop() { closeNode() }

//export VgStarted
func VgStarted() C.int {
	if get() != nil {
		return 1
	}
	return 0
}

//export VgOnline
func VgOnline() C.int {
	n := get()
	if n != nil && n.online {
		return 1
	}
	return 0
}

// ---- seed (filestore --nocopy) ----

//export VgAddNoCopy
func VgAddNoCopy(path *C.char, outCid **C.char, errOut **C.char) C.int {
	n := get()
	if n == nil {
		setStr(errOut, "node not started")
		return -1
	}
	c, err := n.addNoCopy(C.GoString(path))
	if err != nil {
		return fail(errOut, err)
	}
	setStr(outCid, c.String())
	return 0
}

// Seed a TEXT-ONLY Meta-CID for a package dir/collection: references the *.json manifests IN PLACE (no staging copy),
// skipping content files + DEFPREFIX/USERDATA runtime subtrees. Same CID as adding a JSON-only mirror of the tree.
//
//export VgAddNoCopyMeta
func VgAddNoCopyMeta(path *C.char, outCid **C.char, errOut **C.char) C.int {
	n := get()
	if n == nil {
		setStr(errOut, "node not started")
		return -1
	}
	c, err := n.addMetaNoCopy(C.GoString(path))
	if err != nil {
		return fail(errOut, err)
	}
	setStr(outCid, c.String())
	return 0
}

// ---- status ----

//export VgDebugCounts
func VgDebugCounts(outJson **C.char) C.int {
	n := get()
	if n == nil {
		return -1
	}
	fs, mb := n.counts()
	b, _ := json.Marshal(map[string]int{"fsRefs": fs, "mainBlocks": mb})
	setStr(outJson, string(b))
	return 0
}

//export VgPeerID
func VgPeerID(out **C.char) C.int {
	n := get()
	if n == nil {
		return -1
	}
	setStr(out, n.peerID())
	return 0
}

//export VgListenAddrs
func VgListenAddrs(out **C.char) C.int {
	n := get()
	if n == nil {
		return -1
	}
	b, _ := json.Marshal(n.listenAddrs())
	setStr(out, string(b))
	return 0
}

//export VgConnect
func VgConnect(maddr *C.char, errOut **C.char) C.int {
	n := get()
	if n == nil {
		setStr(errOut, "node not started")
		return -1
	}
	if err := n.connect(C.GoString(maddr)); err != nil {
		return fail(errOut, err)
	}
	return 0
}

//export VgHasLocal
func VgHasLocal(cidStr *C.char) C.int {
	n := get()
	if n == nil {
		return -1
	}
	c, err := cid.Decode(C.GoString(cidStr))
	if err != nil {
		return -1
	}
	if n.hasLocal(c) {
		return 1
	}
	return 0
}

//export VgDropRef
func VgDropRef(cidStr *C.char, errOut **C.char) C.int {
	n := get()
	if n == nil {
		setStr(errOut, "node not started")
		return -1
	}
	c, err := cid.Decode(C.GoString(cidStr))
	if err != nil {
		return fail(errOut, err)
	}
	n.dropRef(c)
	return 0
}

//export VgDropCached
func VgDropCached(cidStr *C.char, errOut **C.char) C.int {
	n := get()
	if n == nil {
		setStr(errOut, "node not started")
		return -1
	}
	c, err := cid.Decode(C.GoString(cidStr))
	if err != nil {
		return fail(errOut, err)
	}
	n.dropClosure(c)       // delete the partial's cached blocks (offline walk; absent leaves skipped)
	n.scheduleCompaction() // reclaim the tombstone disk
	return 0
}

// VgComputeCid: what a file's CID WOULD be, with NO side effects (nothing enters the blockstore, filestore or
// pinset). Same importer settings as VgAddNoCopy, so it answers "do these bytes still match the published CID?".
// Needs no started node. Returns "" on failure with the reason in errOut.
//
//export VgComputeCid
func VgComputeCid(path *C.char, outCid **C.char, errOut **C.char) C.int {
	c, err := computeCid(C.GoString(path))
	if err != nil {
		setStr(errOut, err.Error())
		return 1
	}
	setStr(outCid, c.String())
	return 0
}

//export VgVerifyCid
// Returns "" when the whole DAG reads cleanly out of the local blockstore, otherwise the first read error
// ("<cid>: data in file did not match ..."). Unlike VgCidMissing this READS the referenced bytes, so it detects a
// stale reference whose backing file still exists but no longer matches — the class that makes peers hang.
// Caller owns the returned string (VgFree).
func VgVerifyCid(cidStr *C.char) *C.char {
	n := get()
	if n == nil {
		return C.CString("node not started")
	}
	c, err := cid.Decode(C.GoString(cidStr))
	if err != nil {
		return C.CString("bad cid: " + err.Error())
	}
	return C.CString(n.cidUnservable(c))
}

//export VgCidMissing
func VgCidMissing(cidStr *C.char) C.int {
	n := get()
	if n == nil {
		return -1
	}
	c, err := cid.Decode(C.GoString(cidStr))
	if err != nil {
		return -1
	}
	if n.cidMissing(c) {
		return 1
	}
	return 0
}

//export VgCidSize
func VgCidSize(cidStr *C.char) C.longlong {
	n := get()
	if n == nil {
		return -1
	}
	c, err := cid.Decode(C.GoString(cidStr))
	if err != nil {
		return -1
	}
	return C.longlong(n.cidSize(c))
}

//export VgCidSizeLocal
func VgCidSizeLocal(cidStr *C.char) C.longlong {
	n := get()
	if n == nil {
		return -1
	}
	c, err := cid.Decode(C.GoString(cidStr))
	if err != nil {
		return -1
	}
	return C.longlong(n.cidSizeLocal(c))
}

//export VgPinLs
func VgPinLs(outJson **C.char, errOut **C.char) C.int {
	n := get()
	if n == nil {
		setStr(errOut, "node not started")
		return -1
	}
	cids, err := n.pinLs()
	if err != nil {
		return fail(errOut, err)
	}
	strs := make([]string, len(cids))
	for i, c := range cids {
		strs[i] = c.String()
	}
	b, _ := json.Marshal(strs)
	setStr(outJson, string(b))
	return 0
}

//export VgPinRm
func VgPinRm(cidStr *C.char, errOut **C.char) C.int {
	n := get()
	if n == nil {
		setStr(errOut, "node not started")
		return -1
	}
	c, err := cid.Decode(C.GoString(cidStr))
	if err != nil {
		return fail(errOut, err)
	}
	if err := n.unpin(c); err != nil {
		return fail(errOut, err)
	}
	return 0
}

// ---- network ----

//export VgPeerCount
func VgPeerCount() C.int {
	n := get()
	if n == nil {
		return 0
	}
	return C.int(n.peerCount())
}

//export VgRepoStat
func VgRepoStat(outJson **C.char, errOut **C.char) C.int {
	n := get()
	if n == nil {
		setStr(errOut, "node not started")
		return -1
	}
	b, _ := json.Marshal(map[string]int64{"RepoSize": dirSize(n.repoPath), "StorageMax": -1})
	setStr(outJson, string(b))
	return 0
}

//export VgProviderCount
func VgProviderCount(cidStr *C.char, timeoutMs C.int) C.int {
	n := get()
	if n == nil {
		return -1
	}
	c, err := cid.Decode(C.GoString(cidStr))
	if err != nil {
		return -1
	}
	return C.int(n.providerCount(c, int(timeoutMs)))
}

// Record the level-3 collection + level-2 package meta-CIDs (JSON arrays of CID strings) and start/refresh the 3-pass
// level-ordered seed announce (seedannounce.go). Content = every other pinned root. Idempotent.
//
//export VgSetSeedLevels
func VgSetSeedLevels(collectionsJson *C.char, packagesJson *C.char) C.int {
	n := get()
	if n == nil {
		return -1
	}
	parse := func(s string) []cid.Cid {
		var arr []string
		_ = json.Unmarshal([]byte(s), &arr)
		out := make([]cid.Cid, 0, len(arr))
		for _, x := range arr {
			if c, e := cid.Decode(x); e == nil {
				out = append(out, c)
			}
		}
		return out
	}
	n.setSeedLevels(parse(C.GoString(collectionsJson)), parse(C.GoString(packagesJson)))
	return 0
}

// 1 if a pinned CID's DHT announce has completed (→ "seeding"), 0 if still queued (or unknown). Cheap in-memory lookup.
//
//export VgSeedAnnounced
func VgSeedAnnounced(cidStr *C.char) C.int {
	n := get()
	if n == nil {
		return 0
	}
	if n.seedAnnounced(C.GoString(cidStr)) {
		return 1
	}
	return 0
}

// Writes the node's current global receive/send rates (bytes/sec) into *inBps/*outBps. Zero when offline.
//
//export VgBandwidthRates
func VgBandwidthRates(inBps *C.double, outBps *C.double) {
	n := get()
	if n == nil {
		return
	}
	ri, ro := n.bandwidth()
	if inBps != nil {
		*inBps = C.double(ri)
	}
	if outBps != nil {
		*outBps = C.double(ro)
	}
}

// JSON array of pinned-root CIDs served to a peer within the last windowMs (the items currently being uploaded).
//
//export VgActiveUploads
func VgActiveUploads(windowMs C.int, outJson **C.char) C.int {
	n := get()
	if n == nil {
		setStr(outJson, "[]")
		return 0
	}
	b, _ := json.Marshal(n.activeUploads(int64(windowMs)))
	setStr(outJson, string(b))
	return 0
}

// JSON array of DISTINCT filestore backing paths whose file is gone — orphaned no-copy references the node can't
// serve. Empty = nothing to heal. Cheap probe (one filestore scan, one stat per distinct file) the app polls to
// trigger an on-demand re-seed/heal without waiting for the next launch.
//
//export VgOrphanedRefPaths
func VgOrphanedRefPaths(outJson **C.char) C.int {
	n := get()
	if n == nil {
		setStr(outJson, "[]")
		return 0
	}
	b, _ := json.Marshal(n.orphanedRefPaths())
	setStr(outJson, string(b))
	return 0
}

// VgUnservableRefs: JSON array of every filestore entry whose bytes no longer verify —
// [{"cid":…,"path":…,"status":11|12,"err":…}]. status 12 = file contents changed, 11 = backing file gone.
// This is the check that finds references we ADVERTISE but cannot serve, including ones no manifest points at
// any more; such a reference makes a requesting peer hang rather than fail over. I/O-bound (reads the bytes).
//
//export VgUnservableRefs
func VgUnservableRefs(outJson **C.char) C.int {
	n := get()
	if n == nil {
		setStr(outJson, "[]")
		return 0
	}
	b, _ := json.Marshal(n.unservableRefs())
	setStr(outJson, string(b))
	return 0
}

// ---- fetch + cancellation + transfer callback ----

// transferCb holds the registered C callback; fetch progress/lifecycle is reported through it.
var transferCb C.vg_transfer_cb

// TransferEvent kinds — must match IpfsWrapper::TransferEvent::Kind on the C++ side.
const (
	kindStarted    = 0
	kindProgress   = 1
	kindFinished   = 2
	kindFinalizing = 3 // all bytes down; the re-reference/"pinning" step is running
)

//export VgFetchToPath
func VgFetchToPath(cidStr *C.char, dest *C.char, errOut **C.char) C.int {
	n := get()
	if n == nil {
		setStr(errOut, "node not started")
		return -1
	}
	cs := C.GoString(cidStr)
	d := C.GoString(dest)

	// One C string for the CID, reused across every event for this transfer.
	ccid := C.CString(cs)
	defer C.free(unsafe.Pointer(ccid))
	emit := func(kind int, pct float64, ok int, errc *C.char) {
		C.vg_invoke_transfer(transferCb, ccid, C.int(kind), C.double(pct), C.int(ok), errc)
	}

	emit(kindStarted, -1, 0, nil)
	err := n.fetchToPath(cs, d,
		func(pct float64) { emit(kindProgress, pct, 0, nil) },
		func(pct float64) { emit(kindFinalizing, pct, 0, nil) })
	if err != nil {
		ec := C.CString(err.Error())
		defer C.free(unsafe.Pointer(ec))
		emit(kindFinished, -1, 0, ec)
		setStr(errOut, err.Error())
		return -1
	}
	emit(kindFinished, 100, 1, nil)
	return 0
}

// Recursively materialize a UnixFS DIRECTORY CID (a folder of dehydrated packages) to dest. Fetches the small manifest
// tree only — no per-layer content hydration. Requires the node's network stack to be up (blocks arrive via bitswap).
//
//export VgFetchDirToPath
func VgFetchDirToPath(cidStr *C.char, dest *C.char, errOut **C.char) C.int {
	n := get()
	if n == nil {
		setStr(errOut, "node not started")
		return -1
	}
	if VgOnline() == 0 {
		setStr(errOut, "IPFS networking is offline — enable it to fetch a package CID")
		return -1
	}
	cs := C.GoString(cidStr)
	d := C.GoString(dest)

	// Report the source fetch through the same transfer callback as file fetches, so a pending/stuck/slow source shows
	// a live row (Fetching… → Stalled → Pinning… → seeded) in the IPFS tab instead of being silently invisible.
	ccid := C.CString(cs)
	defer C.free(unsafe.Pointer(ccid))
	emit := func(kind int, pct float64, ok int, errc *C.char) {
		C.vg_invoke_transfer(transferCb, ccid, C.int(kind), C.double(pct), C.int(ok), errc)
	}

	emit(kindStarted, -1, 0, nil)
	err := n.fetchDirToPath(cs, d,
		func(pct float64) { emit(kindProgress, pct, 0, nil) },
		func(pct float64) { emit(kindFinalizing, pct, 0, nil) })
	if err != nil {
		ec := C.CString(err.Error())
		defer C.free(unsafe.Pointer(ec))
		emit(kindFinished, -1, 0, ec)
		setStr(errOut, err.Error())
		return -1
	}
	emit(kindFinished, 100, 1, nil)
	return 0
}

//export VgRequestCancel
func VgRequestCancel(cidStr *C.char) { requestCancel(C.GoString(cidStr)) }

//export VgClearCancel
func VgClearCancel(cidStr *C.char) { clearCancel(C.GoString(cidStr)) }

//export VgSetTransferCb
func VgSetTransferCb(cb C.vg_transfer_cb) { transferCb = cb }
