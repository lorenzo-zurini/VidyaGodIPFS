package main

// api.go — the exported C ABI consumed by VidyaGod's src/ipfswrapper.cpp via cgo.
//
// Convention: fallible calls return 0 on success / -1 on failure; results and error reasons come back through
// char** out-params allocated with C.CString (the C++ side frees them with VgFree). This mirrors the existing
// IpfsWrapper signatures (which already pass `std::string *Error`). Built with `go build -buildmode=c-shared`.

/*
#include <stdlib.h>

// Transfer lifecycle callback (M2+): kind 0=Started 1=Progress 2=Finished 3=Finalizing 4=Phase. err carries the
// failure reason on a failed Finished — and, for kind 4, the PHASE TEXT (what the transfer is doing right now).
typedef void (*vg_transfer_cb)(const char* cid, int kind, double percent, int ok, const char* err);

static inline void vg_invoke_transfer(vg_transfer_cb cb,
                                      const char* cid, int kind, double percent, int ok, const char* err) {
    if (cb) cb(cid, kind, percent, ok, err);
}
*/
import "C"

import (
	"context"
	"encoding/json"
	"time"
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

// VgSetLogVerbose flips the node's verbose diagnostic log (vglog.go) on/off at runtime. The app calls this from its
// --log path AFTER redirecting the process stderr to the log file, so every subsequent vlog line is captured there.
//
//export VgSetLogVerbose
func VgSetLogVerbose(on C.int) {
	gLogVerbose.Store(on != 0)
	vlog("log", "verbose logging %s", map[bool]string{true: "ENABLED", false: "disabled"}[on != 0])
}

// Live health of every Go service, for Settings → Network: JSON [{name,status,detail}] with status
// ok|warn|down|off. Passive introspection — cheap enough to poll every few seconds (health.go).
//
//export VgHealth
func VgHealth(outJson **C.char) C.int {
	setStr(outJson, healthJSON())
	return 0
}

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

// ---- dag-json node graph (the gigagraph: one node = one dag-json block, identity = CID) ----

// VgDagPut canonicalizes a node's JSON, stores it as one dag-json block (direct-pinned + announced), and returns the
// block CID — the node's identity. Deterministic: any key order in the input yields the same CID (dag.go).
//
//export VgDagPut
func VgDagPut(jsonStr *C.char, outCid **C.char, errOut **C.char) C.int {
	n := get()
	if n == nil {
		setStr(errOut, "node not started")
		return -1
	}
	c, err := n.dagPut([]byte(C.GoString(jsonStr)))
	if err != nil {
		return fail(errOut, err)
	}
	setStr(outCid, c.String())
	return 0
}

// VgDagGet returns a node block's raw dag-json bytes (valid JSON; links as {"/":"<cid>"}). Fetches over bitswap when
// the block is not local. Errors if the CID is not a dag-json node.
//
//export VgDagGet
func VgDagGet(cidStr *C.char, outJson **C.char, errOut **C.char) C.int {
	n := get()
	if n == nil {
		setStr(errOut, "node not started")
		return -1
	}
	c, err := cid.Decode(C.GoString(cidStr))
	if err != nil {
		return fail(errOut, err)
	}
	b, err := n.dagGet(c)
	if err != nil {
		return fail(errOut, err)
	}
	setStr(outJson, string(b))
	return 0
}

// VgDagGetMany fetches many dag-json node blocks at once through the windowed session (see dagGetMany) — the browse
// path's batched fetch, so grouping a friend's 900-variant game doesn't do 900 serial round-trips. Input: JSON array
// of CID strings. Output: a JSON object {cid: <block's dag-json>} for those fetched (missing ones absent).
//
//export VgDagGetMany
func VgDagGetMany(cidsJson *C.char, outJson **C.char, errOut **C.char) C.int {
	n := get()
	if n == nil {
		setStr(errOut, "node not started")
		return -1
	}
	var cidStrs []string
	if err := json.Unmarshal([]byte(C.GoString(cidsJson)), &cidStrs); err != nil {
		return fail(errOut, err)
	}
	need := make([]cid.Cid, 0, len(cidStrs))
	for _, s := range cidStrs {
		c, err := cid.Decode(s)
		if err != nil || c.Prefix().Codec != cid.DagJSON {
			continue
		}
		need = append(need, c)
	}
	out := make(map[string]json.RawMessage, len(need))
	for k, v := range n.dagGetMany(need) {
		out[k] = json.RawMessage(v)
	}
	b, _ := json.Marshal(out)
	setStr(outJson, string(b))
	return 0
}

// VgDagHas: 1 if the node block is present locally, 0 if not, -1 if the node is not started or the CID is invalid.
//
//export VgDagHas
func VgDagHas(cidStr *C.char) C.int {
	n := get()
	if n == nil {
		return -1
	}
	c, err := cid.Decode(C.GoString(cidStr))
	if err != nil {
		return -1
	}
	if n.dagHas(c) {
		return 1
	}
	return 0
}

// VgDagCid: the CID a node's JSON WOULD have, with NO side effects (nothing stored/pinned). Same canonicalization as
// VgDagPut, so it matches what freezing would produce. Needs no started node.
//
//export VgDagCid
func VgDagCid(jsonStr *C.char, outCid **C.char, errOut **C.char) C.int {
	c, err := computeDagJSONCid([]byte(C.GoString(jsonStr)))
	if err != nil {
		return fail(errOut, err)
	}
	setStr(outCid, c.String())
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

// VgCidFileSizeLocal: the UnixFS FILE size (payload bytes) of a CID from the local store, -1 if unknown. Reads
// only the root block, so it is the cheap gate for "does the file on disk still match what we published?".
//
//export VgCidFileSizeLocal
func VgCidFileSizeLocal(cidStr *C.char) C.longlong {
	n := get()
	if n == nil {
		return -1
	}
	c, err := cid.Decode(C.GoString(cidStr))
	if err != nil {
		return -1
	}
	return C.longlong(n.cidFileSizeLocal(c))
}

// VgCidServeStatus: "" when a CID looks deliverable (all backing files present and still the size their
// references cover), else the reason. Cheap — stat only, no content read. Caller owns the string (VgFree).
//
//export VgCidServeStatus
func VgCidServeStatus(cidStr *C.char) *C.char {
	n := get()
	if n == nil {
		return C.CString("node not started")
	}
	c, err := cid.Decode(C.GoString(cidStr))
	if err != nil {
		return C.CString("bad cid: " + err.Error())
	}
	return C.CString(n.cidServeStatus(c))
}

// VgServeFailures: JSON [{"cid","err","when"}] of blocks a PEER requested that we could not deliver, draining
// the list. This is the uploader-side corruption signal BitTorrent lacks — there only the downloader hashes, so a
// seeder with rotted data never learns. Poll it to turn the offending rows red.
//
//export VgServeFailures
func VgServeFailures(outJson **C.char) C.int {
	n := get()
	if n == nil || n.serveFails == nil {
		setStr(outJson, "[]")
		return 0
	}
	b, _ := json.Marshal(n.serveFails.drain())
	setStr(outJson, string(b))
	return 0
}

// Returns "" when the whole DAG reads cleanly out of the local blockstore, otherwise the first read error
// ("<cid>: data in file did not match ..."). Unlike VgCidMissing this READS the referenced bytes, so it detects a
// stale reference whose backing file still exists but no longer matches — the class that makes peers hang.
// Caller owns the returned string (VgFree).
//
//export VgVerifyCid
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
	kindPhase      = 4 // a human-readable line saying what the transfer is DOING right now (text rides the err param)
)

// phaseHook lets tests observe phase lines without a C callback installed.
var phaseHook func(cid, text string)

// phase narrates a transfer: one line per state change ("attempt 3 — connecting to providers", "downloading from
// gateway.pinata.cloud", "stalled — no data for 20s"), shown verbatim on the transfer's UI row. THE FAILURE MODE IS
// SILENCE doctrine applied to the UI: a fetch that is hunting, backing off or falling back must never just sit on a
// generic "Downloading" label looking stuck. Also fdbg'd, so the netpaths matrix oracle sees the same narration.
func phase(cidStr, text string) {
	fdbg("phase %s: %s", shortCid2(cidStr), text)
	if phaseHook != nil {
		phaseHook(cidStr, text)
	}
	ccid := C.CString(cidStr)
	ct := C.CString(text)
	C.vg_invoke_transfer(transferCb, ccid, C.int(kindPhase), C.double(-1), C.int(0), ct)
	C.free(unsafe.Pointer(ccid))
	C.free(unsafe.Pointer(ct))
}

// shortCid2 trims a CID string for the debug trace (the UI gets the full CID through the callback).
func shortCid2(s string) string {
	if len(s) <= 12 {
		return s
	}
	return s[:6] + ".." + s[len(s)-4:]
}

// VgFetchOnce runs ONE fetch attempt for a CID to dest and returns a CLASSIFIED outcome for the C++ rolling queue:
// 0 = Done (complete + seeded), 1 = Retryable (stalled / no providers / offline / a cleared stale ref — the queue
// backs off and re-dispatches), 2 = Terminal (cancelled, malformed CID, or a local/disk error). isDir != 0 fetches a
// directory (meta) CID. There is NO internal retry loop and NO wall-clock budget: retry/backoff/stall-demotion live
// in the dispatcher; a bounded CALLER instead bounds its WAIT (WaitBatch) while the item keeps rolling in the queue.
//
//export VgFetchOnce
func VgFetchOnce(cidStr *C.char, dest *C.char, isDir C.int, errOut **C.char) C.int {
	n := get()
	if n == nil {
		setStr(errOut, "node not started")
		return fetchRetryable // not fatal: the node may come up; the queue will re-dispatch
	}
	cs := C.GoString(cidStr)
	d := C.GoString(dest)
	ccid := C.CString(cs)
	defer C.free(unsafe.Pointer(ccid))
	emit := func(kind int, pct float64, ok int, errc *C.char) {
		C.vg_invoke_transfer(transferCb, ccid, C.int(kind), C.double(pct), C.int(ok), errc)
	}
	onP := func(pct float64) { emit(kindProgress, pct, 0, nil) }
	onF := func(pct float64) { emit(kindFinalizing, pct, 0, nil) }

	emit(kindStarted, -1, 0, nil)
	var err error
	if isDir != 0 {
		err = n.fetchDirToPath(cs, d, onP, onF)
	} else {
		err = n.fetchToPath(cs, d, onP, onF) // single attempt; clears a stale ref on errMissingFiles
	}
	rc := classifyFetchErr(cs, err)
	if err != nil {
		setStr(errOut, err.Error())
		// A TERMINAL failure stamps the row Errored. A RETRYABLE one does NOT emit a terminal event — the row keeps
		// its last phase ("stalled …") and the dispatcher re-dispatches; flapping it to Errored on every rotation
		// would be a lie. (IpfsModel also heals a stray Errored→Downloading on the next Started/progress.)
		if rc == fetchTerminal {
			ec := C.CString(err.Error())
			defer C.free(unsafe.Pointer(ec))
			emit(kindFinished, -1, 0, ec)
		}
		return C.int(rc)
	}
	emit(kindFinished, 100, 1, nil)
	return fetchDone
}

//export VgRequestCancel
func VgRequestCancel(cidStr *C.char) { requestCancel(C.GoString(cidStr)) }

//export VgClearCancel
func VgClearCancel(cidStr *C.char) { clearCancel(C.GoString(cidStr)) }

// VgSetExpectedSize records a CID's known payload byte size (the manifest's stamped SOURCE.SIZE) so a gateway
// fallback can show a real progress %. size <= 0 clears it. Idempotent; safe to call repeatedly.
//export VgSetExpectedSize
func VgSetExpectedSize(cidStr *C.char, size C.longlong) { setExpectedSize(C.GoString(cidStr), int64(size)) }

//export VgSetTransferCb
func VgSetTransferCb(cb C.vg_transfer_cb) { transferCb = cb }

// VgIpnsPublish signs + publishes an IPNS record for OUR name (the identity key = friend code) pointing at cidStr,
// with a long EOL; ttlSeconds is the resolver-cache hint (<=0 → default). The record is re-published before EOL by
// the node's republish loop for as long as it stays online. Blocks on the DHT put; call off the UI thread.
//
//export VgIpnsPublish
func VgIpnsPublish(cidStr *C.char, ttlSeconds C.int, errOut **C.char) C.int {
	n := get()
	if n == nil {
		setStr(errOut, "node not started")
		return -1
	}
	ctx, cancel := context.WithTimeout(n.ctx, 90*time.Second)
	defer cancel()
	if err := n.ipnsPublish(ctx, C.GoString(cidStr), time.Duration(ttlSeconds)*time.Second); err != nil {
		return fail(errOut, err)
	}
	return 0
}

// VgIpnsResolve resolves /ipns/<name> (a peer ID / friend code, with or without the /ipns/ prefix) to its current
// /ipfs/<cid> path string. DHT first, then the HTTPS-gateway fallback; the signature is verified against the name, so
// a forged record is rejected. Returns the resolved path (e.g. "/ipfs/Qm…") via out.
//
//export VgIpnsResolve
func VgIpnsResolve(name *C.char, out **C.char, errOut **C.char) C.int {
	n := get()
	if n == nil {
		setStr(errOut, "node not started")
		return -1
	}
	ctx, cancel := context.WithTimeout(n.ctx, 60*time.Second)
	defer cancel()
	p, err := n.ipnsResolve(ctx, C.GoString(name))
	if err != nil {
		return fail(errOut, err)
	}
	setStr(out, p)
	return 0
}

// VgExportIdentity backs up the Ed25519 identity key (== friend code + library address) to destPath. Loss is permanent.
//
//export VgExportIdentity
func VgExportIdentity(destPath *C.char, errOut **C.char) C.int {
	n := get()
	if n == nil {
		setStr(errOut, "node not started")
		return -1
	}
	if err := n.exportIdentity(C.GoString(destPath)); err != nil {
		return fail(errOut, err)
	}
	return 0
}

// VgImportIdentity installs a backed-up identity key. The running node keeps the old key until RESTARTED — the caller
// must warn + restart for the new peer ID / friend code / library address to take effect.
//
//export VgImportIdentity
func VgImportIdentity(srcPath *C.char, errOut **C.char) C.int {
	n := get()
	if n == nil {
		setStr(errOut, "node not started")
		return -1
	}
	if err := n.importIdentity(C.GoString(srcPath)); err != nil {
		return fail(errOut, err)
	}
	return 0
}
