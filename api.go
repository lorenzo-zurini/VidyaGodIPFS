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

// ---- status ----

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

// ---- network (M3 — stubs returning offline sentinels for now) ----

//export VgPeerCount
func VgPeerCount() C.int { return 0 }

//export VgRepoStat
func VgRepoStat(outJson **C.char, errOut **C.char) C.int {
	setStr(outJson, `{"RepoSize":-1,"StorageMax":-1}`)
	return 0
}

//export VgProviderCount
func VgProviderCount(cidStr *C.char, timeoutMs C.int) C.int { return -1 }

// ---- fetch + cancellation + transfer callback (M2) ----

//export VgFetchToPath
func VgFetchToPath(cidStr *C.char, dest *C.char, errOut **C.char) C.int {
	setStr(errOut, "fetch not implemented yet (M2)")
	return -1
}

//export VgRequestCancel
func VgRequestCancel(cidStr *C.char) {}

//export VgClearCancel
func VgClearCancel(cidStr *C.char) {}

// transferCb holds the registered C callback (M2 wires fetch progress through it).
var transferCb C.vg_transfer_cb

//export VgSetTransferCb
func VgSetTransferCb(cb C.vg_transfer_cb) { transferCb = cb }
