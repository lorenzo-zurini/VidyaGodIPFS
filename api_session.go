package main

// api_session.go — the C ABI for the multiplayer session/lobby layer (session.go), mirrored in src/vgipfsapi.h.
// Same conventions as api.go / api_social.go. Inbound session events (invite / roster / ended) are delivered through
// a single registered callback (kind + JSON payload).

/*
#include <stdlib.h>

// Inbound session event: kind mirrors session.go's evSession* (0=invite 1=roster 2=ended); json carries the event
// payload (invite: {id,game,host}; roster: {id,host,game,subnet,members:[...]}; ended: {id}). Owned by the callee.
typedef void (*vg_session_cb)(int kind, const char* json);

static inline void vg_invoke_session(vg_session_cb cb, int kind, const char* json) {
    if (cb) cb(kind, json);
}
*/
import "C"

import (
	"encoding/json"
	"unsafe"
)

var sessionCb C.vg_session_cb

//export VgSetSessionCb
func VgSetSessionCb(cb C.vg_session_cb) { sessionCb = cb }

// emitSessionEvent bridges a Go-side session event to the registered C callback (installed as the sessionService emit
// sink in goOnline).
func emitSessionEvent(kind int, jsonPayload string) {
	if sessionCb == nil {
		return
	}
	cj := C.CString(jsonPayload)
	defer C.free(unsafe.Pointer(cj))
	C.vg_invoke_session(sessionCb, C.int(kind), cj)
}

func sessionSvc() *sessionService {
	n := get()
	if n == nil {
		return nil
	}
	return n.session
}

//export VgSessionCreate
// Create a session we host for the given game CID. Returns the session JSON ({id,host,game,subnet,members}).
func VgSessionCreate(gameCid *C.char, outJson **C.char, errOut **C.char) C.int {
	ss := sessionSvc()
	if ss == nil {
		setStr(errOut, "networking is offline")
		return -1
	}
	s, err := ss.createSession(C.GoString(gameCid))
	if err != nil {
		return fail(errOut, err)
	}
	if snap, ok := ss.snapshot(s.ID); ok {
		b, _ := json.Marshal(snap)
		setStr(outJson, string(b))
	}
	return 0
}

//export VgSessionInvite
func VgSessionInvite(sid *C.char, peerID *C.char, errOut **C.char) C.int {
	ss := sessionSvc()
	if ss == nil {
		setStr(errOut, "networking is offline")
		return -1
	}
	if err := ss.invite(C.GoString(sid), C.GoString(peerID)); err != nil {
		return fail(errOut, err)
	}
	return 0
}

//export VgSessionJoin
func VgSessionJoin(sid *C.char, hostPeer *C.char, errOut **C.char) C.int {
	ss := sessionSvc()
	if ss == nil {
		setStr(errOut, "networking is offline")
		return -1
	}
	if err := ss.join(C.GoString(sid), C.GoString(hostPeer)); err != nil {
		return fail(errOut, err)
	}
	return 0
}

//export VgSessionLeave
func VgSessionLeave(sid *C.char, errOut **C.char) C.int {
	ss := sessionSvc()
	if ss == nil {
		setStr(errOut, "networking is offline")
		return -1
	}
	if err := ss.leave(C.GoString(sid)); err != nil {
		return fail(errOut, err)
	}
	return 0
}

//export VgSessionReady
func VgSessionReady(sid *C.char, ready C.int, errOut **C.char) C.int {
	ss := sessionSvc()
	if ss == nil {
		setStr(errOut, "networking is offline")
		return -1
	}
	if err := ss.setReady(C.GoString(sid), ready != 0); err != nil {
		return fail(errOut, err)
	}
	return 0
}

//export VgSessionList
// JSON array of all sessions ({id,host,game,subnet,amHost,members}).
func VgSessionList(outJson **C.char) C.int {
	ss := sessionSvc()
	if ss == nil {
		setStr(outJson, "[]")
		return 0
	}
	b, _ := json.Marshal(ss.snapshotAll())
	setStr(outJson, string(b))
	return 0
}

//export VgSessionRoster
// JSON of one session ({id,host,game,subnet,amHost,members}); -1 if unknown.
func VgSessionRoster(sid *C.char, outJson **C.char) C.int {
	ss := sessionSvc()
	if ss == nil {
		return -1
	}
	snap, ok := ss.snapshot(C.GoString(sid))
	if !ok {
		return -1
	}
	b, _ := json.Marshal(snap)
	setStr(outJson, string(b))
	return 0
}
