package main

// api_social.go — the C ABI for the friends/multiplayer social layer (see social.go + friend.go), mirrored in
// src/vgipfsapi.h and consumed by src/ipfswrapper.cpp. Same conventions as api.go: 0/-1 return, char** out-params
// freed with VgFree. Inbound friend events are delivered through a single registered callback (kind + JSON payload).

/*
#include <stdlib.h>

// Inbound friend event: kind mirrors friend.go's evFriend* (0=request 1=accept 2=decline 3=presence 4=profile
// 5=removed); json is the affected contact as a JSON object (peer/nick/pic/state/online), owned by the callee.
typedef void (*vg_friend_cb)(int kind, const char* json);

static inline void vg_invoke_friend(vg_friend_cb cb, int kind, const char* json) {
    if (cb) cb(kind, json);
}
*/
import "C"

import (
	"encoding/json"
	"unsafe"
)

// friendCb holds the registered C callback; friend events are reported through it.
var friendCb C.vg_friend_cb

//export VgSetFriendCb
func VgSetFriendCb(cb C.vg_friend_cb) { friendCb = cb }

// emitFriendEvent bridges a Go-side friend event to the registered C callback. Installed as the friendService emit
// sink in goOnline. Invoked on whatever goroutine produced the event; the C++ side marshals to the GUI thread.
func emitFriendEvent(kind int, jsonPayload string) {
	if friendCb == nil {
		return
	}
	cj := C.CString(jsonPayload)
	defer C.free(unsafe.Pointer(cj))
	C.vg_invoke_friend(friendCb, C.int(kind), cj)
}

// friendSvc returns the live friend service, or nil if the node is offline (no host yet).
func friendSvc() *friendService {
	n := get()
	if n == nil {
		return nil
	}
	return n.friend
}

// This node's shareable friend code — its libp2p peer ID (Ed25519 → embeds the public key). "" if offline.
//
//export VgFriendCode
func VgFriendCode(out **C.char) C.int {
	n := get()
	if n == nil {
		return -1
	}
	setStr(out, n.peerID())
	return 0
}

//export VgSetProfile
func VgSetProfile(nick *C.char, picCid *C.char, errOut **C.char) C.int {
	n := get()
	if n == nil || n.social == nil {
		setStr(errOut, "node not started")
		return -1
	}
	n.social.setProfile(C.GoString(nick), C.GoString(picCid))
	if n.friend != nil {
		n.friend.broadcastProfile() // push the new profile to accepted friends
	}
	return 0
}

//export VgGetProfile
func VgGetProfile(outJson **C.char) C.int {
	n := get()
	if n == nil || n.social == nil {
		return -1
	}
	p := n.social.getProfile()
	b, _ := json.Marshal(map[string]string{"nick": p.Nick, "pic": p.PicCID})
	setStr(outJson, string(b))
	return 0
}

// JSON array of contacts: [{peer,nick,pic,state,online,seen,added,play,plabel,pident,psince,popen}].
// The play keys MUST match emitContact (friend.go) exactly — the UI re-reads this list on every event, so a field
// present in only one of the two is erased by the refresh the event triggers.
//
//export VgFriendList
func VgFriendList(outJson **C.char) C.int {
	n := get()
	if n == nil || n.social == nil {
		return -1
	}
	cs := n.social.list()
	arr := make([]map[string]any, 0, len(cs))
	for _, c := range cs {
		arr = append(arr, map[string]any{
			"peer": c.PeerID, "nick": c.Nick, "pic": c.PicCID,
			"state": string(c.State), "online": c.online, "seen": c.LastSeen, "added": c.AddedAt,
		})
	}
	b, _ := json.Marshal(arr)
	setStr(outJson, string(b))
	return 0
}

//export VgFriendAdd
func VgFriendAdd(peerID *C.char, note *C.char, errOut **C.char) C.int {
	f := friendSvc()
	if f == nil {
		setStr(errOut, "networking is offline")
		return -1
	}
	if err := f.addFriend(C.GoString(peerID), C.GoString(note)); err != nil {
		return fail(errOut, err)
	}
	return 0
}

//export VgFriendAccept
func VgFriendAccept(peerID *C.char, errOut **C.char) C.int {
	f := friendSvc()
	if f == nil {
		setStr(errOut, "networking is offline")
		return -1
	}
	if err := f.acceptFriend(C.GoString(peerID)); err != nil {
		return fail(errOut, err)
	}
	return 0
}

//export VgFriendDecline
func VgFriendDecline(peerID *C.char, errOut **C.char) C.int {
	f := friendSvc()
	if f == nil {
		setStr(errOut, "networking is offline")
		return -1
	}
	if err := f.declineFriend(C.GoString(peerID)); err != nil {
		return fail(errOut, err)
	}
	return 0
}

//export VgFriendBlock
func VgFriendBlock(peerID *C.char, errOut **C.char) C.int {
	f := friendSvc()
	if f == nil {
		setStr(errOut, "networking is offline")
		return -1
	}
	if err := f.blockFriend(C.GoString(peerID)); err != nil {
		return fail(errOut, err)
	}
	return 0
}

//export VgFriendRemove
func VgFriendRemove(peerID *C.char) C.int {
	n := get()
	if n == nil || n.social == nil {
		return -1
	}
	if n.social.remove(C.GoString(peerID)) {
		return 0
	}
	return -1
}

// Actively probe whether a friend is reachable right now (returns 1 online, 0 offline, -1 n/a).
//
//export VgFriendPing
func VgFriendPing(peerID *C.char) C.int {
	f := friendSvc()
	if f == nil {
		return -1
	}
	if f.pingPresence(C.GoString(peerID)) {
		return 1
	}
	return 0
}
