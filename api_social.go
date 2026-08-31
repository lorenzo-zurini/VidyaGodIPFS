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
			"play": c.play.NodeID, "plabel": c.play.Label, "pident": c.play.Ident,
			"psince": c.play.Since, "popen": c.play.Open,
		})
	}
	b, _ := json.Marshal(arr)
	setStr(outJson, string(b))
	return 0
}

// announce pushes our play state to friends and re-shares the co-play roster. Every mutator of the play/visibility
// state ends with this, mirroring how VgSetProfile ends with broadcastProfile.
func announcePlay(n *node) {
	if n.friend != nil {
		n.friend.broadcastPresence()
		n.friend.shareCoPlay()
	}
}

// Record what was just launched. Succeeds with the node offline (the state is local; the broadcast is skipped) —
// the launch path calls this unconditionally.
//
//export VgSetPlaying
func VgSetPlaying(nodeID *C.char, label *C.char, ident *C.char, errOut **C.char) C.int {
	n := get()
	if n == nil || n.social == nil {
		setStr(errOut, "node not started")
		return -1
	}
	n.social.setPlaying(C.GoString(nodeID), C.GoString(label), C.GoString(ident))
	announcePlay(n)
	return 0
}

//export VgClearPlaying
func VgClearPlaying() C.int {
	n := get()
	if n == nil || n.social == nil {
		return -1
	}
	n.social.clearPlaying()
	announcePlay(n)
	return 0
}

// {"node","label","ident","since","open"}
//
//export VgGetPlaying
func VgGetPlaying(outJson **C.char) C.int {
	n := get()
	if n == nil || n.social == nil {
		return -1
	}
	p := n.social.getPlay()
	b, _ := json.Marshal(map[string]any{
		"node": p.NodeID, "label": p.Label, "ident": p.Ident, "since": p.Since, "open": p.Open,
	})
	setStr(outJson, string(b))
	return 0
}

// Advisory only: it is displayed as a badge and never gates the Join affordance.
//
//export VgSetOpenToJoin
func VgSetOpenToJoin(on C.int) C.int {
	n := get()
	if n == nil || n.social == nil {
		return -1
	}
	n.social.setOpenToJoin(on != 0)
	announcePlay(n)
	return 0
}

//export VgSetInvisible
func VgSetInvisible(on C.int) C.int {
	n := get()
	if n == nil || n.social == nil {
		return -1
	}
	n.social.setInvisible(on != 0)
	announcePlay(n) // pushes a zeroed play block, so friends stop seeing us immediately
	return 0
}

// 1 = hidden, 0 = visible, -1 = no node.
//
//export VgInvisible
func VgInvisible() C.int {
	n := get()
	if n == nil || n.social == nil {
		return -1
	}
	if n.social.getInvisible() {
		return 1
	}
	return 0
}

// JSON array of strangers met in a shared game: [{peer,nick,via,game,at}]. These are NOT contacts and never enter
// the address book — adding one sends an ordinary mutual-consent friend request.
//
//export VgFriendSuggestions
func VgFriendSuggestions(outJson **C.char) C.int {
	n := get()
	if n == nil || n.social == nil {
		return -1
	}
	sgs := n.social.listSuggestions()
	arr := make([]map[string]any, 0, len(sgs))
	for _, s := range sgs {
		arr = append(arr, map[string]any{
			"peer": s.Peer, "nick": s.Nick, "via": s.Via, "game": s.Game, "at": s.At,
		})
	}
	b, _ := json.Marshal(arr)
	setStr(outJson, string(b))
	return 0
}

//export VgDismissSuggestion
func VgDismissSuggestion(peerID *C.char) {
	n := get()
	if n == nil || n.social == nil {
		return
	}
	n.social.dismissSuggestion(C.GoString(peerID))
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
