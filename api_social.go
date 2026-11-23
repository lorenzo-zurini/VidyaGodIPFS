package main

// api_social.go — the C ABI for the peers/multiplayer social layer (see social.go + peer.go), mirrored in
// src/vgipfsapi.h and consumed by src/ipfswrapper.cpp. Same conventions as api.go: 0/-1 return, char** out-params
// freed with VgFree. Inbound peer events are delivered through a single registered callback (kind + JSON payload).

/*
#include <stdlib.h>

// Inbound peer event: kind mirrors peer.go's evPeer* (0=request 1=accept 2=decline 3=presence 4=profile
// 5=removed 6=library); json is the affected contact as a JSON object (peer/nick/pic/state/online) for 0..5, or the
// snapshot {peer, libs:{name:[cids]}} for 6; owned by the callee.
typedef void (*vg_peer_cb)(int kind, const char* json);

static inline void vg_invoke_peer(vg_peer_cb cb, int kind, const char* json) {
    if (cb) cb(kind, json);
}
*/
import "C"

import (
	"encoding/json"
	"unsafe"
)

// peerCb holds the registered C callback; peer events are reported through it.
var peerCb C.vg_peer_cb

//export VgSetPeerCb
func VgSetPeerCb(cb C.vg_peer_cb) { peerCb = cb }

// emitPeerEvent bridges a Go-side peer event to the registered C callback. Installed as the peerService emit
// sink in goOnline. Invoked on whatever goroutine produced the event; the C++ side marshals to the GUI thread.
func emitPeerEvent(kind int, jsonPayload string) {
	if peerCb == nil {
		return
	}
	cj := C.CString(jsonPayload)
	defer C.free(unsafe.Pointer(cj))
	C.vg_invoke_peer(peerCb, C.int(kind), cj)
}

// peerSvc returns the live peer service, or nil if the node is offline (no host yet).
func peerSvc() *peerService {
	n := get()
	if n == nil {
		return nil
	}
	return n.peer
}

// This node's shareable peer code — its libp2p peer ID (Ed25519 → embeds the public key). "" if offline.
//
//export VgPeerCode
func VgPeerCode(out **C.char) C.int {
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
	if n.peer != nil {
		n.peer.broadcastProfile() // push the new profile to accepted peers
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
	nick := p.Nick
	if nick == "" { // display default only: the stored value stays empty until the user explicitly sets a nick
		nick = defaultNick()
	}
	b, _ := json.Marshal(map[string]string{"nick": nick, "pic": p.PicCID})
	setStr(outJson, string(b))
	return 0
}

// JSON array of contacts: [{peer,nick,pic,state,online,seen,added,play,plabel,pident,psince,popen}].
// The play keys MUST match emitContact (peer.go) exactly — the UI re-reads this list on every event, so a field
// present in only one of the two is erased by the refresh the event triggers.
//
//export VgPeerList
func VgPeerList(outJson **C.char) C.int {
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

//export VgPeerAdd
func VgPeerAdd(peerID *C.char, note *C.char, errOut **C.char) C.int {
	f := peerSvc()
	if f == nil {
		setStr(errOut, "networking is offline")
		return -1
	}
	if err := f.addPeer(C.GoString(peerID), C.GoString(note)); err != nil {
		return fail(errOut, err)
	}
	return 0
}

//export VgPeerAccept
func VgPeerAccept(peerID *C.char, errOut **C.char) C.int {
	f := peerSvc()
	if f == nil {
		setStr(errOut, "networking is offline")
		return -1
	}
	if err := f.acceptPeer(C.GoString(peerID)); err != nil {
		return fail(errOut, err)
	}
	return 0
}

//export VgPeerDecline
func VgPeerDecline(peerID *C.char, errOut **C.char) C.int {
	f := peerSvc()
	if f == nil {
		setStr(errOut, "networking is offline")
		return -1
	}
	if err := f.declinePeer(C.GoString(peerID)); err != nil {
		return fail(errOut, err)
	}
	return 0
}

//export VgPeerBlock
func VgPeerBlock(peerID *C.char, errOut **C.char) C.int {
	f := peerSvc()
	if f == nil {
		setStr(errOut, "networking is offline")
		return -1
	}
	if err := f.blockPeer(C.GoString(peerID)); err != nil {
		return fail(errOut, err)
	}
	return 0
}

//export VgPeerRemove
func VgPeerRemove(peerID *C.char) C.int {
	n := get()
	if n == nil || n.social == nil {
		return -1
	}
	if n.peer != nil {
		n.peer.purgeShares(C.GoString(peerID)) // consent ends → stop serving them
	}
	if n.social.remove(C.GoString(peerID)) {
		return 0
	}
	return -1
}

// Actively probe whether a peer is reachable right now (returns 1 online, 0 offline, -1 n/a).
//
//export VgPeerPing
func VgPeerPing(peerID *C.char) C.int {
	f := peerSvc()
	if f == nil {
		return -1
	}
	if f.pingPresence(C.GoString(peerID)) {
		return 1
	}
	return 0
}

// ---- peer library sharing (bilateral, per-(peer,library)) ----

// VgShareLibrary: share library `lib` with one peer — the seeder's "share" toggle. itemsJson is a JSON array of
// shareItem objects ({cid, node, uid, title, tilecid, tilenode} — the node CID plus the metadata the receiver needs
// to route the block to its final library path). Recorded + pushed to the peer if online. Re-call to update.
//
//export VgShareLibrary
func VgShareLibrary(peerID *C.char, lib *C.char, itemsJson *C.char, errOut **C.char) C.int {
	f := peerSvc()
	if f == nil {
		setStr(errOut, "networking is offline")
		return -1
	}
	var items []shareItem
	if err := json.Unmarshal([]byte(C.GoString(itemsJson)), &items); err != nil {
		return fail(errOut, err)
	}
	f.setShareLib(C.GoString(peerID), C.GoString(lib), items)
	return 0
}

// VgUnshareLibrary: stop sharing library `lib` with a peer (and tell them it's withdrawn).
//
//export VgUnshareLibrary
func VgUnshareLibrary(peerID *C.char, lib *C.char, errOut **C.char) C.int {
	f := peerSvc()
	if f == nil {
		setStr(errOut, "networking is offline")
		return -1
	}
	f.removeShareLib(C.GoString(peerID), C.GoString(lib))
	return 0
}

// VgRequestPeerLibraries: ask a peer to send everything they currently share with us (leecher initial sync /
// refresh). Their replies arrive as evPeerLibrary events on the peer callback.
//
//export VgRequestPeerLibraries
func VgRequestPeerLibraries(peerID *C.char, errOut **C.char) C.int {
	f := peerSvc()
	if f == nil {
		setStr(errOut, "networking is offline")
		return -1
	}
	f.requestLibraries(C.GoString(peerID))
	return 0
}

// VgSetPresenceDeny replaces the set of peers we hide our online presence from (JSON string array of peer IDs). The
// Network tab's per-peer "Presence" toggle (off = denied); pushed at node-ready like the vLAN roster.
//
//export VgSetPresenceDeny
func VgSetPresenceDeny(jsonArr *C.char) C.int {
	f := peerSvc()
	if f == nil {
		return -1
	}
	var peers []string
	if err := json.Unmarshal([]byte(C.GoString(jsonArr)), &peers); err != nil {
		return -1
	}
	f.setPresenceDeny(peers)
	return 0
}

// The VidyaGod devices on our network, by name, for one-press pairing (nearby.go): JSON
// [{peer, nick, state}]. Never waits on the network (a device not asked yet shows on a later call), so the GUI
// may poll it. Empty while offline.
//
//export VgNearbyPeers
func VgNearbyPeers(outJson **C.char) C.int {
	n := get()
	if n == nil || n.nearby == nil {
		setStr(outJson, "[]")
		return 0
	}
	b, _ := json.Marshal(n.nearby.list())
	if string(b) == "null" {
		b = []byte("[]")
	}
	setStr(outJson, string(b))
	return 0
}
