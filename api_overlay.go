package main

// api_overlay.go — the C ABI for the friend-LAN overlay tunnel (overlay.go + friendlan.go + overlaytun_linux.go),
// mirrored in src/vgipfsapi.h. There is NO session/host: the vIP + routing table come from lanConfig() (the
// accepted-friends set, each peer's vIP a pure function of its peer ID). Brings up a TUN and forwards IP packets
// between friends over libp2p — with broadcast/multicast fanned out so LAN games discover each other.

/*
#include <stdlib.h>
*/
import "C"

import (
	"encoding/json"
	"fmt"
	"strings"
)

//export VgOverlayStart
// Bring up the friend-LAN overlay on the HOST netns (debug/CLI path): create a TUN, assign our vIP + the /16 route,
// and forward packets to/from friends over libp2p. Requires CAP_NET_ADMIN. Returns the TUN name through outName.
// (The production launch path uses VgOverlayServe — the TUN lives inside the game's sandbox netns, never the host.)
func VgOverlayStart(outName **C.char, errOut **C.char) C.int {
	n := get()
	if n == nil || n.overlay == nil {
		setStr(errOut, "networking is offline")
		return -1
	}
	myVIP, subnet, peerByVIP, ok := n.lanConfig()
	if !ok {
		setStr(errOut, "friend LAN unavailable (no social state)")
		return -1
	}
	mask := "16"
	if i := strings.LastIndexByte(subnet, '/'); i >= 0 {
		mask = subnet[i+1:]
	}
	tun, err := newTUN("vg-lan", overlayMTU)
	if err != nil {
		return fail(errOut, fmt.Errorf("create TUN: %w", err))
	}
	if err := tun.configureIP(myVIP + "/" + mask); err != nil {
		_ = tun.Close()
		return fail(errOut, fmt.Errorf("configure %s: %w", myVIP, err))
	}
	if err := n.overlay.configure(myVIP, subnet, peerByVIP); err != nil {
		_ = tun.Close()
		return fail(errOut, err)
	}
	n.overlay.attach(tun)
	setStr(outName, tun.name)
	return 0
}

//export VgOverlayServe
// Nested-sandbox overlay (the production path): configure the friend-LAN routes and listen on sockPath for the TUN fd
// the in-sandbox sandbox-init will send (it creates + addresses the TUN inside the game's OWN netns — the host stack is
// never touched). Non-blocking — returns once listening, so the caller can spawn bwrap. Forwarding attaches when the fd
// arrives.
func VgOverlayServe(sockPath *C.char, errOut **C.char) C.int {
	n := get()
	if n == nil || n.overlay == nil {
		setStr(errOut, "networking is offline")
		return -1
	}
	myVIP, subnet, peerByVIP, ok := n.lanConfig()
	if !ok {
		setStr(errOut, "friend LAN unavailable (no social state)")
		return -1
	}
	if err := n.overlay.configure(myVIP, subnet, peerByVIP); err != nil {
		return fail(errOut, err)
	}
	if err := n.overlay.serve(C.GoString(sockPath)); err != nil {
		return fail(errOut, err)
	}
	return 0
}

//export VgOverlayStop
// Tear the overlay down (removes the TUN + closes forwarding streams).
func VgOverlayStop() {
	n := get()
	if n != nil && n.overlay != nil {
		n.overlay.detach()
	}
}

//export VgOverlayActive
// 1 if the overlay is currently forwarding on an attached TUN, else 0.
func VgOverlayActive() C.int {
	n := get()
	if n == nil || n.overlay == nil {
		return 0
	}
	n.overlay.mu.Lock()
	defer n.overlay.mu.Unlock()
	if n.overlay.running {
		return 1
	}
	return 0
}

//export VgLanLaunchVars
// The custom variables a sandboxed game launch consumes to join the friend LAN + configure its LAN emulator
// (VIDYAGOD_SANDBOX / SANDBOX_NET=isolated / SELF_VIP / SUBNET / PEER_VIPS / PEER_NAMES). Returns a JSON object through
// outJson; -1 if the friend LAN is unavailable.
func VgLanLaunchVars(outJson **C.char) C.int {
	n := get()
	if n == nil {
		return -1
	}
	vars, ok := n.lanLaunchVars()
	if !ok {
		return -1
	}
	b, err := json.Marshal(vars)
	if err != nil {
		return -1
	}
	setStr(outJson, string(b))
	return 0
}
