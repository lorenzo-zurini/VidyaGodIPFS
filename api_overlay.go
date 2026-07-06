package main

// api_overlay.go — the C ABI for the overlay tunnel (overlay.go + overlaytun_linux.go), mirrored in src/vgipfsapi.h.
// Brings up a TUN configured from a session's roster and starts forwarding IP packets between members over libp2p.

/*
#include <stdlib.h>
*/
import "C"

import (
	"fmt"
	"strings"
)

//export VgOverlayStart
// Bring up the overlay for a session: create a TUN named vg-<sid6>, assign our vIP + the /24 route, and forward
// packets to/from the other members over libp2p. Requires CAP_NET_ADMIN in the current netns. Returns the TUN
// interface name through outName. Idempotent-ish: starting again re-attaches a fresh TUN.
func VgOverlayStart(sid *C.char, outName **C.char, errOut **C.char) C.int {
	n := get()
	if n == nil || n.overlay == nil || n.session == nil {
		setStr(errOut, "networking is offline")
		return -1
	}
	s := C.GoString(sid)
	myVIP, subnet, peerByVIP, ok := n.session.overlayConfig(s)
	if !ok {
		setStr(errOut, "not a member of session (or no vIP assigned yet)")
		return -1
	}
	mask := "24"
	if i := strings.LastIndexByte(subnet, '/'); i >= 0 {
		mask = subnet[i+1:]
	}
	name := "vg-" + s
	if len(name) > 15 {
		name = name[:15]
	}
	tun, err := newTUN(name, overlayMTU)
	if err != nil {
		return fail(errOut, fmt.Errorf("create TUN: %w", err))
	}
	if err := tun.configureIP(myVIP + "/" + mask); err != nil {
		_ = tun.Close()
		return fail(errOut, fmt.Errorf("configure %s: %w", myVIP, err))
	}
	if err := n.overlay.configure(myVIP, peerByVIP); err != nil {
		_ = tun.Close()
		return fail(errOut, err)
	}
	n.overlay.attach(tun)
	setStr(outName, tun.name)
	return 0
}

//export VgOverlayServe
// Nested-sandbox overlay: configure this session's routes and listen on sockPath for the TUN fd that the in-sandbox
// sandbox-init will send (it creates + addresses the TUN inside the sandbox's netns). Non-blocking — returns once
// listening, so the caller can then spawn bwrap. When the fd arrives, forwarding attaches automatically.
func VgOverlayServe(sid *C.char, sockPath *C.char, errOut **C.char) C.int {
	n := get()
	if n == nil || n.overlay == nil || n.session == nil {
		setStr(errOut, "networking is offline")
		return -1
	}
	s := C.GoString(sid)
	myVIP, _, peerByVIP, ok := n.session.overlayConfig(s)
	if !ok {
		setStr(errOut, "not a member of session (or no vIP assigned yet)")
		return -1
	}
	if err := n.overlay.configure(myVIP, peerByVIP); err != nil {
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
