package main

// friendlan.go — the host-less virtual LAN of friends. There is NO session/lobby and NO authoritative host: friends
// are simply on one shared virtual LAN (10.66.0.0/16), and each peer's vIP is a PURE deterministic function of its
// peer ID — so everyone computes the same vIP for everyone from the peer ID alone, with zero coordination. Adding a
// friend = you both already know each other's vIP, instantly on the LAN ("seamlessly extendable"). The LAN membership
// is just the accepted-friends set (socialState); the routing table for the overlay datapath (overlay.go) and the
// game-launch vars are derived from it here. The LAN exists ONLY inside a game's bubblewrap netns (sandbox-only) — see
// the isolated-netns TUN in sandboxinit.go/overlayserve.

import (
	"crypto/sha256"
	"fmt"
	"sort"
	"strings"
)

// The single shared /16 all friends share. A /16 keeps collisions negligible at friend scale (65k hosts) and is one
// broadcast domain (directed broadcast 10.66.255.255), so LAN-game discovery reaches every peer via overlay fan-out.
const lanSubnet = "10.66.0.0/16"
const lanBroadcast = "10.66.255.255"

// lanVIP derives a peer's stable virtual LAN IP purely from its peer ID: 10.66.<h0>.<h1> from sha256(peerID). Both
// sides compute the same value with no exchange. Only the /16 network (10.66.0.0) and broadcast (10.66.255.255) are
// reserved, so nudge those two off the edges; every other 16-bit value is a valid host.
func lanVIP(peerID string) string {
	h := sha256.Sum256([]byte(peerID))
	hi, lo := h[0], h[1]
	if hi == 0 && lo == 0 {
		lo = 1
	}
	if hi == 255 && lo == 255 {
		lo = 254
	}
	return fmt.Sprintf("10.66.%d.%d", hi, lo)
}

// lanConfigFrom builds the overlay parameters for the friend LAN: our own vIP, the /16 subnet, and the vIP→peer route
// table for EVERY accepted friend — deliberately including currently-offline ones: routing to an offline peer just
// drops packets, while the always-on link maintainer (overlaylink.go) brings them live the moment they appear, so a
// friend can join MID-GAME instead of "next launch". excluded (peer-id set, may be nil) is the launch-window
// roster's un-ticked members. ok is false only if there is no social state.
func lanConfigFrom(self string, social *socialState, excluded map[string]bool) (myVIP, subnet string, peerByVIP map[string]string, ok bool) {
	if social == nil {
		return "", "", nil, false
	}
	peerByVIP = map[string]string{}
	for _, c := range social.list() {
		if c.State != stAccepted || c.PeerID == self || excluded[c.PeerID] {
			continue
		}
		peerByVIP[lanVIP(c.PeerID)] = c.PeerID
	}
	return lanVIP(self), lanSubnet, peerByVIP, true
}

// lanLaunchVarsFrom maps the friend LAN into the custom variables a sandboxed game launch consumes to join the overlay
// and configure its LAN emulator (Goldberg et al.): the sandbox toggle, ISOLATED net (the LAN is sandbox-only, never
// the host stack), our vIP + the /16 subnet, and the parallel comma-lists of peer vIPs + nicknames.
func lanLaunchVarsFrom(self string, social *socialState, excluded map[string]bool) (map[string]string, bool) {
	myVIP, subnet, peerByVIP, ok := lanConfigFrom(self, social, excluded)
	if !ok {
		return nil, false
	}
	nickByPeer := map[string]string{}
	for _, c := range social.list() {
		if c.Nick != "" {
			nickByPeer[c.PeerID] = c.Nick
		}
	}
	// Order by peer id, not by map iteration. Ranging peerByVIP directly made both lists come out in a DIFFERENT
	// order on every call — so anything positional ("the first friend", as cli/cliipfs.cpp takes) silently picked a
	// different peer each run. Sorting once here also keeps the two lists parallel by construction.
	pids := make([]string, 0, len(peerByVIP))
	vipByPeer := make(map[string]string, len(peerByVIP))
	for vip, pid := range peerByVIP {
		pids = append(pids, pid)
		vipByPeer[pid] = vip
	}
	sort.Strings(pids)
	vips := make([]string, 0, len(pids))
	names := make([]string, 0, len(pids))
	for _, pid := range pids {
		vips = append(vips, vipByPeer[pid])
		n := nickByPeer[pid]
		if n == "" {
			// NOT pid[:8]: every Ed25519 peer id starts with the constant "12D3KooW", so that rendered every
			// nickname-less friend as the same string. shortPeer takes the distinguishing tail.
			n = shortPeer(pid)
		}
		names = append(names, n)
	}
	// The local player's own display name, so a package can write it into a game's config and the in-game name is
	// really theirs. Falls back to something stable and distinct per peer rather than a shared placeholder.
	selfName := social.getProfile().Nick
	if selfName == "" {
		selfName = "Player-" + shortPeer(self)
	}
	return map[string]string{
		"VIDYAGOD_SANDBOX":     "on",
		"VIDYAGOD_SANDBOX_NET": "isolated", // the LAN lives ONLY in the game's netns — the host stack is never touched
		"VIDYAGOD_SELF_VIP":    myVIP,
		"VIDYAGOD_SELF_NAME":   selfName,
		"VIDYAGOD_SUBNET":      subnet,
		"VIDYAGOD_PEER_VIPS":   strings.Join(vips, ","),
		"VIDYAGOD_PEER_NAMES":  strings.Join(names, ","),
	}, true
}

// node convenience wrappers (self = our peer ID).
func (n *node) lanConfig() (string, string, map[string]string, bool) {
	if n.social == nil || n.host == nil {
		return "", "", nil, false
	}
	return lanConfigFrom(n.host.ID().String(), n.social, n.lanExcludedSnapshot())
}

func (n *node) lanLaunchVars() (map[string]string, bool) {
	if n.social == nil || n.host == nil {
		return nil, false
	}
	return lanLaunchVarsFrom(n.host.ID().String(), n.social, n.lanExcludedSnapshot())
}

// lanExcludedSnapshot copies the roster's excluded set (peer-id strings) under the lock.
func (n *node) lanExcludedSnapshot() map[string]bool {
	n.lanExclMu.Lock()
	defer n.lanExclMu.Unlock()
	out := make(map[string]bool, len(n.lanExcluded))
	for k := range n.lanExcluded {
		out[k] = true
	}
	return out
}
