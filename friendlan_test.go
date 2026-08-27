package main

import (
	"strings"
	"testing"
)

func TestLanVIPDeterministicAndScoped(t *testing.T) {
	const p = "12D3KooWExamplePeerIDForVIPTest"
	a, b := lanVIP(p), lanVIP(p)
	if a != b {
		t.Fatalf("vip not deterministic: %s != %s", a, b)
	}
	if !strings.HasPrefix(a, "10.66.") {
		t.Fatalf("vip not in %s: %s", lanSubnet, a)
	}
	if a == lanBroadcast || a == "10.66.0.0" {
		t.Fatalf("vip landed on a reserved edge: %s", a)
	}
	// Distinct peers get distinct vIPs (no coordination needed — both sides derive the same map).
	if lanVIP("peerAlice") == lanVIP("peerBob") {
		t.Fatalf("distinct peers collided")
	}
}

func TestLanConfigFromFriends(t *testing.T) {
	s := &socialState{contacts: map[string]*contact{
		"me":        {PeerID: "me", State: stAccepted, online: true}, // self — excluded
		"friendOn":  {PeerID: "friendOn", State: stAccepted, online: true, Nick: "Al"},
		"friendOff": {PeerID: "friendOff", State: stAccepted, online: false}, // offline — excluded
		"pending":   {PeerID: "pending", State: stPending, online: true},     // not accepted — excluded
		"blocked":   {PeerID: "blocked", State: stBlocked, online: true},     // blocked — excluded
	}}

	myVIP, subnet, routes, ok := lanConfigFrom("me", s, nil)
	if !ok {
		t.Fatal("lanConfigFrom not ok")
	}
	if myVIP != lanVIP("me") {
		t.Fatalf("myVIP = %s, want %s", myVIP, lanVIP("me"))
	}
	if subnet != lanSubnet {
		t.Fatalf("subnet = %s, want %s", subnet, lanSubnet)
	}
	// v2: ALL accepted friends are routed — including offline ones (packets to them just drop; the always-on link
	// maintainer brings them live so a friend can join MID-GAME instead of "next launch").
	if len(routes) != 2 {
		t.Fatalf("want 2 routes (every accepted friend, online or not), got %d: %v", len(routes), routes)
	}
	if routes[lanVIP("friendOn")] != "friendOn" || routes[lanVIP("friendOff")] != "friendOff" {
		t.Fatalf("routes missing/wrong: %v", routes)
	}

	vars, ok := lanLaunchVarsFrom("me", s, nil)
	if !ok {
		t.Fatal("lanLaunchVarsFrom not ok")
	}
	if vars["VIDYAGOD_SANDBOX_NET"] != "isolated" {
		t.Fatalf("LAN must be sandbox-only (isolated), got %q", vars["VIDYAGOD_SANDBOX_NET"])
	}
	if vars["VIDYAGOD_SELF_VIP"] != lanVIP("me") || vars["VIDYAGOD_SUBNET"] != lanSubnet {
		t.Fatalf("bad self/subnet vars: %v", vars)
	}
	// v2: both accepted friends appear (offline included) — the LAN emulator's peer list matches the route table.
	gotVips := strings.Split(vars["VIDYAGOD_PEER_VIPS"], ",")
	gotNames := strings.Split(vars["VIDYAGOD_PEER_NAMES"], ",")
	if len(gotVips) != 2 || len(gotNames) != 2 {
		t.Fatalf("bad peer vars: vips=%q names=%q", vars["VIDYAGOD_PEER_VIPS"], vars["VIDYAGOD_PEER_NAMES"])
	}
	found := map[string]string{}
	for i := range gotVips {
		found[gotVips[i]] = gotNames[i]
	}
	if found[lanVIP("friendOn")] != "Al" {
		t.Fatalf("friendOn missing/wrong nick: %v", found)
	}
	if _, ok := found[lanVIP("friendOff")]; !ok {
		t.Fatalf("offline accepted friend missing from peer vars: %v", found)
	}

	// The roster's excluded set drops a friend from routes AND launch vars alike.
	_, _, exRoutes, _ := lanConfigFrom("me", s, map[string]bool{"friendOff": true})
	if len(exRoutes) != 1 || exRoutes[lanVIP("friendOn")] != "friendOn" {
		t.Fatalf("excluded set not applied: %v", exRoutes)
	}
}
