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

	myVIP, subnet, routes, ok := lanConfigFrom("me", s)
	if !ok {
		t.Fatal("lanConfigFrom not ok")
	}
	if myVIP != lanVIP("me") {
		t.Fatalf("myVIP = %s, want %s", myVIP, lanVIP("me"))
	}
	if subnet != lanSubnet {
		t.Fatalf("subnet = %s, want %s", subnet, lanSubnet)
	}
	if len(routes) != 1 {
		t.Fatalf("want 1 route (the online accepted friend), got %d: %v", len(routes), routes)
	}
	if routes[lanVIP("friendOn")] != "friendOn" {
		t.Fatalf("route missing/wrong for friendOn: %v", routes)
	}

	vars, ok := lanLaunchVarsFrom("me", s)
	if !ok {
		t.Fatal("lanLaunchVarsFrom not ok")
	}
	if vars["VIDYAGOD_SANDBOX_NET"] != "isolated" {
		t.Fatalf("LAN must be sandbox-only (isolated), got %q", vars["VIDYAGOD_SANDBOX_NET"])
	}
	if vars["VIDYAGOD_SELF_VIP"] != lanVIP("me") || vars["VIDYAGOD_SUBNET"] != lanSubnet {
		t.Fatalf("bad self/subnet vars: %v", vars)
	}
	if vars["VIDYAGOD_PEER_VIPS"] != lanVIP("friendOn") || vars["VIDYAGOD_PEER_NAMES"] != "Al" {
		t.Fatalf("bad peer vars: vips=%q names=%q", vars["VIDYAGOD_PEER_VIPS"], vars["VIDYAGOD_PEER_NAMES"])
	}
}
