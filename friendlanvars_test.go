package main

// friendlanvars_test.go — the launch variables derived from the friend LAN.
//
// The ordering test guards a bug that is invisible in any single run: PEER_VIPS/PEER_NAMES used to be built by
// ranging a Go map, so both lists came out in a different order every call. Anything positional ("the first
// friend", as the CLI overlay harness takes) therefore picked a DIFFERENT peer on each launch.

import (
	"strings"
	"testing"
)

func lanVarsFixture(t *testing.T, nicks map[string]string) (*socialState, []string) {
	t.Helper()
	s := newSocialState(t.TempDir())
	peers := []string{
		"12D3KooWAaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1",
		"12D3KooWBbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb2",
		"12D3KooWCcccccccccccccccccccccccccccccccccccccccc3",
		"12D3KooWDddddddddddddddddddddddddddddddddddddddd4",
		"12D3KooWEeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee5",
	}
	for _, p := range peers {
		nick := nicks[p]
		s.upsert(p, func(c *contact) { c.State = stAccepted; c.Nick = nick })
	}
	return s, peers
}

func TestLanLaunchVarsDeterministicOrder(t *testing.T) {
	s, peers := lanVarsFixture(t, nil)
	self := "12D3KooWSelfffffffffffffffffffffffffffffffffffff0"

	first, ok := lanLaunchVarsFrom(self, s, nil)
	if !ok {
		t.Fatal("lanLaunchVarsFrom failed")
	}
	for i := 0; i < 20; i++ {
		got, _ := lanLaunchVarsFrom(self, s, nil)
		if got["VIDYAGOD_PEER_VIPS"] != first["VIDYAGOD_PEER_VIPS"] ||
			got["VIDYAGOD_PEER_NAMES"] != first["VIDYAGOD_PEER_NAMES"] {
			t.Fatalf("order is not stable across calls:\n  %q / %q\n  %q / %q",
				first["VIDYAGOD_PEER_VIPS"], first["VIDYAGOD_PEER_NAMES"],
				got["VIDYAGOD_PEER_VIPS"], got["VIDYAGOD_PEER_NAMES"])
		}
	}

	vips := strings.Split(first["VIDYAGOD_PEER_VIPS"], ",")
	names := strings.Split(first["VIDYAGOD_PEER_NAMES"], ",")
	if len(vips) != len(peers) || len(names) != len(peers) {
		t.Fatalf("expected %d entries, got %d vips / %d names", len(peers), len(vips), len(names))
	}
	// The two lists must stay positionally parallel: name[i] must belong to the peer owning vip[i].
	for i, name := range names {
		var owner string
		for _, p := range peers {
			if shortPeer(p) == name {
				owner = p
			}
		}
		if owner == "" {
			t.Fatalf("name %q matches no peer", name)
		}
		if lanVIP(owner) != vips[i] {
			t.Fatalf("entry %d is mispaired: name %q owns %s but the vip column says %s",
				i, name, lanVIP(owner), vips[i])
		}
	}
}

func TestLanLaunchVarsNamelessPeersAreDistinct(t *testing.T) {
	// Every Ed25519 peer id starts with "12D3KooW", so a prefix-based fallback rendered every nickname-less friend
	// as the same string.
	s, _ := lanVarsFixture(t, nil)
	vars, _ := lanLaunchVarsFrom("12D3KooWSelf00000000000000000000000000000000000", s, nil)
	names := strings.Split(vars["VIDYAGOD_PEER_NAMES"], ",")
	seen := map[string]bool{}
	for _, n := range names {
		if seen[n] {
			t.Fatalf("two nickname-less friends render identically as %q", n)
		}
		seen[n] = true
	}
}

func TestLanLaunchVarsSelfName(t *testing.T) {
	s, _ := lanVarsFixture(t, nil)
	selfA := "12D3KooWSelfAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA1"
	selfB := "12D3KooWSelfBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB2"

	varsA, _ := lanLaunchVarsFrom(selfA, s, nil)
	varsB, _ := lanLaunchVarsFrom(selfB, s, nil)
	if varsA["VIDYAGOD_SELF_NAME"] == "" || varsB["VIDYAGOD_SELF_NAME"] == "" {
		t.Fatal("SELF_NAME must never be empty — a package writes it into a game config verbatim")
	}
	if varsA["VIDYAGOD_SELF_NAME"] == varsB["VIDYAGOD_SELF_NAME"] {
		t.Fatalf("two peers share the fallback name %q — they would collide in-game",
			varsA["VIDYAGOD_SELF_NAME"])
	}

	s.setProfile("Lorenzo", "")
	vars, _ := lanLaunchVarsFrom(selfA, s, nil)
	if vars["VIDYAGOD_SELF_NAME"] != "Lorenzo" {
		t.Fatalf("SELF_NAME should be the chosen nick, got %q", vars["VIDYAGOD_SELF_NAME"])
	}
}
