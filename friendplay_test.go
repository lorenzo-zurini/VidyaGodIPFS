package main

// friendplay_test.go — rich presence (what a friend is playing) and the "players you met" suggestion path.
//
// Two properties here are worth more than the rest put together, and both are the kind that pass by accident and
// fail silently in the field:
//   * "I stopped playing" must be OBSERVABLE. It only is because the play fields carry no omitempty; tidying that
//     tag away makes a peer who quit look in-game to their friends forever.
//   * A suggestion must NEVER become reachability. TestCoPlaySuggestsOnlyCoPresentStrangers asserts the routing
//     table is untouched — that is the guard on the whole no-transitive-routing decision.

import (
	"context"
	"testing"
	"time"

	host "github.com/libp2p/go-libp2p/core/host"
)

// befriend runs the real mutual-consent handshake between two live services and waits for both sides to settle.
func befriend(t *testing.T, fa, fb *friendService, ha, hb host.Host, sa, sb *socialState) {
	t.Helper()
	if err := fa.addFriend(hb.ID().String(), ""); err != nil {
		t.Fatalf("addFriend: %v", err)
	}
	waitFor(t, "incoming request lands", func() bool {
		c, ok := sb.get(ha.ID().String())
		return ok && c.State == stIncoming
	})
	if err := fb.acceptFriend(ha.ID().String()); err != nil {
		t.Fatalf("acceptFriend: %v", err)
	}
	waitFor(t, "both sides accepted", func() bool {
		ca, oka := sa.get(hb.ID().String())
		cb, okb := sb.get(ha.ID().String())
		return oka && okb && ca.State == stAccepted && cb.State == stAccepted
	})
}

func TestPresenceCarriesAndClearsPlayState(t *testing.T) {
	hA, hB := testHost(t), testHost(t)
	connectHosts(t, hA, hB)
	sA, sB := newSocialState(t.TempDir()), newSocialState(t.TempDir())
	ctx := context.Background()
	fA, fB := newFriendService(ctx, hA, nil, sA, nil), newFriendService(ctx, hB, nil, sB, nil)
	fA.start()
	fB.start()
	befriend(t, fA, fB, hA, hB, sA, sB)

	sA.setPlaying("wipeout_xl_mp", "Wipeout XL — Multiplayer", "v1:17260@abc")
	fA.broadcastPresence()
	waitFor(t, "bob sees what alice launched", func() bool {
		c, _ := sB.get(hA.ID().String())
		return c.play.NodeID == "wipeout_xl_mp" &&
			c.play.Label == "Wipeout XL — Multiplayer" &&
			c.play.Ident == "v1:17260@abc" &&
			c.play.Since > 0
	})

	sA.setOpenToJoin(true)
	fA.broadcastPresence()
	waitFor(t, "bob sees the advisory open flag", func() bool {
		c, _ := sB.get(hA.ID().String())
		return c.play.Open
	})

	// The assertion that pins the deliberate absence of omitempty on the play fields.
	sA.clearPlaying()
	fA.broadcastPresence()
	waitFor(t, "bob sees alice stop playing", func() bool {
		c, _ := sB.get(hA.ID().String())
		return c.play.NodeID == ""
	})
}

func TestInvisibleSuppressesPlayState(t *testing.T) {
	hA, hB := testHost(t), testHost(t)
	connectHosts(t, hA, hB)
	sA, sB := newSocialState(t.TempDir()), newSocialState(t.TempDir())
	ctx := context.Background()
	fA, fB := newFriendService(ctx, hA, nil, sA, nil), newFriendService(ctx, hB, nil, sB, nil)
	fA.start()
	fB.start()
	befriend(t, fA, fB, hA, hB, sA, sB)

	sA.setPlaying("some_game", "Some Game", "v1:1@x")
	sA.setInvisible(true)
	fA.broadcastPresence()

	// Reachability is still advertised — invisibility hides the GAME, not your existence.
	waitFor(t, "bob still sees alice online", func() bool {
		c, _ := sB.get(hA.ID().String())
		return c.online
	})
	if c, _ := sB.get(hA.ID().String()); c.play.NodeID != "" {
		t.Fatalf("invisible peer leaked a play block: %+v", c.play)
	}

	// Invisibility must survive a restart: it is persisted, and a nickname save must not quietly clear it.
	sA.setProfile("alice", "")
	if !sA.getInvisible() {
		t.Fatal("setProfile cleared Invisible — it overwrites the whole struct and must preserve it")
	}
	if reloaded := newSocialState(sA.path[:len(sA.path)-len("/social.json")]); !reloaded.getInvisible() {
		t.Fatal("Invisible did not survive a reload")
	}
}

func TestPlayStateClearedWhenPeerGoesOffline(t *testing.T) {
	s := newSocialState(t.TempDir())
	s.upsert("peerX", func(c *contact) { c.State = stAccepted })
	s.setPeerPlay("peerX", playState{NodeID: "g", Label: "G", Since: 1})
	if changed, c := s.setPresence("peerX", false); !changed || c.play.NodeID != "" {
		t.Fatalf("going offline must clear the play block, got changed=%v play=%+v", changed, c.play)
	}
}

func TestOldPeerPayloadFreePingClearsPlayState(t *testing.T) {
	// A peer on an older build sends {"t":"ping"} with no play fields. Decoding gives zero values, which must read
	// as "not playing" rather than leaving stale state behind.
	s := newSocialState(t.TempDir())
	s.upsert("peerX", func(c *contact) { c.State = stAccepted })
	s.setPeerPlay("peerX", playState{NodeID: "stale", Label: "Stale"})
	f := &friendService{social: s}
	if _, c := f.absorbPlay("peerX", friendMsg{Type: "ping"}); c.play.NodeID != "" {
		t.Fatalf("a payload-free ping must clear the play block, got %+v", c.play)
	}
}

// Three peers: A<->B and B<->C are friends, A and C are strangers. All three play the same node. B introduces them.
func TestCoPlaySuggestsOnlyCoPresentStrangers(t *testing.T) {
	hA, hB, hC := testHost(t), testHost(t), testHost(t)
	connectHosts(t, hA, hB)
	connectHosts(t, hB, hC)
	sA, sB, sC := newSocialState(t.TempDir()), newSocialState(t.TempDir()), newSocialState(t.TempDir())
	ctx := context.Background()
	fA := newFriendService(ctx, hA, nil, sA, nil)
	fB := newFriendService(ctx, hB, nil, sB, nil)
	fC := newFriendService(ctx, hC, nil, sC, nil)
	fA.start()
	fB.start()
	fC.start()
	befriend(t, fA, fB, hA, hB, sA, sB)
	befriend(t, fC, fB, hC, hB, sC, sB)
	sA.setProfile("alice", "")
	sC.setProfile("carol", "")

	const game = "aoe2_base_game"
	for _, x := range []struct {
		s *socialState
		f *friendService
	}{{sA, fA}, {sB, fB}, {sC, fC}} {
		x.s.setPlaying(game, "Age of Empires II", "v1:749@z")
		x.f.broadcastPresence()
	}
	waitFor(t, "B sees both friends in the same game", func() bool {
		ca, _ := sB.get(hA.ID().String())
		cc, _ := sB.get(hC.ID().String())
		return ca.play.NodeID == game && cc.play.NodeID == game && ca.online && cc.online
	})

	fB.shareCoPlay()

	waitFor(t, "alice is offered carol", func() bool {
		for _, s := range sA.listSuggestions() {
			if s.Peer == hC.ID().String() && s.Via == hB.ID().String() && s.Game == game {
				return true
			}
		}
		return false
	})
	waitFor(t, "carol is offered alice", func() bool {
		for _, s := range sC.listSuggestions() {
			if s.Peer == hA.ID().String() {
				return true
			}
		}
		return false
	})

	// THE GUARD: a suggestion is a prompt, never reachability. Carol must not be a contact, must not be in the
	// accepted set, and must have NO route in the overlay table.
	if _, known := sA.get(hC.ID().String()); known {
		t.Fatal("a suggestion became a contact — hearsay must never enter the address book")
	}
	for _, p := range sA.acceptedPeers() {
		if p == hC.ID().String() {
			t.Fatal("a suggested stranger reached acceptedPeers")
		}
	}
	_, _, routes, ok := lanConfigFrom(hA.ID().String(), sA, nil)
	if !ok {
		t.Fatal("lanConfigFrom failed")
	}
	if pid, routed := routes[lanVIP(hC.ID().String())]; routed {
		t.Fatalf("suggested stranger got an overlay route (%s) — the vLAN must stay non-transitive", pid)
	}

	// A received coplay must NOT cascade: alice, having been told about carol, tells nobody.
	before := len(sC.listSuggestions())
	fA.handleCoPlay(hB.ID().String(), friendMsg{Type: "coplay", Game: game,
		CoPlay: []coPlayer{{Peer: hC.ID().String(), Nick: "carol"}}})
	time.Sleep(200 * time.Millisecond)
	if after := len(sC.listSuggestions()); after != before {
		t.Fatalf("handling a coplay re-emitted one (%d → %d) — it must never cascade", before, after)
	}
}

func TestCoPlayGuards(t *testing.T) {
	self := testHost(t)
	s := newSocialState(t.TempDir())
	f := newFriendService(context.Background(), self, nil, s, nil)
	s.upsert("friendly", func(c *contact) { c.State = stAccepted })
	s.upsert("stranger", func(c *contact) { c.State = stIncoming })
	s.setPlaying("game_x", "X", "v1:1@x")

	f.handleCoPlay("stranger", friendMsg{Game: "game_x", CoPlay: []coPlayer{{Peer: "p1"}}})
	if len(s.listSuggestions()) != 0 {
		t.Fatal("a non-accepted sender must not be able to inject suggestions")
	}
	f.handleCoPlay("friendly", friendMsg{Game: "game_y", CoPlay: []coPlayer{{Peer: "p2"}}})
	if len(s.listSuggestions()) != 0 {
		t.Fatal("a roster scoped to a game we are NOT playing must be ignored")
	}
	s.setInvisible(true)
	f.handleCoPlay("friendly", friendMsg{Game: "game_x", CoPlay: []coPlayer{{Peer: "p3"}}})
	if len(s.listSuggestions()) != 0 {
		t.Fatal("an invisible peer must not collect suggestions")
	}
	s.setInvisible(false)
	f.handleCoPlay("friendly", friendMsg{Game: "game_x", CoPlay: []coPlayer{{Peer: "friendly"}, {Peer: "p4"}}})
	got := s.listSuggestions()
	if len(got) != 1 || got[0].Peer != "p4" {
		t.Fatalf("expected only the unknown stranger p4, got %+v", got)
	}
	// Suggestions are scoped to the shared game and expire with it.
	s.clearPlaying()
	if len(s.listSuggestions()) != 0 {
		t.Fatal("clearPlaying must drop suggestions")
	}
}
