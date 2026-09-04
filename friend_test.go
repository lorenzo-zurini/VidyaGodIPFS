package main

// friend_test.go — the friends layer exercised against TWO real libp2p hosts in-process (no DHT: the hosts are
// pre-connected, so the friendService's router path is skipped and NewStream rides the direct connection). Proves the
// mutual-consent handshake, profile exchange, and address-book persistence end-to-end over real streams.

import (
	"context"
	"testing"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	host "github.com/libp2p/go-libp2p/core/host"
	peer "github.com/libp2p/go-libp2p/core/peer"
)

func testHost(t *testing.T) host.Host {
	t.Helper()
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("libp2p host: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	return h
}

func connectHosts(t *testing.T, a, b host.Host) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := a.Connect(ctx, peer.AddrInfo{ID: b.ID(), Addrs: b.Addrs()}); err != nil {
		t.Fatalf("connect: %v", err)
	}
}

// waitFor polls cond up to 3s (streams settle asynchronously).
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}

func TestFriendHandshakeAndProfileExchange(t *testing.T) {
	hA, hB := testHost(t), testHost(t)
	connectHosts(t, hA, hB)

	sA := newSocialState(t.TempDir())
	sB := newSocialState(t.TempDir())
	sA.setProfile("alice", "QmAlicePic")
	sB.setProfile("bob", "QmBobPic")

	ctx := context.Background()
	fA := newFriendService(ctx, hA, nil, sA, nil)
	fB := newFriendService(ctx, hB, nil, sB, nil)
	fA.start()
	fB.start()

	// Alice sends a friend request to Bob.
	if err := fA.addFriend(hB.ID().String(), "hey bob"); err != nil {
		t.Fatalf("addFriend: %v", err)
	}
	// Bob should see an incoming request carrying Alice's profile.
	waitFor(t, "bob sees incoming request", func() bool {
		c, ok := sB.get(hA.ID().String())
		return ok && c.State == stIncoming && c.Nick == "alice" && c.PicCID == "QmAlicePic"
	})

	// Bob accepts.
	if err := fB.acceptFriend(hA.ID().String()); err != nil {
		t.Fatalf("acceptFriend: %v", err)
	}
	// Bob's side is accepted immediately; Alice's flips to accepted once the accept stream lands, with Bob's profile.
	waitFor(t, "alice sees acceptance + bob's profile", func() bool {
		c, ok := sA.get(hB.ID().String())
		return ok && c.State == stAccepted && c.Nick == "bob" && c.PicCID == "QmBobPic"
	})
	if c, _ := sB.get(hA.ID().String()); c.State != stAccepted {
		t.Fatalf("bob's contact for alice should be accepted, got %q", c.State)
	}

	// Profile update propagation: Alice renames and broadcasts.
	sA.setProfile("alice2", "QmAlicePic2")
	fA.broadcastProfile()
	waitFor(t, "bob sees alice's profile update", func() bool {
		c, _ := sB.get(hA.ID().String())
		return c.Nick == "alice2" && c.PicCID == "QmAlicePic2"
	})
}

// TestFriendMutualCrossingConverges models the field case where BOTH peers add each other but one side's initial
// request never lands (the second-to-start peer can't yet resolve the first, so its send fails) — leaving that side
// stuck in "pending". When the delivered request crosses the pending contact, the crosser must notify back so BOTH
// converge to accepted. Regression for the cross-network handshake hang seen 2026-09-04.
func TestFriendMutualCrossingConverges(t *testing.T) {
	hA, hB := testHost(t), testHost(t)
	connectHosts(t, hA, hB)

	sA := newSocialState(t.TempDir())
	sB := newSocialState(t.TempDir())
	ctx := context.Background()
	fA := newFriendService(ctx, hA, nil, sA, nil)
	fB := newFriendService(ctx, hB, nil, sB, nil)
	fA.start()
	fB.start()

	// Bob "tried to add Alice first" but his send failed before they were connected — model that as a local pending
	// contact with no message ever delivered to Alice.
	sB.upsert(hA.ID().String(), func(c *contact) { c.State = stPending })

	// Alice now adds Bob for real (her request reaches him). It crosses Bob's pending contact → Bob accepted.
	if err := fA.addFriend(hB.ID().String(), "hi"); err != nil {
		t.Fatalf("addFriend: %v", err)
	}
	// Both sides must end accepted: Bob by crossing, Alice by the accept-back the crossing now sends.
	waitFor(t, "bob accepted by crossing", func() bool {
		c, ok := sB.get(hA.ID().String())
		return ok && c.State == stAccepted
	})
	waitFor(t, "alice converges to accepted (was left pending before the fix)", func() bool {
		c, ok := sA.get(hB.ID().String())
		return ok && c.State == stAccepted
	})
}

func TestFriendBlockedPeerIgnored(t *testing.T) {
	hA, hB := testHost(t), testHost(t)
	connectHosts(t, hA, hB)

	sA := newSocialState(t.TempDir())
	sB := newSocialState(t.TempDir())
	ctx := context.Background()
	fA := newFriendService(ctx, hA, nil, sA, nil)
	fB := newFriendService(ctx, hB, nil, sB, nil)
	fA.start()
	fB.start()

	// Bob blocks Alice before she reaches out.
	if err := fB.blockFriend(hA.ID().String()); err != nil {
		t.Fatalf("block: %v", err)
	}
	_ = fA.addFriend(hB.ID().String(), "")
	// Give the stream a moment; Bob must NOT record an incoming request from a blocked peer.
	time.Sleep(300 * time.Millisecond)
	if c, _ := sB.get(hA.ID().String()); c.State != stBlocked {
		t.Fatalf("blocked peer's request should be dropped, state=%q", c.State)
	}
}

func TestSocialPersistenceRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s := newSocialState(dir)
	s.setProfile("me", "QmMyPic")
	s.upsert("12D3KooWFriend", func(c *contact) { c.State = stAccepted; c.Nick = "pal"; c.PicCID = "QmPal" })

	// Reload from disk into a fresh instance.
	s2 := newSocialState(dir)
	if p := s2.getProfile(); p.Nick != "me" || p.PicCID != "QmMyPic" {
		t.Fatalf("profile not persisted: %+v", p)
	}
	c, ok := s2.get("12D3KooWFriend")
	if !ok || c.State != stAccepted || c.Nick != "pal" || c.PicCID != "QmPal" {
		t.Fatalf("contact not persisted: %+v ok=%v", c, ok)
	}
}

// Presence is online/offline LIVENESS only (no play-state anymore): toggling it reports a change once per
// transition, updates the contact snapshot, and is transient (never persisted).
func TestPresenceLivenessToggles(t *testing.T) {
	dir := t.TempDir()
	s := newSocialState(dir)
	s.upsert("12D3KooWFriend", func(c *contact) { c.State = stAccepted })

	if changed, c := s.setPresence("12D3KooWFriend", true); !changed || !c.online {
		t.Fatalf("going online should change + report online: changed=%v online=%v", changed, c.online)
	}
	if changed, _ := s.setPresence("12D3KooWFriend", true); changed {
		t.Fatal("staying online should not report a change")
	}
	if changed, c := s.setPresence("12D3KooWFriend", false); !changed || c.online {
		t.Fatalf("going offline should change + report offline: changed=%v online=%v", changed, c.online)
	}
	if changed, _ := s.setPresence("nobody", true); changed {
		t.Fatal("presence for an unknown peer must be a no-op")
	}
	// online is transient: a reload never resurrects it.
	if c, _ := newSocialState(dir).get("12D3KooWFriend"); c.online {
		t.Fatal("online state must not persist across reload")
	}
}
