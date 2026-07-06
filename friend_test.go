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
