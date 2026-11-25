package main

// peer_test.go — the peers layer exercised against TWO real libp2p hosts in-process (no DHT: the hosts are
// pre-connected, so the peerService's router path is skipped and NewStream rides the direct connection). Proves the
// mutual-consent handshake, profile exchange, and address-book persistence end-to-end over real streams.

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

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

func TestPeerHandshakeAndProfileExchange(t *testing.T) {
	hA, hB := testHost(t), testHost(t)
	connectHosts(t, hA, hB)

	sA := newSocialState(t.TempDir())
	sB := newSocialState(t.TempDir())
	sA.setProfile("alice", "QmAlicePic")
	sB.setProfile("bob", "QmBobPic")

	ctx := context.Background()
	fA := newPeerService(ctx, hA, nil, sA, nil)
	fB := newPeerService(ctx, hB, nil, sB, nil)
	fA.start()
	fB.start()

	// Alice sends a peer request to Bob.
	if err := fA.addPeer(hB.ID().String(), "hey bob"); err != nil {
		t.Fatalf("addPeer: %v", err)
	}
	// Bob should see an incoming request carrying Alice's profile.
	waitFor(t, "bob sees incoming request", func() bool {
		c, ok := sB.get(hA.ID().String())
		return ok && c.State == stIncoming && c.Nick == "alice" && c.PicCID == "QmAlicePic"
	})

	// Bob accepts.
	if err := fB.acceptPeer(hA.ID().String()); err != nil {
		t.Fatalf("acceptPeer: %v", err)
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

// TestPeerLibraryExchange: once two peers are peers, a seeder's shared library (shareItem entries) reaches the
// leecher — pushed on setShareLib, cleared on removeShareLib, and served on an explicit request. The bilateral
// consumer/UI gates live in C++; this proves the wire exchange itself.
func TestPeerLibraryExchange(t *testing.T) {
	hA, hB := testHost(t), testHost(t)
	connectHosts(t, hA, hB)
	sA := newSocialState(t.TempDir())
	sB := newSocialState(t.TempDir())
	ctx := context.Background()

	var mu sync.Mutex
	cur := map[string][]shareItem(nil) // Bob's CURRENT received snapshot (replaced wholesale on each event)
	var curSeq uint64                  // highest stamp applied — models C++'s last-writer-wins (the seq authority lives there)
	events := 0
	emitB := func(kind int, payload string) {
		if kind != evPeerLibrary {
			return
		}
		var m struct {
			Peer string
			Libs map[string][]shareItem
			Seq  uint64
		}
		_ = json.Unmarshal([]byte(payload), &m)
		mu.Lock()
		// Snapshots ride independent, concurrently-handled streams, so a stale one can arrive after a fresher one. The
		// receiver (C++ in production) keeps only the highest stamp; a Go-side receive gate can't (the emit is off-lock).
		if m.Seq == 0 || m.Seq > curSeq {
			cur = m.Libs
			curSeq = m.Seq
			events++
		}
		mu.Unlock()
	}
	fA := newPeerService(ctx, hA, nil, sA, nil)
	fB := newPeerService(ctx, hB, nil, sB, emitB)
	fA.start()
	fB.start()

	// BILATERAL GATE: before acceptance, a request must yield NOTHING (fA won't serve a non-accepted peer).
	fB.requestLibraries(hA.ID().String())
	time.Sleep(200 * time.Millisecond)
	mu.Lock()
	if events != 0 {
		mu.Unlock()
		t.Fatalf("received a library before pairing was accepted — bilateral gate breached")
	}
	mu.Unlock()

	if err := fA.addPeer(hB.ID().String(), ""); err != nil {
		t.Fatalf("addPeer: %v", err)
	}
	waitFor(t, "bob sees incoming", func() bool { c, ok := sB.get(hA.ID().String()); return ok && c.State == stIncoming })
	if err := fB.acceptPeer(hA.ID().String()); err != nil {
		t.Fatalf("acceptPeer: %v", err)
	}
	waitFor(t, "alice accepted", func() bool { c, ok := sA.get(hB.ID().String()); return ok && c.State == stAccepted })

	// Share → full snapshot pushed to Bob.
	fA.setShareLib(hB.ID().String(), "Games", []shareItem{
		{Cid: "cidX", Node: "x_exec", Pkg: "[1] Game X", Uid: "1", Title: "Game X", TileCid: "cidXt", TileNode: "x_tile"},
		{Cid: "cidY"},
	})
	waitFor(t, "bob receives shared snapshot", func() bool {
		mu.Lock()
		defer mu.Unlock()
		// The routing metadata must survive the wire VERBATIM — the receiver derives on-disk paths from it.
		return len(cur["Games"]) == 2 && cur["Games"][0] == shareItem{
			Cid: "cidX", Node: "x_exec", Pkg: "[1] Game X", Uid: "1", Title: "Game X", TileCid: "cidXt", TileNode: "x_tile"}
	})

	// Add a second library, then WITHDRAW the first → snapshot replaces wholesale, "Games" is gone (no lost withdraw).
	fA.setShareLib(hB.ID().String(), "Retro", []shareItem{{Cid: "cidZ"}})
	fA.removeShareLib(hB.ID().String(), "Games")
	waitFor(t, "snapshot reflects Retro-only after withdraw", func() bool {
		mu.Lock()
		defer mu.Unlock()
		_, hasGames := cur["Games"]
		return !hasGames && len(cur["Retro"]) == 1 && cur["Retro"][0].Cid == "cidZ"
	})

	// Request path returns the authoritative snapshot on demand.
	fB.requestLibraries(hA.ID().String())
	waitFor(t, "request yields the current snapshot", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(cur) == 1 && len(cur["Retro"]) == 1
	})

	// WITHDRAW THE LAST LIBRARY → an EMPTY snapshot must reach the wire as an explicit {} (not a dropped/omitted map),
	// so the receiver drops the peer entirely. The flagship path: "share nothing" has to be transmissible.
	fA.removeShareLib(hB.ID().String(), "Retro")
	waitFor(t, "empty snapshot clears everything", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return cur != nil && len(cur) == 0
	})
}

// TestInboundIdentityBounds: the nick is attacker-controlled and becomes a receiver-side DIRECTORY segment, so an
// over-long or non-UTF-8 nick must be bounded/dropped AT DISPATCH, before any branch stores it; an over-long PicCID
// is dropped. Teeth: remove the capUtf8/PicCID gate in dispatch and these fail.
func TestInboundIdentityBounds(t *testing.T) {
	s := newSocialState(t.TempDir())
	f := newPeerService(context.Background(), nil, nil, s, nil)
	const peer = "12D3KooWBounds"
	f.dispatch(peer, peerMsg{Type: "request", Nick: strings.Repeat("x", 10000), PicCID: strings.Repeat("y", 500)})
	c, ok := s.get(peer)
	if !ok {
		t.Fatal("request not recorded")
	}
	if len(c.Nick) != maxNickLen {
		t.Fatalf("nick not capped: %d bytes", len(c.Nick))
	}
	if c.PicCID != "" {
		t.Fatalf("oversized PicCID stored: %d bytes", len(c.PicCID))
	}
	// Non-UTF-8 nick degrades to EMPTY (downstream falls back to the peer-id suffix), never to invalid bytes.
	f.dispatch(peer, peerMsg{Type: "profile", Nick: "ok\xff\xfe"})
	c, _ = s.get(peer)
	if c.Nick != "" {
		t.Fatalf("non-UTF-8 nick stored: %q", c.Nick)
	}
	// A capped multi-byte nick is trimmed to a VALID boundary, not split mid-rune.
	if got := capUtf8(strings.Repeat("é", 40), maxNickLen); len(got) != 64 || !utf8.ValidString(got) {
		t.Fatalf("capUtf8 split a rune: %d bytes valid=%v", len(got), utf8.ValidString(got))
	}
}

// TestPeerPushSeederGate proves the seeder half of the bilateral gate at the SEND side (not just the receiver's
// inbound check): pushSnapshot serves an ACCEPTED peer only. A pending/blocked peer is never pushed to, and marking a
// share for a non-accepted peer schedules nothing — so blockPeer's purge can't be undone by a stray outbound push.
func TestPeerPushSeederGate(t *testing.T) {
	s := newSocialState(t.TempDir())
	f := newPeerService(context.Background(), nil, nil, s, nil)
	const peer = "12D3KooWGate"

	// Pending peer: pushSnapshot must schedule no work (pushPending stays clear — nothing to send).
	s.upsert(peer, func(c *contact) { c.State = stPending })
	f.pushSnapshot(peer)
	f.setShareLib(peer, "Games", []shareItem{{Cid: "cidX"}}) // records intent, but must not push to a non-accepted peer
	time.Sleep(50 * time.Millisecond)
	f.shareMu.Lock()
	pendingPending := f.pushPending[peer]
	f.shareMu.Unlock()
	if pendingPending {
		t.Fatal("a non-accepted peer must never have a push scheduled")
	}

	// Blocked peer: same — no push scheduled.
	s.upsert(peer, func(c *contact) { c.State = stBlocked })
	f.pushSnapshot(peer)
	time.Sleep(50 * time.Millisecond)
	f.shareMu.Lock()
	blockedPending := f.pushPending[peer]
	f.shareMu.Unlock()
	if blockedPending {
		t.Fatal("a blocked peer must never have a push scheduled")
	}
}

// TestPeerMutualCrossingConverges models the field case where BOTH peers add each other but one side's initial
// request never lands (the second-to-start peer can't yet resolve the first, so its send fails) — leaving that side
// stuck in "pending". When the delivered request crosses the pending contact, the crosser must notify back so BOTH
// converge to accepted. Regression for the cross-network handshake hang seen 2026-09-04.
func TestPeerMutualCrossingConverges(t *testing.T) {
	hA, hB := testHost(t), testHost(t)
	connectHosts(t, hA, hB)

	sA := newSocialState(t.TempDir())
	sB := newSocialState(t.TempDir())
	ctx := context.Background()
	fA := newPeerService(ctx, hA, nil, sA, nil)
	fB := newPeerService(ctx, hB, nil, sB, nil)
	fA.start()
	fB.start()

	// Bob "tried to add Alice first" but his send failed before they were connected — model that as a local pending
	// contact with no message ever delivered to Alice.
	sB.upsert(hA.ID().String(), func(c *contact) { c.State = stPending })

	// Alice now adds Bob for real (her request reaches him). It crosses Bob's pending contact → Bob accepted.
	if err := fA.addPeer(hB.ID().String(), "hi"); err != nil {
		t.Fatalf("addPeer: %v", err)
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

// addPeer returns INSTANTLY (never blocks on the send — the freeze bug) and records the contact as pending
// immediately, and a request that could NOT be delivered at add time (peer unreachable then) still lands once the
// peer is reachable, via the request-retry loop folded into the presence tick. Teeth: make addPeer block on send
// again → the "returned promptly" assert fails; drop the retry loop → the "lands after becoming reachable" fails.
func TestPeerRequestIsAsyncAndRetries(t *testing.T) {
	hA, hB := testHost(t), testHost(t) // deliberately NOT connected yet — Alice's first send cannot reach Bob
	sA := newSocialState(t.TempDir())
	sB := newSocialState(t.TempDir())
	ctx := context.Background()
	fA := newPeerService(ctx, hA, nil, sA, nil)
	fB := newPeerService(ctx, hB, nil, sB, nil)
	fA.start()
	fB.start()
	fA.startPresence(200 * time.Millisecond) // fast tick so the retry fires quickly in-test

	// (1) INSTANT + OPTIMISTIC: addPeer returns promptly and the contact is pending at once, though the send to an
	// unreachable peer is still in flight/failing in the background.
	start := time.Now()
	if err := fA.addPeer(hB.ID().String(), "hi"); err != nil {
		t.Fatalf("addPeer: %v", err)
	}
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Fatalf("addPeer blocked for %v — it must return without waiting on the send", d)
	}
	if c, ok := sA.get(hB.ID().String()); !ok || c.State != stPending {
		t.Fatal("contact must be recorded as pending immediately, independent of handshake")
	}

	_ = hB // reachability/retry is exercised by TestPeerRequestRetryDeliversPersistedPending below
}

// Our own request is announced as SENT, never as an incoming request: an app that auto-accepts incoming requests
// accepted its own outgoing one — the contact flipped to accepted, the retry loop (pending only) stopped, and a
// request whose first send missed (a new peer not yet findable) never reached the peer at all (laptop replication:
// the seeder "accepted" a peer who never heard of it). Teeth: emit evPeerRequest from addPeer.
func TestOurOwnRequestIsNotAnIncomingOne(t *testing.T) {
	var mu sync.Mutex
	var kinds []int
	f := newPeerService(context.Background(), testHost(t), nil, newSocialState(t.TempDir()), func(k int, _ string) {
		mu.Lock()
		kinds = append(kinds, k)
		mu.Unlock()
	})
	if err := f.addPeer(testHost(t).ID().String(), ""); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(kinds) != 1 || kinds[0] != evPeerSent {
		t.Fatalf("addPeer emitted %v, want only evPeerSent (%d) — never evPeerRequest (%d)", kinds, evPeerSent, evPeerRequest)
	}
}

// The request-retry loop delivers a PERSISTED pending request with NO addPeer call this session — the app-restart
// case (a pending contact loaded from social.json, or a peer that was offline past the one-shot send's window). ONLY
// the retry loop can deliver here: nothing fires a send at start. Teeth: drop the pendingPeers re-send in the
// presence tick and Bob never sees the request → this fails.
func TestPeerRequestRetryDeliversPersistedPending(t *testing.T) {
	hA, hB := testHost(t), testHost(t)
	connectHosts(t, hA, hB) // reachable, but no send has EVER been fired for this contact
	sA := newSocialState(t.TempDir())
	sB := newSocialState(t.TempDir())
	sA.upsert(hB.ID().String(), func(c *contact) { c.State = stPending }) // as if loaded from disk
	ctx := context.Background()
	fA := newPeerService(ctx, hA, nil, sA, nil)
	fB := newPeerService(ctx, hB, nil, sB, nil)
	fA.start()
	fB.start()
	fA.startPresence(200 * time.Millisecond) // the retry rides this tick

	waitFor(t, "retry loop delivers the persisted pending request", func() bool {
		c, ok := sB.get(hA.ID().String())
		return ok && (c.State == stIncoming || c.State == stAccepted)
	})
}

// The nickname is never empty where it is USED: defaultNick is always non-empty, setProfile refuses an empty nick
// (falls back to the hostname), and an explicit nick is kept verbatim and persists. The STORED nick stays empty until
// the user chooses one (so the LAN self-name keeps its per-peer fallback — see TestLanLaunchVarsSelfName).
func TestNicknameDefaultsToHostnameAndNeverEmpty(t *testing.T) {
	if defaultNick() == "" {
		t.Fatal("defaultNick must never be empty")
	}
	dir := t.TempDir()
	if newSocialState(dir).getProfile().Nick != "" {
		t.Fatal("a fresh stored nick must stay empty until the user chooses one (display default lives in VgGetProfile)")
	}
	s := newSocialState(dir)
	s.setProfile("", "pic") // explicit empty → refused, falls back to a non-empty default (the hostname)
	if s.getProfile().Nick == "" {
		t.Fatal("setProfile must never store an empty nickname")
	}
	s.setProfile("alice", "") // a real nick is kept verbatim
	if s.getProfile().Nick != "alice" {
		t.Fatal("a non-empty nickname must be kept as-is")
	}
	if newSocialState(dir).getProfile().Nick != "alice" {
		t.Fatal("nickname must persist across reload")
	}
}

// The presence-deny set is per-peer and replaced wholesale.
func TestPresenceDenyGate(t *testing.T) {
	f := newPeerService(context.Background(), nil, nil, nil, nil)
	f.setPresenceDeny([]string{"peerA", "peerB"})
	if !f.presenceDenied("peerA") || !f.presenceDenied("peerB") {
		t.Fatal("denied peers must report denied")
	}
	if f.presenceDenied("peerC") {
		t.Fatal("a non-denied peer must not be denied")
	}
	f.setPresenceDeny([]string{"peerC"}) // replace-wholesale clears the old set
	if f.presenceDenied("peerA") || !f.presenceDenied("peerC") {
		t.Fatal("setPresenceDeny must replace the whole set, not merge")
	}
}

// End-to-end teeth for the OUTBOUND gate: a presence-denied peer must NOT receive our profile broadcast.
func TestPresenceDenySuppressesBroadcast(t *testing.T) {
	hA, hB := testHost(t), testHost(t)
	connectHosts(t, hA, hB)
	sA, sB := newSocialState(t.TempDir()), newSocialState(t.TempDir())
	sA.setProfile("alice", "pic1")
	ctx := context.Background()
	fA := newPeerService(ctx, hA, nil, sA, nil)
	fB := newPeerService(ctx, hB, nil, sB, nil)
	fA.start()
	fB.start()
	if err := fA.addPeer(hB.ID().String(), ""); err != nil {
		t.Fatalf("addPeer: %v", err)
	}
	waitFor(t, "bob incoming", func() bool { c, ok := sB.get(hA.ID().String()); return ok && c.State == stIncoming })
	if err := fB.acceptPeer(hA.ID().String()); err != nil {
		t.Fatalf("acceptPeer: %v", err)
	}
	waitFor(t, "alice accepted", func() bool { c, ok := sA.get(hB.ID().String()); return ok && c.State == stAccepted })
	// Alice DENIES presence to Bob, renames, and broadcasts → Bob must not see the new nickname.
	fA.setPresenceDeny([]string{hB.ID().String()})
	sA.setProfile("alice2", "pic2")
	fA.broadcastProfile()
	time.Sleep(300 * time.Millisecond)
	if c, _ := sB.get(hA.ID().String()); c.Nick == "alice2" {
		t.Fatal("a presence-denied peer must not receive our profile broadcast")
	}
}

// The presence-deny gate is symmetric on the peer protocol: if we hide our presence from a peer, we also do not
// record THEIR inbound liveness probe (at minimum we never answer/track a denied peer's ping).
func TestPresenceDenyIgnoresInboundPing(t *testing.T) {
	hA, hB := testHost(t), testHost(t)
	connectHosts(t, hA, hB)
	sA, sB := newSocialState(t.TempDir()), newSocialState(t.TempDir())
	ctx := context.Background()
	fA := newPeerService(ctx, hA, nil, sA, nil)
	fB := newPeerService(ctx, hB, nil, sB, nil)
	fA.start()
	fB.start()
	// Make them peers.
	if err := fA.addPeer(hB.ID().String(), ""); err != nil {
		t.Fatalf("addPeer: %v", err)
	}
	waitFor(t, "bob incoming", func() bool { c, ok := sB.get(hA.ID().String()); return ok && c.State == stIncoming })
	if err := fB.acceptPeer(hA.ID().String()); err != nil {
		t.Fatalf("acceptPeer: %v", err)
	}
	waitFor(t, "alice accepted", func() bool { c, ok := sA.get(hB.ID().String()); return ok && c.State == stAccepted })
	// Bob hides presence from Alice, then Alice pings Bob → Bob must NOT mark Alice online.
	fB.setPresenceDeny([]string{hA.ID().String()})
	fA.pingPresence(hB.ID().String())
	time.Sleep(250 * time.Millisecond)
	if c, _ := sB.get(hA.ID().String()); c.online {
		t.Fatal("a presence-denied peer's inbound ping must not mark them online on our side")
	}
}

func TestPeerBlockedPeerIgnored(t *testing.T) {
	hA, hB := testHost(t), testHost(t)
	connectHosts(t, hA, hB)

	sA := newSocialState(t.TempDir())
	sB := newSocialState(t.TempDir())
	ctx := context.Background()
	fA := newPeerService(ctx, hA, nil, sA, nil)
	fB := newPeerService(ctx, hB, nil, sB, nil)
	fA.start()
	fB.start()

	// Bob blocks Alice before she reaches out.
	if err := fB.blockPeer(hA.ID().String()); err != nil {
		t.Fatalf("block: %v", err)
	}
	_ = fA.addPeer(hB.ID().String(), "")
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
	s.upsert("12D3KooWPeer", func(c *contact) { c.State = stAccepted; c.Nick = "pal"; c.PicCID = "QmPal" })

	// Reload from disk into a fresh instance.
	s2 := newSocialState(dir)
	if p := s2.getProfile(); p.Nick != "me" || p.PicCID != "QmMyPic" {
		t.Fatalf("profile not persisted: %+v", p)
	}
	c, ok := s2.get("12D3KooWPeer")
	if !ok || c.State != stAccepted || c.Nick != "pal" || c.PicCID != "QmPal" {
		t.Fatalf("contact not persisted: %+v ok=%v", c, ok)
	}
}

// Presence is online/offline LIVENESS only (no play-state anymore): toggling it reports a change once per
// transition, updates the contact snapshot, and is transient (never persisted).
func TestPresenceLivenessToggles(t *testing.T) {
	dir := t.TempDir()
	s := newSocialState(dir)
	s.upsert("12D3KooWPeer", func(c *contact) { c.State = stAccepted })

	if changed, c := s.setPresence("12D3KooWPeer", true); !changed || !c.online {
		t.Fatalf("going online should change + report online: changed=%v online=%v", changed, c.online)
	}
	if changed, _ := s.setPresence("12D3KooWPeer", true); changed {
		t.Fatal("staying online should not report a change")
	}
	if changed, c := s.setPresence("12D3KooWPeer", false); !changed || c.online {
		t.Fatalf("going offline should change + report offline: changed=%v online=%v", changed, c.online)
	}
	if changed, _ := s.setPresence("nobody", true); changed {
		t.Fatal("presence for an unknown peer must be a no-op")
	}
	// online is transient: a reload never resurrects it.
	if c, _ := newSocialState(dir).get("12D3KooWPeer"); c.online {
		t.Fatal("online state must not persist across reload")
	}
}

// A stranger can open a stream and send requests: the same request again changes nothing (no address-book write, no
// event), and at most maxIncoming requests wait — the oldest go. Teeth: write on every request (the count grows); keep
// every request (the book grows without bound).
func TestRequestFloodIsBounded(t *testing.T) {
	s := newSocialState(t.TempDir())
	f := newPeerService(context.Background(), nil, nil, s, nil)
	f.dispatch("12D3KooWSame", peerMsg{Type: "request", Nick: "x"})
	before := s.saves
	for i := 0; i < 50; i++ {
		f.dispatch("12D3KooWSame", peerMsg{Type: "request", Nick: "x"})
	}
	if s.saves != before {
		t.Fatalf("the same request rewrote the address book %d times", s.saves-before)
	}
	for i := 0; i < maxIncoming+20; i++ {
		f.dispatch(fmt.Sprintf("12D3KooWFlood%03d", i), peerMsg{Type: "request"})
		time.Sleep(time.Millisecond) // distinct AddedAt
	}
	incoming := 0
	for _, c := range s.list() {
		if c.State == stIncoming {
			incoming++
		}
	}
	if incoming > maxIncoming {
		t.Fatalf("%d requests wait, more than %d", incoming, maxIncoming)
	}
	if _, ok := s.get("12D3KooWFlood000"); ok {
		t.Fatal("the oldest waiting request was kept past the bound")
	}
	if _, ok := s.get(fmt.Sprintf("12D3KooWFlood%03d", maxIncoming+19)); !ok {
		t.Fatal("the newest request was dropped")
	}
}

// One inbound stream carries at most maxPeerMsgs messages: a stream packed with thousands is cut there. Teeth: read
// to EOF (the last profile message wins).
func TestOneStreamCarriesFewMessages(t *testing.T) {
	hA, hB := testHost(t), testHost(t)
	connectHosts(t, hA, hB)
	sB := newSocialState(t.TempDir())
	fB := newPeerService(context.Background(), hB, nil, sB, nil)
	fB.start()
	st, err := hA.NewStream(context.Background(), hB.ID(), peerProtoID)
	if err != nil {
		t.Fatal(err)
	}
	enc := json.NewEncoder(st)
	_ = enc.Encode(peerMsg{Type: "request", Nick: "n0"})
	for i := 1; i < 40; i++ {
		_ = enc.Encode(peerMsg{Type: "profile", Nick: fmt.Sprintf("n%d", i)})
	}
	_ = st.CloseWrite()
	deadline := time.Now().Add(3 * time.Second)
	var c contact
	for time.Now().Before(deadline) {
		c, _ = sB.get(hA.ID().String())
		if c.Nick == fmt.Sprintf("n%d", maxPeerMsgs-1) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)
	c, _ = sB.get(hA.ID().String())
	if c.Nick != fmt.Sprintf("n%d", maxPeerMsgs-1) {
		t.Fatalf("after one stream the nick is %q, want n%d (the stream read past %d messages, or not at all)", c.Nick, maxPeerMsgs-1, maxPeerMsgs)
	}
}

// A peer that ends the pairing ends what we share with it too: a pairing made again starts from fresh consent.
// Teeth: drop the purge (shareLibs keeps the old share).
func TestTheirDeclinePurgesOurShares(t *testing.T) {
	s := newSocialState(t.TempDir())
	f := newPeerService(context.Background(), nil, nil, s, nil)
	const peer = "12D3KooWDecliner"
	s.upsert(peer, func(c *contact) { c.State = stAccepted })
	f.shareMu.Lock()
	if f.shareLibs == nil {
		f.shareLibs = map[string]map[string][]shareItem{}
	}
	f.shareLibs[peer] = map[string][]shareItem{"Games": nil}
	f.shareMu.Unlock()
	f.dispatch(peer, peerMsg{Type: "decline"})
	f.shareMu.Lock()
	_, still := f.shareLibs[peer]
	f.shareMu.Unlock()
	if still {
		t.Fatal("their decline left what we share with them")
	}
}
