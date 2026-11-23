package main

// nearby_test.go — the nearby list over two (three) real libp2p hosts in-process: who shows, by what name, and who
// gets our name. "On our network" is injected (the node's is mDNS: netgate.go lanPeers).

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	host "github.com/libp2p/go-libp2p/core/host"
	network "github.com/libp2p/go-libp2p/core/network"
	peer "github.com/libp2p/go-libp2p/core/peer"
)

func lanOf(ps ...peer.ID) func(peer.ID) bool {
	set := map[peer.ID]bool{}
	for _, p := range ps {
		set[p] = true
	}
	return func(p peer.ID) bool { return set[p] }
}

func nearbyOn(t *testing.T, h host.Host, s *socialState, isLan func(peer.ID) bool, nick string) *nearbyService {
	t.Helper()
	n := newNearbyService(context.Background(), h, s, isLan, func() nearbyInfo { return nearbyInfo{Nick: nick} })
	n.start()
	return n
}

// waitNearby polls list() (it asks in the background and shows a device from a later call) until cond holds.
func waitNearby(t *testing.T, n *nearbyService, what string, cond func([]nearbyPeer) bool) []nearbyPeer {
	t.Helper()
	var got []nearbyPeer
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if got = n.list(); cond(got) {
			return got
		}
		time.Sleep(20 * time.Millisecond)
	}
	b, _ := json.Marshal(got)
	t.Fatalf("timed out waiting for: %s (list %s)", what, b)
	return nil
}

// A VidyaGod device on our network shows by its name and host, with what it is to us; a libp2p node that does not
// speak nearby (another IPFS node on the LAN) never shows. Teeth: the name not asked, a non-VidyaGod node listed,
// a node that does not speak it asked at all, the friend state missing, a device dropped by one missed re-ask.
func TestNearbyListsVidyaGodDevicesByName(t *testing.T) {
	hA, hB, hC := testHost(t), testHost(t), testHost(t)
	connectHosts(t, hA, hB)
	connectHosts(t, hA, hC) // hC: a plain libp2p node, no nearby protocol
	sA, sB := newSocialState(t.TempDir()), newSocialState(t.TempDir())
	a := nearbyOn(t, hA, sA, lanOf(hB.ID(), hC.ID()), "pc")
	nearbyOn(t, hB, sB, lanOf(hA.ID()), "deck")

	got := waitNearby(t, a, "the deck by name", func(l []nearbyPeer) bool { return len(l) == 1 && l[0].Nick == "deck" })
	if got[0].Peer != hB.ID().String() || got[0].State != "" {
		t.Fatalf("row = %+v", got[0])
	}
	a.mu.Lock()
	_, askedC := a.known[hC.ID()]
	a.mu.Unlock()
	if askedC {
		t.Fatalf("a node that does not speak nearby was asked")
	}
	sA.upsert(hB.ID().String(), func(c *contact) { c.State = stPending })
	if l := a.list(); len(l) != 1 || l[0].State != string(stPending) {
		t.Fatalf("friend state not shown: %+v", l)
	}

	// A re-ask that fails (a slow moment) keeps the device listed: the row would vanish and come back otherwise.
	hB.SetStreamHandler(nearbyProtoID, func(st network.Stream) { _ = st.Reset() }) // still speaks it, answers nothing
	a.mu.Lock()
	for _, x := range a.known {
		x.at = time.Time{}
	}
	a.mu.Unlock()
	for i := 0; i < 40; i++ {
		if l := a.list(); len(l) != 1 {
			t.Fatalf("a device dropped by one missed re-ask: %+v", l)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// Our name goes only to our own network: a peer we do not count as on it gets the stream reset, and one it does not
// count as nearby is not listed by it. A blocked peer is neither answered nor listed. Teeth: answering anyone,
// listing a peer off our network, a blocked peer shown.
func TestNearbyStaysOnTheLocalNetwork(t *testing.T) {
	hA, hB := testHost(t), testHost(t)
	connectHosts(t, hA, hB)
	sA, sB := newSocialState(t.TempDir()), newSocialState(t.TempDir())

	// B does not count A as on its network: A asks, B refuses, A lists nothing.
	a := nearbyOn(t, hA, sA, lanOf(hB.ID()), "pc")
	nearbyOn(t, hB, sB, lanOf(), "deck")
	time.Sleep(50 * time.Millisecond)
	for i := 0; i < 20; i++ {
		if l := a.list(); len(l) != 0 {
			t.Fatalf("a peer off the responder's network got its name: %+v", l)
		}
		time.Sleep(25 * time.Millisecond)
	}

	// A does not count B as on its network: never listed, whatever B would say.
	hC, hD := testHost(t), testHost(t)
	connectHosts(t, hC, hD)
	c := nearbyOn(t, hC, newSocialState(t.TempDir()), lanOf(), "c")
	nearbyOn(t, hD, newSocialState(t.TempDir()), lanOf(hC.ID()), "d")
	for i := 0; i < 40; i++ { // long enough for an ask and its answer: a device shows on the call after
		if l := c.list(); len(l) != 0 {
			t.Fatalf("a peer off our network listed: %+v", l)
		}
		time.Sleep(25 * time.Millisecond)
	}

	// Blocked: B blocks A → A gets no answer; A blocks B → B is not listed.
	hE, hF := testHost(t), testHost(t)
	connectHosts(t, hE, hF)
	sE, sF := newSocialState(t.TempDir()), newSocialState(t.TempDir())
	e := nearbyOn(t, hE, sE, lanOf(hF.ID()), "e")
	nearbyOn(t, hF, sF, lanOf(hE.ID()), "f")
	waitNearby(t, e, "F listed before the block", func(l []nearbyPeer) bool { return len(l) == 1 })
	sE.upsert(hF.ID().String(), func(c *contact) { c.State = stBlocked })
	if l := e.list(); len(l) != 0 {
		t.Fatalf("a blocked peer listed: %+v", l)
	}
	sE.upsert(hF.ID().String(), func(c *contact) { c.State = stAccepted })
	sF.upsert(hE.ID().String(), func(c *contact) { c.State = stBlocked })
	e.mu.Lock()
	for _, x := range e.known {
		x.at, x.ok = time.Time{}, false // ask again now
	}
	e.mu.Unlock()
	time.Sleep(50 * time.Millisecond)
	for i := 0; i < 20; i++ {
		if l := e.list(); len(l) != 0 {
			t.Fatalf("a peer that blocked us still answered: %+v", l)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// A name longer than a nickname may be is cut to one, as a friend request's is: the far end is anyone's code.
func TestNearbyBoundsTheNames(t *testing.T) {
	hA, hB := testHost(t), testHost(t)
	connectHosts(t, hA, hB)
	big := strings.Repeat("x", 300)
	a := nearbyOn(t, hA, newSocialState(t.TempDir()), lanOf(hB.ID()), "a")
	hB.SetStreamHandler(nearbyProtoID, func(st network.Stream) { // a responder that does not bound its own names
		defer st.Close()
		_ = json.NewEncoder(st).Encode(nearbyInfo{Nick: big})
	})
	got := waitNearby(t, a, "B listed", func(l []nearbyPeer) bool { return len(l) == 1 })
	if len(got[0].Nick) > maxNickLen {
		t.Fatalf("name not bounded: %d", len(got[0].Nick))
	}
}

// An "accept" from a peer we never asked makes it nothing: before, it made the sender a friend (on our virtual LAN,
// told our presence) with no consent of ours. A real accept, to our request, still lands. Teeth: the accept gate.
func TestUnsolicitedAcceptIsIgnored(t *testing.T) {
	hA, hB := testHost(t), testHost(t)
	connectHosts(t, hA, hB)
	sA, sB := newSocialState(t.TempDir()), newSocialState(t.TempDir())
	ctx := context.Background()
	fA := newFriendService(ctx, hA, nil, sA, nil)
	fB := newFriendService(ctx, hB, nil, sB, nil)
	fA.start()
	fB.start()

	_ = fB.send(hA.ID().String(), fB.helloMsg("accept"))
	time.Sleep(300 * time.Millisecond)
	if c, ok := sA.get(hB.ID().String()); ok && c.State == stAccepted {
		t.Fatalf("an unsolicited accept made a friend: %+v", c)
	}
	// Its own request first, then an accept of it: still only a request waiting on us.
	_ = fB.send(hA.ID().String(), fB.helloMsg("request"))
	waitFor(t, "A sees B's request", func() bool { c, ok := sA.get(hB.ID().String()); return ok && c.State == stIncoming })
	_ = fB.send(hA.ID().String(), fB.helloMsg("accept"))
	time.Sleep(300 * time.Millisecond)
	if c, _ := sA.get(hB.ID().String()); c.State != stIncoming {
		t.Fatalf("a requester accepting its own request made a friend: %+v", c)
	}
	sA.remove(hB.ID().String())

	_ = fA.addFriend(hB.ID().String(), "")
	waitFor(t, "B sees the request", func() bool { c, ok := sB.get(hA.ID().String()); return ok && c.State == stIncoming })
	_ = fB.acceptFriend(hA.ID().String())
	waitFor(t, "A sees the real accept", func() bool { c, ok := sA.get(hB.ID().String()); return ok && c.State == stAccepted })
}

// A friend still asking (it missed our accept: asleep, offline when we accepted) gets our accept again, so it stops
// waiting in "request sent" while we show it as a friend. Teeth: a friend's request left unanswered.
func TestAFriendsRequestIsAnsweredWithAccept(t *testing.T) {
	hA, hB := testHost(t), testHost(t)
	connectHosts(t, hA, hB)
	sA, sB := newSocialState(t.TempDir()), newSocialState(t.TempDir())
	ctx := context.Background()
	fA := newFriendService(ctx, hA, nil, sA, nil)
	fB := newFriendService(ctx, hB, nil, sB, nil)
	fA.start()
	fB.start()
	sA.upsert(hB.ID().String(), func(c *contact) { c.State = stPending })  // A asked; B's accept never arrived
	sB.upsert(hA.ID().String(), func(c *contact) { c.State = stAccepted }) // B accepted
	_ = fA.send(hB.ID().String(), fA.helloMsg("request"))                  // A's retry loop asks again
	waitFor(t, "A hears B's accept again", func() bool { c, _ := sA.get(hB.ID().String()); return c.State == stAccepted })
}

// A friend asking again is answered once per reAcceptEvery, not once per request: a stream packed with requests must
// not become a goroutine and a dial each. Teeth: the limit gone.
func TestReAcceptIsLimitedPerFriend(t *testing.T) {
	f := newFriendService(context.Background(), testHost(t), nil, newSocialState(t.TempDir()), nil)
	if !f.reAcceptDue("p1") || f.reAcceptDue("p1") {
		t.Fatalf("a friend asking twice at once is answered twice")
	}
	if !f.reAcceptDue("p2") {
		t.Fatalf("one friend's answer held another's back")
	}
}

// An accept from a friend already (the answer to a request sent again) refreshes its profile and is not the accept
// event: the app applies its New-peer defaults on that, over what was chosen for the friend. Asking a friend again
// sends nothing. Teeth: the accept event on a re-accept, a request sent to a friend.
func TestAReAcceptIsNoNewFriendship(t *testing.T) {
	hA, hB := testHost(t), testHost(t)
	connectHosts(t, hA, hB)
	sA, sB := newSocialState(t.TempDir()), newSocialState(t.TempDir())
	var mu sync.Mutex
	var kinds []int
	fA := newFriendService(context.Background(), hA, nil, sA, func(k int, _ string) { mu.Lock(); kinds = append(kinds, k); mu.Unlock() })
	fB := newFriendService(context.Background(), hB, nil, sB, nil)
	fA.start()
	fB.start()
	sA.upsert(hB.ID().String(), func(c *contact) { c.State = stAccepted })
	sB.upsert(hA.ID().String(), func(c *contact) { c.State = stAccepted })
	_ = fB.send(hA.ID().String(), fB.helloMsg("accept"))
	waitFor(t, "A takes B's accept", func() bool { mu.Lock(); defer mu.Unlock(); return len(kinds) == 1 })
	mu.Lock()
	if kinds[0] != evFriendProfile {
		t.Fatalf("a friend's accept again was event %d, not a profile refresh", kinds[0])
	}
	mu.Unlock()

	var got int
	var gmu sync.Mutex
	fB.emit = func(k int, _ string) { gmu.Lock(); got++; gmu.Unlock() }
	_ = fA.addFriend(hB.ID().String(), "")
	time.Sleep(300 * time.Millisecond)
	gmu.Lock()
	defer gmu.Unlock()
	if got != 0 {
		t.Fatalf("asking a friend again reached it (%d events)", got)
	}
}
