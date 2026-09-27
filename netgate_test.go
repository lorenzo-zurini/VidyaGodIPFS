package main

import (
	"context"
	"testing"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	peer "github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

func withBudget(t *testing.T, rate, burst float64, wait time.Duration) {
	r, b, w := dialRate, dialBurst, dialMaxWait
	dialRate, dialBurst, dialMaxWait = rate, burst, wait
	t.Cleanup(func() { dialRate, dialBurst, dialMaxWait = r, b, w })
}

// A burst goes through at once; past it, dials are paced at the rate; a dial that would wait past the limit is refused.
func TestDialBudgetPacesAndRefuses(t *testing.T) {
	withBudget(t, 20, 3, 200*time.Millisecond)
	g := &netGate{n: &node{}}
	t0 := time.Now()
	for i := 0; i < 3; i++ {
		if !g.InterceptPeerDial(peer.ID("p")) {
			t.Fatalf("dial %d of the burst refused", i)
		}
	}
	if time.Since(t0) > 50*time.Millisecond {
		t.Fatalf("the burst waited %s", time.Since(t0))
	}
	t1 := time.Now()
	if !g.InterceptPeerDial(peer.ID("p")) {
		t.Fatal("a paced dial was refused")
	}
	if d := time.Since(t1); d < 30*time.Millisecond {
		t.Fatalf("past the burst, a dial went out after %s — not paced (want ~50 ms at 20/s)", d)
	}
	dialRate = 1 // the next token is ~1 s away, beyond the 200 ms limit
	if g.InterceptPeerDial(peer.ID("p")) {
		t.Fatal("a dial that would wait past the limit was allowed")
	}
}

// Friends, LAN peers and a fetch's providers skip the line even when the budget is spent.
func TestDialBudgetLetsWhatTheUserWaitsOnThrough(t *testing.T) {
	withBudget(t, 0.001, 1, 10*time.Millisecond)
	s := newSocialState(t.TempDir())
	friend := peer.ID("friend")
	s.contacts[friend.String()] = &contact{State: stAccepted}
	n := &node{social: s}
	g := &netGate{n: n}
	g.InterceptPeerDial("stranger") // spends the only token
	if g.InterceptPeerDial("stranger2") {
		t.Fatal("budget not spent — the test proves nothing")
	}
	n.noteLanPeer("lan")
	n.noteWantedProvider("seed")
	for _, p := range []peer.ID{friend, "lan", "seed"} {
		if !g.InterceptPeerDial(p) {
			t.Fatalf("%s was held behind the budget", p)
		}
	}
	n.wantedProv.Store(peer.ID("old"), time.Now().Add(-time.Second))
	if g.InterceptPeerDial("old") {
		t.Fatal("an expired provider still skipped the line")
	}
}

// A stranger's private addresses are not dialed across the internet; a friend's or LAN peer's are; public ones always.
func TestPrivateAddressesOnlyForClosePeers(t *testing.T) {
	s := newSocialState(t.TempDir())
	s.contacts[peer.ID("friend").String()] = &contact{State: stAccepted}
	n := &node{social: s}
	n.noteLanPeer("lan")
	g := &netGate{n: n}
	private := []string{"/ip4/10.10.10.2/tcp/41001", "/ip4/192.168.1.152/udp/4001/quic-v1", "/ip4/172.20.0.3/tcp/4001",
		"/ip4/100.71.4.138/tcp/4001", "/ip4/127.0.0.1/tcp/4001", "/ip6/::1/udp/4001/quic-v1", "/ip6/fd00::1/tcp/4001",
		"/ip6/fe80::1/tcp/4001"}
	for _, s := range private {
		a := ma.StringCast(s)
		if g.InterceptAddrDial("stranger", a) {
			t.Errorf("stranger's %s was dialed", s)
		}
		if !g.InterceptAddrDial("friend", a) || !g.InterceptAddrDial("lan", a) {
			t.Errorf("a close peer's %s was refused", s)
		}
	}
	for _, s := range []string{"/ip4/95.217.150.240/udp/4001/quic-v1", "/ip6/2a01:4f9:c013:b5c2::1/tcp/4001", "/dns4/bitswap.pinata.cloud/tcp/443/wss"} {
		if !g.InterceptAddrDial("stranger", ma.StringCast(s)) {
			t.Errorf("public %s refused", s)
		}
	}
}

// An explicit connect names the peer's address: a stranger on the LAN, a tunnel or loopback is reachable that way
// (direct peering, a controlled benchmark) — the private-address filter refused it with nothing but a counter.
// Teeth: drop noteLanPeer in connect() and the dial below is refused by our own gater.
func TestAnExplicitConnectReachesAStrangersPrivateAddress(t *testing.T) {
	target, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	n := &node{ctx: ctx}
	h, err := libp2p.New(libp2p.NoListenAddrs, libp2p.ConnectionGater(&netGate{n: n}))
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	n.host = h
	addr := target.Addrs()[0].String() + "/p2p/" + target.ID().String()
	if err := n.connect(addr); err != nil {
		t.Fatalf("explicit connect to %s: %v", addr, err)
	}
}
