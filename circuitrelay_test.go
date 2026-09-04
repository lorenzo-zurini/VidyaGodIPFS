package main

// ADVERSARY-TODO (2026-09-04): the changeset that added the conn-close assertion below + the battery
// recovery-gate correction did NOT clear the Fable adversary gate — Fable hit its usage limit mid-review and
// returned no verdict; it was pushed on an Opus inline self-review at the user's call. Re-run the adversary
// (.claude/agents/adversary.md) on commit 02b42e9 / main bfe7c65 once Fable credits are back; fix any finding
// as a follow-up. Self-review notes: maintainer down-path self-heals (kickDial backoff, no wedge), traffic-
// resumed is the real recovery proof; judged a legitimate harness correction, not theater.
// circuitrelay_test.go — an IN-PROCESS circuit-v2 relay fixture: a relay host R and two UNREACHABLE clients that
// can only meet through it. This is the topology the unit suite structurally lacked (every other fixture wires
// directly-connected hosts, so Connectedness is always Connected): libp2p reports a relayed conn as
// network.Limited, and treating Limited as "not connected" showed a live relay-only friend link as DOWN in the
// panel while re-dialing it uselessly every beat — a real field bug the E2E battery caught on 2026-09-04 and the
// unit suite could not (adversarial finding: "no test anywhere constructs a Limited conn").

import (
	"context"
	"testing"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	host "github.com/libp2p/go-libp2p/core/host"
	network "github.com/libp2p/go-libp2p/core/network"
	peer "github.com/libp2p/go-libp2p/core/peer"
	relayclient "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/client"
	relayv2 "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
	ma "github.com/multiformats/go-multiaddr"
)

// relayedPair builds R (relay) + A,B (no listen addrs — unreachable directly) and connects A→B THROUGH R.
// Returns the hosts with A holding a Limited connection to B.
func relayedPair(t *testing.T) (a, b host.Host) {
	t.Helper()
	r, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("relay host: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })
	rsvc, err := relayv2.New(r)
	if err != nil {
		t.Fatalf("relay service: %v", err)
	}
	t.Cleanup(func() { _ = rsvc.Close() })
	mk := func() host.Host {
		h, err := libp2p.New(libp2p.NoListenAddrs, libp2p.EnableRelay())
		if err != nil {
			t.Fatalf("client host: %v", err)
		}
		t.Cleanup(func() { _ = h.Close() })
		return h
	}
	a, b = mk(), mk()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rinfo := peer.AddrInfo{ID: r.ID(), Addrs: r.Addrs()}
	if err := a.Connect(ctx, rinfo); err != nil {
		t.Fatalf("a→relay: %v", err)
	}
	if err := b.Connect(ctx, rinfo); err != nil {
		t.Fatalf("b→relay: %v", err)
	}
	if _, err := relayclient.Reserve(ctx, b, rinfo); err != nil {
		t.Fatalf("b reserve: %v", err)
	}
	circuit, err := ma.NewMultiaddr("/p2p/" + r.ID().String() + "/p2p-circuit")
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Connect(ctx, peer.AddrInfo{ID: b.ID(), Addrs: []ma.Multiaddr{circuit}}); err != nil {
		t.Fatalf("a→b via relay: %v", err)
	}
	return a, b
}

// countingRouter is a peerRouter that records FindPeer calls; a resolve it should never make is a bug.
type countingRouter struct{ calls int }

func (c *countingRouter) FindPeer(context.Context, peer.ID) (peer.AddrInfo, error) {
	c.calls++
	return peer.AddrInfo{}, context.Canceled // never used: a Limited-connected dialPeer must short-circuit first
}

func TestRelayOnlyConnIsLimitedAndCountsAsConnected(t *testing.T) {
	a, b := relayedPair(t)

	// Premise: this topology yields Limited, not Connected — the state the rest of the suite never produces.
	if c := a.Network().Connectedness(b.ID()); c != network.Limited {
		t.Fatalf("fixture broken: want Limited connectedness, got %v", c)
	}

	// The maintainer must classify a relay-only friend as RELAYED (usable via the limited-conn stream), never
	// down — and must not be stuck dialing it. Mutation-verified: dropping `|| Limited` in evaluate fails this.
	m := newLinkMaintainer(context.Background(), a, nil, func() map[peer.ID]string {
		return map[peer.ID]string{b.ID(): "b"}
	})
	m.tick()
	var state string
	for _, li := range m.snapshot(func(peer.ID) string { return "" }) {
		if li.Peer == b.ID().String() {
			state = li.Link
		}
	}
	if state != linkRelayed {
		t.Fatalf("relay-only friend must read %q, got %q (Limited misclassified as down?)", linkRelayed, state)
	}

	// dialPeer must treat the limited conn as a live link with NO redial churn: over a real router it must
	// short-circuit BEFORE any FindPeer. A counting router makes the churn observable — reverting dialPeer's
	// `|| Limited` sends it to FindPeer every call (mutation-verified), which err==nil alone could not catch.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	cr := &countingRouter{}
	if err := dialPeer(ctx, a, cr, b.ID()); err != nil {
		t.Fatalf("dialPeer over a limited conn must be a no-op success, got %v", err)
	}
	if cr.calls != 0 {
		t.Fatalf("dialPeer over a limited conn must NOT resolve/redial: %d FindPeer call(s) (Limited treated as down?)", cr.calls)
	}

	// And the friend protocol must WORK over it end-to-end (WithAllowLimitedConn — the original relay-only fix).
	sA, sB := newSocialState(t.TempDir()), newSocialState(t.TempDir())
	fA, fB := newFriendService(context.Background(), a, nil, sA, nil), newFriendService(context.Background(), b, nil, sB, nil)
	fA.start()
	fB.start()
	if err := fA.addFriend(b.ID().String(), "over the relay"); err != nil {
		t.Fatalf("friend request over a limited conn: %v", err)
	}
	waitFor(t, "b sees the incoming request through the relay", func() bool {
		c, ok := sB.get(a.ID().String())
		return ok && c.State == stIncoming
	})

	// Outage detection, deterministic (no DHT): closing the only conn must move the maintainer OFF relayed —
	// the state-machine half of "recovery" the field battery can't assert without flaky cold re-discovery. Done
	// LAST so the assertions above still had a live relay conn.
	_ = a.Network().ClosePeer(b.ID())
	m.tick()
	for _, li := range m.snapshot(func(peer.ID) string { return "" }) {
		if li.Peer == b.ID().String() && (li.Link == linkRelayed || li.Link == linkDirect) {
			t.Fatalf("maintainer still reports %q after the only conn closed — outage not detected", li.Link)
		}
	}
}
