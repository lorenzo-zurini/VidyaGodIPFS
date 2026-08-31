package main

// overlaylink_test.go — the link maintainer over TWO real in-process libp2p hosts on QUIC (datagrams need it).
// Proves: the datagram RX gate admits maintained (non-route) peers, ping→echo→RTT lands, the state machine reaches
// "direct", and the zombie detector (missed pongs) tears the connection down and flips to connecting.

import (
	"context"
	"testing"
	"time"

	host "github.com/libp2p/go-libp2p/core/host"
	network "github.com/libp2p/go-libp2p/core/network"
	peer "github.com/libp2p/go-libp2p/core/peer"
)

// lanPair wires two hosts with overlay services + maintainers watching each other, pre-connected over QUIC.
func lanPair(t *testing.T) (ha, hb host.Host, ma, mb *linkMaintainer) {
	t.Helper()
	ha, hb = quicHost(t), quicHost(t)
	ctx := context.Background()

	ma = newLinkMaintainer(ctx, ha, nil, func() map[peer.ID]string { return map[peer.ID]string{hb.ID(): "b"} })
	mb = newLinkMaintainer(ctx, hb, nil, func() map[peer.ID]string { return map[peer.ID]string{ha.ID(): "a"} })
	// Constructor injection — the same wiring production gets. NO manual datagram-loop kicks here: the old
	// harness hand-started the loops "because the notifiee races the maintained set", which papered over the
	// production bug where that race left friend conns loop-less forever. The maintainer must now attach loops
	// BY ITSELF (ensureRecvLoops, every beat) or these tests fail.
	oa := newOverlayService(ctx, ha, nil, ma)
	ob := newOverlayService(ctx, hb, nil, mb)
	oa.start()
	ob.start()

	connectHosts(t, ha, hb)
	ma.tick()
	mb.tick()
	return ha, hb, ma, mb
}

func TestLinkMaintainerReachesDirectWithRTT(t *testing.T) {
	_, hb, ma, _ := lanPair(t)
	waitFor(t, "direct link with RTT", func() bool {
		ma.tick() // manual ticks — no waiting out the 4s cadence
		for _, li := range ma.snapshot(func(p peer.ID) string { return lanVIP(p.String()) }) {
			if li.Peer == hb.ID().String() && li.Link == linkDirect && li.RttMs >= 0 && li.Online {
				return true
			}
		}
		return false
	})
}

func TestLinkMaintainerDetectsZombieAndReconnects(t *testing.T) {
	ha, hb, ma, _ := lanPair(t)
	// Reach direct first (the zombie verdict only applies to previously-responsive peers).
	waitFor(t, "direct link", func() bool {
		ma.tick()
		for _, li := range ma.snapshot(func(peer.ID) string { return "" }) {
			if li.Link == linkDirect && li.RttMs >= 0 {
				return true
			}
		}
		return false
	})
	// Backdate the pong so the next ticks count misses without waiting linkBeatEvery out; the peer stays up (the
	// zombie case is a DEAD PATH under a live-looking conn, which a backdated pong models exactly).
	ma.mu.Lock()
	ma.links[hb.ID()].lastPong = time.Now().Add(-time.Minute)
	ma.mu.Unlock()
	for i := 0; i < linkBeatMiss; i++ {
		ma.tick()
	}
	// The zombie verdict must have closed the connection (and moved off "direct").
	waitFor(t, "connection torn down after missed beats", func() bool {
		ma.tick()
		return ha.Network().Connectedness(hb.ID()) != network.Connected ||
			func() bool { // or already reconnected fresh — either proves the teardown fired
				ma.mu.Lock()
				defer ma.mu.Unlock()
				return ma.links[hb.ID()].repunch > 0
			}()
	})
}

func TestLanVIPStableAndInSubnet(t *testing.T) {
	v := lanVIP("12D3KooWTestPeer")
	if v != lanVIP("12D3KooWTestPeer") {
		t.Fatal("vIP not deterministic")
	}
	if len(v) < 8 || v[:6] != "10.66." {
		t.Fatalf("vIP outside subnet: %s", v)
	}
}

// The 2026-08-31 field failure, round two. A direct conn whose peer never answers datagrams must (a) never be
// reported "direct", but (b) NOT be torn down either — the first fix closed the peer, which also killed the
// presence streams riding the same connection and flapped friends offline every proving window. The right
// behavior: state stays honest (relayed = usable via stream fallback), the conn survives, no repunch churn.
func TestLinkMaintainerNeverPongedConnStaysUpAsRelayed(t *testing.T) {
	ha, hb := quicHost(t), quicHost(t)
	ctx := context.Background()

	ma := newLinkMaintainer(ctx, ha, nil, func() map[peer.ID]string { return map[peer.ID]string{hb.ID(): "b"} })
	// Only A runs the overlay service — B never reads datagrams, so A's pings land in the void while the QUIC
	// connection itself stays perfectly alive. This is the half-dead datagram path, modeled exactly.
	oa := newOverlayService(ctx, ha, nil, ma)
	oa.start()
	connectHosts(t, ha, hb)
	ma.tick()

	for i := 0; i < linkProveBeats+3; i++ {
		ma.tick()
		for _, li := range ma.snapshot(func(peer.ID) string { return "" }) {
			if li.Link == linkDirect {
				t.Fatalf("unproven conn reported as direct on beat %d (rtt=%d)", i, li.RttMs)
			}
		}
	}
	ma.mu.Lock()
	repunch := ma.links[hb.ID()].repunch
	state := ma.links[hb.ID()].state
	ma.mu.Unlock()
	if repunch != 0 {
		t.Fatalf("unproven conn was torn down (repunch=%d) — that churn is what flapped friends offline", repunch)
	}
	if len(ha.Network().ConnsToPeer(hb.ID())) == 0 {
		t.Fatalf("the connection (and the streams riding it) must survive an unproven datagram path")
	}
	if state != linkRelayed {
		t.Fatalf("unproven conn should read 'relayed' (stream-fallback-usable), got %q", state)
	}
}
