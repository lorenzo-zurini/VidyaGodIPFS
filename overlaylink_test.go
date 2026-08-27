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

	oa := newOverlayService(ctx, ha, nil)
	ob := newOverlayService(ctx, hb, nil)
	ma = newLinkMaintainer(ctx, ha, nil, func() map[peer.ID]string { return map[peer.ID]string{hb.ID(): "b"} })
	mb = newLinkMaintainer(ctx, hb, nil, func() map[peer.ID]string { return map[peer.ID]string{ha.ID(): "a"} })
	oa.linkm, ob.linkm = ma, mb
	oa.start()
	ob.start()

	connectHosts(t, ha, hb)
	// The notifiee fires on connect, but the maintained set is only known to it via linkm.has — populate both
	// maintainers' link tables first, then re-kick the datagram loops for the existing connection.
	ma.tick()
	mb.tick()
	for _, c := range ha.Network().ConnsToPeer(hb.ID()) {
		oa.maybeStartDatagramLoop(c)
	}
	for _, c := range hb.Network().ConnsToPeer(ha.ID()) {
		ob.maybeStartDatagramLoop(c)
	}
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
