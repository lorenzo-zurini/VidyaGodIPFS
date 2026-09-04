package main

// overlaylink_test.go — the link maintainer over TWO real in-process libp2p hosts on QUIC (datagrams need it).
// Proves the new contract (Invariants 1+2, see overlaylink.go):
//   • the datagram RX gate admits maintained (non-route) peers, ping→echo→RTT lands, and the state machine reaches
//     "direct" only AFTER the first pong proves the path;
//   • the overlay TX trust gate routes packets over the reliable stream while the path is unproven — delivery
//     never waits for datagrams (the half-dead/one-way-punch case DELIVERS instead of silently losing 100%);
//   • a proven path whose pongs stop is DEMOTED (datagrams distrusted, traffic back on stream) WITHOUT the
//     connection ever being torn down — bitswap/friend streams ride the same conn — and re-proves itself on the
//     next pong (graceful recovery in both directions).

import (
	"bytes"
	"context"
	"testing"
	"time"

	host "github.com/libp2p/go-libp2p/core/host"
	network "github.com/libp2p/go-libp2p/core/network"
	peer "github.com/libp2p/go-libp2p/core/peer"
)

// lanPair wires two hosts with overlay services + maintainers watching each other, pre-connected over QUIC.
func lanPair(t *testing.T) (ha, hb host.Host, oa, ob *overlayService, ma, mb *linkMaintainer) {
	t.Helper()
	ha, hb = quicHost(t), quicHost(t)
	ctx := context.Background()

	ma = newLinkMaintainer(ctx, ha, nil, func() map[peer.ID]string { return map[peer.ID]string{hb.ID(): "b"} })
	mb = newLinkMaintainer(ctx, hb, nil, func() map[peer.ID]string { return map[peer.ID]string{ha.ID(): "a"} })
	oa = newOverlayService(ctx, ha, nil, ma)
	ob = newOverlayService(ctx, hb, nil, mb)
	oa.start()
	ob.start()

	connectHosts(t, ha, hb)
	// Populate both maintainers' link tables; their evaluate() then re-kicks the datagram RX loops for the
	// existing connection via ensureRecvLoops (the production wiring, exercised here on purpose).
	ma.tick()
	mb.tick()
	return ha, hb, oa, ob, ma, mb
}

func TestLinkMaintainerReachesDirectWithRTT(t *testing.T) {
	_, hb, _, _, ma, _ := lanPair(t)
	waitFor(t, "direct link with RTT (proven by pong)", func() bool {
		ma.tick() // manual ticks — no waiting out the 4s cadence
		for _, li := range ma.snapshot(func(p peer.ID) string { return lanVIP(p.String()) }) {
			if li.Peer == hb.ID().String() && li.Link == linkDirect && li.RttMs >= 0 && li.Online {
				return true
			}
		}
		return false
	})
	if !ma.datagramsTrusted(hb.ID()) {
		t.Fatal("direct+proven link must trust datagrams")
	}
}

func TestUnprovenDirectConnIsNotReportedDirectAndNotTrusted(t *testing.T) {
	_, hb, _, _, ma, _ := lanPair(t)
	// FORCE the unproven state (a pong may already have raced in on fast hardware — skipping here meant the
	// test sometimes never ran its asserts; adversarial L2): distrust exactly as a fresh conn starts out.
	time.Sleep(250 * time.Millisecond) // let ALL of lanPair's tick-pings' pongs land: no new pings are sent between
	// this drain and the asserts below, so after it the forced state cannot be re-proven (adversarial round-2 L2)
	ma.mu.Lock()
	l := ma.links[hb.ID()]
	l.everPonged, l.provenConnID, l.missed = false, "", 0
	ma.mu.Unlock()
	if ma.datagramsTrusted(hb.ID()) {
		t.Fatal("unproven direct path must not be trusted with game traffic")
	}
	ma.evaluate(l)
	ma.mu.Lock()
	st := l.state
	ma.mu.Unlock()
	if st == linkDirect {
		t.Fatalf("unproven conn reported %q; want relayed/connecting until the first pong", st)
	}
}

func TestDeadDatagramPathDemotesWithoutTeardownThenReProves(t *testing.T) {
	ha, hb, _, _, ma, _ := lanPair(t)
	// Reach direct/proven first.
	waitFor(t, "direct link", func() bool {
		ma.tick()
		for _, li := range ma.snapshot(func(peer.ID) string { return "" }) {
			if li.Link == linkDirect && li.RttMs >= 0 {
				return true
			}
		}
		return false
	})
	// Backdate the pong so the next ticks count misses without waiting linkBeatEvery out (models a dead datagram
	// path under a live-looking conn — the NAT-idle / one-way-punch field case).
	ma.mu.Lock()
	ma.links[hb.ID()].lastPong = time.Now().Add(-time.Minute)
	ma.mu.Unlock()
	demoted := false
	for i := 0; i < linkBeatMiss+1 && !demoted; i++ {
		ma.tick()
		ma.mu.Lock()
		demoted = ma.links[hb.ID()].demoted > 0
		// Keep the pong stale: real pongs from the healthy test conn would reset the miss counter mid-loop.
		if !demoted {
			ma.links[hb.ID()].lastPong = time.Now().Add(-time.Minute)
			ma.links[hb.ID()].rttMs = -1
		}
		ma.mu.Unlock()
	}
	if !demoted {
		t.Fatal("missed pongs never demoted the link")
	}
	// INVARIANT 1: the connection was NOT torn down — bitswap/friend streams ride it.
	if ha.Network().Connectedness(hb.ID()) != network.Connected {
		t.Fatal("demote tore down the connection — ClosePeer is banned")
	}
	// INVARIANT 2: datagrams are distrusted the moment the demote lands.
	if ma.datagramsTrusted(hb.ID()) {
		t.Fatal("demoted link still trusts datagrams")
	}
	// Graceful recovery: the conn is actually healthy in this test, so the very next pings re-prove the path.
	waitFor(t, "re-proven direct after demote", func() bool {
		ma.tick()
		return ma.datagramsTrusted(hb.ID())
	})
}

// TestUnprovenPathStillDeliversViaStream is THE half-dead-punch regression test: with a direct QUIC conn up but
// the datagram path NOT proven, a forwarded packet must still arrive (over the reliable stream). On the old code
// this exact situation was 100% silent loss.
func TestUnprovenPathStillDeliversViaStream(t *testing.T) {
	ha, hb, oa, ob, ma, _ := lanPair(t)

	aVIP, bVIP := [4]byte{10, 66, 5, 1}, [4]byte{10, 66, 5, 2}
	if err := oa.configure("10.66.5.1", "10.66.0.0/16", map[string]string{"10.66.5.2": hb.ID().String()}); err != nil {
		t.Fatalf("configure A: %v", err)
	}
	if err := ob.configure("10.66.5.2", "10.66.0.0/16", map[string]string{"10.66.5.1": ha.ID().String()}); err != nil {
		t.Fatalf("configure B: %v", err)
	}
	lA, lB := newChanLink(), newChanLink()
	oa.attach(lA)
	ob.attach(lB)
	defer oa.detach()
	defer ob.detach()

	// Force the unproven state (a pong may already have landed from lanPair's ticks): distrust the path exactly
	// as a demote does. The TX gate must reroute over the stream with no other cooperation.
	ma.mu.Lock()
	ma.links[hb.ID()].everPonged = false
	ma.links[hb.ID()].missed = 0
	ma.mu.Unlock()
	if ma.datagramsTrusted(hb.ID()) {
		t.Fatal("test premise broken: path still trusted")
	}

	pkt := ipv4Packet(aVIP, bVIP, []byte("must-arrive-via-stream"))
	lA.in <- pkt
	select {
	case got := <-lB.out:
		if !bytes.Equal(got, pkt) {
			t.Fatalf("delivered packet mangled: %d bytes, want %d", len(got), len(pkt))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("packet lost: unproven path must deliver via the reliable stream")
	}
	ma.mu.Lock()
	streamTx, dgTx := ma.links[hb.ID()].streamTx, ma.links[hb.ID()].dgTx
	ma.mu.Unlock()
	if streamTx == 0 {
		t.Fatalf("delivery did not ride the stream (streamTx=0, dgTx=%d)", dgTx)
	}
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
