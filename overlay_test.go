package main

// overlay_test.go — Spike 0, headless and rootless: two overlayServices wired to a real in-process libp2p host pair,
// each with a fake channel-backed link. A raw IPv4 packet injected on node A with node B's vIP as its destination
// must emerge, byte-for-byte, from node B's link — proving the L3 datapath (route-by-dest-IP → correct peer →
// deliver) works over libp2p without any TUN, netns, root, or userspace TCP/IP stack.

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	host "github.com/libp2p/go-libp2p/core/host"
)

// chanLink is a fake packetLink: ReadPacket drains `in` (packets the "OS/game" sent), WritePacket pushes to `out`
// (packets delivered back to the "OS/game").
type chanLink struct {
	in     chan []byte
	out    chan []byte
	closed chan struct{}
}

func newChanLink() *chanLink {
	return &chanLink{in: make(chan []byte, 8), out: make(chan []byte, 8), closed: make(chan struct{})}
}

func (c *chanLink) ReadPacket() ([]byte, error) {
	select {
	case p := <-c.in:
		return p, nil
	case <-c.closed:
		return nil, io.EOF
	}
}

func (c *chanLink) WritePacket(p []byte) error {
	cp := append([]byte(nil), p...)
	select {
	case c.out <- cp:
		return nil
	case <-c.closed:
		return io.EOF
	}
}

func (c *chanLink) Close() error {
	select {
	case <-c.closed:
	default:
		close(c.closed)
	}
	return nil
}

// ipv4Packet crafts a minimal (header-only) IPv4 packet with the given src/dst dotted-quad octets and a payload. The
// overlay only reads the destination, so the header need not be otherwise valid.
func ipv4Packet(src, dst [4]byte, payload []byte) []byte {
	pkt := make([]byte, 20+len(payload))
	pkt[0] = 0x45 // version 4, IHL 5
	total := uint16(len(pkt))
	pkt[2] = byte(total >> 8)
	pkt[3] = byte(total)
	pkt[9] = 17 // UDP, arbitrary
	copy(pkt[12:16], src[:])
	copy(pkt[16:20], dst[:])
	copy(pkt[20:], payload)
	return pkt
}

func TestOverlayForwardsPacketToOwningPeer(t *testing.T) {
	hA, hB := testHost(t), testHost(t) // helpers from friend_test.go
	connectHosts(t, hA, hB)

	ctx := context.Background()
	oA := newOverlayService(ctx, hA, nil, nil)
	oB := newOverlayService(ctx, hB, nil, nil)
	oA.start()
	oB.start()

	aVIP := [4]byte{10, 66, 5, 1}
	bVIP := [4]byte{10, 66, 5, 2}
	// A knows B owns 10.66.5.2; B knows A owns 10.66.5.1.
	if err := oA.configure("10.66.5.1", "10.66.0.0/16", map[string]string{"10.66.5.2": hB.ID().String()}); err != nil {
		t.Fatalf("configure A: %v", err)
	}
	if err := oB.configure("10.66.5.2", "10.66.0.0/16", map[string]string{"10.66.5.1": hA.ID().String()}); err != nil {
		t.Fatalf("configure B: %v", err)
	}

	lA, lB := newChanLink(), newChanLink()
	oA.attach(lA)
	oB.attach(lB)
	defer oA.detach()
	defer oB.detach()

	// Inject a packet on A destined for B's vIP → it should surface on B's link unchanged.
	payload := []byte("hello-overlay")
	pkt := ipv4Packet(aVIP, bVIP, payload)
	lA.in <- pkt
	select {
	case got := <-lB.out:
		if !bytes.Equal(got, pkt) {
			t.Fatalf("B received a mangled packet: got %d bytes, want %d", len(got), len(pkt))
		}
		if !bytes.Contains(got, payload) {
			t.Fatalf("payload missing from delivered packet")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("packet never arrived at B")
	}

	// Reverse direction on the same mesh: B → A.
	pkt2 := ipv4Packet(bVIP, aVIP, []byte("reply"))
	lB.in <- pkt2
	select {
	case got := <-lA.out:
		if !bytes.Equal(got, pkt2) {
			t.Fatalf("A received a mangled packet")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("reply never arrived at A")
	}

	// A packet to an unknown vIP must be dropped (no owner), not crash or misdeliver.
	lA.in <- ipv4Packet(aVIP, [4]byte{10, 66, 5, 99}, []byte("void"))
	select {
	case <-lB.out:
		t.Fatal("packet to an unrouted vIP should not have been delivered")
	case <-time.After(300 * time.Millisecond):
		// expected: dropped
	}
}

// quicHost is a real libp2p host on QUIC (not TCP), so the overlay's datagram fast path is exercisable — quicConnTo
// unwraps a direct QUIC connection to a *quic.Conn, and quic-go datagrams (RFC 9221) carry the packets.
func quicHost(t *testing.T) host.Host {
	t.Helper()
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/udp/0/quic-v1"))
	if err != nil {
		t.Fatalf("quic host: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	return h
}

// The datagram fast path: over a DIRECT QUIC connection the overlay must ship IP packets as unreliable QUIC datagrams
// (no reliable stream), and they must still arrive byte-for-byte on the peer's link. Proves the new transport works,
// not just the stream fallback that the TCP-host tests cover.
func TestOverlayDatagramFastPathOverQUIC(t *testing.T) {
	hA, hB := quicHost(t), quicHost(t)
	connectHosts(t, hA, hB)

	// Precondition: a direct QUIC connection unwraps to a *quic.Conn on BOTH sides (else we'd be testing the fallback).
	if quicConnTo(hA, hB.ID()) == nil || quicConnTo(hB, hA.ID()) == nil {
		t.Fatal("expected a direct QUIC connection so the datagram path is used")
	}

	ctx := context.Background()
	oA, oB := newOverlayService(ctx, hA, nil, nil), newOverlayService(ctx, hB, nil, nil)
	oA.start()
	oB.start()
	if err := oA.configure("10.66.9.1", "10.66.0.0/16", map[string]string{"10.66.9.2": hB.ID().String()}); err != nil {
		t.Fatalf("configure A: %v", err)
	}
	if err := oB.configure("10.66.9.2", "10.66.0.0/16", map[string]string{"10.66.9.1": hA.ID().String()}); err != nil {
		t.Fatalf("configure B: %v", err)
	}

	// configure() must have started a datagram receive loop on the existing direct QUIC conn (else inbound would only
	// arrive via the stream handler and this wouldn't test datagrams).
	oB.dgMu.Lock()
	nLoops := len(oB.dgRecv)
	oB.dgMu.Unlock()
	if nLoops == 0 {
		t.Fatal("B did not start a datagram receive loop on its direct QUIC conn to A")
	}

	lA, lB := newChanLink(), newChanLink()
	oA.attach(lA)
	oB.attach(lB)
	defer oA.detach()
	defer oB.detach()

	// A datagram is unreliable: retry a few packets to tolerate a startup-race drop, but each MUST arrive intact.
	payload := []byte("datagram-hello")
	pkt := ipv4Packet([4]byte{10, 66, 9, 1}, [4]byte{10, 66, 9, 2}, payload)
	var got []byte
	deadline := time.After(5 * time.Second)
	for got == nil {
		select {
		case <-deadline:
			t.Fatal("no datagram arrived at B within 5s")
		default:
		}
		lA.in <- pkt
		select {
		case got = <-lB.out:
		case <-time.After(250 * time.Millisecond):
		}
	}
	if !bytes.Equal(got, pkt) {
		t.Fatalf("B received a mangled datagram: %d bytes, want %d", len(got), len(pkt))
	}
}

// A broadcast packet (dst = the /16 directed broadcast) must be replicated to EVERY LAN peer — this is what makes
// classic LAN-game discovery work over the routed overlay (which has no real broadcast domain).
func TestOverlayFansOutBroadcast(t *testing.T) {
	hA, hB, hC := testHost(t), testHost(t), testHost(t)
	connectHosts(t, hA, hB)
	connectHosts(t, hA, hC)

	ctx := context.Background()
	oA, oB, oC := newOverlayService(ctx, hA, nil, nil), newOverlayService(ctx, hB, nil, nil), newOverlayService(ctx, hC, nil, nil)
	oA.start()
	oB.start()
	oC.start()

	// A routes to both B and C (its two online LAN friends).
	if err := oA.configure("10.66.0.1", "10.66.0.0/16", map[string]string{
		"10.66.0.2": hB.ID().String(), "10.66.0.3": hC.ID().String(),
	}); err != nil {
		t.Fatalf("configure A: %v", err)
	}
	_ = oB.configure("10.66.0.2", "10.66.0.0/16", map[string]string{"10.66.0.1": hA.ID().String()})
	_ = oC.configure("10.66.0.3", "10.66.0.0/16", map[string]string{"10.66.0.1": hA.ID().String()})

	lA, lB, lC := newChanLink(), newChanLink(), newChanLink()
	oA.attach(lA)
	oB.attach(lB)
	oC.attach(lC)
	defer oA.detach()
	defer oB.detach()
	defer oC.detach()

	// A broadcasts to 10.66.255.255 → both B and C must receive it.
	bcast := ipv4Packet([4]byte{10, 66, 0, 1}, [4]byte{10, 66, 255, 255}, []byte("who-is-there"))
	lA.in <- bcast
	for name, l := range map[string]*chanLink{"B": lB, "C": lC} {
		select {
		case got := <-l.out:
			if !bytes.Equal(got, bcast) {
				t.Fatalf("%s received a mangled broadcast", name)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("broadcast never reached %s (fan-out failed)", name)
		}
	}
}
