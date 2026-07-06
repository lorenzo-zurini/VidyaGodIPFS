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
	oA := newOverlayService(ctx, hA, nil)
	oB := newOverlayService(ctx, hB, nil)
	oA.start()
	oB.start()

	aVIP := [4]byte{10, 66, 5, 1}
	bVIP := [4]byte{10, 66, 5, 2}
	// A knows B owns 10.66.5.2; B knows A owns 10.66.5.1.
	if err := oA.configure("10.66.5.1", map[string]string{"10.66.5.2": hB.ID().String()}); err != nil {
		t.Fatalf("configure A: %v", err)
	}
	if err := oB.configure("10.66.5.2", map[string]string{"10.66.5.1": hA.ID().String()}); err != nil {
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
