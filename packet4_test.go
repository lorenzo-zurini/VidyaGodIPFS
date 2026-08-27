package main

import (
	"bytes"
	"net"
	"testing"

	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/checksum"
	"gvisor.dev/gvisor/pkg/tcpip/header"
)

func TestUDP4RoundTrip(t *testing.T) {
	src, dst := net.IPv4(192, 168, 1, 50).To4(), net.IPv4(10, 66, 235, 0).To4()
	payload := []byte("dplay session announce")
	pkt := buildUDP4(src, dst, 6073, 6073, payload)

	u, ok := parseUDP4(pkt)
	if !ok {
		t.Fatal("parseUDP4 rejected our own packet")
	}
	if !u.Src.Equal(src) || !u.Dst.Equal(dst) || u.SrcPort != 6073 || u.DstPort != 6073 || !bytes.Equal(u.Payload, payload) {
		t.Fatalf("round-trip mismatch: %+v", u)
	}
}

// The checksums must satisfy an INDEPENDENT implementation — gvisor's, since we ship it anyway.
func TestUDP4ChecksumsAgainstGvisor(t *testing.T) {
	src, dst := net.IPv4(10, 66, 1, 2).To4(), net.IPv4(255, 255, 255, 255).To4()
	pkt := buildUDP4(src, dst, 34567, 6073, []byte{1, 2, 3, 4, 5})

	ip := header.IPv4(pkt)
	if !ip.IsChecksumValid() {
		t.Fatal("IPv4 header checksum invalid per gvisor")
	}
	udp := header.UDP(pkt[20:])
	sa, da := tcpip.AddrFrom4([4]byte(src)), tcpip.AddrFrom4([4]byte(dst))
	// IsChecksumValid folds the pseudo-header in itself — pass only the payload's own checksum.
	if !udp.IsChecksumValid(sa, da, checksum.Checksum(udp.Payload(), 0)) {
		t.Fatal("UDP checksum invalid per gvisor")
	}
}

func TestParseUDP4Rejects(t *testing.T) {
	if _, ok := parseUDP4([]byte{0x60, 0, 0, 0}); ok { // IPv6
		t.Fatal("accepted IPv6")
	}
	tcp := buildUDP4(net.IPv4(1, 2, 3, 4), net.IPv4(5, 6, 7, 8), 1, 2, nil)
	tcp[9] = 6 // flip proto to TCP
	if _, ok := parseUDP4(tcp); ok {
		t.Fatal("accepted TCP")
	}
	if _, ok := parseUDP4([]byte{0x45}); ok {
		t.Fatal("accepted truncated")
	}
}
