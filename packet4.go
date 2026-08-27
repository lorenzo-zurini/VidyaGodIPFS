package main

// packet4.go — minimal IPv4/UDP packet parse + build, shared by the host-LAN broadcast reflector (hostrelay.go)
// and the gateway's DNS synthesizer (gateway.go). Hand-rolled on purpose: these are the ONLY two shapes we ever
// synthesize, and the writers (a game's kernel / our own builder) always produce well-formed headers — a full
// packet library would be dead weight next to gvisor, which handles everything else.

import (
	"encoding/binary"
	"net"
)

// udp4 is one parsed IPv4/UDP datagram.
type udp4 struct {
	Src     net.IP // 4-byte
	Dst     net.IP
	SrcPort uint16
	DstPort uint16
	Payload []byte // aliases the input packet — copy before retaining
}

// parseUDP4 dissects an IPv4/UDP packet. ok=false for anything else (v6, TCP, fragments' tails, truncated).
func parseUDP4(pkt []byte) (u udp4, ok bool) {
	if len(pkt) < 20 || pkt[0]>>4 != 4 {
		return u, false
	}
	ihl := int(pkt[0]&0x0f) * 4
	if ihl < 20 || len(pkt) < ihl+8 || pkt[9] != 17 { // proto 17 = UDP
		return u, false
	}
	if pkt[6]&0x1f != 0 || pkt[7] != 0 { // fragment offset ≠ 0 → not the first fragment; skip
		return u, false
	}
	u.Src = net.IP(pkt[12:16])
	u.Dst = net.IP(pkt[16:20])
	u.SrcPort = binary.BigEndian.Uint16(pkt[ihl:])
	u.DstPort = binary.BigEndian.Uint16(pkt[ihl+2:])
	ulen := int(binary.BigEndian.Uint16(pkt[ihl+4:]))
	if ulen < 8 || ihl+ulen > len(pkt) {
		return u, false
	}
	u.Payload = pkt[ihl+8 : ihl+ulen]
	return u, true
}

// buildUDP4 assembles a checksummed IPv4/UDP packet (20-byte header, no options, DF clear, TTL 64).
func buildUDP4(src, dst net.IP, sport, dport uint16, payload []byte) []byte {
	src, dst = src.To4(), dst.To4()
	ulen := 8 + len(payload)
	pkt := make([]byte, 20+ulen)

	pkt[0] = 0x45 // v4, IHL 5
	binary.BigEndian.PutUint16(pkt[2:], uint16(len(pkt)))
	pkt[8] = 64 // TTL
	pkt[9] = 17 // UDP
	copy(pkt[12:16], src)
	copy(pkt[16:20], dst)
	binary.BigEndian.PutUint16(pkt[10:], ipChecksum(pkt[:20]))

	udp := pkt[20:]
	binary.BigEndian.PutUint16(udp[0:], sport)
	binary.BigEndian.PutUint16(udp[2:], dport)
	binary.BigEndian.PutUint16(udp[4:], uint16(ulen))
	copy(udp[8:], payload)
	binary.BigEndian.PutUint16(udp[6:], udpChecksum(src, dst, udp))
	return pkt
}

// ipChecksum — RFC 791 header checksum over hdr (its checksum field must be zero on entry).
func ipChecksum(hdr []byte) uint16 {
	var sum uint32
	for i := 0; i+1 < len(hdr); i += 2 {
		sum += uint32(binary.BigEndian.Uint16(hdr[i:]))
	}
	for sum > 0xffff {
		sum = (sum >> 16) + (sum & 0xffff)
	}
	return ^uint16(sum)
}

// udpChecksum — UDP checksum with the IPv4 pseudo-header (0 is transmitted as 0xffff per RFC 768).
func udpChecksum(src, dst net.IP, udp []byte) uint16 {
	var sum uint32
	add16 := func(b []byte) {
		for i := 0; i+1 < len(b); i += 2 {
			sum += uint32(binary.BigEndian.Uint16(b[i:]))
		}
		if len(b)%2 == 1 {
			sum += uint32(b[len(b)-1]) << 8
		}
	}
	add16(src)
	add16(dst)
	sum += 17 // proto
	sum += uint32(len(udp))
	add16(udp)
	for sum > 0xffff {
		sum = (sum >> 16) + (sum & 0xffff)
	}
	c := ^uint16(sum)
	if c == 0 {
		c = 0xffff
	}
	return c
}
