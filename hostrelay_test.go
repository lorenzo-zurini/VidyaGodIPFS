package main

// hostrelay_test.go — the broadcast reflector over REAL sockets on loopback. Real 255.255.255.255 broadcasts
// don't loop back, so the relay's broadcast address is pointed at 127.0.0.1: outbound reflection is then
// observable by a plain listener, and inbound injection is driven by a "real-LAN peer" socket sending to the
// relay's learned port.

import (
	"bytes"
	"net"
	"testing"
	"time"
)

func waitCond(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

func TestHostRelayReflectsBothWays(t *testing.T) {
	vip := net.IPv4(10, 66, 42, 7)
	var mu = make(chan []byte, 16)
	r := newHostRelay(func(pkt []byte) error { mu <- append([]byte(nil), pkt...); return nil }, vip)
	defer r.close()
	r.bcastAddr = net.IPv4(127, 0, 0, 1) // loopback stands in for the broadcast domain

	// The "real-LAN peer": a plain UDP socket. It must NOT be a host-IP-loop victim — but 127.0.0.1 IS a host
	// address, so disarm loop prevention for the test (the loop-drop path gets its own assertion below).
	peer, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	peerPort := peer.LocalAddr().(*net.UDPAddr).Port

	// OUTBOUND: the game "broadcasts" src=34567 dst=peerPort → the relay reflects it to bcastAddr:peerPort.
	game := udp4{Src: vip, Dst: net.IPv4bcast, SrcPort: 34567, DstPort: uint16(peerPort), Payload: []byte("announce")}
	r.hostMu.Lock()
	r.hostIPs = map[string]bool{} // allow loopback traffic through for this test
	r.hostRefT = time.Now().Add(time.Hour)
	r.hostMu.Unlock()
	r.fromGame(game)

	buf := make([]byte, 256)
	_ = peer.SetReadDeadline(time.Now().Add(3 * time.Second))
	n, from, err := peer.ReadFromUDP(buf)
	if err != nil {
		t.Fatalf("real-LAN peer never saw the reflected broadcast: %v", err)
	}
	if !bytes.Equal(buf[:n], []byte("announce")) {
		t.Fatalf("payload mismatch: %q", buf[:n])
	}

	// INBOUND: the peer replies to the socket that sent (bound :34567) → synthesized into the TUN as unicast to
	// the game's vIP with the peer as source.
	if _, err := peer.WriteToUDP([]byte("session-info"), from); err != nil {
		t.Fatal(err)
	}
	select {
	case pkt := <-mu:
		u, ok := parseUDP4(pkt)
		if !ok {
			t.Fatal("injected packet unparsable")
		}
		if !u.Dst.Equal(vip) || u.DstPort != 34567 || !bytes.Equal(u.Payload, []byte("session-info")) {
			t.Fatalf("bad injected packet: %+v", u)
		}
		if int(u.SrcPort) != peerPort {
			t.Fatalf("src port %d, want the real peer's %d", u.SrcPort, peerPort)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("inbound datagram never injected")
	}

	// LOOP PREVENTION: with host IPs armed (loopback included), the same inbound is dropped.
	r.refreshHostIPs()
	drops := r.loopDropped.Load()
	if _, err := peer.WriteToUDP([]byte("self-echo"), from); err != nil {
		t.Fatal(err)
	}
	waitCond(t, "loop drop", func() bool { return r.loopDropped.Load() > drops })
	select {
	case pkt := <-mu:
		t.Fatalf("looped packet was injected: %v", pkt)
	default:
	}
}

func TestHostRelayPortCapAndConflict(t *testing.T) {
	r := newHostRelay(func([]byte) error { return nil }, net.IPv4(10, 66, 1, 1))
	defer r.close()
	// A port already owned by another socket WITHOUT SO_REUSEADDR still binds ours (we set REUSEADDR)… so use the
	// cap as the deterministic failure: fill it, then one more port must be refused.
	for p := 0; p < relayMaxPorts; p++ {
		if r.ensure(uint16(20000+p)) == nil {
			t.Fatalf("bind failed below cap at %d", p)
		}
	}
	if r.ensure(30000) != nil {
		t.Fatal("cap not enforced")
	}
}
