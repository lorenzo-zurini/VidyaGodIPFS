package main

// hostrelay.go — the real-LAN broadcast reflector: the third plane's missing half. The gateway (natgateway.go)
// gives the sandbox outbound unicast to the real LAN, but LAN-game DISCOVERY is broadcast — and broadcasts don't
// traverse a NAT in either direction. Unprivileged host sockets CAN send and receive real-LAN broadcasts
// (SO_BROADCAST needs no capabilities), so the reflector bridges the two broadcast domains in userspace:
//
//   OUT: every broadcast the game emits (readLoop's fanout branch) is ALSO re-sent as a REAL host broadcast from
//        a host socket bound to the game's source port — so real-LAN players see the sandboxed game's sessions.
//   IN:  the same socket (plus one bound to the destination port) receives real-LAN peers' replies AND their
//        unsolicited announcements; each datagram is synthesized into an IPv4/UDP packet and injected into the
//        TUN — so the sandboxed game's browser lists sessions hosted on the physical LAN.
//
// Ports are LEARNED from the game's own traffic (a LAN protocol announces on the ports it listens on) — no
// whitelist. Loop prevention: datagrams sourced from one of the host's own addresses are dropped (that's our own
// reflection, or another local process). Inbound is delivered UNICAST to the game's vIP: the dplay-era pattern is
// broadcast-request/unicast-reply, and 0.0.0.0-bound game sockets accept unicast regardless.

import (
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const relayMaxPorts = 32 // learned ports are game discovery ports — a handful; cap defends the map

type hostRelay struct {
	inject  func([]byte) error // synthesized packets → the game's TUN
	gameVIP net.IP
	// bcastAddr is 255.255.255.255 in production; tests point it at loopback (real broadcasts don't loop back).
	bcastAddr net.IP

	mu    sync.Mutex
	socks map[uint16]*net.UDPConn // local port → host socket (send + recv)
	done  bool

	hostMu   sync.Mutex
	hostIPs  map[string]bool // our own addresses — loop prevention
	hostRefT time.Time

	rxInjected, txReflected, loopDropped atomic.Int64
}

func newHostRelay(inject func([]byte) error, gameVIP net.IP) *hostRelay {
	r := &hostRelay{inject: inject, gameVIP: gameVIP, bcastAddr: net.IPv4bcast,
		socks: map[uint16]*net.UDPConn{}, hostIPs: map[string]bool{}}
	r.refreshHostIPs()
	return r
}

func (r *hostRelay) close() {
	r.mu.Lock()
	r.done = true
	for _, c := range r.socks {
		_ = c.Close()
	}
	r.socks = map[uint16]*net.UDPConn{}
	r.mu.Unlock()
}

// fromGame reflects one game-emitted broadcast onto the real LAN and makes sure both flow ports are listened on.
func (r *hostRelay) fromGame(u udp4) {
	src := r.ensure(u.SrcPort)
	r.ensure(u.DstPort) // announcements from real-LAN hosts arrive here
	if src == nil {
		return
	}
	dst := r.bcastAddr
	if ip4 := u.Dst.To4(); ip4 != nil && ip4[0] >= 224 && ip4[0] <= 239 {
		dst = ip4 // multicast group: pass through as-is
	}
	if _, err := src.WriteToUDP(u.Payload, &net.UDPAddr{IP: dst, Port: int(u.DstPort)}); err == nil {
		r.txReflected.Add(1)
	}
}

// ensure returns the host socket bound to :port, creating it (with SO_BROADCAST + its recv loop) on first sight.
// nil when the port can't be bound (a host service owns it) or the cap is hit.
func (r *hostRelay) ensure(port uint16) *net.UDPConn {
	if port == 0 {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.done {
		return nil
	}
	if c := r.socks[port]; c != nil {
		return c
	}
	if len(r.socks) >= relayMaxPorts {
		return nil
	}
	c, err := listenBroadcastUDP(int(port))
	if err != nil {
		return nil // port taken by a host service — reflection for it is simply off
	}
	r.socks[port] = c
	go r.recvLoop(port, c)
	return c
}

// recvLoop turns every real-LAN datagram on :port into an in-sandbox packet (src = the real peer, dst = the
// game's vIP), skipping our own reflections.
func (r *hostRelay) recvLoop(port uint16, c *net.UDPConn) {
	buf := make([]byte, 64<<10)
	for {
		n, from, err := c.ReadFromUDP(buf)
		if err != nil {
			return // socket closed
		}
		if from == nil || from.IP == nil {
			continue
		}
		if r.isHostIP(from.IP) {
			r.loopDropped.Add(1)
			continue
		}
		pkt := buildUDP4(from.IP.To4(), r.gameVIP.To4(), uint16(from.Port), port, buf[:n])
		if r.inject(pkt) == nil {
			r.rxInjected.Add(1)
		}
	}
}

// isHostIP reports whether ip is one of this machine's own addresses (refreshed lazily — interfaces change).
func (r *hostRelay) isHostIP(ip net.IP) bool {
	r.hostMu.Lock()
	defer r.hostMu.Unlock()
	if time.Since(r.hostRefT) > 30*time.Second {
		r.hostMu.Unlock()
		r.refreshHostIPs()
		r.hostMu.Lock()
	}
	return r.hostIPs[ip.String()]
}

func (r *hostRelay) refreshHostIPs() {
	m := map[string]bool{}
	if addrs, err := net.InterfaceAddrs(); err == nil {
		for _, a := range addrs {
			if ipn, ok := a.(*net.IPNet); ok {
				m[ipn.IP.String()] = true
			}
		}
	}
	r.hostMu.Lock()
	r.hostIPs = m
	r.hostRefT = time.Now()
	r.hostMu.Unlock()
}
