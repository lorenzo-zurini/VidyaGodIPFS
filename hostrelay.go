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
	bcastTo  []net.IP        // where to re-emit a game broadcast (255.255.255.255 + each iface directed-bcast)
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
	if ip4 := u.Dst.To4(); ip4 != nil && ip4[0] >= 224 && ip4[0] <= 239 {
		// Multicast group: pass through as-is (multicast routing is the switch's job, not ours).
		if _, err := src.WriteToUDP(u.Payload, &net.UDPAddr{IP: ip4, Port: int(u.DstPort)}); err == nil {
			r.txReflected.Add(1)
		}
		return
	}
	// Broadcast: emit to EVERY broadcast target. Real games send to 255.255.255.255 (the limited broadcast), but
	// many networks (WiFi↔wired, AP client isolation) DROP that between hosts while passing the SUBNET-DIRECTED
	// broadcast (e.g. 192.168.1.255) — measured live on this exact machine pair. So we re-emit to both, which is
	// also what lets two reflector-equipped peers discover each other on such a network.
	for _, dst := range r.broadcastTargets() {
		if _, err := src.WriteToUDP(u.Payload, &net.UDPAddr{IP: dst, Port: int(u.DstPort)}); err == nil {
			r.txReflected.Add(1)
		}
	}
}

// broadcastTargets = the limited broadcast plus each IPv4 interface's directed broadcast (recomputed lazily). In
// tests bcastAddr overrides this with a single loopback target.
func (r *hostRelay) broadcastTargets() []net.IP {
	if r.bcastAddr != nil && !r.bcastAddr.Equal(net.IPv4bcast) {
		return []net.IP{r.bcastAddr} // test override (loopback stands in for the broadcast domain)
	}
	r.hostMu.Lock()
	defer r.hostMu.Unlock()
	out := append([]net.IP{net.IPv4bcast}, r.bcastTo...)
	return out
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
	var bcasts []net.IP
	seen := map[string]bool{}
	if addrs, err := net.InterfaceAddrs(); err == nil {
		for _, a := range addrs {
			ipn, ok := a.(*net.IPNet)
			if !ok {
				continue
			}
			m[ipn.IP.String()] = true
			// The directed broadcast of each IPv4 subnet (skip loopback + the /32 host routes that have none).
			if v4 := ipn.IP.To4(); v4 != nil && !v4.IsLoopback() {
				ones, bits := ipn.Mask.Size()
				if bits == 32 && ones < 31 {
					b := make(net.IP, 4)
					for i := 0; i < 4; i++ {
						b[i] = v4[i] | ^ipn.Mask[i]
					}
					if !seen[b.String()] {
						seen[b.String()] = true
						bcasts = append(bcasts, b)
					}
				}
			}
		}
	}
	r.hostMu.Lock()
	r.hostIPs = m
	r.bcastTo = bcasts
	r.hostRefT = time.Now()
	r.hostMu.Unlock()
}
