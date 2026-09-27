package main

// listenports.go — the node listens on the same ports every run.
//
// A node that restarts under the same identity must be heard again at once. With fresh random ports it was not: a
// peer still held the dead process's QUIC connection (a killed process sends no close; QUIC has no kernel RST, so the
// connection lives until its idle timeout), and the swarm sends every new stream down the connection with the most
// open streams — the dead one, still carrying the old bitswap and friend streams. For ~30 s every reply we were owed
// went nowhere (replication 7: the seeder's HAVEs and blocks for the first 38 s after the receiver restarted; two
// Silent Hill 2 root fetches timed out into a 14-minute gateway crawl). On the SAME port the restarted process gets
// the peer's next packet on that dead connection and answers it with a QUIC stateless reset — valid, because
// go-libp2p derives the reset key from the identity — and the peer drops the connection within a round trip.
//
// So: ONE UDP port for QUIC and WebTransport (quicreuse dials out from any wildcard listener; with two UDP ports a
// dead connection could sit on either), and each port the same on IPv4 and IPv6 (port 0 gives the two families
// different ones, and the one saved need not be the one a connection used). Stable ports also keep the addresses
// friends and the DHT hold for us valid across restarts, and our UPnP mappings.

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"

	"github.com/libp2p/go-libp2p/core/host"
	ma "github.com/multiformats/go-multiaddr"
)

type listenPorts struct {
	TCP int `json:"tcp"`
	UDP int `json:"udp"` // QUIC and WebTransport
	WS  int `json:"ws"`
}

const listenPortsFile = "listen-ports.json"

// loadListenPorts: the ports the last run listened on (zero: none yet).
func loadListenPorts(repo string) listenPorts {
	var p listenPorts
	if b, err := os.ReadFile(filepath.Join(repo, listenPortsFile)); err == nil {
		_ = json.Unmarshal(b, &p) // a damaged file is a first start: fresh ports, saved again
	}
	return p
}

func (p listenPorts) addrs() []string {
	var out []string
	for _, ip := range []string{"/ip4/0.0.0.0", "/ip6/::"} {
		out = append(out, p.tcpAddrs(ip)...)
		out = append(out, p.udpAddrs(ip)...)
		out = append(out, p.wsAddrs(ip)...)
	}
	return out
}

func (p listenPorts) tcpAddrs(ip string) []string {
	return []string{fmt.Sprintf("%s/tcp/%d", ip, p.TCP)}
}
func (p listenPorts) wsAddrs(ip string) []string {
	return []string{fmt.Sprintf("%s/tcp/%d/ws", ip, p.WS)}
}
func (p listenPorts) udpAddrs(ip string) []string {
	return []string{fmt.Sprintf("%s/udp/%d/quic-v1", ip, p.UDP), fmt.Sprintf("%s/udp/%d/quic-v1/webtransport", ip, p.UDP)}
}

// listenAddrs: what the node listens on — every transport, on the ports it used last time (fresh ones the first
// time).
func listenAddrs(repo string) []string {
	p := loadListenPorts(repo)
	if p.TCP == 0 {
		p.TCP = freePort("tcp")
	}
	if p.UDP == 0 {
		p.UDP = freePort("udp")
	}
	if p.WS == 0 {
		p.WS = freePort("tcp")
	}
	return p.addrs()
}

// freePort: a port free on both IPv4 and IPv6 right now (0: none found — the OS then picks, per family).
func freePort(proto string) int {
	for i := 0; i < 16; i++ {
		var port int
		var closers []func() error
		if proto == "tcp" {
			l, err := net.Listen("tcp4", "0.0.0.0:0")
			if err != nil {
				return 0
			}
			port, closers = l.Addr().(*net.TCPAddr).Port, append(closers, l.Close)
			if l6, err := net.Listen("tcp6", fmt.Sprintf("[::]:%d", port)); err == nil {
				closers = append(closers, l6.Close)
			} else {
				port = 0
			}
		} else {
			c, err := net.ListenPacket("udp4", "0.0.0.0:0")
			if err != nil {
				return 0
			}
			port, closers = c.LocalAddr().(*net.UDPAddr).Port, append(closers, c.Close)
			if c6, err := net.ListenPacket("udp6", fmt.Sprintf("[::]:%d", port)); err == nil {
				closers = append(closers, c6.Close)
			} else {
				port = 0
			}
		}
		for _, c := range closers {
			_ = c()
		}
		if port != 0 {
			return port
		}
	}
	return 0
}

// portsOf: the port each transport listens on over IPv4 in addrs (zero: not listening).
func portsOf(addrs []ma.Multiaddr) listenPorts {
	var p listenPorts
	for _, a := range addrs {
		if _, err := a.ValueForProtocol(ma.P_IP4); err != nil {
			continue
		}
		var port int
		if v, err := a.ValueForProtocol(ma.P_TCP); err == nil {
			fmt.Sscan(v, &port)
			if _, ws := a.ValueForProtocol(ma.P_WS); ws == nil {
				p.WS = port
			} else {
				p.TCP = port
			}
			continue
		}
		if v, err := a.ValueForProtocol(ma.P_UDP); err == nil {
			if _, q := a.ValueForProtocol(ma.P_QUIC_V1); q == nil {
				if _, wt := a.ValueForProtocol(ma.P_WEBTRANSPORT); wt != nil { // the QUIC listener names the UDP port
					fmt.Sscan(v, &p.UDP)
				}
			}
		}
	}
	return p
}

// keepListenPorts, once the host listens: a transport whose saved port was taken (another program took it since the
// last run) listens on a fresh one, and the ports in use are saved for the next start.
func keepListenPorts(h host.Host, repo string) {
	have := portsOf(h.Network().ListenAddresses())
	var fresh []ma.Multiaddr
	add := func(ss []string) {
		for _, s := range ss {
			if a, err := ma.NewMultiaddr(s); err == nil {
				fresh = append(fresh, a)
			}
		}
	}
	var p listenPorts // one fresh port per missing transport, the same on both families
	if have.TCP == 0 {
		p.TCP = freePort("tcp")
	}
	if have.UDP == 0 {
		p.UDP = freePort("udp")
	}
	if have.WS == 0 {
		p.WS = freePort("tcp")
	}
	for _, ip := range []string{"/ip4/0.0.0.0", "/ip6/::"} {
		if have.TCP == 0 {
			add(p.tcpAddrs(ip))
		}
		if have.UDP == 0 {
			add(p.udpAddrs(ip))
		}
		if have.WS == 0 {
			add(p.wsAddrs(ip))
		}
	}
	if len(fresh) > 0 {
		if err := h.Network().Listen(fresh...); err != nil {
			fmt.Fprintf(os.Stderr, "[net] listen on fresh ports: %v\n", err)
		}
		have = portsOf(h.Network().ListenAddresses())
	}
	if have == loadListenPorts(repo) {
		return
	}
	b, _ := json.Marshal(have)
	tmp := filepath.Join(repo, listenPortsFile+".tmp")
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "[net] saving listen ports: %v\n", err)
		return
	}
	if err := os.Rename(tmp, filepath.Join(repo, listenPortsFile)); err != nil {
		fmt.Fprintf(os.Stderr, "[net] saving listen ports: %v\n", err)
	}
}
