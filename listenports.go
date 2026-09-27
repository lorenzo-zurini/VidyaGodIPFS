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
// go-libp2p derives the reset key from the identity — and the peer drops the connection within a round trip. That
// one packet is still lost (replication 8: the seeder heard again 2 s after the first fetch).
//
// So: ONE UDP port for QUIC and WebTransport (quicreuse dials out from any wildcard listener; with two UDP ports a
// dead connection could sit on either), and each port the same on IPv4 and IPv6 (port 0 gives the two families
// different ones, and the one saved need not be the one a connection used). A saved port that cannot be had — taken,
// excluded (Windows reshuffles its excluded ranges at boot), a damaged file — is replaced by a fresh one for that
// run: the node always listens. Stable ports also keep the addresses friends and the DHT hold for us valid across
// restarts, and our UPnP mappings.
//
// Known limit: TCP listens with SO_REUSEPORT (libp2p's default), so a saved TCP port still held by a live process —
// the previous run not yet exited — is shared with it, not replaced; the kernel splits new TCP connections between
// the two until it exits.

import (
	"encoding/json"
	"errors"
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

// loadListenPorts: the ports the last run listened on (zero: none yet). A value that is no port fails to listen and
// is replaced like a taken one.
func loadListenPorts(repo string) listenPorts {
	var p listenPorts
	if b, err := os.ReadFile(filepath.Join(repo, listenPortsFile)); err == nil {
		_ = json.Unmarshal(b, &p) // a damaged file is a first start: fresh ports, saved again
	}
	return p
}

// transport: one kind of listener, on both families.
type transport struct {
	proto string // what freePort probes: "tcp" or "udp"
	port  func(*listenPorts) *int
	addrs func(ip string, port int) []string
}

var transports = []transport{
	{"tcp", func(p *listenPorts) *int { return &p.TCP }, func(ip string, port int) []string {
		return []string{fmt.Sprintf("%s/tcp/%d", ip, port)}
	}},
	{"udp", func(p *listenPorts) *int { return &p.UDP }, func(ip string, port int) []string {
		return []string{fmt.Sprintf("%s/udp/%d/quic-v1", ip, port), fmt.Sprintf("%s/udp/%d/quic-v1/webtransport", ip, port)}
	}},
	{"tcp", func(p *listenPorts) *int { return &p.WS }, func(ip string, port int) []string {
		return []string{fmt.Sprintf("%s/tcp/%d/ws", ip, port)}
	}},
}

const anyIP4, anyIP6 = "/ip4/0.0.0.0", "/ip6/::"

// listenOn makes a host built with no listen addresses listen on every transport, on the ports of the last run; a
// transport whose port cannot be had gets a fresh one (on IPv6 its own, if only IPv6 refuses it). The ports in use
// are saved for the next start. Fails only if no transport listens (the relay's /p2p-circuit always "listens").
func listenOn(h host.Host, repo string) error {
	saved := loadListenPorts(repo)
	for _, t := range transports {
		port := *t.port(&saved)
		if port == 0 {
			port = freePort(t.proto)
		}
		if port == 0 || !listen(h, t.addrs(anyIP4, port)) {
			fresh := freePort(t.proto)
			if s := *t.port(&saved); s != 0 {
				fmt.Fprintf(os.Stderr, "[net] saved port %d (%s) cannot be had — listening on %d this run\n", s, t.addrs(anyIP4, s)[0], fresh)
			}
			port = fresh
			if !listen(h, t.addrs(anyIP4, port)) {
				fmt.Fprintf(os.Stderr, "[net] not listening on %s: no port\n", t.addrs(anyIP4, port)[0])
			}
		}
		if !listen(h, t.addrs(anyIP6, port)) && ipv6Available() {
			p6 := freePortOn(t.proto + "6")
			fmt.Fprintf(os.Stderr, "[net] IPv6 refused port %d — %s listens on %d\n", port, t.proto, p6)
			if p6 == 0 || !listen(h, t.addrs(anyIP6, p6)) {
				fmt.Fprintf(os.Stderr, "[net] not listening on %s\n", t.addrs(anyIP6, p6)[0])
			}
		}
	}
	var direct []ma.Multiaddr
	for _, a := range h.Network().ListenAddresses() {
		if _, err := a.ValueForProtocol(ma.P_CIRCUIT); err != nil {
			direct = append(direct, a)
		}
	}
	if len(direct) == 0 {
		return errors.New("listening on no transport")
	}
	have := portsOf(direct)
	if have == saved {
		return nil
	}
	b, _ := json.Marshal(have)
	tmp := filepath.Join(repo, listenPortsFile+".tmp")
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "[net] saving listen ports: %v\n", err)
		return nil
	}
	if err := os.Rename(tmp, filepath.Join(repo, listenPortsFile)); err != nil {
		fmt.Fprintf(os.Stderr, "[net] saving listen ports: %v\n", err)
	}
	return nil
}

// listen: the host listens on all of ss (a family of one transport) or on none of them — a QUIC listener left on a
// port whose WebTransport refused would split the two onto separate UDP ports.
func listen(h host.Host, ss []string) bool {
	var done []ma.Multiaddr
	for _, s := range ss {
		a, err := ma.NewMultiaddr(s)
		if err == nil {
			err = h.Network().Listen(a)
		}
		if err != nil {
			if c, ok := h.Network().(interface{ ListenClose(...ma.Multiaddr) }); ok && len(done) > 0 {
				c.ListenClose(done...)
			}
			return false
		}
		done = append(done, a)
	}
	return true
}

// freePortOn: a port free on one network ("tcp6", "udp6", …) right now (0: none).
func freePortOn(network string) int {
	if network == "tcp4" || network == "tcp6" {
		l, err := net.Listen(network, ":0")
		if err != nil {
			return 0
		}
		defer l.Close()
		return l.Addr().(*net.TCPAddr).Port
	}
	c, err := net.ListenPacket(network, ":0")
	if err != nil {
		return 0
	}
	defer c.Close()
	return c.LocalAddr().(*net.UDPAddr).Port
}

// freePort: a port free on both families right now — or on the one this host has (0: none found; the OS picks).
func freePort(proto string) int {
	v6 := ipv6Available()
	for i := 0; i < 16; i++ {
		port, closers := 0, []func() error{}
		if proto == "tcp" {
			if l, err := net.Listen("tcp4", "0.0.0.0:0"); err == nil {
				port, closers = l.Addr().(*net.TCPAddr).Port, append(closers, l.Close)
			}
			if l6, err := net.Listen("tcp6", fmt.Sprintf("[::]:%d", port)); err == nil {
				if port == 0 {
					port = l6.Addr().(*net.TCPAddr).Port
				}
				closers = append(closers, l6.Close)
			} else if v6 {
				port = 0 // free on IPv4 only: try another
			}
		} else {
			if c, err := net.ListenPacket("udp4", "0.0.0.0:0"); err == nil {
				port, closers = c.LocalAddr().(*net.UDPAddr).Port, append(closers, c.Close)
			}
			if c6, err := net.ListenPacket("udp6", fmt.Sprintf("[::]:%d", port)); err == nil {
				if port == 0 {
					port = c6.LocalAddr().(*net.UDPAddr).Port
				}
				closers = append(closers, c6.Close)
			} else if v6 {
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

// ipv6Available: this host can bind IPv6 at all.
func ipv6Available() bool {
	c, err := net.ListenPacket("udp6", "[::1]:0")
	if err != nil {
		return false
	}
	_ = c.Close()
	return true
}

// portsOf: the port each transport listens on in addrs — its IPv4 one, else its IPv6 one (zero: not listening).
func portsOf(addrs []ma.Multiaddr) listenPorts {
	var v4, v6 listenPorts
	for _, a := range addrs {
		p := &v6
		if _, err := a.ValueForProtocol(ma.P_IP4); err == nil {
			p = &v4
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
					fmt.Sscan(v, &port)
					p.UDP = port
				}
			}
		}
	}
	for _, t := range transports {
		if *t.port(&v4) == 0 {
			*t.port(&v4) = *t.port(&v6)
		}
	}
	return v4
}
