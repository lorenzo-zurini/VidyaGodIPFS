package main

// lanpath.go — a peer on the local network is dialed across it first, not around it.
//
// Two machines on one home network often also share a VPN (WireGuard, Tailscale) or a relay, and a peer advertises
// every address it has. libp2p dials private addresses alike, so a connection could land on the VPN: a Steam Deck
// receiving a game from the PC across the room got it through a WireGuard hub on the internet (39 ms, capped by the
// uplink, ~10 MB/s) instead of across the room (2 ms). The dial ranker here dials the addresses on THIS machine's
// local network first and everything else only after them, so a new connection to a peer beside us is a local one.
//
// "Local network" is the subnet of the interface the OS sends internet traffic from — the home LAN — and nothing
// else: not a Docker bridge nor a ZeroTier network (each a network this machine is on, but not the one it is in a
// room on), and never a tunnel: with a full-tunnel VPN the OS sends from the tunnel, and then nothing is local. An existing connection is never touched (NETWORK.md invariant 1): one
// made around the LAN stays until it ends by itself, and the next one is local.

import (
	"net"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/p2p/net/swarm"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

// localFirstDelay: how long after the local addresses the rest are dialed — a local dial answers in milliseconds.
const localFirstDelay = 500 * time.Millisecond

// iface: what localNetsFor needs of an interface.
type iface struct {
	flags net.Flags
	hw    net.HardwareAddr
	addrs []net.Addr
}

// localNetsFor: the subnets of the interface holding one of the outbound source addresses (the address the OS sends
// internet traffic from, per IP version) — when that interface is a network one is IN: up, not loopback, not a
// point-to-point tunnel, with a hardware address (a WireGuard, Tailscale or Wintun tunnel has none). With a full-tunnel
// VPN the source is the tunnel's, and nothing is local: libp2p's own ranking then. Link-local subnets never.
func localNetsFor(sources []net.IP, ifs []iface) []*net.IPNet {
	var out []*net.IPNet
	for _, i := range ifs {
		if i.flags&net.FlagUp == 0 || i.flags&(net.FlagLoopback|net.FlagPointToPoint) != 0 || len(i.hw) == 0 {
			continue
		}
		for _, a := range i.addrs {
			n, ok := a.(*net.IPNet)
			if !ok || n.IP.IsLinkLocalUnicast() || n.IP.IsLoopback() {
				continue
			}
			for _, s := range sources {
				if s != nil && n.IP.Equal(s) {
					out = append(out, &net.IPNet{IP: n.IP.Mask(n.Mask), Mask: n.Mask})
					break
				}
			}
		}
	}
	return out
}

// interfaces: this machine's interfaces that could be the one it is out on, with their addresses (only those are
// listed: a host with dozens of container veths lists their addresses for nothing).
func interfaces() []iface {
	ifs, _ := net.Interfaces()
	var out []iface
	for _, i := range ifs {
		if i.Flags&net.FlagUp == 0 || i.Flags&(net.FlagLoopback|net.FlagPointToPoint) != 0 || len(i.HardwareAddr) == 0 {
			continue
		}
		as, err := i.Addrs()
		if err != nil {
			continue
		}
		out = append(out, iface{flags: i.Flags, hw: i.HardwareAddr, addrs: as})
	}
	return out
}

// outboundSources: the source address the OS picks to reach the internet, per IP version (a UDP "connect" sends
// nothing: it only asks the routing table).
func outboundSources() []net.IP {
	var out []net.IP
	for _, target := range []string{"1.1.1.1:53", "[2606:4700:4700::1111]:53"} {
		c, err := net.Dial("udp", target)
		if err != nil {
			continue
		}
		if a, ok := c.LocalAddr().(*net.UDPAddr); ok {
			out = append(out, a.IP)
		}
		_ = c.Close()
	}
	return out
}

// localNets: this machine's local network now, read again at most every 30 s (it changes when the machine moves).
var localNets = func() func() []*net.IPNet {
	var mu sync.Mutex
	var at time.Time
	var nets []*net.IPNet
	return func() []*net.IPNet {
		mu.Lock()
		defer mu.Unlock()
		if time.Since(at) > 30*time.Second {
			nets = localNetsFor(outboundSources(), interfaces())
			at = time.Now()
		}
		return nets
	}
}()

// onLocalNet: a direct address (no relay hop) whose IP lies on one of nets.
func onLocalNet(a ma.Multiaddr, nets []*net.IPNet) bool {
	if _, err := a.ValueForProtocol(ma.P_CIRCUIT); err == nil {
		return false
	}
	ip, err := manet.ToIP(a)
	if err != nil {
		return false
	}
	for _, n := range nets {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}

// localFirstRanker ranks the local addresses as libp2p would, first, and the rest as it would, after them. A peer
// with no local address (or only local ones) is ranked exactly as libp2p ranks it.
func localFirstRanker(nets func() []*net.IPNet) network.DialRanker {
	return func(addrs []ma.Multiaddr) []network.AddrDelay {
		ns := nets()
		var local, rest []ma.Multiaddr
		for _, a := range addrs {
			if onLocalNet(a, ns) {
				local = append(local, a)
			} else {
				rest = append(rest, a)
			}
		}
		if len(local) == 0 || len(rest) == 0 {
			return swarm.DefaultDialRanker(addrs)
		}
		out := swarm.DefaultDialRanker(local)
		var last time.Duration
		for _, d := range out {
			if d.Delay > last {
				last = d.Delay
			}
		}
		for _, d := range swarm.DefaultDialRanker(rest) {
			d.Delay += last + localFirstDelay
			out = append(out, d)
		}
		return out
	}
}
