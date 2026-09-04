package main

// natgateway.go — the in-node userspace NAT: the piece that makes the sandbox's single TUN a full internet uplink
// WITHOUT any external helper (no pasta/slirp) and without privileges. The overlay readLoop hands every packet
// that is neither overlay-unicast nor broadcast/multicast to this gateway; a gVisor netstack (pure Go, the
// tun2socks/Tailscale architecture) terminates the game's TCP/UDP flows in-process and re-issues them as ordinary
// host sockets — so the game's traffic leaves through the host's real connectivity (internet AND real-LAN
// unicast), while the game itself keeps ONE identity: its vIP.
//
//   game (netns, vIP) ──TUN──> readLoop ──inject──> netstack NIC (promiscuous+spoofing: accepts ANY dst)
//                                                     ├── TCP forwarder → net.Dial(host) ⟷ splice
//                                                     ├── UDP forwarder → host UDP socket ⟷ pump
//                                                     └── UDP :53 (any dst) → answered IN-NODE:
//                                                          A/AAAA via the DoH resolver (doh.go — works on
//                                                          DNS-filtered networks), everything else forwarded
//                                                          to the host's real resolver. In-node DNS is not an
//                                                          optimization: systemd-resolved hosts publish
//                                                          127.0.0.53, which is unreachable from a netns.
//   netstack egress (SYN-ACKs, payloads…) ──pump──> link.WritePacket ──TUN──> game
//
// Bounded: flow caps + idle timeouts; per-flow goroutines exit with their flow. Everything is injectable
// (dial / DNS / egress) so the whole NAT is exercised end-to-end in tests via a second in-process stack.

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/net/dns/dnsmessage"

	"gvisor.dev/gvisor/pkg/buffer"
	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/header"
	"gvisor.dev/gvisor/pkg/tcpip/link/channel"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
	"gvisor.dev/gvisor/pkg/tcpip/transport/tcp"
	"gvisor.dev/gvisor/pkg/tcpip/transport/udp"
	"gvisor.dev/gvisor/pkg/waiter"
)

const (
	gwMaxTCPFlows = 512
	gwMaxUDPFlows = 512
	gwTCPIdle     = 5 * time.Minute
	gwUDPIdle     = 60 * time.Second
	gwDialTimeout = 15 * time.Second
)

type natGateway struct {
	ctx    context.Context
	cancel context.CancelFunc

	st *stack.Stack
	ep *channel.Endpoint

	write   func([]byte) error                                       // egress → the game's TUN
	dial    func(network, addr string) (net.Conn, error)             // host-side dial (injectable)
	resolve func(ctx context.Context, host string) ([]net.IP, error) // A/AAAA (DoH; injectable)

	tcpFlows atomic.Int64
	udpFlows atomic.Int64
	dnsAns   atomic.Int64
	dnsFwd   atomic.Int64
}

// newNATGateway builds and starts the gateway. write is the TUN egress; dial/resolve nil ⇒ real host dialer + DoH.
func newNATGateway(parent context.Context, write func([]byte) error,
	dial func(string, string) (net.Conn, error),
	resolve func(context.Context, string) ([]net.IP, error)) (*natGateway, error) {

	if dial == nil {
		d := &net.Dialer{Timeout: gwDialTimeout}
		dial = func(network, addr string) (net.Conn, error) { return d.Dial(network, addr) }
	}
	if resolve == nil {
		doh := newDoHResolver()
		resolve = func(ctx context.Context, host string) ([]net.IP, error) {
			addrs, err := doh.LookupIPAddr(ctx, host)
			if err != nil {
				return nil, err
			}
			ips := make([]net.IP, 0, len(addrs))
			for _, a := range addrs {
				ips = append(ips, a.IP)
			}
			return ips, nil
		}
	}

	ctx, cancel := context.WithCancel(parent)
	g := &natGateway{ctx: ctx, cancel: cancel, write: write, dial: dial, resolve: resolve}

	g.st = stack.New(stack.Options{
		NetworkProtocols:   []stack.NetworkProtocolFactory{ipv4.NewProtocol},
		TransportProtocols: []stack.TransportProtocolFactory{tcp.NewProtocol, udp.NewProtocol},
	})
	g.ep = channel.New(512, overlayMTU, "")
	if err := g.st.CreateNIC(1, g.ep); err != nil {
		cancel()
		return nil, fmt.Errorf("gateway NIC: %v", err)
	}
	// The tun2socks trick: promiscuous = deliver flows addressed to ANY destination IP to our forwarders;
	// spoofing = let us answer FROM those arbitrary IPs. Together they turn the stack into a transparent NAT.
	g.st.SetPromiscuousMode(1, true)
	g.st.SetSpoofing(1, true)
	g.st.SetRouteTable([]tcpip.Route{{Destination: header.IPv4EmptySubnet, NIC: 1}})

	tcpFwd := tcp.NewForwarder(g.st, 0, gwMaxTCPFlows, g.handleTCP)
	g.st.SetTransportProtocolHandler(tcp.ProtocolNumber, tcpFwd.HandlePacket)
	udpFwd := udp.NewForwarder(g.st, g.handleUDP)
	g.st.SetTransportProtocolHandler(udp.ProtocolNumber, udpFwd.HandlePacket)

	safeGo("natgw.egressPump", func() { g.egressPump() })
	return g, nil
}

func (g *natGateway) close() {
	g.cancel()
	g.st.Close()
	g.ep.Close()
}

// inject feeds one IP packet from the game into the netstack.
func (g *natGateway) inject(pkt []byte) {
	pb := stack.NewPacketBuffer(stack.PacketBufferOptions{
		Payload: buffer.MakeWithData(append([]byte(nil), pkt...)),
	})
	g.ep.InjectInbound(ipv4.ProtocolNumber, pb)
	pb.DecRef()
}

// egressPump writes every packet the netstack emits (handshakes, payloads to the game) out to the TUN.
func (g *natGateway) egressPump() {
	for {
		pb := g.ep.ReadContext(g.ctx)
		if pb == nil {
			return // context cancelled / endpoint closed
		}
		v := pb.ToView()
		data := v.AsSlice()
		if err := g.write(data); err != nil {
			pb.DecRef()
			return // TUN gone — the sandbox exited
		}
		pb.DecRef()
	}
}

// handleTCP — one game TCP flow: accept in the netstack, dial the real destination from the host, splice.
func (g *natGateway) handleTCP(r *tcp.ForwarderRequest) {
	id := r.ID() // LocalAddress:LocalPort = the game's ORIGINAL destination
	if g.tcpFlows.Load() >= gwMaxTCPFlows {
		r.Complete(true)
		return
	}
	var wq waiter.Queue
	ep, err := r.CreateEndpoint(&wq)
	if err != nil {
		r.Complete(true)
		return
	}
	r.Complete(false)
	inner := gonet.NewTCPConn(&wq, ep)

	g.tcpFlows.Add(1)
	safeGo("natgw.tcpFlow", func() {
		defer g.tcpFlows.Add(-1)
		defer inner.Close()
		dst := net.JoinHostPort(id.LocalAddress.String(), fmt.Sprint(id.LocalPort))
		outer, derr := g.dial("tcp", dst)
		if derr != nil {
			return // RST/close toward the game — same as an unreachable host on a real LAN
		}
		defer outer.Close()
		spliceConns(inner, outer, gwTCPIdle)
	})
}

// spliceConns pumps both directions until either side closes or the flow idles out.
func spliceConns(a, b net.Conn, idle time.Duration) {
	done := make(chan struct{}, 2)
	cp := func(dst, src net.Conn) {
		defer func() { done <- struct{}{} }() // signal even on panic (guard recovers it) — the sibling must not hang
		buf := make([]byte, 32<<10)
		for {
			_ = src.SetReadDeadline(time.Now().Add(idle))
			n, err := src.Read(buf)
			if n > 0 {
				if _, werr := dst.Write(buf[:n]); werr != nil {
					break
				}
			}
			if err != nil {
				break
			}
		}
	}
	safeGo("natgw.copyPump", func() { cp(a, b) })
	safeGo("natgw.copyPump", func() { cp(b, a) })
	<-done // either direction ending tears the flow down (closes unblock the sibling)
	_ = a.Close()
	_ = b.Close()
	<-done
}

// handleUDP — one game UDP flow. Port 53 to ANY destination is DNS and is answered in-node; everything else gets
// a host-side UDP socket pump with idle expiry.
func (g *natGateway) handleUDP(r *udp.ForwarderRequest) (handled bool) {
	id := r.ID()
	if g.udpFlows.Load() >= gwMaxUDPFlows {
		return false
	}
	var wq waiter.Queue
	ep, err := r.CreateEndpoint(&wq)
	if err != nil {
		return false
	}
	inner := gonet.NewUDPConn(&wq, ep)

	g.udpFlows.Add(1)
	safeGo("natgw.udpFlow", func() {
		defer g.udpFlows.Add(-1)
		defer inner.Close()
		if id.LocalPort == 53 {
			g.serveDNS(inner)
			return
		}
		dst := net.JoinHostPort(id.LocalAddress.String(), fmt.Sprint(id.LocalPort))
		outer, derr := g.dial("udp", dst)
		if derr != nil {
			return
		}
		defer outer.Close()
		spliceConns(inner, outer, gwUDPIdle)
	})
	return true
}

// serveDNS answers the game's DNS queries in-node: A/AAAA through the (hostile-network-proof) DoH resolver,
// anything else — and DoH failures — forwarded verbatim to the host's real resolver. The flow-bound conn carries
// one client; each datagram is one query.
func (g *natGateway) serveDNS(c net.Conn) {
	buf := make([]byte, 4<<10)
	for {
		_ = c.SetReadDeadline(time.Now().Add(gwUDPIdle))
		n, err := c.Read(buf)
		if err != nil {
			return
		}
		q := append([]byte(nil), buf[:n]...)
		resp := g.answerDNS(q)
		if resp == nil {
			resp = g.forwardDNS(q)
		}
		if resp != nil {
			if _, err := c.Write(resp); err != nil {
				return
			}
		}
	}
}

// answerDNS resolves a single-question A/AAAA query via DoH. nil ⇒ let the caller forward it instead.
func (g *natGateway) answerDNS(query []byte) []byte {
	var p dnsmessage.Parser
	hdr, err := p.Start(query)
	if err != nil {
		return nil
	}
	q, err := p.Question()
	if err != nil || (q.Type != dnsmessage.TypeA && q.Type != dnsmessage.TypeAAAA) {
		return nil
	}
	host := strings.TrimSuffix(q.Name.String(), ".")
	ctx, cancel := context.WithTimeout(g.ctx, 5*time.Second)
	ips, rerr := g.resolve(ctx, host)
	cancel()
	if rerr != nil {
		return nil
	}

	b := dnsmessage.NewBuilder(nil, dnsmessage.Header{
		ID: hdr.ID, Response: true, RecursionDesired: hdr.RecursionDesired, RecursionAvailable: true,
	})
	b.EnableCompression()
	_ = b.StartQuestions()
	_ = b.Question(q)
	_ = b.StartAnswers()
	for _, ip := range ips {
		rh := dnsmessage.ResourceHeader{Name: q.Name, Class: dnsmessage.ClassINET, TTL: 60}
		if v4 := ip.To4(); v4 != nil && q.Type == dnsmessage.TypeA {
			rh.Type = dnsmessage.TypeA
			_ = b.AResource(rh, dnsmessage.AResource{A: [4]byte(v4)})
		} else if v4 == nil && q.Type == dnsmessage.TypeAAAA {
			rh.Type = dnsmessage.TypeAAAA
			_ = b.AAAAResource(rh, dnsmessage.AAAAResource{AAAA: [16]byte(ip.To16())})
		}
	}
	out, berr := b.Finish()
	if berr != nil {
		return nil
	}
	g.dnsAns.Add(1)
	return out
}

// forwardDNS relays a raw query to the host's real resolver (reachable from HERE even when it is a
// systemd-resolved loopback stub — we run on the host).
func (g *natGateway) forwardDNS(query []byte) []byte {
	server := hostResolverAddr()
	if server == "" {
		return nil
	}
	c, err := g.dial("udp", server)
	if err != nil {
		return nil
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(4 * time.Second))
	if _, err := c.Write(query); err != nil {
		return nil
	}
	buf := make([]byte, 4<<10)
	n, err := c.Read(buf)
	if err != nil {
		return nil
	}
	g.dnsFwd.Add(1)
	return append([]byte(nil), buf[:n]...)
}

// hostResolverAddr returns the host's first resolv.conf nameserver as host:53 ("" if none).
func hostResolverAddr() string {
	b, err := os.ReadFile("/etc/resolv.conf")
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(b), "\n") {
		f := strings.Fields(line)
		if len(f) >= 2 && f[0] == "nameserver" {
			ip := f[1]
			if i := strings.IndexByte(ip, '%'); i >= 0 { // scoped v6 — skip
				continue
			}
			return net.JoinHostPort(ip, "53")
		}
	}
	return ""
}

var _ = io.Copy // (io kept for future splice variants)
