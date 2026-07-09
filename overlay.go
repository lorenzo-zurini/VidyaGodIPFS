package main

// overlay.go — the packet datapath that turns a session's vIP roster into a real virtual LAN. It is a plain L3
// (IP) forwarder: it reads IP packets off a local link (a TUN device inside the game's bubblewrap netns in
// production; a fake channel link in tests), looks at each packet's destination IP, maps it to the session peer that
// owns that vIP, and ships the raw packet to them over a libp2p stream (/vidyagod/overlay/1.0.0). Inbound packets
// from peers are injected back into the local link. The game's own kernel does TCP/IP — we only move IP packets
// between peers — so NO userspace TCP/IP stack (gVisor etc.) is needed, and libp2p gives us the encrypted,
// NAT-traversing (DCUtR/relay), authenticated transport for free.
//
// overlayService is decoupled from the node singleton so two instances can be wired to an in-memory libp2p host pair
// and a fake link each, and a packet injected on one must emerge on the other (overlay_test.go) — Spike 0, proven
// headless without root or a real TUN.

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"sync"

	host "github.com/libp2p/go-libp2p/core/host"
	network "github.com/libp2p/go-libp2p/core/network"
	peer "github.com/libp2p/go-libp2p/core/peer"
	protocol "github.com/libp2p/go-libp2p/core/protocol"
)

const overlayProtoID = protocol.ID("/vidyagod/overlay/1.0.0")

// overlayMTU bounds a single IP packet. Kept under a typical path MTU so the framed packet rides one libp2p/QUIC
// datagram-sized write comfortably; the local link (TUN) is configured with the same MTU so the kernel never hands
// us anything larger.
const overlayMTU = 1400

// packetLink is the minimal local endpoint the overlay needs: read one inbound IP packet (from the OS/game side),
// write one packet back to it. A real TUN fd implements this (one packet per read/write); tests use a channel link.
type packetLink interface {
	ReadPacket() ([]byte, error) // blocks for the next IP packet from the local side
	WritePacket(pkt []byte) error
	Close() error
}

// overlayService forwards IP packets between the local link and session peers over libp2p.
type overlayService struct {
	parent context.Context
	host   host.Host
	router peerRouter

	mu       sync.Mutex
	ctx      context.Context
	cancel   context.CancelFunc
	link     packetLink
	localVIP string
	routes   map[string]peer.ID // dest vIP (e.g. "10.66.42.2") → owning peer
	bcast    string             // the subnet's directed-broadcast address (e.g. "10.66.255.255"); "" if unknown

	sendMu sync.Mutex
	sends  map[peer.ID]network.Stream // cached outbound stream per peer (reopened on error)

	writeMu sync.Mutex // serialize WritePacket into the link (inbound arrives on many goroutines)
	running bool

	sockL    net.Listener // unix socket receiving the TUN fd from a nested sandbox-init (nil in host-TUN mode)
	sockPath string
}

// fdLink is a packetLink over an already-open TUN fd (received from the sandbox-init over a unix socket). Unlike
// tunLink it does no device creation/config — the interface was created + addressed inside the sandbox netns; here we
// only move packets across the fd, which stays valid while the sandbox (and thus its netns) lives.
type fdLink struct {
	f   *os.File
	mtu int
}

func (l *fdLink) ReadPacket() ([]byte, error) {
	buf := make([]byte, l.mtu+64)
	n, err := l.f.Read(buf)
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}

func (l *fdLink) WritePacket(p []byte) error { _, err := l.f.Write(p); return err }
func (l *fdLink) Close() error               { return l.f.Close() }

func newOverlayService(ctx context.Context, h host.Host, r peerRouter) *overlayService {
	return &overlayService{parent: ctx, host: h, router: r, routes: map[string]peer.ID{}, sends: map[peer.ID]network.Stream{}}
}

// dialPeer ensures a connection to pid (resolving via the router/DHT if not already connected).
func dialPeer(ctx context.Context, h host.Host, router peerRouter, pid peer.ID) error {
	if h.Network().Connectedness(pid) == network.Connected {
		return nil
	}
	if router != nil {
		ai, err := router.FindPeer(ctx, pid)
		if err != nil {
			return fmt.Errorf("locate peer: %w", err)
		}
		return h.Connect(ctx, ai)
	}
	return h.Connect(ctx, peer.AddrInfo{ID: pid})
}

// start registers the inbound handler once (idempotent). Forwarding only happens while a link is attached.
func (o *overlayService) start() {
	o.host.SetStreamHandler(overlayProtoID, o.handleInbound)
}

// configure sets the local vIP, the /N subnet, and the vIP→peer routing table (peerByVIP maps each remote friend's vIP
// to their peer-ID). The subnet's directed-broadcast address is derived so broadcast/multicast packets can be fanned
// out to every peer (LAN-game discovery) rather than dropped.
func (o *overlayService) configure(localVIP, subnet string, peerByVIP map[string]string) error {
	routes := make(map[string]peer.ID, len(peerByVIP))
	for vip, pidStr := range peerByVIP {
		pid, err := peer.Decode(pidStr)
		if err != nil {
			return fmt.Errorf("bad peer id for vip %s: %w", vip, err)
		}
		routes[vip] = pid
	}
	bcast := ""
	if _, ipnet, err := net.ParseCIDR(subnet); err == nil {
		b := make(net.IP, len(ipnet.IP))
		for i := range ipnet.IP {
			b[i] = ipnet.IP[i] | ^ipnet.Mask[i]
		}
		bcast = b.String()
	}
	o.mu.Lock()
	o.localVIP = localVIP
	o.routes = routes
	o.bcast = bcast
	o.mu.Unlock()
	return nil
}

// isFanout reports whether a destination IP is a broadcast or multicast address that should be replicated to every LAN
// peer (so classic LAN games discover each other over the routed overlay, which has no real broadcast domain):
// the limited broadcast 255.255.255.255, the subnet directed-broadcast (e.g. 10.66.255.255), or multicast 224.0.0.0/4.
func (o *overlayService) isFanout(dst string) bool {
	if dst == "255.255.255.255" || (o.bcast != "" && dst == o.bcast) {
		return true
	}
	if ip := net.ParseIP(dst).To4(); ip != nil && ip[0] >= 224 && ip[0] <= 239 {
		return true
	}
	return false
}

// attach begins forwarding over the given link (the TUN). Idempotent-safe: a second attach replaces the link.
func (o *overlayService) attach(link packetLink) {
	o.mu.Lock()
	if o.running {
		o.mu.Unlock()
		o.detach()
		o.mu.Lock()
	}
	ctx, cancel := context.WithCancel(o.parent)
	o.ctx, o.cancel, o.link, o.running = ctx, cancel, link, true
	o.mu.Unlock()
	go o.readLoop(ctx, link)
}

// detach stops forwarding and tears down the link + cached streams + any pending fd-handoff socket.
func (o *overlayService) detach() {
	o.mu.Lock()
	cancel, link := o.cancel, o.link
	sockL, sockPath := o.sockL, o.sockPath
	wasRunning := o.running
	o.running, o.link, o.sockL, o.sockPath = false, nil, nil, ""
	o.mu.Unlock()
	// Always tear down the fd-handoff socket (the accept goroutine may still be waiting even if no link attached yet).
	if sockL != nil {
		_ = sockL.Close()
	}
	if sockPath != "" {
		_ = os.Remove(sockPath)
	}
	if !wasRunning {
		return
	}
	if cancel != nil {
		cancel()
	}
	o.sendMu.Lock()
	for _, s := range o.sends {
		_ = s.Reset()
	}
	o.sends = map[peer.ID]network.Stream{}
	o.sendMu.Unlock()
	if link != nil {
		_ = link.Close()
	}
}

// readLoop pulls IP packets from the local link and forwards each to the peer that owns its destination vIP.
func (o *overlayService) readLoop(ctx context.Context, link packetLink) {
	for {
		pkt, err := link.ReadPacket()
		if err != nil {
			return // link closed / detached
		}
		if ctx.Err() != nil {
			return
		}
		dst, ok := ipv4Dst(pkt)
		if !ok {
			continue // not IPv4 (e.g. IPv6 ND) — drop for now
		}
		o.mu.Lock()
		fanout := o.isFanout(dst)
		var targets []peer.ID
		if fanout {
			targets = make([]peer.ID, 0, len(o.routes))
			for _, pid := range o.routes {
				targets = append(targets, pid)
			}
		}
		pid, known := o.routes[dst]
		o.mu.Unlock()
		if fanout {
			// Broadcast/multicast → replicate to EVERY LAN peer so their game's kernel sees the discovery packet.
			for _, t := range targets {
				if err := o.forward(ctx, t, pkt); err != nil {
					fmt.Fprintf(os.Stderr, "[overlay] fan-out to %s failed: %v\n", t, err)
				}
			}
			continue
		}
		if !known {
			continue // no LAN friend owns this address
		}
		if err := o.forward(ctx, pid, pkt); err != nil {
			fmt.Fprintf(os.Stderr, "[overlay] forward to %s failed: %v\n", pid, err)
		}
	}
}

// forward writes one packet to a peer over a cached (lazily opened) stream, reopening on a broken stream once.
func (o *overlayService) forward(ctx context.Context, pid peer.ID, pkt []byte) error {
	s, err := o.sendStream(ctx, pid)
	if err != nil {
		return err
	}
	if err := writeFrame(s, pkt); err != nil {
		// Stream broke — drop it and try one fresh stream.
		o.dropStream(pid, s)
		s2, err2 := o.sendStream(ctx, pid)
		if err2 != nil {
			return err2
		}
		return writeFrame(s2, pkt)
	}
	return nil
}

func (o *overlayService) sendStream(ctx context.Context, pid peer.ID) (network.Stream, error) {
	o.sendMu.Lock()
	if s := o.sends[pid]; s != nil {
		o.sendMu.Unlock()
		return s, nil
	}
	o.sendMu.Unlock()
	if err := dialPeer(ctx, o.host, o.router, pid); err != nil {
		return nil, err
	}
	s, err := o.host.NewStream(ctx, pid, overlayProtoID)
	if err != nil {
		return nil, err
	}
	o.sendMu.Lock()
	// Another goroutine may have opened one concurrently; keep the first, reset the loser.
	if existing := o.sends[pid]; existing != nil {
		o.sendMu.Unlock()
		_ = s.Reset()
		return existing, nil
	}
	o.sends[pid] = s
	o.sendMu.Unlock()
	return s, nil
}

func (o *overlayService) dropStream(pid peer.ID, s network.Stream) {
	o.sendMu.Lock()
	if o.sends[pid] == s {
		delete(o.sends, pid)
	}
	o.sendMu.Unlock()
	_ = s.Reset()
}

// handleInbound reads framed packets from a peer and injects them into the local link.
func (o *overlayService) handleInbound(s network.Stream) {
	defer s.Close()
	for {
		pkt, err := readFrame(s)
		if err != nil {
			return
		}
		o.mu.Lock()
		link := o.link
		o.mu.Unlock()
		if link == nil {
			continue // no link attached (session not launched) — drop
		}
		o.writeMu.Lock()
		werr := link.WritePacket(pkt)
		o.writeMu.Unlock()
		if werr != nil {
			return
		}
	}
}

// ipv4Dst extracts the destination IP of an IPv4 packet (bytes 16..19), or ("",false) for non-IPv4 / too short.
func ipv4Dst(pkt []byte) (string, bool) {
	if len(pkt) < 20 || pkt[0]>>4 != 4 {
		return "", false
	}
	return net.IPv4(pkt[16], pkt[17], pkt[18], pkt[19]).String(), true
}

// writeFrame writes a 2-byte big-endian length prefix + the packet.
func writeFrame(w io.Writer, pkt []byte) error {
	if len(pkt) > 0xffff {
		return fmt.Errorf("packet too large: %d", len(pkt))
	}
	var hdr [2]byte
	binary.BigEndian.PutUint16(hdr[:], uint16(len(pkt)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := w.Write(pkt)
	return err
}

// readFrame reads a length-prefixed packet.
func readFrame(r io.Reader) ([]byte, error) {
	var hdr [2]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint16(hdr[:])
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}
