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

	sendMu sync.Mutex
	sends  map[peer.ID]network.Stream // cached outbound stream per peer (reopened on error)

	writeMu sync.Mutex // serialize WritePacket into the link (inbound arrives on many goroutines)
	running bool
}

func newOverlayService(ctx context.Context, h host.Host, r peerRouter) *overlayService {
	return &overlayService{parent: ctx, host: h, router: r, routes: map[string]peer.ID{}, sends: map[peer.ID]network.Stream{}}
}

// start registers the inbound handler once (idempotent). Forwarding only happens while a link is attached.
func (o *overlayService) start() {
	o.host.SetStreamHandler(overlayProtoID, o.handleInbound)
}

// configure sets the local vIP and the vIP→peer routing table from a session roster. peerByVIP maps each remote
// member's vIP string to their peer-ID string.
func (o *overlayService) configure(localVIP string, peerByVIP map[string]string) error {
	routes := make(map[string]peer.ID, len(peerByVIP))
	for vip, pidStr := range peerByVIP {
		pid, err := peer.Decode(pidStr)
		if err != nil {
			return fmt.Errorf("bad peer id for vip %s: %w", vip, err)
		}
		routes[vip] = pid
	}
	o.mu.Lock()
	o.localVIP = localVIP
	o.routes = routes
	o.mu.Unlock()
	return nil
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

// detach stops forwarding and tears down the link + cached streams.
func (o *overlayService) detach() {
	o.mu.Lock()
	if !o.running {
		o.mu.Unlock()
		return
	}
	o.running = false
	cancel, link := o.cancel, o.link
	o.link = nil
	o.mu.Unlock()
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
		pid, known := o.routes[dst]
		o.mu.Unlock()
		if !known {
			continue // no session member owns this address
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
