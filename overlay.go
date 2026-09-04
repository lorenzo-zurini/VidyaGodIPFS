package main

// overlay.go — the packet datapath that turns a session's vIP roster into a real virtual LAN. It is a plain L3
// (IP) forwarder: it reads IP packets off a local link (a TUN device inside the game's bubblewrap netns in
// production; a fake channel link in tests), looks at each packet's destination IP, maps it to the session peer that
// owns that vIP, and ships the raw packet to them. Inbound packets from peers are injected back into the local link.
// The game's own kernel does TCP/IP — we only move IP packets between peers — so NO userspace TCP/IP stack (gVisor
// etc.) is needed, and libp2p gives us the encrypted, NAT-traversing (DCUtR/relay), authenticated transport for free.
//
// TRANSPORT — datagram fast path over a reliable-stream fallback. Real-time game traffic is UDP-native: loss-tolerant
// but latency-sensitive. Carrying it over a RELIABLE ORDERED libp2p stream is the wrong model — a single lost packet
// on a hostile link head-of-line-blocks everything behind it AND drives QUIC's CUBIC congestion window to collapse,
// turning packet loss into multi-second tunnel freezes. So when the peer is reachable over a DIRECT QUIC connection we
// ship each IP packet as an UNRELIABLE QUIC DATAGRAM (RFC 9221): no retransmit, no HoL-blocking, no cwnd-collapse
// stall — a lost game packet is simply lost, exactly as on a real LAN. We fall back to the reliable stream
// (/vidyagod/overlay/1.0.0) when there is no direct QUIC connection (relayed/TCP peer, e.g. before DCUtR upgrades) or
// when a packet exceeds the QUIC datagram size — so delivery still works everywhere, just without the datagram win.
//
// overlayService is decoupled from the node singleton so two instances can be wired to an in-memory libp2p host pair
// and a fake link each, and a packet injected on one must emerge on the other (overlay_test.go) — Spike 0, proven
// headless without root or a real TUN.

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"

	host "github.com/libp2p/go-libp2p/core/host"
	network "github.com/libp2p/go-libp2p/core/network"
	peer "github.com/libp2p/go-libp2p/core/peer"
	protocol "github.com/libp2p/go-libp2p/core/protocol"
	quic "github.com/quic-go/quic-go"
)

const overlayProtoID = protocol.ID("/vidyagod/overlay/1.0.0")

// forceStreamOverlay (VG_OVERLAY_FORCE_STREAM=1) disables the QUIC-datagram fast path so ALL overlay packets ride the
// reliable stream — a diagnostic A/B lever to measure the datagram fix's effect on latency/jitter/freezes under loss.
var forceStreamOverlay = os.Getenv("VG_OVERLAY_FORCE_STREAM") != ""

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

	mu        sync.Mutex
	ctx       context.Context
	cancel    context.CancelFunc
	link      packetLink
	localVIP  string
	routes    map[string]peer.ID // dest vIP (e.g. "10.66.42.2") → owning peer
	bcast     string             // the subnet's directed-broadcast address (e.g. "10.66.255.255"); "" if unknown
	subnetNet *net.IPNet         // the LAN subnet itself — the demux's overlay/host-plane boundary

	sendMu sync.Mutex
	sends  map[peer.ID]network.Stream // cached outbound stream per peer (reopened on error)

	dgMu    sync.Mutex            // guards dgRecv
	dgRecv  map[network.Conn]bool // direct-QUIC conns with a datagram receive loop already running (dedup)
	notifee network.Notifiee      // starts a datagram receive loop on each new direct-QUIC conn to a session peer

	logDatagram sync.Once // one-shot "[overlay] TX path: DATAGRAM" the first time a datagram carries a packet
	logStream   sync.Once // one-shot "[overlay] TX path: STREAM"   the first time the stream fallback carries a packet

	linkm *linkMaintainer // always-on per-friend heartbeat/state/repair (overlaylink.go); nil only in old tests

	exclMu   sync.Mutex
	excluded map[peer.ID]bool // the GLOBAL LAN roster's un-ticked members — carried in routes but never targeted

	sendersMu sync.Mutex
	senders   map[peer.ID]chan []byte // per-peer async stream-fallback queues (drop-oldest; no dials on the pump)

	// Tri-plane bridging (set before serve/attach): the userspace NAT gateway (internet + real-LAN unicast) and
	// the real-LAN broadcast reflector. nil = that plane is off for this launch.
	gwWant, relayWant bool
	gw                *natGateway
	relay             *hostRelay

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

// newOverlayService — lm is the always-on link maintainer, injected HERE because this wiring was once a field
// assignment ("o.linkm = ...") that every TEST performed and production NEVER did: with linkm nil, no friend
// connection ever got a datagram receive loop and inbound heartbeats were ignored, so pongs never happened in
// the field — the LAN ran blind while the state machine guessed. Constructor injection makes the
// forgotten-wiring class unrepresentable; nil stays legal for overlay-only tests (and disables the trust gate).
func newOverlayService(ctx context.Context, h host.Host, r peerRouter, lm *linkMaintainer) *overlayService {
	o := &overlayService{parent: ctx, host: h, router: r, routes: map[string]peer.ID{},
		sends: map[peer.ID]network.Stream{}, dgRecv: map[network.Conn]bool{},
		excluded: map[peer.ID]bool{}, senders: map[peer.ID]chan []byte{}, linkm: lm}
	if lm != nil {
		// The maintainer re-kicks receive loops from its tick: a conn that predated friend membership (a bitswap
		// dial, a presence stream) would otherwise NEVER get a loop — the dgRecv dedupe means a miss was permanent.
		lm.ensureRecvLoops = func(pid peer.ID) {
			for _, c := range h.Network().ConnsToPeer(pid) {
				o.maybeStartDatagramLoop(c)
			}
		}
	}
	return o
}

// dialPeerDirect forces a DIRECT dial attempt even while a relayed/TCP connection exists. dialPeer's
// Connectedness short-circuit made every "upgrade nudge" a no-op for the maintainer: as long as ANY conn was up
// (relay, TCP over a tunnel iface), h.Connect returned early and the better path was never tried. This uses the
// swarm's force-direct escape hatch — mDNS-learned same-LAN addrs in the peerstore finally get dialed.
func dialPeerDirect(ctx context.Context, h host.Host, router peerRouter, pid peer.ID) error {
	ctx = network.WithForceDirectDial(ctx, "vg-lan-upgrade")
	if router != nil {
		if ai, err := router.FindPeer(ctx, pid); err == nil {
			vlog("dial", "force-direct %s via FindPeer addrs=%v", shortPeer(pid.String()), ai.Addrs)
			return h.Connect(ctx, ai)
		}
	}
	vlog("dial", "force-direct %s from peerstore", shortPeer(pid.String()))
	return h.Connect(ctx, peer.AddrInfo{ID: pid})
}

// dialPeer ensures a connection to pid (resolving via the router/DHT if not already connected).
func dialPeer(ctx context.Context, h host.Host, router peerRouter, pid peer.ID) error {
	if c := h.Network().Connectedness(pid); c == network.Connected || c == network.Limited {
		return nil // Limited (relayed) is a usable link — the maintainer's force-direct nudge handles upgrades
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

// start registers the inbound stream handler (the fallback path) and a network notifiee that spins a datagram
// receive loop on each new direct-QUIC connection to a session peer. Idempotent; forwarding only happens while a link
// is attached (the receive paths drop when o.link is nil).
func (o *overlayService) start() {
	o.host.SetStreamHandler(overlayProtoID, o.handleInbound)
	o.notifee = &network.NotifyBundle{
		ConnectedF: func(_ network.Network, c network.Conn) { o.maybeStartDatagramLoop(c) },
	}
	o.host.Network().Notify(o.notifee)
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
	var snet *net.IPNet
	if _, ipnet, err := net.ParseCIDR(subnet); err == nil {
		snet = ipnet
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
	o.subnetNet = snet
	o.mu.Unlock()
	vlog("overlay", "configure selfVIP=%s subnet=%s routes=%d bcast=%s", localVIP, subnet, len(routes), bcast)
	// Existing direct-QUIC connections to these peers (e.g. a fetch already holepunched to a friend) need a datagram
	// receive loop too — the notifiee only catches connections that appear AFTER this point.
	for _, pid := range routes {
		for _, c := range o.host.Network().ConnsToPeer(pid) {
			o.maybeStartDatagramLoop(c)
		}
	}
	return nil
}

// setExcluded replaces the roster's excluded set (peer ids). Takes effect on the next packet — live mid-game.
func (o *overlayService) setExcluded(pids map[peer.ID]bool) {
	o.exclMu.Lock()
	o.excluded = pids
	o.exclMu.Unlock()
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
	// Gate on ANY leftover state, not just running: readLoop's defer clears running while link/gw/relay may
	// still exist (fd-handoff attach racing an instant game death) — replacing without teardown would leak the
	// old TUN fd + gateway and bind a SECOND reflector on the real LAN (adversarial round-3).
	if o.running || o.link != nil || o.gw != nil || o.relay != nil {
		o.mu.Unlock()
		o.detach()
		o.mu.Lock()
	}
	ctx, cancel := context.WithCancel(o.parent)
	o.ctx, o.cancel, o.link, o.running = ctx, cancel, link, true
	gwWant, relayWant := o.gwWant, o.relayWant
	localVIP := o.localVIP
	o.mu.Unlock()
	// Tri-plane bring-up: both planes write into the SAME link through the serialized injector.
	// Conditional installs: a concurrent detach between our unlock above and here has already snapshotted
	// gw/relay as nil and closed the link — installing now would leave a live gateway/reflector behind a dead
	// TUN with nothing to tear them down until the next attach (adversarial round-4). If we lost the race,
	// close the fresh instance instead of installing it.
	if gwWant {
		if gw, err := newNATGateway(ctx, o.injectToLink, nil, nil); err == nil {
			o.mu.Lock()
			stillOurs := o.running && o.link == link
			if stillOurs {
				o.gw = gw // under o.mu: healthSnapshot reads it concurrently (adversarial M1b)
			}
			o.mu.Unlock()
			if stillOurs {
				fmt.Fprintf(os.Stderr, "[overlay] gateway ON — internet + real-LAN unicast via in-node NAT (no helper)\n")
			} else {
				gw.close()
			}
		} else {
			fmt.Fprintf(os.Stderr, "[overlay] gateway failed to start: %v\n", err)
		}
	}
	if relayWant && localVIP != "" {
		if vip := net.ParseIP(localVIP); vip != nil {
			r := newHostRelay(o.injectToLink, vip)
			o.mu.Lock()
			stillOurs := o.running && o.link == link
			if stillOurs {
				o.relay = r
			}
			o.mu.Unlock()
			if stillOurs {
				fmt.Fprintf(os.Stderr, "[overlay] reflector ON — real-LAN broadcasts bridged both ways\n")
			} else {
				r.close()
			}
		}
	}
	safeGo("overlay.readLoop", func() { o.readLoop(ctx, link) })
	vlog("overlay", "attach: TUN link up (gateway=%v reflector=%v selfVIP=%s)", gwWant, relayWant && localVIP != "", localVIP)
}

// detach stops forwarding and tears down the link + cached streams + any pending fd-handoff socket.
func (o *overlayService) detach() {
	o.mu.Lock()
	cancel, link := o.cancel, o.link
	sockL, sockPath := o.sockL, o.sockPath
	gw, relay := o.gw, o.relay
	o.running, o.link, o.cancel, o.sockL, o.sockPath = false, nil, nil, nil, ""
	o.gw, o.relay = nil, nil // cleared under o.mu — healthSnapshot reads them concurrently (adversarial M1b)
	o.mu.Unlock()
	// Always tear down the fd-handoff socket (the accept goroutine may still be waiting even if no link attached yet).
	if sockL != nil {
		_ = sockL.Close()
	}
	if sockPath != "" {
		_ = os.Remove(sockPath)
	}
	// NO running-gate here: teardown is driven by what actually exists (snapshot above), not by a flag another
	// path may have cleared — gating on wasRunning leaked gw/relay/link when readLoop noticed the TUN die first
	// (adversarial round-2 HIGH-1). Everything below is nil-checked and the fields were nil'd under o.mu, so a
	// second detach is a no-op by construction.
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
	if gw != nil {
		gw.close()
	}
	if relay != nil {
		relay.close()
	}
}

// injectToLink writes one synthesized/forwarded packet into the game's TUN, serialized against every other writer.
func (o *overlayService) injectToLink(pkt []byte) error {
	o.mu.Lock()
	link := o.link
	o.mu.Unlock()
	if link == nil {
		return fmt.Errorf("no link attached")
	}
	o.writeMu.Lock()
	defer o.writeMu.Unlock()
	return link.WritePacket(pkt)
}

// readLoop pulls IP packets from the local link and forwards each to the peer that owns its destination vIP.
func (o *overlayService) readLoop(ctx context.Context, link packetLink) {
	// On ANY exit (link death, detach, ctx cancel) the datapath is factually down: say so and clear running,
	// otherwise healthSnapshot keeps reporting "forwarding (game active)" over a dead pump (adversarial C2).
	// ONLY running is cleared — link/gw/relay teardown stays detach's job: clearing o.link here made detach's
	// old wasRunning gate skip cancel/gw/relay/link cleanup entirely, leaking the tri-plane and letting the
	// next launch start a SECOND reflector on the same LAN (adversarial round-2 HIGH-1).
	defer func() {
		o.mu.Lock()
		wasRunning := o.running && o.link == link // detach() already cleared/replaced these on a normal teardown
		if wasRunning {
			o.running = false
		}
		o.mu.Unlock()
		if wasRunning {
			fmt.Fprintf(os.Stderr, "[overlay] readLoop exited while attached — TUN datapath DOWN (detach pending)\n")
		}
	}()
	for {
		pkt, err := link.ReadPacket()
		if err != nil {
			return // link closed / detached
		}
		if ctx.Err() != nil {
			return
		}
		// Per-packet guard: a panic on one hostile/garbled packet (parse, gateway inject, reflector) must cost
		// that packet only — a dead readLoop is the game's entire outbound datapath gone (adversarial C2).
		guard("overlay.readPacket", func() { o.handleOutbound(ctx, pkt) })
	}
}

// handleOutbound routes ONE packet from the game's TUN: fan-out, LAN unicast, or the host-plane NAT.
func (o *overlayService) handleOutbound(ctx context.Context, pkt []byte) {
	dst, ok := ipv4Dst(pkt)
	if !ok {
		return // not IPv4 (e.g. IPv6 ND) — drop for now
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
	gw, relay := o.gw, o.relay // snapshot with routes: detach nils them concurrently (adversarial M1b)
	o.mu.Unlock()
	// The GLOBAL roster (launch-window ticks): un-ticked members stay friends and stay routed-in-name, but no
	// game traffic flows to them in either intention — outbound filtered here, and they simply never announce.
	o.exclMu.Lock()
	if fanout {
		kept := targets[:0]
		for _, t := range targets {
			if !o.excluded[t] {
				kept = append(kept, t)
			}
		}
		targets = kept
	}
	if known && o.excluded[pid] {
		known = false
	}
	o.exclMu.Unlock()
	if fanout {
		// Broadcast/multicast → every LAN peer (overlay plane) AND, when the reflector is up, the REAL LAN.
		for _, t := range targets {
			if err := o.forward(ctx, t, pkt); err != nil {
				fmt.Fprintf(os.Stderr, "[overlay] fan-out to %s failed: %v\n", t, err)
			}
		}
		if relay != nil {
			if u, uok := parseUDP4(pkt); uok {
				relay.fromGame(u)
			}
		}
		return
	}
	if !known {
		// Not a LAN address → the HOST plane: hand it to the in-node NAT (internet / real-LAN unicast).
		// In-subnet addresses with no owning friend stay dropped — the vLAN never leaks into the NAT.
		o.mu.Lock()
		snet := o.subnetNet
		o.mu.Unlock()
		if gw != nil {
			if ip := net.ParseIP(dst); ip != nil && (snet == nil || !snet.Contains(ip)) {
				gw.inject(pkt)
			}
		}
		return
	}
	if err := o.forward(ctx, pid, pkt); err != nil {
		fmt.Fprintf(os.Stderr, "[overlay] forward to %s failed: %v\n", pid, err)
	}
}

// forward ships one packet to a peer: an unreliable QUIC datagram over a direct connection (the fast path), else the
// reliable stream (relayed/TCP peer, or an oversized packet). Datagram sends that "fail" as loss are the intended
// behaviour for game traffic — only a MISSING datagram transport (no direct QUIC conn / too large) falls through.
func (o *overlayService) forward(ctx context.Context, pid peer.ID, pkt []byte) error {
	if o.sendDatagram(pid, pkt) {
		return nil
	}
	// Stream fallback is ASYNC: sendStream may dial (FindPeer + connect = seconds) and this is called from the TUN
	// pump — blocking here would stall the whole LAN. Each peer gets a small drop-oldest queue drained by its own
	// sender goroutine: correct semantics for game traffic (stale announcements are worthless; fresh ones follow).
	o.enqueueStream(pid, pkt)
	return nil
}

// enqueueStream queues pkt for pid's async sender (started lazily; service-lifetime). Drop-oldest on overflow.
func (o *overlayService) enqueueStream(pid peer.ID, pkt []byte) {
	o.sendersMu.Lock()
	ch := o.senders[pid]
	if ch == nil {
		ch = make(chan []byte, 64)
		o.senders[pid] = ch
		// The guard sits INSIDE the loop, per packet: a panic in forwardStream must cost one packet, not the
		// pump. A dead pump with its channel still registered would silently drop-oldest EVERY future packet to
		// this peer for the process lifetime — the worst possible failure for the "reliable" path. (Adversarial
		// review C1: the original whole-loop wrap did exactly that.)
		go func() {
			for {
				select {
				case <-o.parent.Done():
					return
				case p := <-ch:
					guard("overlay.streamPump", func() {
						if err := o.forwardStream(o.parent, pid, p); err != nil {
							if overlayDebug {
								fmt.Fprintf(os.Stderr, "[overlay] stream fallback to %s failed: %v\n", pid, err)
							}
						} else if o.linkm != nil {
							o.linkm.noteStream(pid)
						}
					})
				}
			}
		}()
	}
	o.sendersMu.Unlock()
	select {
	case ch <- pkt:
	default:
		select { // full → drop the OLDEST queued packet, keep the freshest
		case <-ch:
		default:
		}
		select {
		case ch <- pkt:
		default:
		}
	}
}

// sendDatagram tries to ship pkt as one QUIC datagram over a direct connection to pid. Returns true if a direct QUIC
// connection carried it (or dropped it as tolerable game-traffic loss), false if there is no datagram-capable
// connection or the packet exceeds the datagram size — signalling the caller to use the reliable stream instead.
// No length framing: QUIC datagrams preserve message boundaries, so one datagram IS one IP packet.
func (o *overlayService) sendDatagram(pid peer.ID, pkt []byte) bool {
	if forceStreamOverlay {
		return false // A/B harness: force the old reliable-stream path to compare against datagrams
	}
	// TRUST GATE (Invariant 2, overlaylink.go): a direct QUIC conn is not enough — its datagram path must be
	// PROVEN by heartbeat pongs. A half-dead/one-way path (hairpin punch, asymmetric firewall) reports
	// send-success while eating every packet — measured live as 100% one-way loss with the link showing
	// "direct". Until proven (and whenever pongs stop) the packet rides the reliable stream instead: delivery
	// is never entrusted to an unverified path; the datagram win is an upgrade, not a prerequisite.
	var qc *quic.Conn
	if o.linkm != nil {
		// ONE lock take per packet (adversarial round-2 L9). Trust is PER-CONN: ride the exact connection the
		// pongs proved, never a sibling that happens to sort first — a fresh re-punch coexisting with the
		// proven conn is an untested path (adversarial H2). Untracked peers under a live maintainer are
		// UNTRUSTED (adversarial M2) — the stream fallback still delivers.
		id, tracked := o.linkm.trustedConnFor(pid)
		if tracked && id == "" {
			return false
		}
		if tracked {
			for _, c := range o.host.Network().ConnsToPeer(pid) {
				if c.ID() == id {
					_ = c.As(&qc)
					break
				}
			}
		} else {
			return false // maintainer live but peer unknown: never trust (removed friend's stale route)
		}
	} else {
		qc = quicConnTo(o.host, pid) // maintainer-less (tests): old behavior
	}
	if qc == nil {
		return false // no (proven) direct QUIC connection → stream fallback
	}
	if err := qc.SendDatagram(pkt); err != nil {
		var tooLarge *quic.DatagramTooLargeError
		if errors.As(err, &tooLarge) {
			return false // packet bigger than the path's datagram size → send it reliably over the stream
		}
		// A real send ERROR is a suspect path (the field bug: a zombie conn whose NAT mapping died ate every
		// packet while this returned "handled"). Tell the maintainer (fast-tracks the zombie verdict + re-punch)
		// and fall back to the reliable stream so the packet still travels.
		if o.linkm != nil {
			o.linkm.suspect(pid)
		}
		return false
	}
	if o.linkm != nil {
		o.linkm.noteTx(pid)
	}
	o.logDatagram.Do(func() { fmt.Fprintln(os.Stderr, "[overlay] TX path: DATAGRAM (unreliable QUIC datagrams)") })
	return true
}

// quicConnTo returns the underlying *quic.Conn of a DIRECT QUIC connection to pid, or nil if the only connections are
// relayed/TCP (whose transport conn does not unwrap to a *quic.Conn). network.Conn.As delegates through the swarm to
// the QUIC transport's As, which sets the pointer.
func quicConnTo(h host.Host, pid peer.ID) *quic.Conn {
	for _, c := range h.Network().ConnsToPeer(pid) {
		var qc *quic.Conn
		if c.As(&qc) && qc != nil {
			return qc
		}
	}
	return nil
}

// maybeStartDatagramLoop starts a datagram receive loop on c if it is a direct-QUIC connection to a session peer and
// no loop is running on it yet. Idempotent (deduped by conn); the loop lives until the connection closes.
func (o *overlayService) maybeStartDatagramLoop(c network.Conn) {
	o.mu.Lock()
	_, isRoute := routesContain(o.routes, c.RemotePeer())
	o.mu.Unlock()
	if !isRoute && (o.linkm == nil || !o.linkm.has(c.RemotePeer())) {
		return // neither a session peer nor a maintained friend
	}
	var qc *quic.Conn
	if !c.As(&qc) || qc == nil {
		return // relayed/TCP conn — inbound arrives on the stream handler instead
	}
	o.dgMu.Lock()
	if o.dgRecv[c] {
		o.dgMu.Unlock()
		return
	}
	o.dgRecv[c] = true
	o.dgMu.Unlock()
	vlog("overlay", "datagram RX loop started for %s (%s)", shortPeer(c.RemotePeer().String()), c.RemoteMultiaddr())
	safeGo("overlay.datagramRecvLoop", func() { o.datagramRecvLoop(c, qc) })
}

// datagramRecvLoop injects each inbound QUIC datagram (one IP packet) into the local link, until the connection closes.
func (o *overlayService) datagramRecvLoop(c network.Conn, qc *quic.Conn) {
	defer func() {
		o.dgMu.Lock()
		delete(o.dgRecv, c)
		o.dgMu.Unlock()
	}()
	for {
		pkt, err := qc.ReceiveDatagram(o.parent)
		if err != nil {
			return // connection closed or service shutting down
		}
		if isHeartbeat(pkt) {
			// Maintainer control datagram (overlaylink.go) — never an IPv4 packet ('V' fails the version check).
			if o.linkm != nil {
				o.linkm.handleHeartbeat(c.RemotePeer(), c.ID(), pkt, func(b []byte) { _ = qc.SendDatagram(b) })
			}
			continue
		}
		if _, ok := ipv4Dst(pkt); !ok {
			continue // not an IP packet we forward (junk / non-overlay datagram) — drop
		}
		if o.linkm != nil {
			o.linkm.noteRx(c.RemotePeer())
		}
		o.mu.Lock()
		link := o.link
		o.mu.Unlock()
		if link == nil {
			continue // no session link attached — drop
		}
		o.writeMu.Lock()
		werr := link.WritePacket(pkt)
		o.writeMu.Unlock()
		if werr != nil {
			return
		}
	}
}

// routesContain reports whether pid owns any vIP in the routing table.
func routesContain(routes map[string]peer.ID, pid peer.ID) (string, bool) {
	for vip, p := range routes {
		if p == pid {
			return vip, true
		}
	}
	return "", false
}

// forwardStream writes one packet to a peer over a cached (lazily opened) stream, reopening on a broken stream once.
func (o *overlayService) forwardStream(ctx context.Context, pid peer.ID, pkt []byte) error {
	o.logStream.Do(func() {
		fmt.Fprintf(os.Stderr, "[overlay] TX path: STREAM (reliable fallback%s)\n",
			map[bool]string{true: ", forced via VG_OVERLAY_FORCE_STREAM", false: ""}[forceStreamOverlay])
	})
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
	// WithAllowLimitedConn: without it, NewStream refuses to open over a RELAYED (circuit-v2 "limited")
	// connection — so a relay-only pair (hole-punch failed) had NO working overlay path at all: datagrams need a
	// direct conn AND the "reliable fallback" couldn't open. The stream must work over ANY connection.
	s, err := o.host.NewStream(network.WithAllowLimitedConn(ctx, "vidyagod-overlay"), pid, overlayProtoID)
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
	// AUTHORIZATION (adversarial M8): the datagram RX path is gated on routes/maintained peers, but this stream
	// handler is registered on the public host at goOnline — without this check ANY dialable peer could open
	// /vidyagod/overlay/1.0.0 and inject arbitrary L3 frames into the game's netns. Only friends may speak here.
	remote := s.Conn().RemotePeer()
	authorized := func() bool {
		o.mu.Lock()
		_, isRoute := routesContain(o.routes, remote)
		o.mu.Unlock()
		return isRoute || (o.linkm != nil && o.linkm.has(remote))
	}
	if !authorized() {
		vlog("overlay", "REJECT inbound overlay stream from non-friend %s", shortPeer(remote.String()))
		return
	}
	for {
		pkt, err := readFrame(s)
		if err != nil {
			return
		}
		// Re-checked PER FRAME, not just at open: a friend removed mid-session keeps their long-lived stream,
		// and the never-ClosePeer policy means the conn won't die on its own — revocation must close this door
		// (adversarial round-2). One mutex + map hit per frame; the stream path is not the fast path.
		if !authorized() {
			vlog("overlay", "REJECT: %s lost authorization mid-stream", shortPeer(remote.String()))
			return
		}
		if _, ok := ipv4Dst(pkt); !ok {
			continue // not an IPv4 packet — never inject junk into the TUN
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
