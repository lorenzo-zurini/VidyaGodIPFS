package main

// overlaylink.go — the ALWAYS-ON link maintainer behind the virtual LAN: one tiny heartbeat/state machine per
// accepted friend, running for the app's lifetime (not just while a game is up). It exists because the field
// failure mode of the overlay was SILENT: a mobile hotspot's NAT idles out the UDP mapping in ~30s, the direct
// QUIC connection turns zombie, and every game announcement vanishes with no error and no fallback — the LAN menu
// just stays empty. The maintainer makes the link's health explicit and self-repairing:
//
//   • HEARTBEAT: a ~13-byte control datagram every linkBeatEvery over the same direct-QUIC connection the game
//     traffic rides. That alone defeats NAT idle timeouts (4s ≪ 30s) and yields live RTT for the UI.
//   • STATE: direct (QUIC + fresh pongs) / relayed (connected, no direct QUIC) / connecting / down — exposed via
//     VgLanPeers for the Friends tab and the launch window's Virtual LAN panel.
//   • REPAIR: linkBeatMiss missed pongs on a previously-responsive peer ⇒ the connection is a zombie — close it,
//     re-dial (relay first; DCUtR re-punches to direct), with 2s→30s backoff. Stuck on relayed ⇒ gentle periodic
//     re-dial nudges an upgrade without flapping a working relay.
//   • PROTECT: maintained peers' connections are pinned in the ConnManager so background churn never trims them.
//
// Wire format (QUIC datagram, intercepted in datagramRecvLoop BEFORE the IPv4 check — 'V' = 0x56 can never be an
// IPv4 header byte): "VGHB" + kind (0 ping / 1 pong) + 8-byte big-endian unix-nano send-timestamp, echoed verbatim
// in the pong.
//
// A direct link must PROVE itself: it is not reported "direct" until the first pong, and a conn that never pongs
// within linkProveBeats beats is closed and re-punched just like a zombie. This replaces an earlier leniency for
// pre-heartbeat builds ("never ponged still counts as healthy") that field-failed 2026-08-31: a half-dead QUIC
// conn (datagrams silently dropped, e.g. a hairpin-NAT punch) sat in "direct" with rtt=-1 FOREVER on one side
// while the other flapped — the host advertised a healthy LAN and every join went into the void. An unproven conn
// is reported "relayed" while it proves (streams retransmit, so presence/announcements still work; only the
// datagram path is unproven).

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"time"

	host "github.com/libp2p/go-libp2p/core/host"
	network "github.com/libp2p/go-libp2p/core/network"
	peer "github.com/libp2p/go-libp2p/core/peer"
)

const (
	linkBeatEvery   = 4 * time.Second // heartbeat cadence — well under mobile-NAT UDP idle timeouts (~30s)
	linkBeatMiss    = 3               // missed pongs on a previously-responsive peer ⇒ zombie connection
	linkDialBackoff = 2 * time.Second // reconnect backoff floor (doubles to linkDialMax)
	linkDialMax     = 30 * time.Second
	linkUpgradeTry  = 30 * time.Second // how often to nudge a relayed link toward a direct (DCUtR) upgrade
	linkProveBeats  = 5                // beats a NEW direct conn gets to produce its first pong before re-punch
	linkProtectTag  = "vg-lan"
)

var overlayDebug = os.Getenv("VG_OVERLAY_DEBUG") != ""

// Link states, ordered by quality. Exposed as strings through VgLanPeers.
const (
	linkDown       = "down"
	linkConnecting = "connecting"
	linkRelayed    = "relayed"
	linkDirect     = "direct"
)

const linkMagic = "VGHB"

func heartbeatPacket(kind byte, tsNano int64) []byte {
	b := make([]byte, 4+1+8)
	copy(b, linkMagic)
	b[4] = kind
	binary.BigEndian.PutUint64(b[5:], uint64(tsNano))
	return b
}

// isHeartbeat reports whether a datagram is a maintainer control packet (vs a forwarded IP packet).
func isHeartbeat(pkt []byte) bool { return len(pkt) == 13 && string(pkt[:4]) == linkMagic }

// peerLink is one friend's maintained link.
type peerLink struct {
	pid  peer.ID
	nick string

	state         string
	rttMs         int64 // -1 = unknown
	lastPong      time.Time
	everPonged    bool
	missed        int
	beatsNoPong   int  // beats sent on a conn that has NEVER ponged (proving phase; reset on pong / teardown)
	sendErrLogged bool // first heartbeat send error named once (reset on pong)

	dialing  bool      // an async (re)dial is in flight
	nextDial time.Time // backoff gate
	backoff  time.Duration
	lastUp   time.Time // last relayed→direct upgrade nudge

	// Counters for the state-change log line (written by the overlay datapath via the maintainer).
	dgTx, dgRx, streamTx, repunch uint64
}

// linkMaintainer runs the per-friend heartbeat/state/repair loop.
type linkMaintainer struct {
	ctx    context.Context
	host   host.Host
	router peerRouter
	// peersFn returns the CURRENT accepted-friend set (pid → nick) each tick — the maintainer never caches
	// membership, so friend add/remove takes effect within one beat with no event plumbing.
	peersFn func() map[peer.ID]string

	// ensureRecvLoops re-kicks datagram receive loops for a peer's existing conns (set by newOverlayService).
	// Called every evaluate: idempotent and cheap, it closes the race where a conn established BEFORE the peer
	// became maintained never got a loop and silently dropped every datagram — including our pings' pongs.
	ensureRecvLoops func(pid peer.ID)

	mu    sync.Mutex
	links map[peer.ID]*peerLink
}

func newLinkMaintainer(ctx context.Context, h host.Host, r peerRouter, peersFn func() map[peer.ID]string) *linkMaintainer {
	return &linkMaintainer{ctx: ctx, host: h, router: r, peersFn: peersFn, links: map[peer.ID]*peerLink{}}
}

func (m *linkMaintainer) run() {
	t := time.NewTicker(linkBeatEvery)
	defer t.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-t.C:
			m.tick()
		}
	}
}

// has reports whether pid is a maintained friend (the overlay's datagram RX gate includes maintained peers so
// heartbeats flow even when no game/link is attached).
func (m *linkMaintainer) has(pid peer.ID) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.links[pid]
	return ok
}

// handleHeartbeat processes a control datagram; sendPong ships a reply datagram on the same direct connection.
func (m *linkMaintainer) handleHeartbeat(pid peer.ID, pkt []byte, sendPong func([]byte)) {
	if !isHeartbeat(pkt) {
		return
	}
	switch pkt[4] {
	case 0: // ping → echo the timestamp back
		sendPong(heartbeatPacket(1, int64(binary.BigEndian.Uint64(pkt[5:]))))
	case 1: // pong → RTT from our echoed timestamp
		sent := time.Unix(0, int64(binary.BigEndian.Uint64(pkt[5:])))
		m.mu.Lock()
		if l := m.links[pid]; l != nil {
			l.rttMs = time.Since(sent).Milliseconds()
			l.lastPong = time.Now()
			l.everPonged = true
			l.missed = 0
			l.beatsNoPong = 0
			l.sendErrLogged = false
		}
		m.mu.Unlock()
	}
}

// noteTx/noteRx/noteStream/noteRepunch — datapath counters for the state-change log.
func (m *linkMaintainer) noteTx(pid peer.ID)     { m.bump(pid, func(l *peerLink) { l.dgTx++ }) }
func (m *linkMaintainer) noteRx(pid peer.ID)     { m.bump(pid, func(l *peerLink) { l.dgRx++ }) }
func (m *linkMaintainer) noteStream(pid peer.ID) { m.bump(pid, func(l *peerLink) { l.streamTx++ }) }
func (m *linkMaintainer) bump(pid peer.ID, f func(*peerLink)) {
	m.mu.Lock()
	if l := m.links[pid]; l != nil {
		f(l)
	}
	m.mu.Unlock()
}

// suspect marks pid's direct path as failing NOW (a datagram send error) — the next tick treats it like a miss
// burst instead of waiting out the full miss window.
func (m *linkMaintainer) suspect(pid peer.ID) {
	m.mu.Lock()
	if l := m.links[pid]; l != nil && l.everPonged {
		l.missed = linkBeatMiss // fast-path the zombie verdict
	}
	m.mu.Unlock()
}

// tick — the whole maintainer: sync membership, beat every link, judge, repair.
func (m *linkMaintainer) tick() {
	want := m.peersFn()

	m.mu.Lock()
	// Membership sync: new friends get a link; removed friends get unprotected + dropped.
	for pid, nick := range want {
		if _, ok := m.links[pid]; !ok {
			m.links[pid] = &peerLink{pid: pid, nick: nick, state: linkDown, rttMs: -1, backoff: linkDialBackoff}
		} else {
			m.links[pid].nick = nick
		}
	}
	for pid := range m.links {
		if _, ok := want[pid]; !ok {
			m.host.ConnManager().Unprotect(pid, linkProtectTag)
			delete(m.links, pid)
		}
	}
	links := make([]*peerLink, 0, len(m.links))
	for _, l := range m.links {
		links = append(links, l)
	}
	m.mu.Unlock()

	for _, l := range links {
		m.evaluate(l)
	}
}

func (m *linkMaintainer) evaluate(l *peerLink) {
	connected := m.host.Network().Connectedness(l.pid) == network.Connected
	direct := directQUICSender(m.host, l.pid)
	m.mu.Lock()
	proven := l.everPonged
	m.mu.Unlock()

	next := l.state
	switch {
	case direct != nil && proven:
		next = linkDirect
	case connected:
		next = linkRelayed // includes an UNPROVEN direct conn: streams work (retransmission), datagrams not yet trusted
	default:
		m.mu.Lock()
		dialing := l.dialing
		m.mu.Unlock()
		if dialing {
			next = linkConnecting
		} else {
			next = linkDown
		}
	}

	// Any conn for a maintained peer must have a datagram receive loop — re-kicked every beat because a conn
	// established before this peer became maintained never got one (and quic-go silently drops unread datagrams,
	// which reads as "the network ate my pongs").
	if m.ensureRecvLoops != nil && connected {
		m.ensureRecvLoops(l.pid)
	}

	if direct != nil {
		// Beat. Send errors were discarded here ('_ ='), so "datagrams not negotiated"-class failures were
		// indistinguishable from packet loss; now they count as misses and get named once.
		if err := direct(heartbeatPacket(0, time.Now().UnixNano())); err != nil {
			m.mu.Lock()
			l.missed++
			first := !l.sendErrLogged
			l.sendErrLogged = true
			m.mu.Unlock()
			if first {
				fmt.Fprintf(os.Stderr, "[lan] %s: heartbeat SEND failed on the direct conn: %v\n", l.nick, err)
			}
		}
		m.mu.Lock()
		if l.everPonged && time.Since(l.lastPong) > linkBeatEvery {
			l.missed++
		} else if !l.everPonged {
			l.beatsNoPong++
		}
		miss, unproven, proven := l.missed, l.beatsNoPong, l.everPonged
		m.mu.Unlock()
		switch {
		case proven && miss >= linkBeatMiss:
			// A previously-responsive peer that stops ponging has a ZOMBIE connection (dead NAT mapping): the
			// QUIC stack keeps "sending" into the void until its own idle timeout — meanwhile every game packet
			// is lost silently. Close it ourselves and re-dial; DCUtR re-punches a fresh direct path.
			fmt.Fprintf(os.Stderr, "[lan] %s: direct link went ZOMBIE (%d missed beats) — closing + re-punching\n", l.nick, miss)
			_ = m.host.Network().ClosePeer(l.pid)
			m.mu.Lock()
			l.missed = 0
			l.beatsNoPong = 0
			l.everPonged = false // the fresh conn must prove itself again
			l.repunch++
			m.mu.Unlock()
			m.kickDial(l, false)
			next = linkConnecting
		case !proven:
			// UNPROVEN: do NOT tear down — the same connection carries the presence/friend streams, and closing
			// it flapped friends to offline every proving window (field regression, 2026-08-31). The link stays
			// usable via stream fallback (state: relayed); we keep beating, say so ONCE, and nudge a force-direct
			// re-dial on the upgrade cadence so a better path can appear without killing the working one.
			if unproven == linkProveBeats {
				fmt.Fprintf(os.Stderr, "[lan] %s: direct conn not answering datagram pings (%d beats) — datagram "+
					"path unproven; traffic rides stream fallback while we keep nudging\n", l.nick, unproven)
			}
			if time.Since(l.lastUp) > linkUpgradeTry {
				l.lastUp = time.Now()
				m.kickDial(l, true)
			}
		}
	} else if connected {
		// Relayed/TCP: usable (stream fallback carries the LAN at announcement rates) but worth upgrading. The
		// nudge must FORCE a direct dial — a plain dial no-ops while any conn exists, so this branch never
		// actually dialed anything before.
		if time.Since(l.lastUp) > linkUpgradeTry {
			l.lastUp = time.Now()
			m.kickDial(l, true)
		}
	} else {
		m.kickDial(l, false)
	}

	if connected || direct != nil {
		m.host.ConnManager().Protect(l.pid, linkProtectTag)
	}

	if next != l.state {
		m.mu.Lock()
		prev := l.state
		l.state = next
		rtt, dgTx, dgRx, st, rp := l.rttMs, l.dgTx, l.dgRx, l.streamTx, l.repunch
		m.mu.Unlock()
		fmt.Fprintf(os.Stderr, "[lan] %s: %s → %s (rtt=%dms dgTx=%d dgRx=%d streamTx=%d repunch=%d)\n",
			l.nick, prev, next, rtt, dgTx, dgRx, st, rp)
	} else if overlayDebug {
		fmt.Fprintf(os.Stderr, "[lan] %s: %s rtt=%dms\n", l.nick, l.state, l.rttMs)
	}
}

// kickDial starts one async (re)dial for l, respecting the backoff and never stacking dials. force uses a
// direct-only dial that bypasses the "already connected" short-circuit — the upgrade path for relayed/unproven
// links (a plain dial is a no-op while any connection exists).
func (m *linkMaintainer) kickDial(l *peerLink, force bool) {
	m.mu.Lock()
	if l.dialing || time.Now().Before(l.nextDial) {
		m.mu.Unlock()
		return
	}
	l.dialing = true
	m.mu.Unlock()
	go func() {
		ctx, cancel := context.WithTimeout(m.ctx, 25*time.Second)
		var err error
		if force {
			err = dialPeerDirect(ctx, m.host, m.router, l.pid)
		} else {
			err = dialPeer(ctx, m.host, m.router, l.pid)
		}
		cancel()
		m.mu.Lock()
		l.dialing = false
		if err != nil {
			// Rate-limited by the backoff itself. This was silent — a peer flapped down↔connecting for an hour
			// tonight and nothing anywhere said WHY the dial failed.
			fmt.Fprintf(os.Stderr, "[lan] %s: dial failed (%v) — retrying in %s\n", l.nick, err, l.backoff)
			l.nextDial = time.Now().Add(l.backoff)
			if l.backoff < linkDialMax {
				l.backoff *= 2
			}
		} else {
			l.backoff = linkDialBackoff
			l.nextDial = time.Time{}
		}
		m.mu.Unlock()
	}()
}

// directQUICSender returns a datagram-send closure over a direct QUIC connection to pid, or nil when the only
// connections are relayed/TCP (heartbeats are datagram-only — their whole point is exercising the direct path).
func directQUICSender(h host.Host, pid peer.ID) func([]byte) error {
	qc := quicConnTo(h, pid)
	if qc == nil {
		return nil
	}
	return func(b []byte) error { return qc.SendDatagram(b) }
}

// snapshot returns the UI view of every maintained link.
type linkInfo struct {
	Peer   string `json:"peer"`
	Nick   string `json:"nick"`
	VIP    string `json:"vip"`
	Online bool   `json:"online"`
	Link   string `json:"link"`
	RttMs  int64  `json:"rttMs"`
}

func (m *linkMaintainer) snapshot(vipOf func(peer.ID) string) []linkInfo {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]linkInfo, 0, len(m.links))
	for _, l := range m.links {
		out = append(out, linkInfo{
			Peer:   l.pid.String(),
			Nick:   l.nick,
			VIP:    vipOf(l.pid),
			Online: l.state == linkDirect || l.state == linkRelayed,
			Link:   l.state,
			RttMs:  l.rttMs,
		})
	}
	return out
}
