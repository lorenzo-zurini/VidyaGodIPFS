package main

// overlaylink.go — the ALWAYS-ON link maintainer behind the virtual LAN: one tiny heartbeat/state machine per
// accepted friend, running for the app's lifetime (not just while a game is up). It exists because the field
// failure mode of the overlay was SILENT: a NAT mapping idles out (or a hole-punch comes up one-way), the direct
// QUIC connection turns half-dead, and every game packet vanishes with no error and no fallback — the LAN menu
// just stays empty. The maintainer makes the link's health explicit and self-repairing, under two invariants
// distilled from the field (2026-09-03/04):
//
//   INVARIANT 1 — NEVER TEAR DOWN A WORKING CONNECTION. The one libp2p connection to a friend multiplexes
//   bitswap downloads, the friend/presence protocol AND the overlay. An earlier repair strategy ClosePeer'd on a
//   zombie verdict and took live downloads with it ("the whole panel said direct 5ms and the download died").
//   The maintainer only observes, dials and routes; teardown is banned.
//
//   INVARIANT 2 — DATAGRAMS MUST PROVE THEMSELVES. A direct QUIC conn whose datagrams flow only one way (hairpin
//   punch, asymmetric firewall) reports send-success while eating every packet — measured live as 100% one-way
//   loss with the link showing "direct". So the datagram fast path is TRUSTED only while pongs are arriving:
//   the overlay TX path consults datagramsTrusted() and rides the reliable stream (which works over ANY conn,
//   including relayed) until the first pong, and demotes back to the stream the moment pongs stop. Loss of the
//   fast path degrades bandwidth/latency, never delivery.
//
// The per-friend state machine, every state with an exit (graceful recovery from ANY state):
//
//   down ──kickDial(backoff 2s→30s)──▶ connecting ──▶ relayed (stream carries traffic; works over relay via
//   limited-conn streams) ──force-direct nudge every linkUpgradeTry──▶ direct-UNPROVEN (still stream; heartbeats
//   probing) ──first pong──▶ direct-PROVEN (datagram fast path) ──linkBeatMiss missed pongs / send error──▶
//   demoted to UNPROVEN (traffic instantly back on stream) + fresh force-direct nudge. Friend removed → link
//   dropped + unprotected. Friend added → link appears within one beat (membership re-read every tick).
//
//   • HEARTBEAT: a ~13-byte control datagram every linkBeatEvery over the same direct-QUIC connection the game
//     traffic rides. Defeats NAT idle timeouts (4s ≪ 30s), yields live RTT for the UI, and IS the trust signal.
//   • STATE: direct (QUIC + fresh pongs) / relayed (usable via stream — includes an unproven direct conn) /
//     connecting / down — exposed via VgLanPeers for the Friends tab and the launch window's Virtual LAN panel.
//   • PROTECT: maintained peers' connections are pinned in the ConnManager so background churn never trims them.
//   • RECV LOOPS: every evaluate re-kicks the overlay's datagram receive loops for the peer's conns
//     (ensureRecvLoops) — a conn established BEFORE the peer became a friend would otherwise never get a loop
//     and silently drop every datagram, including the pongs that prove the path.
//
// Wire format (QUIC datagram, intercepted in datagramRecvLoop BEFORE the IPv4 check — 'V' = 0x56 can never be an
// IPv4 header byte): "VGHB" + kind (0 ping / 1 pong) + 8-byte big-endian unix-nano send-timestamp, echoed
// verbatim in the pong. Peers on builds without heartbeat support never pong — their link simply stays on the
// reliable stream path forever (correct, just not fast), and the demote logic never fires on them (everPonged
// stays false).

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
	quic "github.com/quic-go/quic-go"
)

const (
	linkBeatEvery   = 4 * time.Second // heartbeat cadence — well under mobile-NAT UDP idle timeouts (~30s)
	linkBeatMiss    = 3               // missed pongs on a previously-responsive peer ⇒ demote datagrams to stream
	linkDialBackoff = 2 * time.Second // reconnect backoff floor (doubles to linkDialMax)
	linkDialMax     = 30 * time.Second
	linkUpgradeTry  = 30 * time.Second // how often to nudge a relayed/unproven link toward a fresh direct path
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

	state        string
	rttMs        int64 // -1 = unknown
	lastPong     time.Time
	everPonged   bool   // true while the CURRENT direct path is proven (reset on demote)
	provenConnID string // the network.Conn.ID() the last pong arrived on — trust is PER-CONN (adversarial H2):
	// pongs prove one specific connection; a different QUIC conn to the same peer is a different, untested path.
	missed int

	dialing  bool      // an async (re)dial is in flight
	nextDial time.Time // backoff gate
	backoff  time.Duration
	lastUp   time.Time // last relayed/unproven → direct upgrade nudge

	// Counters for the state-change log line (written by the overlay datapath via the maintainer).
	dgTx, dgRx, streamTx, demoted uint64
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
			// guard: a panic in one tick (a bad conn, a nil peerstore entry) is logged and skipped — the
			// maintainer keeps running and, critically, never takes the whole node down with it.
			guard("linkm.tick", m.tick)
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

// datagramsTrusted is the overlay TX gate (Invariant 2): the datagram fast path may carry pid's packets only
// while the direct path is PROVEN — pongs arrived and haven't stopped. Everything else (fresh conn, one-way
// path, pongs gone quiet, peer without heartbeat support, and peers the maintainer does NOT track) rides the
// reliable stream, so delivery never depends on an unverified path.
func (m *linkMaintainer) datagramsTrusted(pid peer.ID) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	l := m.links[pid]
	if l == nil {
		// A peer the maintainer does not track gets NO trust: a friend removed mid-game leaves a stale route
		// whose heartbeats have stopped — exactly the forever-unproven path Invariant 2 bans (adversarial M2).
		// The stream fallback still delivers. (Overlay-only tests run with linkm==nil and never reach here.)
		return false
	}
	return l.everPonged && l.missed < linkBeatMiss
}

// trustedConnFor is the single per-packet gate query (one lock take on the hot path — adversarial round-2 L9):
// tracked reports whether the maintainer knows pid at all (false → maintainer-less legacy path, tests), and
// connID is the ONE connection game datagrams may ride ("" = untrusted → stream). Trust binds to the specific
// conn the last pong proved; any sibling is an untested path.
func (m *linkMaintainer) trustedConnFor(pid peer.ID) (connID string, tracked bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	l := m.links[pid]
	if l == nil {
		return "", false
	}
	if !l.everPonged || l.missed >= linkBeatMiss {
		return "", true
	}
	return l.provenConnID, true
}

// handleHeartbeat processes a control datagram; sendPong ships a reply datagram on the same direct connection.
func (m *linkMaintainer) handleHeartbeat(pid peer.ID, connID string, pkt []byte, sendPong func([]byte)) {
	if !isHeartbeat(pkt) {
		return
	}
	switch pkt[4] {
	case 0: // ping → echo the timestamp back
		sendPong(heartbeatPacket(1, int64(binary.BigEndian.Uint64(pkt[5:]))))
	case 1: // pong → RTT from our echoed timestamp; a pong is what PROVES (or re-proves) the datagram path
		sent := time.Unix(0, int64(binary.BigEndian.Uint64(pkt[5:])))
		m.mu.Lock()
		if l := m.links[pid]; l != nil {
			l.rttMs = time.Since(sent).Milliseconds()
			l.lastPong = time.Now()
			wasProven := l.everPonged
			l.everPonged = true
			l.provenConnID = connID
			l.missed = 0
			if !wasProven {
				vlog("linkm", "PONG %s: datagram path PROVEN (rtt=%dms)", l.nick, l.rttMs)
			} else {
				vlog("linkm", "pong %s rtt=%dms", l.nick, l.rttMs)
			}
		}
		m.mu.Unlock()
	}
}

// noteTx/noteRx/noteStream — datapath counters for the state-change log.
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

// suspect marks pid's direct path as failing NOW (a datagram send error): the trust gate flips to stream on the
// spot (missed saturates), and the next tick demotes + nudges a fresh direct path. No teardown.
func (m *linkMaintainer) suspect(pid peer.ID) {
	m.mu.Lock()
	if l := m.links[pid]; l != nil && l.everPonged {
		l.missed = linkBeatMiss // instant distrust; evaluate() turns it into a demote + upgrade nudge
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
	// Limited (circuit-v2 relayed) COUNTS as connected: our stream fallback opts into limited conns, so a
	// relay-only friend has a fully working link — treating Limited as "down" showed a live link as down in the
	// panel and re-dialed it uselessly every beat (caught by the battery's post-outage state assert, 2026-09-04).
	connState := m.host.Network().Connectedness(l.pid)
	connected := connState == network.Connected || connState == network.Limited
	conns := quicConnsTo(m.host, l.pid) // every direct QUIC conn — each gets a beat, any may prove itself
	direct := len(conns) > 0
	connGone := false
	m.mu.Lock()
	// Instant demote when the PROVEN conn is gone (closed, replaced by a re-punch): trust died with it — waiting
	// out the miss window would ride an untested sibling conn for up to 3 beats (adversarial H2).
	if l.everPonged && l.provenConnID != "" {
		alive := false
		for _, c := range conns {
			if c.ID() == l.provenConnID {
				alive = true
				break
			}
		}
		if !alive {
			l.everPonged, l.provenConnID, l.missed, l.rttMs = false, "", 0, -1
			l.demoted++
			connGone = true // printed AFTER Unlock: this lock is on the per-packet TX path (adversarial round-3)
		}
	}
	proven := l.everPonged && l.missed < linkBeatMiss
	missed, rtt := l.missed, l.rttMs
	m.mu.Unlock()
	if connGone {
		fmt.Fprintf(os.Stderr, "[lan] %s: proven conn gone — datagrams distrusted until a new pong\n", l.nick)
	}
	vlog("linkm", "eval %s state=%s connected=%v directQUIC=%v proven=%v missed=%d rtt=%dms",
		l.nick, l.state, connected, direct, proven, missed, rtt)

	// Re-kick datagram receive loops for every existing conn — a conn that predated friendship (a bitswap dial,
	// a presence stream) would otherwise NEVER get a loop, and a missed loop means our pings' pongs land in the
	// void forever. Idempotent per conn.
	if m.ensureRecvLoops != nil && connected {
		m.ensureRecvLoops(l.pid)
	}

	next := l.state
	switch {
	case direct && proven:
		next = linkDirect
	case connected:
		next = linkRelayed // includes an UNPROVEN direct conn: the stream carries traffic while heartbeats probe
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

	if direct {
		// Beat — proving a fresh path or confirming a proven one. A previously-responsive peer that stops
		// ponging has a dead datagram path (idled-out NAT mapping, path change): DEMOTE it (Invariant 2) so
		// traffic rides the reliable stream, and nudge a fresh force-direct dial for a new datagram path.
		// NEVER ClosePeer (Invariant 1) — bitswap and the friend protocol share this connection.
		for _, c := range conns { // beat EVERY direct conn: any of them may pong and become the proven path
			var qc *quic.Conn
			if c.As(&qc) && qc != nil {
				_ = qc.SendDatagram(heartbeatPacket(0, time.Now().UnixNano()))
			}
		}
		m.mu.Lock()
		if l.everPonged && time.Since(l.lastPong) > linkBeatEvery {
			l.missed++
		}
		miss, wasProven := l.missed, l.everPonged
		if wasProven && miss >= linkBeatMiss {
			l.everPonged = false // demote: datagrams no longer trusted until the next pong re-proves them
			l.missed = 0
			l.rttMs = -1
			l.demoted++
			m.mu.Unlock()
			fmt.Fprintf(os.Stderr, "[lan] %s: datagram pongs stopped (%d misses) — stream fallback, nudging a fresh direct path (no teardown)\n", l.nick, miss)
			m.kickUpgrade(l)
			next = linkRelayed // still connected and usable via stream
		} else {
			m.mu.Unlock()
			if !wasProven {
				// Unproven direct conn (fresh punch, one-way path, or a demoted link): traffic is on the
				// stream; periodically nudge a force-direct dial — a NEW punch can replace a one-way path.
				m.kickUpgrade(l)
			}
		}
	} else if connected {
		// Relayed/TCP: usable (limited-conn stream fallback carries the LAN) but worth upgrading. A gentle
		// periodic force-direct dial gives DCUtR/mDNS-addrs fresh chances without flapping the working relay.
		m.kickUpgrade(l)
	} else {
		m.kickDial(l, false)
	}

	if connected || direct {
		m.host.ConnManager().Protect(l.pid, linkProtectTag)
	}

	if next != l.state {
		m.mu.Lock()
		prev := l.state
		l.state = next
		rtt, dgTx, dgRx, st, dm := l.rttMs, l.dgTx, l.dgRx, l.streamTx, l.demoted
		m.mu.Unlock()
		fmt.Fprintf(os.Stderr, "[lan] %s: %s → %s (rtt=%dms dgTx=%d dgRx=%d streamTx=%d demoted=%d)\n",
			l.nick, prev, next, rtt, dgTx, dgRx, st, dm)
	} else if overlayDebug {
		fmt.Fprintf(os.Stderr, "[lan] %s: %s rtt=%dms\n", l.nick, next, rtt) // snapshotted values — l.rttMs is
		// written under m.mu from recv goroutines; a bare read here was the fixed HIGH-2's surviving sibling
	}
}

// kickUpgrade rate-limits force-direct upgrade dials to the linkUpgradeTry cadence (shared by the relayed,
// unproven-direct and just-demoted cases, so a link never dials more often than the cadence allows).
func (m *linkMaintainer) kickUpgrade(l *peerLink) {
	m.mu.Lock()
	due := time.Since(l.lastUp) > linkUpgradeTry
	if due {
		l.lastUp = time.Now()
	}
	m.mu.Unlock()
	if due {
		m.kickDial(l, true)
	}
}

// kickDial starts one async (re)dial for l, respecting the backoff and never stacking dials. direct=true forces
// a direct-path dial attempt even while a relayed/TCP connection exists (dialPeer would short-circuit on
// Connectedness and never try the better path).
func (m *linkMaintainer) kickDial(l *peerLink, direct bool) {
	m.mu.Lock()
	if l.dialing || time.Now().Before(l.nextDial) {
		m.mu.Unlock()
		return
	}
	l.dialing = true
	nick := l.nick // captured under m.mu: tick() rewrites l.nick concurrently — an unlocked read from the dial
	// goroutine was a torn-string race (-race caught it; adversarial round-2 HIGH-2)
	m.mu.Unlock()
	safeGo("linkm.kickDial", func() {
		// DEFERRED, not sequential: a recovered panic in the dial would otherwise leave dialing=true forever and
		// the link permanently stuck in "connecting" with every future kickDial short-circuiting (adversarial H1).
		defer func() {
			m.mu.Lock()
			l.dialing = false
			m.mu.Unlock()
		}()
		vlog("linkm", "dial %s (forceDirect=%v)", nick, direct)
		ctx, cancel := context.WithTimeout(m.ctx, 25*time.Second)
		var err error
		if direct {
			err = dialPeerDirect(ctx, m.host, m.router, l.pid)
		} else {
			err = dialPeer(ctx, m.host, m.router, l.pid)
		}
		cancel()
		m.mu.Lock()
		if err == nil {
			vlog("linkm", "dial %s ok (forceDirect=%v)", nick, direct)
		}
		if err != nil {
			// Rate-limited by the backoff itself. Was once silent — a peer flapped down↔connecting for an hour
			// and nothing anywhere said WHY the dial failed.
			fmt.Fprintf(os.Stderr, "[lan] %s: dial failed (%v) — retrying in %s\n", nick, err, l.backoff)
			l.nextDial = time.Now().Add(l.backoff)
			if l.backoff < linkDialMax {
				l.backoff *= 2
			}
		} else {
			l.backoff = linkDialBackoff
			l.nextDial = time.Time{}
		}
		m.mu.Unlock()
	})
}

// quicConnsTo returns EVERY direct QUIC connection to pid (relayed/TCP conns don't unwrap). All of them get
// heartbeats — proving is per-conn, and after a re-punch the old and new conn briefly coexist.
func quicConnsTo(h host.Host, pid peer.ID) []network.Conn {
	var out []network.Conn
	for _, c := range h.Network().ConnsToPeer(pid) {
		var qc *quic.Conn
		if c.As(&qc) && qc != nil {
			out = append(out, c)
		}
	}
	return out
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
