package main

// netgate.go — the dial budget: the one queue at the level a home router counts. A router's connection-tracking
// table holds one entry per new flow (16 384 on the GL-MT3600BE that fell over), kept 60 s unanswered, 180 s answered.
// The node's queue (netq.go) bounds OPERATIONS, but one DHT walk is hundreds of dials: measured 2026-09-27, one idle
// node's DHT walks reached ~40 dials/s and two nodes filled that table in seconds — "nf_conntrack: table full,
// dropping packet", every device in the house offline.
//
// So every new outbound PEER DIAL takes a token from one per-node bucket (dialRate/s, dialBurst) before it touches the
// network, whichever subsystem asks. (One dial may try a few of the peer's addresses and transports, so a token is a
// few flows at most — the budget is in dials, the table in flows.) A friend, a peer found on our LAN, and a provider we are fetching from go
// first (they are what the user is waiting on); a dial that cannot get a token within dialMaxWait is refused (the DHT
// moves on to the next peer). An existing connection costs nothing — the gater is consulted only for a new one.
//
// And a remote peer's PRIVATE addresses (10/8, 172.16/12, 192.168/16, 100.64/10 CGNAT, loopback, link-local, ULA) are
// never dialed across the internet — they can't answer, and each attempt is a dead table entry for 60–120 s (Kubo's
// "server" profile filters them for the same reason) — unless the peer is a friend, was found on our LAN (mDNS), or
// was named by address in an explicit connect.

import (
	"net"
	"sync"
	"time"

	coreconnmgr "github.com/libp2p/go-libp2p/core/connmgr"
	"github.com/libp2p/go-libp2p/core/control"
	"github.com/libp2p/go-libp2p/core/network"
	peer "github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

// The budget. Vars for tests.
var (
	dialRate    = 4.0             // new outbound connections per second, sustained
	dialBurst   = 12.0            // … and at once
	dialMaxWait = 3 * time.Second // longer than this in line and the dial is refused
)

// tokenBucket: dialRate tokens/s up to dialBurst. take waits up to max for one; false = none in time.
type tokenBucket struct {
	mu     sync.Mutex
	tokens float64
	last   time.Time
}

func (b *tokenBucket) take(max time.Duration) bool {
	deadline := time.Now().Add(max)
	for {
		b.mu.Lock()
		now := time.Now()
		if b.last.IsZero() {
			b.last, b.tokens = now, dialBurst
		}
		b.tokens += now.Sub(b.last).Seconds() * dialRate
		if b.tokens > dialBurst {
			b.tokens = dialBurst
		}
		b.last = now
		if b.tokens >= 1 {
			b.tokens--
			b.mu.Unlock()
			return true
		}
		wait := time.Duration((1 - b.tokens) / dialRate * float64(time.Second))
		b.mu.Unlock()
		if now.Add(wait).After(deadline) {
			return false
		}
		time.Sleep(wait)
	}
}

// netGate is the node's connection gater: the dial budget + private-address filter (+ the bench gater, the dial trace).
type netGate struct {
	n      *node
	bucket tokenBucket
	inner  coreconnmgr.ConnectionGater // bench.go's subnet blocker, or nil
	trace  *dialTracer                 // VG_DIAL_TRACE, or nil
}

// priority: dials the user is waiting on skip the line (still counted).
func (g *netGate) priority(p peer.ID) bool {
	return g.n.isFriend(p) || g.n.isLanPeer(p) || g.n.isWantedProvider(p)
}

func (g *netGate) InterceptPeerDial(p peer.ID) bool {
	if g.trace != nil {
		g.trace.note()
	}
	if g.inner != nil && !g.inner.InterceptPeerDial(p) {
		return false
	}
	if g.priority(p) {
		netStats.dialsPriority.Add(1)
		return true
	}
	if !g.bucket.take(dialMaxWait) {
		netStats.dialsRefused.Add(1)
		return false
	}
	netStats.dialsBudget.Add(1)
	return true
}

func (g *netGate) InterceptAddrDial(p peer.ID, a ma.Multiaddr) bool {
	if g.inner != nil && !g.inner.InterceptAddrDial(p, a) {
		return false
	}
	if privateAddr(a) && !g.n.isFriend(p) && !g.n.isLanPeer(p) {
		netStats.dialsPrivateSkipped.Add(1)
		return false
	}
	return true
}

func (g *netGate) InterceptAccept(c network.ConnMultiaddrs) bool {
	return g.inner == nil || g.inner.InterceptAccept(c)
}
func (g *netGate) InterceptSecured(d network.Direction, p peer.ID, c network.ConnMultiaddrs) bool {
	return g.inner == nil || g.inner.InterceptSecured(d, p, c)
}
func (g *netGate) InterceptUpgraded(c network.Conn) (bool, control.DisconnectReason) {
	if g.inner == nil {
		return true, 0
	}
	return g.inner.InterceptUpgraded(c)
}

var cgnat = func() *net.IPNet { _, n, _ := net.ParseCIDR("100.64.0.0/10"); return n }()

// privateAddr: an address only reachable on its owner's own network (or its ISP's): not worth a dial from outside.
func privateAddr(a ma.Multiaddr) bool {
	ip, err := manet.ToIP(a)
	if err != nil {
		return false // /dns…, /p2p-circuit relays: public by construction
	}
	return ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsUnspecified() || cgnat.Contains(ip)
}

// ---- who the node knows is close ----

// isFriend: an accepted friend.
func (n *node) isFriend(p peer.ID) bool {
	if n.social == nil {
		return false
	}
	s := p.String()
	for _, f := range n.social.acceptedPeers() {
		if f == s {
			return true
		}
	}
	return false
}

// lanPeers: peers found on our own network (mDNS, re-announced while they stay) or connected to by address, for as
// long as the peerstore keeps the addresses mDNS found. A laptop that left the network (or a benchmark peer) is a
// stranger again after that: its private addresses are not dialed across the internet, its dials take the budget.
const lanPeerTTL = time.Hour

func (n *node) noteLanPeer(p peer.ID) { n.lanPeers.Store(p, time.Now().Add(lanPeerTTL)) }
func (n *node) isLanPeer(p peer.ID) bool {
	v, ok := n.lanPeers.Load(p)
	if !ok {
		return false
	}
	if time.Now().After(v.(time.Time)) {
		n.lanPeers.Delete(p)
		return false
	}
	return true
}

// wantedProviders: providers a fetch found, for a while — dialing them is what the user is waiting on.
const wantedProviderTTL = 2 * time.Minute

func (n *node) noteWantedProvider(p peer.ID) {
	n.wantedProv.Store(p, time.Now().Add(wantedProviderTTL))
}
func (n *node) isWantedProvider(p peer.ID) bool {
	v, ok := n.wantedProv.Load(p)
	if !ok {
		return false
	}
	if time.Now().After(v.(time.Time)) {
		n.wantedProv.Delete(p)
		return false
	}
	return true
}
