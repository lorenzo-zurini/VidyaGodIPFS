package main

// bench.go — experiment instrumentation for open-internet transfer optimization. Two env-gated facilities, both
// no-ops unless their variable is set, so normal builds/behavior are untouched:
//
//   VG_BENCH_NO_TUNNEL=1  — install a connection gater that BLOCKS dialing/accepting on the private overlay subnets
//                           this pair of machines shares (WireGuard 10.10.0.0/24, ZeroTier 172.25.0.0/16). Without
//                           this, libp2p happily discovers the peer over the tunnel and a "fast" benchmark is a lie.
//                           Extra CIDRs can be added via VG_BENCH_BLOCK_CIDRS (comma-separated).
//
//   VG_BENCH_OBSERVE=1    — every 2s, log ground truth: our current external/relay addresses, and for each live
//                           connection the peer, remote multiaddr, direction, transport, whether it is RELAYED
//                           (/p2p-circuit) or DIRECT, and its rolling up/down bandwidth. This is how both agents see
//                           which path traffic actually takes.

import (
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	metrics "github.com/libp2p/go-libp2p/core/metrics"
	network "github.com/libp2p/go-libp2p/core/network"
	conngater "github.com/libp2p/go-libp2p/p2p/net/conngater"
)

func benchNoTunnel() bool { return os.Getenv("VG_BENCH_NO_TUNNEL") != "" }
func benchObserve() bool  { return os.Getenv("VG_BENCH_OBSERVE") != "" }

// benchBlockedCIDRs is the tunnel/overlay subnets to exclude from benchmark traffic, plus any from VG_BENCH_BLOCK_CIDRS.
// Includes fc00::/7 (ALL IPv6 ULA) because ZeroTier assigns RFC4193 (fd…) + 6PLANE (fc…) ULAs in addition to its IPv4
// range — an IPv4-only block leaks: libp2p finds the peer over the ULA overlay and dials direct, so a "gated" run
// silently rides the tunnel. ULA is never internet-routable, so blocking the whole /7 can't remove a legit open-internet
// path (that is 2000::/3 global unicast, untouched). fe80::/10 (link-local) blocked for the same reason.
func benchBlockedCIDRs() []*net.IPNet {
	spec := "10.10.0.0/24,172.25.0.0/16,fc00::/7,fe80::/10" // WireGuard wg0 + ZeroTier ztksezll47 (IPv4 range + all ULA)
	if extra := os.Getenv("VG_BENCH_BLOCK_CIDRS"); extra != "" {
		spec += "," + extra
	}
	var out []*net.IPNet
	for _, c := range strings.Split(spec, ",") {
		c = strings.TrimSpace(c)
		if c == "" {
			continue
		}
		if _, n, err := net.ParseCIDR(c); err == nil {
			out = append(out, n)
		}
	}
	return out
}

// newBenchGater returns a connection gater blocking the tunnel subnets, or nil if the facility is off / build fails.
// The gater intercepts both outbound address dials and inbound accepts by IP, so neither side can ride the tunnel.
func newBenchGater() *conngater.BasicConnectionGater {
	if !benchNoTunnel() {
		return nil
	}
	g, err := conngater.NewBasicConnectionGater(nil) // nil datastore → in-memory, rules not persisted (benchmark-only)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[bench] gater build failed: %v\n", err)
		return nil
	}
	var blocked []string
	for _, n := range benchBlockedCIDRs() {
		if err := g.BlockSubnet(n); err == nil {
			blocked = append(blocked, n.String())
		}
	}
	fmt.Fprintf(os.Stderr, "[bench] NO_TUNNEL active — blocking subnets: %s\n", strings.Join(blocked, " "))
	return g
}

// maTransport names the outermost wire transport in a multiaddr string (for the observer's readout).
func maTransport(s string) string {
	switch {
	case strings.Contains(s, "/webtransport"):
		return "webtransport"
	case strings.Contains(s, "/quic-v1"):
		return "quic"
	case strings.Contains(s, "/ws"):
		return "ws"
	case strings.Contains(s, "/tcp/"):
		return "tcp"
	default:
		return "?"
	}
}

// startBenchObserver spins a goroutine that periodically logs external addrs + per-connection path & bandwidth.
func (n *node) startBenchObserver() {
	if !benchObserve() || n.host == nil {
		return
	}
	go func() {
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-n.ctx.Done():
				return
			case <-t.C:
				n.logBenchSnapshot()
			}
		}
	}()
}

func (n *node) logBenchSnapshot() {
	h := n.host
	if h == nil {
		return
	}
	var b strings.Builder
	b.WriteString("[bench] ── snapshot ──\n")

	// Our externally-observed addresses (what peers can reach us on): flag relay circuits explicitly.
	for _, a := range h.Addrs() {
		s := a.String()
		if strings.Contains(s, "/p2p-circuit") {
			b.WriteString("[bench] self RELAY-addr " + s + "\n")
		} else if !benchIsPrivate(s) {
			b.WriteString("[bench] self PUBLIC-addr " + s + "\n")
		}
	}

	conns := h.Network().Conns()
	b.WriteString(fmt.Sprintf("[bench] %d live connection(s)\n", len(conns)))
	for _, c := range conns {
		p := c.RemotePeer()
		ras := c.RemoteMultiaddr().String()
		path := "DIRECT"
		if strings.Contains(ras, "/p2p-circuit") {
			path = "RELAYED"
		}
		dir := "out"
		if c.Stat().Direction == network.DirInbound {
			dir = "in "
		}
		var rate metrics.Stats
		if n.bwc != nil {
			rate = n.bwc.GetBandwidthForPeer(p)
		}
		b.WriteString(fmt.Sprintf("[bench]   %s %-7s %-11s %s  ↓%.2f MB/s ↑%.2f MB/s  %s\n",
			dir, path, maTransport(ras), shortPeer(p.String()), rate.RateIn/1e6, rate.RateOut/1e6, ras))
	}
	if n.bwc != nil {
		tot := n.bwc.GetBandwidthTotals()
		b.WriteString(fmt.Sprintf("[bench] TOTAL ↓%.2f MB/s ↑%.2f MB/s (cum ↓%d ↑%d MB)\n",
			tot.RateIn/1e6, tot.RateOut/1e6, tot.TotalIn/1_000_000, tot.TotalOut/1_000_000))
	}
	fmt.Fprint(os.Stderr, b.String())
}

func shortPeer(p string) string {
	if len(p) > 10 {
		return p[len(p)-8:]
	}
	return p
}

// benchIsPrivate reports whether a multiaddr's IP is a private/loopback/link-local one (not an internet address).
func benchIsPrivate(s string) bool {
	fields := strings.Split(s, "/")
	for i := 0; i+1 < len(fields); i++ {
		if fields[i] == "ip4" || fields[i] == "ip6" {
			if ip := net.ParseIP(fields[i+1]); ip != nil {
				return ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast()
			}
		}
	}
	return true // no IP (e.g. a /dns or /p2p-circuit relay addr) → treat as "not a public IP literal"
}
