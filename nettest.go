package main

// nettest.go — the Settings → Network "is my firewall the problem?" sweep.
//
// Every network subsystem VidyaGod has fails SOFT: a blocked UDP port degrades QUIC to TCP, a blocked 5353
// silences same-LAN discovery, a hostile NAT quietly parks friends on relays — and each of those just looks
// like "slow" or "can't see my friend". This runs one bounded, concurrent probe per subsystem and reports
// ok/warn/fail with a human sentence, so the DIFFERENTIALS do the diagnosis: TCP peers but zero QUIC conns
// means UDP is blocked; internet fine but no mDNS answers means multicast/5353 is filtered; public HTTPS OK
// but zero DHT providers means the P2P ports specifically are the problem.
//
// Design constraints: every check has its own timeout (the whole sweep is bounded by nettestBudget), checks
// run CONCURRENTLY (a dead route must not serialize into a minute of waiting), and the sweep never mutates
// node state — it only observes, so it is safe to run mid-download or mid-game.

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/zeroconf/v2"
)

const nettestBudget = 30 * time.Second

type netCheck struct {
	Name   string `json:"name"`
	Status string `json:"status"` // "ok" | "warn" | "fail"
	Detail string `json:"detail"`
}

// runNetworkTest runs every probe and returns them in a stable order. Callable only while online (the API
// layer reports the offline case itself).
func (n *node) runNetworkTest() []netCheck {
	ctx, cancel := context.WithTimeout(n.ctx, nettestBudget)
	defer cancel()

	// Warm-up grace: a freshly-started node has 0 peers and an empty link table for a few seconds, and judging
	// during that window produced confident nonsense on perfectly healthy machines — "all outbound P2P traffic
	// may be blocked" on the first live run, then "UDP looks BLOCKED" on the second because the single warm-up
	// peer happened to be TCP. Wait (bounded) for a few peers, not just the first.
	for waited := 0; len(n.host.Network().Peers()) < 3 && waited < 48; waited++ {
		select {
		case <-ctx.Done():
			waited = 48
		case <-time.After(250 * time.Millisecond):
		}
	}

	var mu sync.Mutex
	out := map[string]netCheck{}
	put := func(c netCheck) { mu.Lock(); out[c.Name] = c; mu.Unlock() }

	var wg sync.WaitGroup
	run := func(f func()) { wg.Add(1); safeGo("nettest.check", func() { defer wg.Done(); f() }) }

	run(func() { put(n.checkHTTPS(ctx)) })
	run(func() { put(n.checkDoH(ctx)) })
	run(func() { put(n.checkPeers()) })
	run(func() { put(n.checkUDP()) })
	run(func() { put(n.checkInbound()) })
	run(func() { put(n.checkMDNS(ctx)) })
	run(func() { put(n.checkDHT(ctx)) })
	run(func() { put(n.checkFriendLinks()) })
	wg.Wait()

	order := []string{"Internet (HTTPS)", "DNS over HTTPS", "Peer connectivity", "UDP / QUIC",
		"Inbound reachability", "LAN discovery (mDNS)", "DHT lookups", "Friend links"}
	res := make([]netCheck, 0, len(order))
	for _, k := range order {
		if c, ok := out[k]; ok {
			res = append(res, c)
		}
	}
	return res
}

// Outbound HTTPS through the same DoH-resolving client the delegated router uses: proves TCP 443 + TLS + DNS.
func (n *node) checkHTTPS(ctx context.Context) netCheck {
	c := netCheck{Name: "Internet (HTTPS)"}
	hc := dohHTTPClient(newDoHResolver())
	hc.Timeout = 8 * time.Second
	req, _ := http.NewRequestWithContext(ctx, "GET", "https://delegated-ipfs.dev/routing/v1/peers/12D3KooWD3eckifWpRn9wQpMG9R9hX3sD158z7EqHWmweQAJU5SA", nil)
	t0 := time.Now()
	resp, err := hc.Do(req)
	if err != nil {
		c.Status, c.Detail = "fail", "cannot reach the public router over HTTPS — outbound 443 or DNS is blocked ("+rootErr(err)+")"
		return c
	}
	resp.Body.Close()
	c.Status = "ok"
	c.Detail = fmt.Sprintf("reached delegated-ipfs.dev in %d ms (HTTP %d)", time.Since(t0).Milliseconds(), resp.StatusCode)
	return c
}

// DoH resolution on its own — separates "DNS is filtered" from "everything 443 is dead".
func (n *node) checkDoH(ctx context.Context) netCheck {
	c := netCheck{Name: "DNS over HTTPS"}
	cctx, cancel := context.WithTimeout(ctx, 6*time.Second)
	defer cancel()
	t0 := time.Now()
	// delegated-ipfs.dev, not bootstrap.libp2p.io: the probe target must be a name that EXISTS and that we
	// depend on anyway — the first live run failed here purely because the old libp2p bootstrap hostname is gone.
	ips, err := newDoHResolver().LookupIPAddr(cctx, "delegated-ipfs.dev")
	if err != nil || len(ips) == 0 {
		c.Status, c.Detail = "fail", "DoH resolution failed — HTTPS DNS providers unreachable ("+rootErr(err)+")"
		return c
	}
	c.Status = "ok"
	c.Detail = fmt.Sprintf("resolved delegated-ipfs.dev → %d addr(s) in %d ms", len(ips), time.Since(t0).Milliseconds())
	return c
}

// Swarm connectivity, broken down by transport — the raw material for the UDP differential below.
func (n *node) checkPeers() netCheck {
	c := netCheck{Name: "Peer connectivity"}
	peers := n.host.Network().Peers()
	if len(peers) == 0 {
		c.Status, c.Detail = "fail", "connected to 0 peers — all outbound P2P traffic may be blocked"
		return c
	}
	c.Status = "ok"
	if len(peers) < 5 {
		c.Status = "warn"
	}
	c.Detail = fmt.Sprintf("connected to %d peer(s)", len(peers))
	return c
}

// The UDP-blocked signal: plenty of TCP conns but not a single QUIC one is the classic "firewall allows TCP,
// drops UDP" shape — and it silently costs hole-punching, datagram tunnels, and half the throughput.
func (n *node) checkUDP() netCheck {
	c := netCheck{Name: "UDP / QUIC"}
	count := func() (quic, tcp int) {
		for _, conn := range n.host.Network().Conns() {
			a := conn.RemoteMultiaddr().String()
			switch {
			case strings.Contains(a, "/quic"):
				quic++
			case strings.Contains(a, "/tcp"):
				tcp++
			}
		}
		return
	}
	quic, tcp := count()
	// "UDP blocked" is the scariest verdict on the page; do not shout it because the first couple of dials
	// happened to land on TCP. Give QUIC a bounded window to appear before judging.
	for waited := 0; quic == 0 && waited < 32; waited++ {
		time.Sleep(250 * time.Millisecond)
		quic, tcp = count()
	}
	switch {
	case quic > 0:
		c.Status, c.Detail = "ok", fmt.Sprintf("%d QUIC (UDP) connection(s), %d TCP", quic, tcp)
	case tcp > 0:
		c.Status, c.Detail = "fail", fmt.Sprintf("0 QUIC connections but %d over TCP — outbound UDP looks BLOCKED; "+
			"expect no hole-punching, relayed multiplayer links and reduced throughput", tcp)
	default:
		c.Status, c.Detail = "warn", "no connections to classify yet — run again once peers connect"
	}
	return c
}

// Inbound: do we advertise any direct public address, or only relay circuits? Relay-only is not an error
// (hostile NAT is a fact of life) but it is worth telling the user their peers must hole-punch to reach them.
func (n *node) checkInbound() netCheck {
	c := netCheck{Name: "Inbound reachability"}
	direct, relayed := 0, 0
	for _, a := range n.host.Addrs() {
		s := a.String()
		if strings.Contains(s, "/p2p-circuit") {
			relayed++
			continue
		}
		if isPublicish(s) {
			direct++
		}
	}
	switch {
	case direct > 0:
		c.Status, c.Detail = "ok", fmt.Sprintf("advertising %d direct public address(es)", direct)
	case relayed > 0:
		c.Status, c.Detail = "warn", "no direct public address — reachable via relay only (behind NAT); peers will hole-punch"
	default:
		c.Status, c.Detail = "warn", "no public or relay address yet — inbound reachability unknown (node may still be warming up)"
	}
	return c
}

func isPublicish(a string) bool {
	for _, priv := range []string{"/ip4/10.", "/ip4/127.", "/ip4/172.1", "/ip4/172.2", "/ip4/172.3",
		"/ip4/192.168.", "/ip6/::1", "/ip6/fc", "/ip6/fd", "/ip6/fe80"} {
		if strings.HasPrefix(a, priv) {
			return false
		}
	}
	return true
}

// LAN discovery: browse the libp2p mDNS service for a few seconds. Hearing ANY answer (even just our own
// responder on another interface) proves multicast/UDP-5353 is open; a LAN with another VidyaGod shows up as
// peers. Silence on a network that otherwise works = 5353/multicast filtered (guest wifi, strict firewall).
func (n *node) checkMDNS(ctx context.Context) netCheck {
	c := netCheck{Name: "LAN discovery (mDNS)"}
	cctx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	entries := make(chan *zeroconf.ServiceEntry, 16)
	heard := map[string]bool{}
	done := make(chan struct{})
	safeGo("nettest.mdnsListen", func() {
		defer close(done)
		for e := range entries {
			if e != nil && e.Instance != "" {
				heard[e.Instance] = true
			}
		}
	})
	err := zeroconf.Browse(cctx, "_p2p._udp", "local.", entries)
	<-done
	if err != nil && len(heard) == 0 {
		c.Status, c.Detail = "fail", "mDNS browse failed — multicast likely blocked ("+rootErr(err)+")"
		return c
	}
	if len(heard) == 0 {
		c.Status = "warn"
		c.Detail = "no mDNS answers in 4s — UDP 5353/multicast may be filtered (or no other libp2p node on this LAN); " +
			"same-network friends would fall back to slower internet paths"
		return c
	}
	self := n.host.ID().String()
	others := 0
	for inst := range heard {
		if !strings.Contains(self, inst) && !strings.Contains(inst, self[max(0, len(self)-8):]) {
			others++
		}
	}
	c.Status = "ok"
	c.Detail = fmt.Sprintf("multicast works — %d responder(s) heard (%d besides this node)", len(heard), others)
	return c
}

// DHT: find providers for the canonical empty UnixFS directory — permanently provided by half the network,
// so zero results with working internet means the P2P ports specifically are filtered.
func (n *node) checkDHT(ctx context.Context) netCheck {
	c := netCheck{Name: "DHT lookups"}
	if n.dht == nil {
		c.Status, c.Detail = "fail", "DHT not running"
		return c
	}
	id, err := cid.Decode("QmUNLLsPACCz1vLxQVkXqqLX5R1X345qqfHbsf67hvA3Nn")
	if err != nil {
		c.Status, c.Detail = "fail", "internal: bad probe CID"
		return c
	}
	cctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	t0 := time.Now()
	found := 0
	for range n.dht.FindProvidersAsync(cctx, id, 3) {
		found++
		if found >= 3 {
			break
		}
	}
	if found == 0 {
		c.Status, c.Detail = "fail", "no providers found for a universally-available CID — DHT unreachable (P2P ports blocked?)"
		return c
	}
	c.Status = "ok"
	c.Detail = fmt.Sprintf("found %d provider(s) in %d ms", found, time.Since(t0).Milliseconds())
	return c
}

// Friend links: the always-on maintainer's live view, summarized.
func (n *node) checkFriendLinks() netCheck {
	c := netCheck{Name: "Friend links"}
	if n.linkm == nil {
		c.Status, c.Detail = "warn", "link maintainer not running"
		return c
	}
	infos := n.linkm.snapshot(func(peer.ID) string { return "" })
	if len(infos) == 0 {
		// The maintainer's table fills on its 4s tick; right after node start it can be empty while the address
		// book is not. Reporting "no friends" to someone WITH friends is a lie (first live run did exactly that) —
		// distinguish the two.
		accepted := 0
		if n.social != nil {
			for _, ct := range n.social.list() {
				if ct.State == stAccepted {
					accepted++
				}
			}
		}
		if accepted > 0 {
			c.Status, c.Detail = "warn", fmt.Sprintf("%d friend(s) known but links not evaluated yet — run again in a few seconds", accepted)
		} else {
			c.Status, c.Detail = "ok", "no friends yet — nothing to maintain"
		}
		return c
	}
	counts := map[string]int{}
	var parts []string
	for _, li := range infos {
		counts[li.Link]++
	}
	for _, k := range []string{"direct", "relayed", "connecting", "down"} {
		if counts[k] > 0 {
			parts = append(parts, fmt.Sprintf("%d %s", counts[k], k))
		}
	}
	sort.Strings(parts)
	switch {
	case counts["direct"] == len(infos):
		c.Status = "ok"
	case counts["direct"]+counts["relayed"] > 0:
		c.Status = "warn"
	default:
		c.Status = "fail"
	}
	c.Detail = strings.Join(parts, ", ")
	return c
}

// rootErr trims Go's nested error chains to the informative tail.
func rootErr(err error) string {
	if err == nil {
		return "unknown"
	}
	s := err.Error()
	if i := strings.LastIndex(s, ": "); i >= 0 && i+2 < len(s) {
		return s[i+2:]
	}
	return s
}
