package main

// health.go — the PROACTIVE health report for every Go service, surfaced live in Settings → Network. Where the
// on-demand network sweep (nettest.go) actively PROBES the outside world for up to ~15s, this report passively
// INTROSPECTS the node's own state — every call is a handful of mutex-guarded reads, cheap enough for the GUI to
// poll every few seconds. The one exception is the DoH check, which needs a real lookup to mean anything: it runs
// asynchronously in the background at most once a minute and the report shows the cached verdict.
//
// Statuses: "ok" (working), "warn" (degraded — the detail says how), "down" (should be up but isn't),
// "off" (intentionally not running right now, e.g. the game-time tri-plane while no game is up).
// THIS CODEBASE'S FAILURE MODE IS SILENCE — a service that silently died must show up here as "down".

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"
)

type healthEntry struct {
	Name   string `json:"name"`
	Status string `json:"status"` // ok | warn | down | off
	Detail string `json:"detail"`
}

// count reports how many serve failures are pending WITHOUT draining them (drain() feeds the GUI's repair flow;
// health must never eat its data).
func (f *failureLog) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.order)
}

// ---- cached async DoH probe (the only health item that talks to the network) ----
var dohProbe struct {
	mu      sync.Mutex
	last    time.Time
	running bool
	verdict string // "" = never probed
}

func kickDoHProbe() {
	dohProbe.mu.Lock()
	if dohProbe.running || time.Since(dohProbe.last) < time.Minute {
		dohProbe.mu.Unlock()
		return
	}
	dohProbe.running = true
	dohProbe.mu.Unlock()
	safeGo("health.dohProbe", func() {
		t0 := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := newDoHResolver().LookupIPAddr(ctx, "cloudflare.com")
		v := fmt.Sprintf("resolving ok (%dms)", time.Since(t0).Milliseconds())
		if err != nil {
			v = "lookup failed: " + rootErr(err)
		}
		dohProbe.mu.Lock()
		dohProbe.verdict, dohProbe.last, dohProbe.running = v, time.Now(), false
		dohProbe.mu.Unlock()
	})
}

// healthReport builds the full service list. Safe to call at any node state (nil node → single "down" row).
func healthReport() []healthEntry {
	var out []healthEntry
	add := func(name, status, detail string) { out = append(out, healthEntry{name, status, detail}) }

	n := get()
	if n == nil {
		add("Node", "down", "not started — networking is off or startup failed")
		return out
	}
	add("Node", "ok", "repo open at "+n.repoPath)

	// --- libp2p host / connectivity ---
	if !n.online || n.host == nil {
		add("Network", "down", "offline — the network stack has not come up (still retrying in the background)")
		return out // everything below needs the host
	}
	peers := len(n.host.Network().Peers())
	st := "ok"
	if peers == 0 {
		st = "warn"
	}
	add("Network", st, fmt.Sprintf("connected to %d peer(s), %d listen addr(s)", peers, len(n.host.Addrs())))

	// --- reachability: what we advertise decides whether OTHERS can reach us ---
	pub, relay := 0, 0
	for _, a := range n.host.Addrs() {
		s := a.String()
		if strings.Contains(s, "/p2p-circuit") {
			relay++
		} else if isPublicish(s) {
			pub++
		}
	}
	switch {
	case pub > 0:
		add("Reachability", "ok", fmt.Sprintf("publicly reachable (%d public addr(s))", pub))
	case relay > 0:
		add("Reachability", "warn", "relay-only — peers reach you via relays; expect slower transfers until a hole-punch lands")
	default:
		add("Reachability", "warn", "no public/relay address advertised yet (normal in the first minute)")
	}

	// --- DHT (peer + content discovery) ---
	if n.dht == nil {
		add("DHT", "down", "not running — peers and content cannot be discovered")
	} else if sz := n.dht.RoutingTable().Size(); sz == 0 {
		add("DHT", "warn", "routing table empty — discovery will fail until bootstrap completes")
	} else {
		add("DHT", "ok", fmt.Sprintf("routing table: %d peer(s)", sz))
	}

	// --- bitswap (transfers) + deliverability ---
	if n.exchange == nil {
		add("Transfers (bitswap)", "down", "exchange not running — downloads and seeding are dead")
	} else {
		detail := "running"
		if n.bwc != nil {
			t := n.bwc.GetBandwidthTotals()
			detail = fmt.Sprintf("running — ↓%.0f KB/s ↑%.0f KB/s", t.RateIn/1024, t.RateOut/1024)
		}
		add("Transfers (bitswap)", "ok", detail)
	}
	if n.serveFails != nil {
		if c := n.serveFails.count(); c > 0 {
			add("Deliverability", "warn", fmt.Sprintf("%d block(s) peers asked for could NOT be served (drifted/missing content) — see the IPFS tab", c))
		} else {
			add("Deliverability", "ok", "no failed serves")
		}
	}

	// --- content announce (how peers find what we seed) ---
	if n.provider == nil {
		add("Content announce", "down", "reprovider not running — nothing we seed is discoverable")
	} else {
		n.seedMu.RLock()
		started, done := n.seedStarted, len(n.seedDone)
		n.seedMu.RUnlock()
		if started {
			add("Content announce", "ok", fmt.Sprintf("announce sweep active — %d CID(s) announced", done))
		} else {
			add("Content announce", "ok", fmt.Sprintf("reprovider up (%d CID(s) announced); full sweep starts with the library", done))
		}
	}

	// --- same-LAN discovery ---
	if n.mdns == nil {
		add("LAN discovery (mDNS)", "warn", "not running — same-network peers won't be found automatically")
	} else {
		add("LAN discovery (mDNS)", "ok", "announcing + listening on the local network")
	}

	// --- friends service ---
	if n.friend == nil || n.social == nil {
		add("Friends", "down", "friend service not running")
	} else {
		accepted, online := 0, 0
		for _, c := range n.social.list() {
			if c.State == stAccepted {
				accepted++
				if c.online {
					online++
				}
			}
		}
		add("Friends", "ok", fmt.Sprintf("%d friend(s), %d online (presence pings every 45s)", accepted, online))
	}

	// --- virtual-LAN link maintainer ---
	if n.linkm == nil {
		add("Virtual LAN links", "down", "link maintainer not running — friend links are not being kept alive")
	} else {
		counts := map[string]int{}
		for _, state := range n.linkm.snapshotStates() {
			counts[state]++
		}
		total := 0
		for _, v := range counts {
			total += v
		}
		add("Virtual LAN links", "ok", fmt.Sprintf("%d link(s): %d direct, %d relayed, %d connecting, %d down",
			total, counts[linkDirect], counts[linkRelayed], counts[linkConnecting], counts[linkDown]))
	}

	// --- overlay datapath + tri-plane (game-time services) ---
	if n.overlay == nil {
		add("Overlay datapath", "down", "overlay service not registered — the virtual LAN cannot carry traffic")
	} else {
		running, routes, gw, relayOn := n.overlay.healthSnapshot()
		if running {
			add("Overlay datapath", "ok", fmt.Sprintf("forwarding (game active) — %d route(s)", routes))
			if gw {
				add("Internet gateway", "ok", "in-game internet + real-LAN unicast active")
			} else {
				add("Internet gateway", "off", "disabled for this launch")
			}
			if relayOn {
				add("Broadcast reflector", "ok", "bridging real-LAN broadcasts")
			} else {
				add("Broadcast reflector", "off", "disabled for this launch")
			}
		} else {
			add("Overlay datapath", "ok", fmt.Sprintf("standby — attaches at game launch (%d route(s) ready)", routes))
			add("Internet gateway", "off", "starts with a game")
			add("Broadcast reflector", "off", "starts with a game")
		}
	}

	// --- DoH (async cached probe) ---
	kickDoHProbe()
	dohProbe.mu.Lock()
	v := dohProbe.verdict
	dohProbe.mu.Unlock()
	switch {
	case v == "":
		add("DNS-over-HTTPS", "ok", "probing…")
	case strings.HasPrefix(v, "resolving ok"):
		add("DNS-over-HTTPS", "ok", v)
	default:
		add("DNS-over-HTTPS", "warn", v+" — DNS-filtered network? The in-game DNS and dnsaddr dials depend on this")
	}

	return out
}

// snapshotStates returns just the link-state strings (health only needs the counts, not the full UI view).
func (m *linkMaintainer) snapshotStates() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, 0, len(m.links))
	for _, l := range m.links {
		out = append(out, l.state)
	}
	return out
}

// healthSnapshot exposes the overlay's health facts under its own locks.
func (o *overlayService) healthSnapshot() (running bool, routes int, gw bool, relay bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.running, len(o.routes), o.gw != nil, o.relay != nil
}

// healthJSON marshals the report (for the VgHealth export).
func healthJSON() string {
	b, err := json.Marshal(healthReport())
	if err != nil {
		return "[]"
	}
	return string(b)
}
