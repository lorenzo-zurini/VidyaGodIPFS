package main

// netstats.go — what the node does to the network, counted. A home router tracks every NEW flow (a dial, a routing
// walk's burst of dials), not the connections we hold, so the number that matters is how many new connections and
// routing walks we start per unit of time. The connection cap bounds what we hold; nothing bounded what we started —
// and nothing SHOWED it: a receive that ran ~90 overlapping DHT walks took a household router down in silence. The
// "[net]" line prints what changed every netStatsEvery, only when something did.

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"
)

type netCounters struct {
	connsOpened         atomic.Int64 // connections established (either direction)
	connsInbound        atomic.Int64 // … of which a remote peer dialed us
	countLookups        atomic.Int64 // provider-count DHT walks (explicit requests only), through netq
	dialsBudget         atomic.Int64 // new outbound connections that took a budget token (netgate.go)
	dialsPriority       atomic.Int64 // … that skipped the line (friend, LAN peer, a fetch's provider)
	dialsRefused        atomic.Int64 // … refused: no token within dialMaxWait
	dialsPrivateSkipped atomic.Int64 // private addresses of remote peers not dialed
	warmWalks           atomic.Int64 // provider walks warmProviders actually started
	warmSkipped         atomic.Int64 // warm walks not needed: the fetch moved (or ended) within the search delay
	findPeers           atomic.Int64 // live FindPeer walks (provider address refresh, friend warm-up)
	finderDHT           atomic.Int64 // bitswap provider queries that reached the DHT
	finderIndexer       atomic.Int64 // … that reached a delegated HTTP indexer
	finderHeld          atomic.Int64 // … held behind a connected friend, and never needed
	provides            atomic.Int64 // provider-record messages sent (sweeping provider, through netq)
	provideWalks        atomic.Int64 // DHT walks the sweeping provider made (one per keyspace region, through netq)
	provideDials        atomic.Int64 // record sends that needed a new connection (through netq)
}

var netStats netCounters

const netStatsEvery = 30 * time.Second

type netSnapshot [16]int64

func (s *netCounters) snapshot() netSnapshot {
	return netSnapshot{s.connsOpened.Load(), s.warmWalks.Load(), s.warmSkipped.Load(),
		s.findPeers.Load(), s.finderDHT.Load(), s.finderIndexer.Load(), s.finderHeld.Load(), s.provides.Load(), s.provideWalks.Load(), s.provideDials.Load(), s.connsInbound.Load(), s.countLookups.Load(),
		s.dialsBudget.Load(), s.dialsPriority.Load(), s.dialsRefused.Load(), s.dialsPrivateSkipped.Load()}
}

// netStatsLine renders the change between two snapshots; "" when nothing changed.
func netStatsLine(prev, cur netSnapshot, peers int) string {
	if prev == cur {
		return ""
	}
	d := func(i int) int64 { return cur[i] - prev[i] }
	active, waiting := netq.load()
	return fmt.Sprintf("[net] dials: +%d budget, +%d priority, %d refused, %d private skipped | +%d conns, %d inbound (%d peers) | warm walks +%d, skipped +%d | findpeer +%d | "+
		"provider queries: dht +%d, indexer +%d, held behind a friend +%d | provide walks +%d, records +%d (new conns +%d) | "+
		"provider counts +%d | queue %d/%d busy, %d waiting",
		d(12), d(13), d(14), d(15), d(0), d(10), peers, d(1), d(2), d(3), d(4), d(5), d(6), d(8), d(7), d(9), d(11), active, netq.size(), waiting)
}

// logNetStats prints the [net] line every netStatsEvery until ctx ends.
func (n *node) logNetStats(ctx context.Context) {
	t := time.NewTicker(netStatsEvery)
	defer t.Stop()
	prev := netStats.snapshot()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			cur := netStats.snapshot()
			peers := 0
			if n.host != nil {
				peers = len(n.host.Network().Peers())
			}
			if line := netStatsLine(prev, cur, peers); line != "" {
				fmt.Fprintln(os.Stderr, line)
			}
			if activeDialTracer != nil {
				fmt.Fprintln(os.Stderr, "[net] outbound dials by caller:"+activeDialTracer.drain())
			}
			prev = cur
		}
	}
}
