package main

// seedannounce.go — announce EVERYTHING we seed to the DHT, in 3 level-ordered passes (the 3-level package schema):
//   pass 1: collection meta-CIDs (the shareable source units a peer fetches first)
//   pass 2: package meta-CIDs
//   pass 3: all remaining content roots (layers/covers)
// Passes run sequentially (so the units that gate discovery go live first); within a pass, provides run concurrently.
// Runs on start and periodically. Each CID is marked "done" only once its blocking dht.Provide completes, so the GUI
// can show "queued for seeding" until then, and "seeding" after — real state, not a guess. The collection/package
// lists come from the app (setSeedLevels); content is every pinned root that isn't one of those.

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	cid "github.com/ipfs/go-cid"
)

const (
	seedProvideConcurrency = 12               // concurrent dht.Provide within a pass
	seedProvideTimeout     = 90 * time.Second // per-CID provide bound (a slow DHT walk shouldn't wedge the pass)
	seedReannounceInterval = 3 * time.Hour    // re-run the 3-pass to refresh DHT provider records (TTL ~24-48h)
)

// setSeedLevels records what the app wants announced first and tracked: the published ROOT CIDs (colls — the share
// record) and every node block of its current tree (pkgs), then starts the seed-announce loop on the first call
// (immediate ordered passes + periodic refresh). Later calls update the lists and kick an immediate re-announce
// (after a publish), so fresh roots go live promptly.
func (n *node) setSeedLevels(colls, pkgs []cid.Cid) {
	n.seedMu.Lock()
	n.seedColls = colls
	n.seedPkgs = pkgs
	start := !n.seedStarted
	n.seedStarted = true
	n.seedMu.Unlock()
	if start {
		safeGo("node.seedAnnounceLoop", func() { n.seedAnnounceLoop() })
	} else {
		safeGo("node.runSeedAnnounce", func() { n.runSeedAnnounce() })
	}
}

func (n *node) seedAnnounceLoop() {
	// Let the DHT bootstrap + routing table warm before the first announce, or the provides barely propagate (an
	// immature routing table = weak provider records). Matches the old startup-reprovide delay.
	select {
	case <-time.After(40 * time.Second):
	case <-n.ctx.Done():
		return
	}
	n.runSeedAnnounce()
	t := time.NewTicker(seedReannounceInterval)
	defer t.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-t.C:
			n.runSeedAnnounce()
		}
	}
}

// runSeedAnnounce performs one full 3-pass announce. Idempotent and safe to run periodically (re-providing refreshes
// the DHT record's TTL); the seedDone set only grows, so a CID never flips back from "seeding" to "queued".
func (n *node) runSeedAnnounce() {
	if n.dht == nil {
		return
	}
	n.seedMu.RLock()
	colls := append([]cid.Cid(nil), n.seedColls...)
	pkgs := append([]cid.Cid(nil), n.seedPkgs...)
	n.seedMu.RUnlock()

	meta := make(map[string]struct{}, len(colls)+len(pkgs))
	for _, c := range colls {
		meta[c.String()] = struct{}{}
	}
	for _, c := range pkgs {
		meta[c.String()] = struct{}{}
	}
	all, err := n.pinLs()
	if err != nil {
		fmt.Fprintf(os.Stderr, "[seed] pinLs failed: %v\n", err)
	}
	// The gigagraph's shareable units are the NODE blocks (dag-json): a share is a set of root CIDs, and a receiver
	// (or a pin-by-CID service) can find nothing until those are provided. They are few and tiny, so they get the
	// blocking, tracked pass FIRST — content (thousands of dag-pb / raw CIDs) goes to the provider's batched queue.
	// Without the split every pin shares one FIFO and a fresh publish's roots sit behind the whole content set.
	nodes, content := splitSeedPins(all, meta)

	fmt.Fprintf(os.Stderr, "[seed] announce: %d root + %d library node + %d other node + %d content CID(s)\n", len(colls), len(pkgs), len(nodes), len(content))
	// Meta levels and node blocks are FEW and the shareable units, so provide them with a blocking DHT walk (ordered,
	// exact "announced" signal). Content is MANY (thousands) — a blocking provide each took ~an hour — so hand it to
	// the boxo provider's batched queue instead (Provide enqueues + returns); it stays "queued for seeding" only
	// across the fast passes, then flips to seeding as it's enqueued.
	n.announcePass("roots", colls)         // the share record: what a receiver / pin-by-CID looks up first
	n.announcePass("library nodes", pkgs)  // every node block of the app's current tree
	n.announcePass("other nodes", nodes)   // dag-json pins the app did not list: earlier publishes' blocks
	n.announceContentBulk(content)
	fmt.Fprintf(os.Stderr, "[seed] announce complete — %d CID(s) marked seeding\n", n.seedCount())
}

// splitSeedPins partitions the pinned set (minus the legacy meta levels) into NODE blocks (dag-json — the shareable
// units, provided first and tracked) and content (everything else — the batched provider queue). Pin order is kept.
func splitSeedPins(all []cid.Cid, meta map[string]struct{}) (nodes, content []cid.Cid) {
	for _, c := range all {
		if _, isMeta := meta[c.String()]; isMeta {
			continue
		}
		if c.Prefix().Codec == cid.DagJSON {
			nodes = append(nodes, c)
		} else {
			content = append(content, c)
		}
	}
	return nodes, content
}

// announceContentBulk enqueues every content root into the boxo provider's efficient batched provide queue rather than
// blocking on a per-CID DHT walk, and marks each seeded now (it's pinned, servable, and queued to announce). Falls
// back to the blocking pass if the provider isn't wired.
func (n *node) announceContentBulk(cids []cid.Cid) {
	if len(cids) == 0 {
		return
	}
	if n.provider == nil {
		n.announcePass("content", cids)
		return
	}
	for _, c := range cids {
		if n.ctx.Err() != nil {
			return
		}
		_ = n.provider.Provide(n.ctx, c, true)
		n.seedMu.Lock()
		n.seedDone[c.String()] = struct{}{}
		n.seedMu.Unlock()
	}
	fmt.Fprintf(os.Stderr, "[seed] pass 'content' enqueued %d CID(s) to the provider queue\n", len(cids))
}

// announcePass provides every CID in cids to the DHT concurrently, marking each done as its blocking Provide returns.
func (n *node) announcePass(label string, cids []cid.Cid) {
	if len(cids) == 0 {
		return
	}
	jobs := make(chan cid.Cid)
	var wg sync.WaitGroup
	for i := 0; i < seedProvideConcurrency; i++ {
		wg.Add(1)
		safeGo("node.seedProvideWorker", func() {
			defer wg.Done()
			for c := range jobs {
				if n.ctx.Err() != nil {
					return
				}
				ctx, cancel := context.WithTimeout(n.ctx, seedProvideTimeout)
				err := n.dht.Provide(ctx, c, true) // blocks until the provider record is written to the closest peers
				cancel()
				if err == nil {
					n.seedMu.Lock()
					n.seedDone[c.String()] = struct{}{}
					n.seedMu.Unlock()
				}
			}
		})
	}
	for _, c := range cids {
		select {
		case jobs <- c:
		case <-n.ctx.Done():
			close(jobs)
			wg.Wait()
			return
		}
	}
	close(jobs)
	wg.Wait()
	fmt.Fprintf(os.Stderr, "[seed] pass '%s' done (%d CID(s))\n", label, len(cids))
}

// warmSeedLevelProviders warms the providers of our COLLECTION meta-CIDs (warm.go) — the discovery anchors of the
// 3-level schema. Content fetches call this each attempt so a file whose own DHT record hasn't propagated yet is
// still reachable through the peers seeding its source. Cheap: ≤ a handful of CIDs, async walks, connect no-ops.
func (n *node) warmSeedLevelProviders() {
	n.seedMu.RLock()
	colls := append([]cid.Cid(nil), n.seedColls...)
	n.seedMu.RUnlock()
	for _, c := range colls {
		n.warmProviders(c)
	}
}

func (n *node) seedAnnounced(c string) bool {
	n.seedMu.RLock()
	_, ok := n.seedDone[c]
	n.seedMu.RUnlock()
	return ok
}

func (n *node) seedCount() int {
	n.seedMu.RLock()
	k := len(n.seedDone)
	n.seedMu.RUnlock()
	return k
}
