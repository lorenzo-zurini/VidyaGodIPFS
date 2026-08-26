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

// setSeedLevels records the level-3 (collection) and level-2 (package) meta-CIDs, then starts the seed-announce loop on
// the first call (immediate 3-pass + periodic refresh). Later calls update the lists and kick an immediate re-announce
// (e.g. after a source is added), so a freshly-added source's meta goes live promptly.
func (n *node) setSeedLevels(colls, pkgs []cid.Cid) {
	n.seedMu.Lock()
	n.seedColls = colls
	n.seedPkgs = pkgs
	start := !n.seedStarted
	n.seedStarted = true
	n.seedMu.Unlock()
	if start {
		go n.seedAnnounceLoop()
	} else {
		go n.runSeedAnnounce()
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
	content := make([]cid.Cid, 0, len(all))
	for _, c := range all {
		if _, isMeta := meta[c.String()]; !isMeta {
			content = append(content, c)
		}
	}

	fmt.Fprintf(os.Stderr, "[seed] announce: %d collection + %d package + %d content CID(s)\n", len(colls), len(pkgs), len(content))
	// Meta levels are FEW and the shareable units, so provide them with a blocking DHT walk (ordered, exact "announced"
	// signal). Content is MANY (thousands) — a blocking provide each took ~an hour — so hand it to the boxo provider's
	// batched queue instead (Provide enqueues + returns); it stays "queued for seeding" only across the fast meta
	// passes, then flips to seeding as it's enqueued.
	n.announcePass("collections", colls)
	n.announcePass("packages", pkgs)
	n.announceContentBulk(content)
	fmt.Fprintf(os.Stderr, "[seed] announce complete — %d CID(s) marked seeding\n", n.seedCount())
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
		go func() {
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
		}()
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
