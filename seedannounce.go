package main

// seedannounce.go — what the app wants findable: its published ROOT CIDs (the share records a receiver or a pinning
// service looks up first), every node block of its current tree, and every other pinned root (content). All of it is
// handed to the sweeping provider (provide.go), which announces what it has never announced and reprovides on its
// schedule — through the one network queue. Nothing is re-walked on start or on a timer: a record lives 48 h and
// outlives a restart. "Seeding" in the GUI = pinned, servable, and scheduled.

import (
	"fmt"
	"os"

	cid "github.com/ipfs/go-cid"
)

// setSeedLevels records what the app wants announced first and tracked — the published ROOT CIDs (colls) and every
// node block of its current tree (pkgs) — and hands them, with every other pinned root, to the provider. Later calls
// (after a publish) hand over the new lists; keys the provider already holds cost nothing.
func (n *node) setSeedLevels(colls, pkgs []cid.Cid) {
	n.seedMu.Lock()
	n.seedColls = colls
	n.seedPkgs = pkgs
	n.seedStarted = true
	n.seedMu.Unlock()
	safeGo("node.runSeedAnnounce", func() { n.runSeedAnnounce() })
}

// runSeedAnnounce hands every seeded CID to the provider: roots first, then node blocks, then content.
func (n *node) runSeedAnnounce() {
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
	content := unlisted(all, meta)
	n.startProviding(colls...)
	n.startProviding(pkgs...)
	n.startProviding(content...)
	fmt.Fprintf(os.Stderr, "[seed] %d root + %d library node + %d content CID(s) handed to the provider\n", len(colls), len(pkgs), len(content))
}

// unlisted is the pinned set minus what the app listed, in pin order.
func unlisted(all []cid.Cid, listed map[string]struct{}) []cid.Cid {
	var out []cid.Cid
	for _, c := range all {
		if _, ok := listed[c.String()]; !ok {
			out = append(out, c)
		}
	}
	return out
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
