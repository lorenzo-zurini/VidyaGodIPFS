package main

// node.go — the embedded IPFS node assembly: storage + lifecycle (the network stack lives in online.go).
//
// Holds a single process-wide node behind a mutex. Storage is a leveldb datastore plus a Boxo filestore so that
// leaf blocks are stored BY REFERENCE into the on-disk file they came from (the whole point — no blockstore copy).
// Intermediate UnixFS nodes (small) live in the plain blockstore over the same datastore.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	blockservice "github.com/ipfs/boxo/blockservice"
	blockstore "github.com/ipfs/boxo/blockstore"
	exchange "github.com/ipfs/boxo/exchange"
	offline "github.com/ipfs/boxo/exchange/offline"
	filestore "github.com/ipfs/boxo/filestore"
	merkledag "github.com/ipfs/boxo/ipld/merkledag"
	ipfspinner "github.com/ipfs/boxo/pinning/pinner"
	dspinner "github.com/ipfs/boxo/pinning/pinner/dspinner"
	provider "github.com/ipfs/boxo/provider"
	cid "github.com/ipfs/go-cid"
	datastore "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	leveldb "github.com/ipfs/go-ds-leveldb"
	ipld "github.com/ipfs/go-ipld-format"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	host "github.com/libp2p/go-libp2p/core/host"
	metrics "github.com/libp2p/go-libp2p/core/metrics"
	goleveldbutil "github.com/syndtr/goleveldb/leveldb/util"
)

// node is the singleton embedded IPFS node.
type node struct {
	ctx    context.Context
	cancel context.CancelFunc

	repoPath string

	ldb        *leveldb.Datastore // the underlying leveldb (for explicit compaction to reclaim tombstone disk)
	compacting atomic.Bool        // coalesces overlapping compaction requests into one in-flight run
	ds         datastore.Batching
	fstore     *filestore.Filestore // routes FilestoreNode leaves to references, everything else to the blockstore
	bstore     blockstore.Blockstore
	bserv      blockservice.BlockService
	dserv      ipld.DAGService
	localDserv ipld.DAGService // always-offline DAG service for local-only checks (never fetches over the network)
	pinner     ipfspinner.Pinner
	serveFails *failureLog // blocks a peer asked for that we could not deliver (see servefail.go)

	// network (M3) — nil/false until goOnline succeeds
	online   bool
	host     host.Host
	dht      *dht.IpfsDHT
	exchange exchange.Interface
	provider provider.System
	mdns     interface{ Close() error } // local-network discovery service (mDNS)
	bwc      *metrics.BandwidthCounter  // libp2p bandwidth counter → global up/down rates (nil until online)

	// friends / multiplayer social layer. social (the persistent address book) is loaded at openNode so contacts
	// survive offline; friend (the live protocol) is wired in goOnline once the host exists.
	social *socialState
	friend *friendService
	linkm  *linkMaintainer // always-on per-friend link maintainer (overlaylink.go)

	lanExclMu   sync.Mutex
	lanExcluded map[string]bool // the LAN roster's un-ticked members (peer-id strings), set via VgLanSetExcluded
	overlay     *overlayService

	// per-CID upload activity: a bitswap tracer records when a PINNED ROOT block is served to a peer, so the GUI can
	// flag which seeded items are being uploaded right now. pinnedSet is refreshed periodically so the hot MessageSent
	// path is a cheap in-memory lookup (no datastore hit per block).
	upMu      sync.Mutex
	upSeen    map[string]int64 // pinned-root CID → last-served unix-ms
	pinnedSet atomic.Value     // holds map[string]struct{} of pinned root CIDs

	// seed announce (seedannounce.go): a 3-pass, level-ordered DHT announce of every pinned CID (collection meta →
	// package meta → content) run on start + periodically, with per-CID completion tracking so the GUI shows "queued
	// for seeding" until a CID is announced, then "seeding". Levels come from the app via setSeedLevels.
	seedMu      sync.RWMutex
	seedDone    map[string]struct{} // CIDs whose DHT announce has completed at least once
	seedColls   []cid.Cid           // level-3 collection meta-CIDs (announced first)
	seedPkgs    []cid.Cid           // level-2 package meta-CIDs (announced second)
	seedStarted bool                // the announce loop is running (setSeedLevels was called)
}

var (
	gMu   sync.Mutex
	gNode *node
)

// openNode initializes the node at repoPath (created if missing). Idempotent: a second call is a no-op.
func openNode(repoPath string) error {
	gMu.Lock()
	defer gMu.Unlock()
	if gNode != nil {
		return nil
	}
	if repoPath == "" {
		return errors.New("empty repo path")
	}
	if err := os.MkdirAll(repoPath, 0o755); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Persistent block/pin/reference metadata in a leveldb datastore under the repo.
	ldb, err := leveldb.NewDatastore(filepath.Join(repoPath, "datastore"), nil)
	if err != nil {
		cancel()
		return err
	}
	ds := dssync.MutexWrap(ldb)

	bstore := blockstore.NewBlockstore(ds)
	// FileManager references on-disk files; its "root" is "/" so absolute paths recorded at add time resolve.
	fm := filestore.NewFileManager(ds, "/")
	fm.AllowFiles = true
	// nil MultihashProvider: M1 is offline, and the filestore Put paths guard provider use with a nil check.
	// M3 wires the real DHT reprovider here.
	fstore := filestore.NewFilestore(bstore, fm, nil)

	// WriteThrough so an Add always Puts (no Has-skip) — a safety invariant ensuring addNoCopy creates the filestore
	// reference even if a block already happens to sit in the blockstore. (The online fetch path clears bitswap's
	// cached blocks via dropClosure before re-adding, which is the primary guard against duplication.)
	// Wrap the filestore so a FAILED serve read is captured, not just logged. This is the moment a leecher asks
	// for a block whose backing bytes changed — the one instant we can be certain the content is undeliverable.
	failLog := newFailureLog()
	served := &recordingBlockstore{Blockstore: fstore, log: failLog}
	bserv := blockservice.New(served, offline.Exchange(served), blockservice.WriteThrough(true))
	dserv := merkledag.NewDAGService(bserv)

	pnr, err := dspinner.New(ctx, ds, dserv)
	if err != nil {
		cancel()
		_ = ldb.Close()
		return err
	}

	gNode = &node{
		ctx: ctx, cancel: cancel, repoPath: repoPath,
		ldb: ldb,
		ds:  ds, fstore: fstore, bstore: served, bserv: bserv, dserv: dserv, pinner: pnr, serveFails: failLog,
		// localDserv stays this offline DAG service even after goOnline swaps dserv to online bitswap — so
		// local-only checks (cidMissing) never trigger a network fetch.
		localDserv: dserv,
		seedDone:   map[string]struct{}{},
	}
	// The address book loads independently of the network so contacts survive offline; the live protocol attaches
	// in goOnline.
	gNode.social = newSocialState(repoPath)

	// Join the public network (best-effort): swaps the DAG service to online bitswap. On failure the node stays
	// fully usable offline (local filestore reads, add-by-reference) — fetch of remote content just won't work.
	// A failed first attempt (machine still bringing its network up at login, transient DNS) retries in the
	// background with backoff instead of leaving the node offline for the whole session.
	// VIDYAGOD_IPFS_OFFLINE forces a purely-local node (used by the tests and as a no-network escape hatch).
	if os.Getenv("VIDYAGOD_IPFS_OFFLINE") == "" {
		if err := gNode.goOnline(); err != nil {
			fmt.Fprintf(os.Stderr, "[node] goOnline failed: %v — retrying in background\n", err)
			go retryOnline(gNode)
		}
	}
	return nil
}

// retryOnline keeps attempting goOnline with backoff until it succeeds, the node is torn down, or a different
// node instance replaces this one. Runs unlocked between attempts; each attempt re-checks identity under gMu.
func retryOnline(n *node) {
	delay := 5 * time.Second
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-time.After(delay):
		}
		gMu.Lock()
		if gNode != n {
			gMu.Unlock()
			return
		}
		err := n.goOnline()
		gMu.Unlock()
		if err == nil {
			fmt.Fprintf(os.Stderr, "[node] goOnline retry succeeded\n")
			return
		}
		fmt.Fprintf(os.Stderr, "[node] goOnline retry failed: %v\n", err)
		if delay *= 2; delay > 60*time.Second {
			delay = 60 * time.Second
		}
	}
}

// closeNode tears the node down.
func closeNode() {
	gMu.Lock()
	defer gMu.Unlock()
	if gNode == nil {
		return
	}
	if gNode.mdns != nil {
		_ = gNode.mdns.Close()
	}
	if gNode.provider != nil {
		_ = gNode.provider.Close()
	}
	if gNode.dht != nil {
		_ = gNode.dht.Close()
	}
	if gNode.host != nil {
		_ = gNode.host.Close()
	}
	gNode.cancel()
	if c, ok := gNode.ds.(datastore.Datastore); ok {
		_ = c.Close()
	}
	gNode = nil
}

// scheduleCompaction kicks off an async whole-DB leveldb compaction to reclaim the disk that deleted blocks leave
// behind as tombstones (go-ds-leveldb defers reclaim to compaction, so `du` on the repo balloons after a fetch even
// though the logical block set is tiny). The datastore only ever holds references + small intermediate nodes, so a
// full-range compaction is cheap. Coalesced via compacting: overlapping requests collapse into one in-flight run.
func (n *node) scheduleCompaction() {
	if n.ldb == nil || !n.compacting.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer n.compacting.Store(false)
		_ = n.ldb.DB.CompactRange(goleveldbutil.Range{})
	}()
}

// get returns the live node or nil.
func get() *node {
	gMu.Lock()
	defer gMu.Unlock()
	return gNode
}
