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
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	blockservice "github.com/ipfs/boxo/blockservice"
	blockstore "github.com/ipfs/boxo/blockstore"
	exchange "github.com/ipfs/boxo/exchange"
	offline "github.com/ipfs/boxo/exchange/offline"
	filestore "github.com/ipfs/boxo/filestore"
	merkledag "github.com/ipfs/boxo/ipld/merkledag"
	namesys "github.com/ipfs/boxo/namesys"
	ipfspinner "github.com/ipfs/boxo/pinning/pinner"
	dspinner "github.com/ipfs/boxo/pinning/pinner/dspinner"
	cid "github.com/ipfs/go-cid"
	datastore "github.com/ipfs/go-datastore"
	leveldb "github.com/ipfs/go-ds-leveldb"
	ipld "github.com/ipfs/go-ipld-format"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	ci "github.com/libp2p/go-libp2p/core/crypto"
	host "github.com/libp2p/go-libp2p/core/host"
	metrics "github.com/libp2p/go-libp2p/core/metrics"
	goleveldbfilter "github.com/syndtr/goleveldb/leveldb/filter"
	goleveldbopt "github.com/syndtr/goleveldb/leveldb/opt"
	goleveldbutil "github.com/syndtr/goleveldb/leveldb/util"
)

// node is the singleton embedded IPFS node.
type node struct {
	ctx    context.Context
	cancel context.CancelFunc

	repoPath string

	ldb *leveldb.Datastore // the underlying leveldb (for explicit compaction to reclaim tombstone disk)
	// Compaction lifecycle: coalesce overlapping requests, refuse new ones once closing, and let closeNode WAIT
	// for the in-flight run — closing the datastore under a live CompactRange races goleveldb's internal
	// compaction/close machinery (-race caught it as a pre-existing flake once CI ran with -race).
	compactMu         sync.Mutex
	compactBusy       bool
	compactClosed     bool
	compactWG         sync.WaitGroup
	ds                datastore.Batching
	refDropped        *atomic.Uint64 // reference drops: only these make the listing of files read again
	refAdded          *refAdded      // files references were written to since the listing
	refPathsMu        sync.Mutex
	refPaths          []string            // the distinct files the references name, listed at refPathsGen (orphanedRefPaths)
	refPathsGen       uint64              // refDropped when the listing was read
	refPathsWritten   bool                // references were written since the listing was read: it may name a file none names now
	refPathsConfirmed map[string]struct{} // gone files a read of the index confirmed still referenced
	putManyMu         sync.Mutex          // one blockPutMany at a time (it turns the pinner's autosync off while it runs)
	refRepointed      atomic.Bool         // a reference some file already had was written over (a heal, a move) since: no confirmation holds
	refPathsValid     bool
	fstore            *filestore.Filestore // routes FilestoreNode leaves to references, everything else to the blockstore
	bstore            blockstore.Blockstore
	plain             blockstore.Blockstore // the blockstore under the filestore: stored bytes, never a reference
	bserv             blockservice.BlockService
	dserv             ipld.DAGService
	localDserv        ipld.DAGService // always-offline DAG service for local-only checks (never fetches over the network)
	madeDirs          sync.Map        // folder CID → struct{}: folders makeDir built, each entry verified whole (blocks.go)
	pinner            ipfspinner.Pinner
	serveFails        *failureLog // blocks a peer asked for that we could not deliver (see servefail.go)

	// network (M3) — nil/false until goOnline succeeds
	online     bool
	host       host.Host
	dht        *dht.IpfsDHT
	exchange   exchange.Interface
	provider   keyProvider // announces what we hold (provide.go), through netq
	provideSt  provideState
	lanPeers   sync.Map // peer.ID → expiry: found on our LAN (mDNS) or connected to by address — netgate.go
	wantedProv sync.Map // peer.ID → expiry: providers a fetch found — netgate.go

	// IPNS (ipns.go): the peer's Ed25519 identity key (also the peer code) signs a tiny mutable record pointing
	// the peer's name at the current library-index CID; namesys publishes/resolves via the DHT, with the datastore
	// carrying the sequence number across restarts. ipnsCurrent is the value we last published under our own name,
	// re-published by a background loop before its EOL. nil/empty until goOnline.
	priv        ci.PrivKey
	ns          namesys.NameSystem
	ipnsMu      sync.Mutex
	ipnsCurrent string
	mdns        interface{ Close() error } // local-network discovery service (mDNS)
	bwc         *metrics.BandwidthCounter  // libp2p bandwidth counter → global up/down rates (nil until online)

	// peers / multiplayer social layer. social (the persistent address book) is loaded at openNode so contacts
	// survive offline; peer (the live protocol) is wired in goOnline once the host exists.
	social *socialState
	peer   *peerService
	nearby *nearbyService  // the VidyaGod devices on our network, by name (nearby.go)
	linkm  *linkMaintainer // always-on per-peer link maintainer (overlaylink.go)

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
	// gClosing: a node being closed outside gMu (closeNode). A start waits for it — the closing one still holds the
	// repo's datastore.
	gClosing sync.WaitGroup
)

// openNode initializes the node at repoPath (created if missing). Idempotent: a second call is a no-op.
// openLevelDB opens the node's store (a test sees the options the node opens it with).
var openLevelDB = leveldb.NewDatastore

func openNode(repoPath string) error {
	gClosing.Wait() // the node closing (if any) lets go of the repo first
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
	ldb, err := openLevelDB(filepath.Join(repoPath, "datastore"), (*leveldb.Options)(storeOptions()))
	if err != nil {
		cancel()
		return err
	}
	refDropped, added := &atomic.Uint64{}, &refAdded{}
	// leveldb is safe for concurrent use and streams a query from an iterator (a snapshot): no mutex wrapper. Its
	// Query read the whole result before giving the first entry — every reference scan held them all (~2.5 GB for
	// 5.8 M references) and every write waited behind it (refscan_test.go).
	ds := refWatch{Batching: ldb, dropped: refDropped, added: added}

	bstore := blockstore.NewBlockstore(ds)
	// FileManager references on-disk files, recorded relative to its root ("/"; a volume on Windows, filestoreroot.go).
	fileRoot = filestoreRoot(runtime.GOOS, repoPath)
	fm := filestore.NewFileManager(ds, fileRoot)
	fm.AllowFiles = true
	// nil MultihashProvider: M1 is offline, and the filestore Put paths guard provider use with a nil check.
	// M3 wires the real DHT reprovider here.
	fstore := filestore.NewFilestore(bstore, fm, nil)

	// WriteThrough so an Add through the block service always reaches the filestore's Put (which still skips a block it
	// holds). addNoCopy does not add through it: it puts its file's references itself, over any held (add.go). (The
	// online fetch path clears bitswap's cached blocks via dropClosure before re-adding, the guard against duplication.)
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
		ds:  ds, refDropped: refDropped, refAdded: added, fstore: fstore, bstore: served, plain: bstore, bserv: bserv, dserv: dserv, pinner: pnr, serveFails: failLog,
		// localDserv stays this offline DAG service even after goOnline swaps dserv to online bitswap — so
		// local-only checks (cidMissing) never trigger a network fetch.
		localDserv: dserv,
		seedDone:   map[string]struct{}{},
	}
	// The address book loads independently of the network so contacts survive offline; the live protocol attaches
	// in goOnline.
	// The want budget follows the link (fetch.go wantPool.adapt): ~2 s of downloading in flight.
	safeGo("node.wantpool", func() { globalWantPool.adapt(ctx, time.Second) })
	gNode.social = newSocialState(repoPath)

	// Join the public network (best-effort): swaps the DAG service to online bitswap. On failure the node stays
	// fully usable offline (local filestore reads, add-by-reference) — fetch of remote content just won't work.
	// A failed first attempt (machine still bringing its network up at login, transient DNS) retries in the
	// background with backoff instead of leaving the node offline for the whole session.
	// VIDYAGOD_IPFS_OFFLINE forces a purely-local node (used by the tests and as a no-network escape hatch).
	if os.Getenv("VIDYAGOD_IPFS_OFFLINE") == "" {
		// guardErr: a panic in goOnline (which now wires the whole peer/overlay LAN stack) must not crash the
		// process — convert it to an error so the existing retry path handles it, leaving a usable offline node.
		if err := guardErr("goOnline", gNode.goOnline); err != nil {
			fmt.Fprintf(os.Stderr, "[node] goOnline failed: %v — retrying in background\n", err)
			safeGo("node.retryOnline", func() { retryOnline(gNode) })
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
		// Deferred unlock inside a closure: a panic in goOnline must NOT leave gMu held — that would wedge every
		// get() (and thus the whole node) forever. guardErr turns a panic into a retryable error.
		stop := false
		err := func() error {
			gMu.Lock()
			defer gMu.Unlock()
			if gNode != n {
				stop = true
				return nil
			}
			return guardErr("goOnline", n.goOnline)
		}()
		if stop {
			return
		}
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
// The node is taken out under gMu and closed OUTSIDE it: every Vg* call takes gMu (get), and a close waits on the
// DHT, the host and an in-flight compaction (minutes on a large store) — every caller, the GUI's included, waited
// with it. From the moment it is taken out, get() answers nil: the node is off.
func closeNode() {
	gMu.Lock()
	n := gNode
	gNode = nil
	if n != nil {
		gClosing.Add(1)
	}
	gMu.Unlock()
	if n == nil {
		return
	}
	defer gClosing.Done()
	if n.mdns != nil {
		_ = n.mdns.Close()
	}
	if n.provider != nil {
		_ = n.provider.Close()
	}
	if n.dht != nil {
		_ = n.dht.Close()
	}
	if n.host != nil {
		_ = n.host.Close()
	}
	n.cancel()
	// Bar new compactions and WAIT for the in-flight one: closing leveldb under a live CompactRange is a race
	// inside goleveldb (compaction ack vs close). Wait happens before the datastore close, deliberately.
	n.compactMu.Lock()
	n.compactClosed = true
	n.compactMu.Unlock()
	n.compactWG.Wait()
	if c, ok := n.ds.(datastore.Datastore); ok {
		_ = c.Close()
	}
}

// scheduleCompaction runs a whole-DB leveldb compaction to reclaim the disk that deleted blocks leave behind as
// tombstones (go-ds-leveldb defers reclaim to compaction, so `du` on the repo balloons after a fetch even though the
// logical block set is tiny) — once no fetch holds or waits for a network slot. While fetches run, the datastore
// also carries every in-flight 256 KiB leaf and the tombstone of every one written through; compacting after EACH
// finished fetch rewrote that whole database back to back through a replication (~100 MB/s of writes), and leveldb
// stalls writers behind it — finalizes took 30–60 s and live fetches starved into the stall watchdog. One run when
// the installs are done reclaims them all. Coalesced: requests while one is pending or running collapse into it.
func (n *node) scheduleCompaction() {
	n.compactMu.Lock()
	if n.ldb == nil || n.compactBusy || n.compactClosed {
		n.compactMu.Unlock()
		return
	}
	n.compactBusy = true
	n.compactWG.Add(1)
	n.compactMu.Unlock()
	safeGo("node.compaction", func() {
		defer func() {
			n.compactMu.Lock()
			n.compactBusy = false
			n.compactMu.Unlock()
			n.compactWG.Done()
		}()
		if !netq.waitFgIdle(n.ctx) {
			return // closing
		}
		_ = n.ldb.DB.CompactRange(goleveldbutil.Range{})
	})
}

// get returns the live node or nil.
func get() *node {
	gMu.Lock()
	defer gMu.Unlock()
	return gNode
}

// storeOptions: leveldb sized for a library's store (~1 GB, 5.8 M references, ~570 tables), not its defaults (an
// 8 MiB block cache, 500 open tables, no filter): a walk over a library's references re-read and re-decompressed the
// same blocks and re-opened tables as it went, ~100 µs a lookup. A bloom filter lets a lookup of an absent key skip a
// table without reading it (a table gets its filter when compaction writes it; one without is read as before).
func storeOptions() *goleveldbopt.Options {
	return &goleveldbopt.Options{
		Filter:                 goleveldbfilter.NewBloomFilter(10),
		BlockCacheCapacity:     64 << 20,
		OpenFilesCacheCapacity: 1024,
	}
}
