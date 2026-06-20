package main

// node.go — the embedded IPFS node assembly: storage + lifecycle (the network stack lives in online.go).
//
// Holds a single process-wide node behind a mutex. Storage is a leveldb datastore plus a Boxo filestore so that
// leaf blocks are stored BY REFERENCE into the on-disk file they came from (the whole point — no blockstore copy).
// Intermediate UnixFS nodes (small) live in the plain blockstore over the same datastore.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"

	blockservice "github.com/ipfs/boxo/blockservice"
	blockstore "github.com/ipfs/boxo/blockstore"
	exchange "github.com/ipfs/boxo/exchange"
	offline "github.com/ipfs/boxo/exchange/offline"
	filestore "github.com/ipfs/boxo/filestore"
	merkledag "github.com/ipfs/boxo/ipld/merkledag"
	ipfspinner "github.com/ipfs/boxo/pinning/pinner"
	dspinner "github.com/ipfs/boxo/pinning/pinner/dspinner"
	provider "github.com/ipfs/boxo/provider"
	datastore "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	leveldb "github.com/ipfs/go-ds-leveldb"
	ipld "github.com/ipfs/go-ipld-format"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	host "github.com/libp2p/go-libp2p/core/host"
)

// node is the singleton embedded IPFS node.
type node struct {
	ctx    context.Context
	cancel context.CancelFunc

	repoPath string

	ds         datastore.Batching
	fstore     *filestore.Filestore // routes FilestoreNode leaves to references, everything else to the blockstore
	bstore     blockstore.Blockstore
	bserv      blockservice.BlockService
	dserv      ipld.DAGService
	localDserv ipld.DAGService // always-offline DAG service for local-only checks (never fetches over the network)
	pinner     ipfspinner.Pinner

	// network (M3) — nil/false until goOnline succeeds
	online   bool
	host     host.Host
	dht      *dht.IpfsDHT
	exchange exchange.Interface
	provider provider.System
	mdns     interface{ Close() error } // local-network discovery service (mDNS)
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
	bserv := blockservice.New(fstore, offline.Exchange(fstore), blockservice.WriteThrough(true))
	dserv := merkledag.NewDAGService(bserv)

	pnr, err := dspinner.New(ctx, ds, dserv)
	if err != nil {
		cancel()
		_ = ldb.Close()
		return err
	}

	gNode = &node{
		ctx: ctx, cancel: cancel, repoPath: repoPath,
		ds: ds, fstore: fstore, bstore: fstore, bserv: bserv, dserv: dserv, pinner: pnr,
		// localDserv stays this offline DAG service even after goOnline swaps dserv to online bitswap — so
		// local-only checks (cidMissing) never trigger a network fetch.
		localDserv: dserv,
	}

	// Join the public network (best-effort): swaps the DAG service to online bitswap. On failure the node stays
	// fully usable offline (local filestore reads, add-by-reference) — fetch of remote content just won't work.
	// VIDYAGOD_IPFS_OFFLINE forces a purely-local node (used by the tests and as a no-network escape hatch).
	if os.Getenv("VIDYAGOD_IPFS_OFFLINE") == "" {
		_ = gNode.goOnline()
	}
	return nil
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

// get returns the live node or nil.
func get() *node {
	gMu.Lock()
	defer gMu.Unlock()
	return gNode
}
