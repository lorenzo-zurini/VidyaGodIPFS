package main

// node.go — the embedded IPFS node assembly (Milestone 1: offline storage only; libp2p/DHT/bitswap added in M3).
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
	offline "github.com/ipfs/boxo/exchange/offline"
	filestore "github.com/ipfs/boxo/filestore"
	merkledag "github.com/ipfs/boxo/ipld/merkledag"
	dspinner "github.com/ipfs/boxo/pinning/pinner/dspinner"
	ipfspinner "github.com/ipfs/boxo/pinning/pinner"
	ipld "github.com/ipfs/go-ipld-format"
	leveldb "github.com/ipfs/go-ds-leveldb"
	dssync "github.com/ipfs/go-datastore/sync"
	datastore "github.com/ipfs/go-datastore"
)

// node is the singleton embedded IPFS node.
type node struct {
	ctx    context.Context
	cancel context.CancelFunc

	repoPath string

	ds        datastore.Batching
	fstore    *filestore.Filestore // routes FilestoreNode leaves to references, everything else to the blockstore
	bstore    blockstore.Blockstore
	bserv     blockservice.BlockService
	dserv     ipld.DAGService
	pinner    ipfspinner.Pinner
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

	bserv := blockservice.New(fstore, offline.Exchange(fstore))
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
