package main

// refwatch.go — knowing when the filestore's references changed, without reading them.
//
// The app asks every 90 s which referenced files are gone (orphanedRefPaths). Answering by listing every reference
// read the whole index — 3.6 M entries, ~270 MB of leveldb on a 900 GB library — every 90 s, at idle, forever. The
// files a reference can name change only when a reference is written or dropped, and every such write goes through
// the node's datastore: refWatch counts them, so the distinct paths listed once stay good until the count moves.

import (
	"context"
	"strings"
	"sync/atomic"

	"github.com/ipfs/boxo/filestore"
	datastore "github.com/ipfs/go-datastore"
)

// refWatch: the node's datastore, counting each committed write under the filestore's references (gen).
type refWatch struct {
	datastore.Batching
	gen *atomic.Uint64
}

func isRefKey(k datastore.Key) bool {
	return strings.HasPrefix(k.String(), filestore.FilestorePrefix.String()+"/")
}

func (w refWatch) Put(ctx context.Context, k datastore.Key, v []byte) error {
	err := w.Batching.Put(ctx, k, v)
	if isRefKey(k) {
		w.gen.Add(1) // after the write: a scan that began before it sees a different count when it ends
	}
	return err
}

func (w refWatch) Delete(ctx context.Context, k datastore.Key) error {
	err := w.Batching.Delete(ctx, k)
	if isRefKey(k) {
		w.gen.Add(1)
	}
	return err
}

func (w refWatch) Batch(ctx context.Context) (datastore.Batch, error) {
	b, err := w.Batching.Batch(ctx)
	if err != nil {
		return nil, err
	}
	return &refWatchBatch{Batch: b, gen: w.gen}, nil
}

type refWatchBatch struct {
	datastore.Batch
	gen     *atomic.Uint64
	touched bool
}

func (b *refWatchBatch) Put(ctx context.Context, k datastore.Key, v []byte) error {
	b.touched = b.touched || isRefKey(k)
	return b.Batch.Put(ctx, k, v)
}

func (b *refWatchBatch) Delete(ctx context.Context, k datastore.Key) error {
	b.touched = b.touched || isRefKey(k)
	return b.Batch.Delete(ctx, k)
}

func (b *refWatchBatch) Commit(ctx context.Context) error {
	err := b.Batch.Commit(ctx)
	if b.touched {
		b.gen.Add(1)
	}
	return err
}
