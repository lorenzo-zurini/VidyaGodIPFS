package main

// refwatch.go — knowing when the filestore's references changed, without reading them.
//
// The app asks every 90 s which referenced files are gone (orphanedRefPaths). Answering by listing every reference
// read the whole index — 3.6 M entries, ~270 MB of leveldb on a 900 GB library — every 90 s, at idle, forever. The
// files a reference can name change only when a reference is written or dropped, and every such write goes through
// the node's datastore: refWatch sees them, so the distinct paths listed once stay good — a write adds its file, a
// drop makes the listing read again.

import (
	"context"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/ipfs/boxo/filestore"
	fspb "github.com/ipfs/boxo/filestore/pb"
	datastore "github.com/ipfs/go-datastore"
	"google.golang.org/protobuf/proto"
)

// refWatch: the node's datastore, counting the drops of the filestore's references (dropped). A reference written adds the file it names to added: the files the references name grow
// with every block a download lands, and were read again from the whole index — 5.8 M references, every 90 s, for as
// long as anything downloaded. Only a drop makes the listing read again; a write over a reference (a move, a leaf
// re-referenced) is caught by the re-check of a file that looks gone (orphanedRefPaths).
type refWatch struct {
	datastore.Batching
	dropped *atomic.Uint64
	added   *refAdded
}

// refAdded: files references were written to since the listing was last read (orphanedRefPaths takes them).
type refAdded struct {
	mu    sync.Mutex
	paths map[string]struct{}
}

// refFile: the file a stored reference names, "" if it does not decode.
func refFile(v []byte) string {
	var dobj fspb.DataObj
	if proto.Unmarshal(v, &dobj) != nil || dobj.GetFilePath() == "" {
		return ""
	}
	return filepath.Join(fileRoot, dobj.GetFilePath())
}

func (a *refAdded) note(v []byte) {
	if a == nil {
		return
	}
	if p := refFile(v); p != "" {
		a.notePath(p)
	}
}

func (a *refAdded) notePath(p string) {
	a.mu.Lock()
	if a.paths == nil {
		a.paths = map[string]struct{}{}
	}
	a.paths[p] = struct{}{}
	a.mu.Unlock()
}

func (a *refAdded) take() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]string, 0, len(a.paths))
	for p := range a.paths {
		out = append(out, p)
	}
	a.paths = nil
	return out
}

func isRefKey(k datastore.Key) bool {
	return strings.HasPrefix(k.String(), filestore.FilestorePrefix.String()+"/")
}

func (w refWatch) Put(ctx context.Context, k datastore.Key, v []byte) error {
	err := w.Batching.Put(ctx, k, v)
	if isRefKey(k) {
		w.added.note(v) // the probe (orphanedRefPaths) takes the files written since it last looked
	}
	return err
}

func (w refWatch) Delete(ctx context.Context, k datastore.Key) error {
	err := w.Batching.Delete(ctx, k)
	if isRefKey(k) {
		w.dropped.Add(1)
	}
	return err
}

func (w refWatch) Batch(ctx context.Context) (datastore.Batch, error) {
	b, err := w.Batching.Batch(ctx)
	if err != nil {
		return nil, err
	}
	return &refWatchBatch{Batch: b, dropped: w.dropped, added: w.added}, nil
}

type refWatchBatch struct {
	datastore.Batch
	dropped *atomic.Uint64
	added   *refAdded
	paths   map[string]struct{} // the files the batch's references name, noted when it commits
	drops   bool
}

func (b *refWatchBatch) Put(ctx context.Context, k datastore.Key, v []byte) error {
	if isRefKey(k) {
		if p := refFile(v); p != "" {
			if b.paths == nil {
				b.paths = map[string]struct{}{}
			}
			b.paths[p] = struct{}{}
		}
	}
	return b.Batch.Put(ctx, k, v)
}

func (b *refWatchBatch) Delete(ctx context.Context, k datastore.Key) error {
	if isRefKey(k) {
		b.drops = true
	}
	return b.Batch.Delete(ctx, k)
}

func (b *refWatchBatch) Commit(ctx context.Context) error {
	err := b.Batch.Commit(ctx)
	if b.added != nil {
		for p := range b.paths {
			b.added.notePath(p)
		}
	}
	b.paths = nil
	if b.drops {
		b.dropped.Add(1)
	}
	return err
}
