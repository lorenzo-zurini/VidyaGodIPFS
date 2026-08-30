package main

// servefail.go — record blocks we FAILED to hand a peer.
//
// This is the uploader-side check BitTorrent does not have. In BitTorrent the seeder reads a piece off disk and
// sends it; only the DOWNLOADER hashes it, so a seeder with rotted data keeps happily serving garbage and never
// finds out (peers just ban it). Here every block is content-addressed and the filestore verifies each read
// against the reference's hash, so the moment a leecher asks for a block whose backing bytes changed, OUR read
// fails — we already know, precisely, at the exact instant it matters.
//
// That signal was being thrown away in a log line ("blockstore.Get(...) error: data in file did not match"). This
// wrapper captures it so the UI can turn the row red instead of the user finding out when a download hangs.

import (
	"context"
	"errors"
	"sync"
	"time"

	blockstore "github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
)

// One failed serve attempt.
type serveFailure struct {
	Cid  string `json:"cid"`
	Err  string `json:"err"`
	When int64  `json:"when"` // unix seconds
}

// Bounded so a pathologically broken repo cannot grow this without limit; the newest failures are the useful
// ones, and each distinct CID is kept once (a stalled peer retries the same block relentlessly).
const maxServeFailures = 256

type failureLog struct {
	mu    sync.Mutex
	byCid map[string]serveFailure
	order []string
}

func newFailureLog() *failureLog { return &failureLog{byCid: map[string]serveFailure{}} }

func (f *failureLog) record(c cid.Cid, err error) {
	if err == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	k := c.String()
	if _, seen := f.byCid[k]; !seen {
		f.order = append(f.order, k)
		for len(f.order) > maxServeFailures {
			delete(f.byCid, f.order[0])
			f.order = f.order[1:]
		}
	}
	f.byCid[k] = serveFailure{Cid: k, Err: err.Error(), When: time.Now().Unix()}
}

func (f *failureLog) drain() []serveFailure {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]serveFailure, 0, len(f.byCid))
	for _, k := range f.order {
		if v, ok := f.byCid[k]; ok {
			out = append(out, v)
		}
	}
	f.byCid = map[string]serveFailure{}
	f.order = nil
	return out
}

// recordingBlockstore delegates everything and notes Get failures. Get is the read bitswap performs to answer a
// peer, so a failure here means precisely "someone asked us for this and we could not deliver it".
type recordingBlockstore struct {
	blockstore.Blockstore
	log *failureLog
}

func (r *recordingBlockstore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	b, err := r.Blockstore.Get(ctx, c)
	var notFound ipld.ErrNotFound
	if err != nil && !errors.As(err, &notFound) {
		// ErrNotFound is ordinary ("we don't host that"), not a broken promise. Everything else — above all the
		// filestore's "data in file did not match" — means we ADVERTISED a block we then could not produce.
		r.log.record(c, err)
	}
	return b, err
}
