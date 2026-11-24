package main

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/ipfs/boxo/filestore"
	posinfo "github.com/ipfs/boxo/filestore/posinfo"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

// A scan of every reference streams them, a few at a time: holding them all at once (a mutex wrapper's Query reads
// the whole result before giving the first) cost a 5.8 M-reference library ~2.5 GB, every 90 s while anything
// changed, and kept every write to the store waiting meanwhile. 200 k references here; reading the first of them
// must not hold the rest. Teeth: wrap the datastore in dssync.MutexWrap again.
func TestAReferenceScanStreams(t *testing.T) {
	n := offlineNode(t)
	const N = 200_000
	refs := make([]*posinfo.FilestoreNode, 0, N)
	for i := 0; i < N; i++ {
		h, err := mh.Sum([]byte(fmt.Sprintf("leaf %d", i)), mh.SHA2_256, -1)
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, &posinfo.FilestoreNode{
			Node:    &refLeaf{c: cid.NewCidV1(cid.Raw, h), size: 262144},
			PosInfo: &posinfo.PosInfo{Offset: uint64(i) * 262144, FullPath: fmt.Sprintf("/library/game%d/content.zip", i%300)},
		})
	}
	if err := n.fstore.FileManager().PutMany(n.ctx, refs); err != nil {
		t.Fatal(err)
	}
	refs = nil
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	next, err := filestore.ListAll(n.ctx, n.fstore, false)
	if err != nil {
		t.Fatal(err)
	}
	if r := next(n.ctx); r == nil || r.FilePath == "" {
		t.Fatalf("first reference: %+v", r)
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	grew := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	if grew > 8<<20 {
		t.Fatalf("reading the first of %d references held %d MB: the scan holds them all", N, grew>>20)
	}
	seen := 1
	for r := next(n.ctx); r != nil; r = next(n.ctx) {
		seen++
	}
	if seen != N {
		t.Fatalf("scanned %d references, want %d", seen, N)
	}
}
