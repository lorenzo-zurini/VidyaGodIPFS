package main

import (
	"testing"

	leveldb "github.com/ipfs/go-ds-leveldb"
	goleveldbopt "github.com/syndtr/goleveldb/leveldb/opt"
)

// The store opens sized for a library: a walk of 3.6 M leaf references took 31 s with leveldb's defaults and 8 s with
// these (packages held, 2026-11-24). Teeth: drop the filter, or leave the block cache or the open tables at their defaults.
func TestStoreOpensSizedForALibrary(t *testing.T) {
	o := storeOptions()
	if o.Filter == nil || o.Filter.Name() != "leveldb.BuiltinBloomFilter" {
		t.Fatalf("filter = %v, want a bloom filter", o.Filter)
	}
	if o.GetBlockCacheCapacity() < 64<<20 {
		t.Fatalf("block cache %d < 64 MiB", o.GetBlockCacheCapacity())
	}
	if o.GetOpenFilesCacheCapacity() < 1024 {
		t.Fatalf("open tables %d < 1024", o.GetOpenFilesCacheCapacity())
	}
}

// And the node opens its store with them (the options above, handed to nothing, would change nothing). Teeth: open the
// store with nil options.
func TestTheNodeOpensItsStoreWithThem(t *testing.T) {
	var got *leveldb.Options
	open := openLevelDB
	openLevelDB = func(path string, o *leveldb.Options) (*leveldb.Datastore, error) { got = o; return open(path, o) }
	defer func() { openLevelDB = open }()
	offlineNode(t)
	o := (*goleveldbopt.Options)(got)
	if o == nil || o.Filter == nil || o.GetBlockCacheCapacity() < 64<<20 {
		t.Fatalf("the store was opened with %+v", got)
	}
}
