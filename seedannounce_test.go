package main

import (
	"testing"

	"github.com/ipfs/go-cid"
)

// The seed announce provides NODE blocks (dag-json) in the blocking, tracked pass BEFORE content: a share is a set
// of root CIDs, and nothing is findable until those are provided. A dag-pb / raw CID is content; a legacy meta
// level is neither (it has its own pass); pin order is preserved within each class.
func TestSplitSeedPinsNodesBeforeContent(t *testing.T) {
	mk := func(s string) cid.Cid {
		c, err := cid.Decode(s)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	node1 := mk("baguqeeracdbs5a4rr5et2wcowrjgmiby3wjrlxkymmg5of77brpuojgphfdq") // dag-json
	node2 := mk("baguqeera2lhnrxf274esyv5oghmopswicrdjr2h4zx6w76vyopr37x3s2d3a") // dag-json
	file := mk("QmPHyCZfwPaf8emAPEUsGeZwesgncCmmFkvLP4AQ9WZtBT")               // dag-pb
	raw := mk("bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy")     // raw
	metaLvl := mk("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")   // dag-pb meta level
	meta := map[string]struct{}{metaLvl.String(): {}}

	nodes, content := splitSeedPins([]cid.Cid{file, node1, metaLvl, raw, node2}, meta)
	if len(nodes) != 2 || !nodes[0].Equals(node1) || !nodes[1].Equals(node2) {
		t.Fatalf("nodes = %v, want [node1 node2] in pin order", nodes)
	}
	if len(content) != 2 || !content[0].Equals(file) || !content[1].Equals(raw) {
		t.Fatalf("content = %v, want [file raw] in pin order", content)
	}
}
