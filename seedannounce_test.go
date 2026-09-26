package main

import (
	"testing"

	"github.com/ipfs/go-cid"
)

// The seed announce provides what the app LISTED (package folders, library node blocks) in the blocking, tracked
// passes; everything else pinned is content for the batched queue, in pin order. A node block is a raw leaf like any
// small file, so only the listing — never the codec — can tell them apart.
func TestUnlistedIsContentInPinOrder(t *testing.T) {
	mk := func(s string) cid.Cid {
		c, err := cid.Decode(s)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	file := mk("QmPHyCZfwPaf8emAPEUsGeZwesgncCmmFkvLP4AQ9WZtBT")             // dag-pb
	raw := mk("bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy")   // raw: content…
	node := mk("bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am")  // …and a listed node block, also raw
	folder := mk("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi") // a listed package folder
	listed := map[string]struct{}{node.String(): {}, folder.String(): {}}

	content := unlisted([]cid.Cid{file, node, folder, raw}, listed)
	if len(content) != 2 || !content[0].Equals(file) || !content[1].Equals(raw) {
		t.Fatalf("content = %v, want [file raw] in pin order", content)
	}
}
