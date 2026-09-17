package main

// dag.go — the node graph lives in IPFS itself: each VidyaGod node is one dag-json IPLD block, its identity is its
// CID (computed recursively over the CIDs it links). This is the substrate for the gigagraph cutover — NODE_ID stops
// being identity, the block CID is. Content blobs (game files) stay dag-pb/nocopy (add.go); a node block just LINKS to
// them, and a dag-json link is codec-agnostic so one root libraryitem CID addresses the whole closure.
//
// CANONICAL ENCODING IS THE KEYSTONE. The same logical node MUST hash to the same CID on every machine or every
// downstream property — LAN/CID-equality, per-node dedup, "same recipe = same CID" — collapses into phantom
// mismatches. dagjson.Encode emits the DAG-JSON canonical form (map keys sorted lexically by their bytes), so we
// decode the caller's JSON (any key order) and RE-ENCODE it canonically before hashing. That single round-trip is the
// determinism guarantee; dag_test.go pins it (reorder input keys → identical CID).

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	ipfspinner "github.com/ipfs/boxo/pinning/pinner"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	basicnode "github.com/ipld/go-ipld-prime/node/basicnode"
	mh "github.com/multiformats/go-multihash"
)

// dagGetTimeout bounds a single node-block fetch. Without it, dagGet on a well-formed CID that no peer provides would
// block on the app-lifetime ctx forever (an unbounded wait reachable from the "add a game" paste box). Bounded → the
// fetch fails cleanly and the caller reports it.
const dagGetTimeout = 60 * time.Second

// upgradeLinksToV1 walks decoded JSON and rewrites every dag-json link ({"/":"<cid>"}) whose CID is a CIDv0 (base58
// "Qm…", e.g. our dag-pb content leaves from `ipfs add --nocopy` parity) into its CIDv1 string form (same multihash
// and codec → the SAME block, fetchable identically). dag-json ACCEPTS a v0 link and re-emits it verbatim, so two
// spellings of the same content (v0 "Qm…" vs its v1 form) would canonicalize to two different CIDs — a dedup hole.
// Normalizing every link to v1 makes the canonical form independent of spelling. Numbers are json.Number (UseNumber)
// so int SIZEs survive the round-trip unmangled.
func upgradeLinksToV1(v interface{}) interface{} {
	switch t := v.(type) {
	case map[string]interface{}:
		if len(t) == 1 {
			if s, ok := t["/"].(string); ok {
				if c, err := cid.Decode(s); err == nil && c.Version() == 0 {
					t["/"] = cid.NewCidV1(c.Type(), c.Hash()).String()
				}
				return t // a link leaf — no children to recurse into
			}
			// a single-key map that is NOT a link ({"SOURCE":{…}}): fall through and recurse into its child, or a
			// v0 link nested one level under it survives un-upgraded (the dedup hole this function exists to close).
		}
		for k, val := range t {
			t[k] = upgradeLinksToV1(val)
		}
		return t
	case []interface{}:
		for i, e := range t {
			t[i] = upgradeLinksToV1(e)
		}
		return t
	}
	return v
}

// canonicalizeDagJSON decodes arbitrary (any-key-order) dag-json and re-encodes it in the DAG-JSON canonical form
// (sorted map keys, canonical number/link/bytes encoding). Links (`{"/":"<cid>"}`) and byte values are preserved.
// The output bytes are what we hash — identical for any two inputs that denote the same IPLD node.
func canonicalizeDagJSON(in []byte) ([]byte, error) {
	// Pre-pass: normalize any CIDv0 link to v1 so the canonical form is spelling-independent (dag-json accepts a v0
	// link but re-emits it verbatim, so v0 vs v1 for the same content would hash differently — a dedup hole).
	// UseNumber keeps integer fields (SIZE) exact across the round-trip.
	dec := json.NewDecoder(bytes.NewReader(in))
	dec.UseNumber()
	var raw interface{}
	if err := dec.Decode(&raw); err != nil {
		return nil, fmt.Errorf("parse json: %w", err)
	}
	fixed, err := json.Marshal(upgradeLinksToV1(raw))
	if err != nil {
		return nil, fmt.Errorf("re-marshal json: %w", err)
	}

	nb := basicnode.Prototype.Any.NewBuilder()
	if err := dagjson.Decode(nb, bytes.NewReader(fixed)); err != nil {
		return nil, fmt.Errorf("decode dag-json: %w", err)
	}
	var out bytes.Buffer
	if err := dagjson.Encode(nb.Build(), &out); err != nil {
		return nil, fmt.Errorf("encode dag-json: %w", err)
	}
	return out.Bytes(), nil
}

// dagJSONCidFor returns the CIDv1 (dag-json codec, sha2-256) of already-canonical bytes.
func dagJSONCidFor(canonical []byte) (cid.Cid, error) {
	h, err := mh.Sum(canonical, mh.SHA2_256, -1)
	if err != nil {
		return cid.Undef, err
	}
	return cid.NewCidV1(cid.DagJSON, h), nil
}

// computeDagJSONCid returns the CID a node block WOULD have, with no side effects (nothing enters the blockstore or
// pinset). Same canonicalization as dagPut, so the answer matches what freezing would store. Needs no started node.
func computeDagJSONCid(in []byte) (cid.Cid, error) {
	canon, err := canonicalizeDagJSON(in)
	if err != nil {
		return cid.Undef, err
	}
	return dagJSONCidFor(canon)
}

// dagPut canonicalizes a node's JSON, stores it as one dag-json block, DIRECT-pins it (each node in a frozen closure
// is put individually, so the whole closure is pinned block-by-block — no recursive dag-json walk needed), and
// announces it to the DHT. Returns the block CID = the node's identity.
func (n *node) dagPut(in []byte) (cid.Cid, error) {
	canon, err := canonicalizeDagJSON(in)
	if err != nil {
		return cid.Undef, err
	}
	c, err := dagJSONCidFor(canon)
	if err != nil {
		return cid.Undef, err
	}
	blk, err := blocks.NewBlockWithCid(canon, c)
	if err != nil {
		return cid.Undef, err
	}
	if err := n.bserv.AddBlock(n.ctx, blk); err != nil {
		return cid.Undef, err
	}
	// Direct pin (not recursive): the block's dag-json links are not walked here — the freezer puts every child
	// before its parent, so every block ends up individually pinned. Recursive pinning would require a dag-json
	// decoder in the DAGService; direct-per-block sidesteps that and matches the additive-graph model.
	if err := n.pinner.PinWithMode(n.ctx, c, ipfspinner.Direct, ""); err != nil {
		return cid.Undef, err
	}
	if err := n.pinner.Flush(n.ctx); err != nil {
		return cid.Undef, err
	}
	n.announce(c) // discoverable now, not after the 22h reprovide
	return c, nil
}

// dagGet returns a node block's raw dag-json bytes (valid JSON; links appear as `{"/":"<cid>"}`). Backed by the
// block service, so a missing block is fetched over bitswap when online. Refuses a non-dag-json CID so a caller
// that hands us a dag-pb (content) CID gets an error rather than binary bytes it will fail to parse as a node.
func (n *node) dagGet(c cid.Cid) ([]byte, error) {
	if c.Prefix().Codec != cid.DagJSON {
		return nil, fmt.Errorf("%s is not a dag-json node (codec 0x%x)", c, c.Prefix().Codec)
	}
	ctx, cancel := context.WithTimeout(n.ctx, dagGetTimeout)
	defer cancel()
	blk, err := n.bserv.GetBlock(ctx, c)
	if err != nil {
		return nil, err
	}
	return blk.RawData(), nil
}

// dagHas reports whether a node block is present in the LOCAL blockstore (no network).
func (n *node) dagHas(c cid.Cid) bool { return n.hasLocal(c) }
