package main

// dag_test.go — the determinism keystone (plan Phase 0). If the same logical node can hash to two different CIDs,
// every property built on the gigagraph (LAN/CID-equality, per-node dedup, "same recipe = same CID") is dead. These
// tests pin canonical dag-json: key order and whitespace are irrelevant, any value change is not, and the CID of a
// fixed fixture is a hardcoded constant so a future change to the encoder (e.g. losing the lexical map sort) fails
// LOUDLY here instead of silently minting mismatched CIDs across machines.

import (
	"bytes"
	"testing"

	cid "github.com/ipfs/go-cid"
)

// A fixed, link-free node fixture. Link-free so its CID is a stable constant independent of any other computed CID;
// links are exercised in TestDagPutGetRoundTrip.
const fixtureNode = `{"NODE_ID":"aoe2_aok_base","TYPE":"Content","TITLE":"Age of Empires II","SOURCE":{"CID":"QmContentPlaceholder","SIZE":6449053512}}`

// The SAME node, keys reordered and whitespace-mangled — must produce the identical canonical bytes and CID.
const fixtureNodeReordered = `
{
    "TYPE":   "Content",
    "SOURCE": { "SIZE": 6449053512, "CID": "QmContentPlaceholder" },
    "TITLE":  "Age of Empires II",
    "NODE_ID":"aoe2_aok_base"
}`

// The canonical CID of fixtureNode. PINNED: if this changes, the dag-json canonical form changed under us — every
// previously-minted node would silently get a new CID. Filled from the first green run; do not "fix" it by pasting a
// new value without understanding why the encoding moved.
const fixtureNodeCid = "baguqeerammca5xhvk6vlljjmvcam2f7adx7ozxeyajyqpme2mcyueo5ylk7q"

func TestDagJSONCanonicalDeterministic(t *testing.T) {
	ca, err := canonicalizeDagJSON([]byte(fixtureNode))
	if err != nil {
		t.Fatalf("canonicalize a: %v", err)
	}
	cb, err := canonicalizeDagJSON([]byte(fixtureNodeReordered))
	if err != nil {
		t.Fatalf("canonicalize b: %v", err)
	}
	if !bytes.Equal(ca, cb) {
		t.Fatalf("canonical bytes differ across key order:\n a=%s\n b=%s", ca, cb)
	}

	idA, err := computeDagJSONCid([]byte(fixtureNode))
	if err != nil {
		t.Fatalf("cid a: %v", err)
	}
	idB, err := computeDagJSONCid([]byte(fixtureNodeReordered))
	if err != nil {
		t.Fatalf("cid b: %v", err)
	}
	if idA != idB {
		t.Fatalf("CID differs across key order: %s vs %s", idA, idB)
	}
	if idA.String() != fixtureNodeCid {
		t.Fatalf("canonical CID drifted: got %s, want %s (dag-json canonical form changed?)", idA, fixtureNodeCid)
	}

	// A value change MUST change the CID (SIZE 6449053512 → 6449053513).
	changed := `{"NODE_ID":"aoe2_aok_base","TYPE":"Content","TITLE":"Age of Empires II","SOURCE":{"CID":"QmContentPlaceholder","SIZE":6449053513}}`
	idC, err := computeDagJSONCid([]byte(changed))
	if err != nil {
		t.Fatalf("cid c: %v", err)
	}
	if idC == idA {
		t.Fatal("a value change did not change the CID — encoding is lossy or the hash is not content-derived")
	}
}

func TestUpgradeNestedV0Link(t *testing.T) {
	// A v0 link nested under a SINGLE-KEY wrapper map — the case the buggy early-return skipped (a multi-key top would
	// recurse regardless and hide the bug). The walk must descend through the single-key map and upgrade the v0 "Qm…"
	// to v1, or the same content spelled v0 vs v1 canonicalizes to two different CIDs (a dedup hole).
	node := `{"WRAP":{"CID":{"/":"QmVdSsqGxp99zxz7pHeHabfJnasvLu5RCnerGHgCVUXNxA"}}}`
	canon, err := canonicalizeDagJSON([]byte(node))
	if err != nil {
		t.Fatalf("canonicalize: %v", err)
	}
	if bytes.Contains(canon, []byte("Qm")) {
		t.Fatalf("nested v0 link was not upgraded to v1: %s", canon)
	}
}

// TestPinLsIncludesDirectPins: dagPut direct-pins each node block, and the IPFS tab's row set = pinLs ∪ in-flight
// transfers — so pinLs MUST stream direct pins too, or every received/minted node row vanishes the moment its
// transfer finishes (the live regression). Teeth: drop the DirectKeys loop in pinLs and this fails.
func TestPinLsIncludesDirectPins(t *testing.T) {
	n := offlineNode(t)
	c, err := n.dagPut([]byte(`{"NODE_ID":"pinls_probe","TYPE":"DeclareLibraryItem","TITLE":"p"}`))
	if err != nil {
		t.Fatalf("dagPut: %v", err)
	}
	pins, err := n.pinLs()
	if err != nil {
		t.Fatalf("pinLs: %v", err)
	}
	for _, p := range pins {
		if p.Equals(c) {
			return
		}
	}
	t.Fatalf("direct-pinned node block %s missing from pinLs (%d pins listed)", c, len(pins))
}

func TestDagPutGetRoundTrip(t *testing.T) {
	n := offlineNode(t)

	// A real node shape: PARENTS and SOURCE.CID are dag-json LINKS ({"/":"<cid>"}). Use genuine CIDs so decode
	// accepts them as links — compute two leaf CIDs to link to.
	leaf1, err := computeDagJSONCid([]byte(`"leaf-one"`))
	if err != nil {
		t.Fatal(err)
	}
	leaf2, err := computeDagJSONCid([]byte(`"leaf-two"`))
	if err != nil {
		t.Fatal(err)
	}
	node := `{"NODE_ID":"game","TYPE":"DeclareLibraryItem","TITLE":"X",` +
		`"PARENTS":[{"/":"` + leaf1.String() + `"}],` +
		`"SOURCE":{"CID":{"/":"` + leaf2.String() + `"},"SIZE":10}}`

	c, err := n.dagPut([]byte(node))
	if err != nil {
		t.Fatalf("dagPut: %v", err)
	}
	if c.Prefix().Codec != 0x0129 { // dag-json
		t.Fatalf("stored under wrong codec: %s", c)
	}
	if !n.dagHas(c) {
		t.Fatal("dagHas false right after put")
	}

	got, err := n.dagGet(c)
	if err != nil {
		t.Fatalf("dagGet: %v", err)
	}
	canon, _ := canonicalizeDagJSON([]byte(node))
	if !bytes.Equal(got, canon) {
		t.Fatalf("round-trip bytes differ:\n got=%s\n canon=%s", got, canon)
	}

	// Putting a key-reordered equivalent yields the SAME CID (content-addressed identity, not insertion order).
	reordered := `{"SOURCE":{"SIZE":10,"CID":{"/":"` + leaf2.String() + `"}},"TITLE":"X",` +
		`"PARENTS":[{"/":"` + leaf1.String() + `"}],"TYPE":"DeclareLibraryItem","NODE_ID":"game"}`
	c2, err := n.dagPut([]byte(reordered))
	if err != nil {
		t.Fatalf("dagPut reordered: %v", err)
	}
	if c2 != c {
		t.Fatalf("key reorder changed identity: %s vs %s", c2, c)
	}

	// A dag-pb (content) CID must be REFUSED by dagGet, not returned as raw bytes a caller would try to parse as a
	// node. Mint a real dag-pb CID via addNoCopy and assert the refusal.
	p := t.TempDir() + "/content.bin"
	writeFile(t, p, []byte("raw content bytes — a dag-pb file, not a node"))
	pbCid, err := n.addNoCopy(p)
	if err != nil {
		t.Fatalf("addNoCopy: %v", err)
	}
	if _, err := n.dagGet(pbCid); err == nil {
		t.Fatalf("dagGet accepted a non-dag-json CID %s", pbCid)
	}
}

// TestDagGetManyLocal pins the blockstore-only read: a block that's present is returned (canonical bytes), and a valid
// but ABSENT CID is OMITTED — never fetched, never fabricated. This is what lets catalog-build fold in only the friend
// blocks that have actually landed, without stalling on the network. Teeth: if the read hit bitswap or returned bytes
// for a block it doesn't hold, the absent-omitted / count checks fail.
func TestDagGetManyLocal(t *testing.T) {
	n := offlineNode(t)
	present := `{"NODE_ID":"x","TYPE":"DeclareLibraryItem","TITLE":"X"}`
	c, err := n.dagPut([]byte(present))
	if err != nil {
		t.Fatalf("dagPut: %v", err)
	}
	absent, err := computeDagJSONCid([]byte(`{"NODE_ID":"ghost","TYPE":"DeclareLibraryItem"}`))
	if err != nil {
		t.Fatalf("computeDagJSONCid: %v", err)
	}
	got := n.dagGetManyLocal([]cid.Cid{c, absent})
	if len(got) != 1 {
		t.Fatalf("want exactly 1 present block, got %d", len(got))
	}
	b, ok := got[c.String()]
	if !ok {
		t.Fatal("present block missing from local read")
	}
	if _, ok := got[absent.String()]; ok {
		t.Fatal("absent block must NOT be returned by a local-only read (would mean a network fetch or fabrication)")
	}
	canon, _ := canonicalizeDagJSON([]byte(present))
	if !bytes.Equal(b, canon) {
		t.Fatalf("local read returned non-canonical bytes:\n got=%s\n want=%s", b, canon)
	}
}
