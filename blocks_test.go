package main

import (
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"syscall"
	"testing"
	"time"

	ipfspinner "github.com/ipfs/boxo/pinning/pinner"
	cid "github.com/ipfs/go-cid"
)

// A node block's CID is the CID `ipfs add` gives the same bytes as a file (one raw leaf) — the CID the app computes
// locally. If these ever differ, a published node and the file a receiver lands are two different things.
func TestBlockPutIsTheFileCidOfItsBytes(t *testing.T) {
	n := offlineNode(t)
	node := []byte(`{"LABEL":"n","LAYERS":[{"ZIP":"a.zip"}]}`)
	c, err := n.blockPut(node)
	if err != nil {
		t.Fatalf("blockPut: %v", err)
	}
	p := filepath.Join(t.TempDir(), c.String()+".json")
	writeFile(t, p, node)
	fc, err := computeCid(p)
	if err != nil {
		t.Fatalf("computeCid: %v", err)
	}
	if !fc.Equals(c) {
		t.Fatalf("blockPut %s != file CID %s", c, fc)
	}
	if want := "bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am"; mustRaw(t, n, []byte("hello\n")).String() != want {
		t.Fatalf("raw CID of \"hello\\n\" drifted from %s", want)
	}
	got, err := n.blockGet(c)
	if err != nil || string(got) != string(node) {
		t.Fatalf("blockGet = %q, %v", got, err)
	}
	if !n.hasLocal(c) {
		t.Fatal("put block not held")
	}
	// One chunk at most: a bigger node would be a chunked file with a different CID.
	if _, err := n.blockPut(make([]byte, chunkSize+1)); err == nil {
		t.Fatal("blockPut accepted more than one chunk")
	}
}

// A node file landed in place and then moved (a pruned copy, an adopt) leaves a reference whose file is gone. Putting
// the same node's bytes must store them anyway: the filestore's Put took the dead reference for "held" and stored
// nothing, so the node read as unheld and publishing its package folder failed — and the whole publish with it.
// Teeth: put through bserv (the filestore) again and blockGet fails; keep the dead reference and it reads as missing.
func TestBlockPutStoresOverADeadReference(t *testing.T) {
	n := offlineNode(t)
	node := []byte(`{"LABEL":"landed","LAYERS":[{"DIR":"x"}]}`)
	p := filepath.Join(t.TempDir(), "landed.json")
	writeFile(t, p, node)
	c, err := n.addNoCopy(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(p); err != nil {
		t.Fatal(err)
	}
	if !n.hasLocal(c) || !n.cidMissing(c) {
		t.Fatal("precondition: a dead reference that still claims the block")
	}
	if got, err := n.blockPut(node); err != nil || !got.Equals(c) {
		t.Fatalf("blockPut = %s, %v", got, err)
	}
	if b, err := n.blockGet(c); err != nil || string(b) != string(node) {
		t.Fatalf("blockGet after the put = %q, %v", b, err)
	}
	if n.cidMissing(c) {
		t.Fatal("the dead reference still reads the block as missing")
	}
	if _, err := n.makeDir(map[string]string{c.String() + ".json": c.String()}); err != nil {
		t.Fatalf("a package folder naming the node: %v", err)
	}
}

func mustRaw(t *testing.T, n *node, b []byte) cid.Cid {
	t.Helper()
	c, err := n.blockPut(b)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// A package is the folder of its node files: same entries, same folder; a changed entry, another folder; an entry the
// node does not hold is refused (a folder must never link bytes nobody can serve); a folder is not a block.
func TestMakeDirLinksHeldBlocks(t *testing.T) {
	n := offlineNode(t)
	a := mustRaw(t, n, []byte(`{"LABEL":"a","LAYERS":[]}`))
	b := mustRaw(t, n, []byte(`{"LABEL":"b","LAYERS":[]}`))
	entries := map[string]string{a.String() + ".json": a.String(), b.String() + ".json": b.String()}
	d1, err := n.makeDir(entries)
	if err != nil {
		t.Fatalf("makeDir: %v", err)
	}
	d2, err := n.makeDir(map[string]string{b.String() + ".json": b.String(), a.String() + ".json": a.String()})
	if err != nil || !d1.Equals(d2) {
		t.Fatalf("same entries → %s and %s (%v)", d1, d2, err)
	}
	if d1.Prefix().Codec != cid.DagProtobuf {
		t.Fatalf("folder codec 0x%x, want dag-pb", d1.Prefix().Codec)
	}
	d3, err := n.makeDir(map[string]string{a.String() + ".json": a.String()})
	if err != nil || d3.Equals(d1) {
		t.Fatalf("a different folder must differ: %s vs %s (%v)", d3, d1, err)
	}
	ghost, _ := rawCid([]byte("never put"))
	if _, err := n.makeDir(map[string]string{"g.json": ghost.String()}); err == nil {
		t.Fatal("makeDir linked a block the node does not hold")
	}
	if _, err := n.blockGet(d1); err == nil {
		t.Fatal("blockGet accepted a folder CID")
	}
	// The folder is pinned recursively (its children are kept by it), and its directory block is stored.
	if _, pinned, err := n.pinner.IsPinnedWithType(n.ctx, d1, ipfspinner.Recursive); err != nil || !pinned {
		t.Fatalf("folder not pinned recursively (%v)", err)
	}
	if !n.hasLocal(d1) {
		t.Fatal("folder block not stored")
	}
}

// A received folder is untrusted: a landed tree holding a symlink (it would point into the user's filesystem) or a
// special file is refused; plain files and directories pass.
func TestLandedFolderRefusesLinksAndSpecialFiles(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "a.json"), []byte("{}"))
	writeFile(t, filepath.Join(root, "sub", "b.json"), []byte("{}"))
	if err := checkLandedFolder(root); err != nil {
		t.Fatalf("plain tree refused: %v", err)
	}
	if err := os.Symlink("/etc/passwd", filepath.Join(root, "sub", "evil.json")); err != nil {
		t.Fatal(err)
	}
	if err := checkLandedFolder(root); !errors.Is(err, errFolderRefused) {
		t.Fatalf("symlink landed: %v", err)
	}
	os.Remove(filepath.Join(root, "sub", "evil.json"))
	if err := syscall.Mkfifo(filepath.Join(root, "fifo"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := checkLandedFolder(root); !errors.Is(err, errFolderRefused) {
		t.Fatalf("fifo landed: %v", err)
	}
}

// A package folder is pinned recursively without walking it, so each entry must be whole first: a file whose backing
// file is gone, or one with a leaf missing, still "has" its root block — linking and pinning it recorded a package as
// pinned that no one could fetch (a pinning service would wait on it forever). Teeth: check only the top block in
// makeDir (hasLocal) and both are linked.
func TestMakeDirRefusesAnEntryThatIsNotWhole(t *testing.T) {
	n := offlineNode(t)
	dir := t.TempDir()
	distinct := func() []byte { // distinct random files: no leaf shared between them
		b := make([]byte, 3*chunkSize+1234)
		if _, err := rand.Read(b); err != nil {
			t.Fatal(err)
		}
		return b
	}
	gone := filepath.Join(dir, "gone.bin")
	writeFile(t, gone, distinct())
	cGone, err := n.addNoCopy(gone)
	if err != nil {
		t.Fatal(err)
	}
	holed := filepath.Join(dir, "holed.bin")
	writeFile(t, holed, distinct())
	cHoled, err := n.addNoCopy(holed)
	if err != nil {
		t.Fatal(err)
	}
	whole := filepath.Join(dir, "whole.bin")
	writeFile(t, whole, distinct())
	cWhole, err := n.addNoCopy(whole)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(gone); err != nil {
		t.Fatal(err)
	}
	root, err := n.localDserv.Get(n.ctx, cHoled) // drop one leaf of holed.bin
	if err != nil || len(root.Links()) < 2 {
		t.Fatalf("fixture: want a multi-leaf file (err=%v)", err)
	}
	if err := n.fstore.DeleteBlock(n.ctx, root.Links()[1].Cid); err != nil {
		t.Fatal(err)
	}
	for name, c := range map[string]cid.Cid{"gone": cGone, "holed": cHoled} {
		if !n.hasLocal(c) {
			t.Fatalf("precondition: %s still has its root block", name)
		}
		if _, err := n.makeDir(map[string]string{name: c.String()}); err == nil {
			t.Fatalf("makeDir linked %s, which is not held whole", name)
		}
	}
	if _, err := n.makeDir(map[string]string{"whole": cWhole.String()}); err != nil {
		t.Fatalf("a whole entry must link: %v", err)
	}
}

// Choosing what a pin folder links: the entries held whole go in, the rest are named (not a refused folder), and with
// none whole there is no folder. A pin folder nesting a folder this node just made does not walk that content again
// (publishing walked every content DAG several times over). Teeth: refuse instead of leaving out and the folder is
// not made; drop the madeDirs check and the pin folder walks the content folder again.
func TestMakeWholeDirLeavesOutWhatIsNotWholeAndWalksOnce(t *testing.T) {
	n := offlineNode(t)
	dir := t.TempDir()
	add := func(name string) cid.Cid {
		b := make([]byte, 2*chunkSize+77)
		if _, err := rand.Read(b); err != nil {
			t.Fatal(err)
		}
		p := filepath.Join(dir, name)
		writeFile(t, p, b)
		c, err := n.addNoCopy(p)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	whole, gone := add("whole.bin"), add("gone.bin")
	if err := os.Remove(filepath.Join(dir, "gone.bin")); err != nil {
		t.Fatal(err)
	}
	content, notWhole, err := n.makeWholeDir(map[string]string{"w": whole.String(), "g": gone.String()}, true)
	if err != nil || !content.Defined() {
		t.Fatalf("the whole entries make a folder: %s, %v", content, err)
	}
	if len(notWhole) != 1 || notWhole[0] != "g" {
		t.Fatalf("not whole: %v, want [g]", notWhole)
	}
	if none, nw, err := n.makeWholeDir(map[string]string{"g": gone.String()}, true); err != nil || none.Defined() || len(nw) != 1 {
		t.Fatalf("nothing whole: folder %s, named %v, err %v — want no folder", none, nw, err)
	}
	// Not a CID at all (a typo in a node's SOURCE) is a gap too. Teeth: fail the folder on a decode error again.
	if c, nw, err := n.makeWholeDir(map[string]string{"w": whole.String(), "typo": "not-a-cid"}, true); err != nil || !c.Defined() || len(nw) != 1 || nw[0] != "typo" {
		t.Fatalf("a malformed entry: folder %s, named %v, err %v — want the folder, the typo named", c, nw, err)
	}
	before := wholeWalks.Load()
	if _, err := n.makeDir(map[string]string{"content": content.String()}); err != nil {
		t.Fatal(err)
	}
	if walked := wholeWalks.Load() - before; walked != 0 {
		t.Fatalf("the pin folder walked the content folder it nests again (%d walks)", walked)
	}
	// A made folder whose own block is gone is refused: entries are read locally only, never fetched to be linked.
	if err := n.fstore.DeleteBlock(n.ctx, content); err != nil {
		t.Fatal(err)
	}
	if _, err := n.makeDir(map[string]string{"content": content.String()}); err == nil {
		t.Fatal("a made folder whose block is gone was linked")
	}
}

// Checking a file's CID keeps no block: memory stays flat however large the file (it held the whole file). Teeth: back
// computeCid with an in-memory datastore again and the heap grows by the file's size.
func TestComputeCidHoldsNoBlocks(t *testing.T) {
	p := filepath.Join(t.TempDir(), "big.bin")
	b := make([]byte, 64<<20)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	writeFile(t, p, b)
	b = nil
	defer debug.SetGCPercent(debug.SetGCPercent(10)) // the heap tracks what is live, not the chunks already dropped
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	c, err := computeCid(p)
	if err != nil || !c.Defined() {
		t.Fatalf("computeCid: %s %v", c, err)
	}
	runtime.ReadMemStats(&after) // no GC in between: what it still holds is live
	if grew := int64(after.HeapInuse) - int64(before.HeapInuse); grew > 16<<20 {
		t.Fatalf("checking a 64 MiB file left %d MiB on the heap", grew>>20)
	}
}

// A held folder lists exactly its entries (name → CID) — a received package's node list; a folder not held is an
// error at once, never a wait. Teeth: list only some links; answer an unheld folder with an empty listing.
func TestDirEntriesListsAHeldFolder(t *testing.T) {
	n := offlineNode(t)
	a := mustRaw(t, n, []byte(`{"LABEL":"a","LAYERS":[]}`))
	b := mustRaw(t, n, []byte(`{"LABEL":"b","LAYERS":[]}`))
	want := map[string]string{a.String() + ".json": a.String(), b.String() + ".json": b.String()}
	d, err := n.makeDir(want)
	if err != nil {
		t.Fatalf("makeDir: %v", err)
	}
	got, err := n.dirEntries(d)
	if err != nil {
		t.Fatalf("dirEntries: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("listed %d entries, want %d: %v", len(got), len(want), got)
	}
	for name, c := range want {
		if got[name] != c {
			t.Fatalf("entry %s → %q, want %s", name, got[name], c)
		}
	}
	ghost, _ := rawCid([]byte("a folder nobody made"))
	start := time.Now()
	if l, err := n.dirEntries(ghost); err == nil {
		t.Fatalf("a folder not held listed as %v", l)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("an unheld folder took %s to refuse: it must not wait", time.Since(start))
	}
}
