package main

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"

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
