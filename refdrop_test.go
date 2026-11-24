package main

import (
	"crypto/rand"
	"path/filepath"
	"runtime"
	"testing"
)

// Dropping a file's references does not read the file: a raw leaf has no links, so the walk that finds what to
// delete never fetches one. Fetching each read its 256 KiB back from the file and hashed it — the whole file, to
// delete it. Teeth: fetch raw leaves in dropRef's walk again.
func TestDroppingAFilesReferencesDoesNotReadIt(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("reads counted from /proc")
	}
	n := offlineNode(t)
	p := filepath.Join(t.TempDir(), "content.bin")
	content := make([]byte, 32<<20)
	if _, err := rand.Read(content); err != nil {
		t.Fatal(err)
	}
	writeFile(t, p, content)
	c, err := n.addNoCopy(p)
	if err != nil {
		t.Fatal(err)
	}
	before := readBytes(t)
	n.dropRef(c)
	if read := readBytes(t) - before; read > int64(len(content))/10 {
		t.Fatalf("dropping a %d MB file's references read %d MB of it", len(content)>>20, read>>20)
	}
	if has, _ := n.plain.Has(n.ctx, c); has {
		t.Fatal("the root is still held")
	}
	if err := n.heldWhole(c); err == nil {
		t.Fatal("the dropped file's content is still held")
	}
}

// Dropping a cached closure (plain blocks bitswap stored) does not read its leaves either: their CIDs are in their
// parents' links. Teeth: fetch raw leaves in dropClosure's walk again.
func TestDroppingACachedClosureDoesNotReadItsLeaves(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("reads counted from /proc")
	}
	n := offlineNode(t)
	content := make([]byte, 32<<20)
	if _, err := rand.Read(content); err != nil {
		t.Fatal(err)
	}
	root := fileNodeFromBytes(t, n, content) // every block stored plain, as bitswap caches them
	before := readBytes(t)
	n.dropClosure(root.Cid())
	if read := readBytes(t) - before; read > int64(len(content))/10 {
		t.Fatalf("dropping a %d MB cached closure read %d MB", len(content)>>20, read>>20)
	}
	if has, _ := n.plain.Has(n.ctx, root.Cid()); has {
		t.Fatal("the root is still cached")
	}
}
