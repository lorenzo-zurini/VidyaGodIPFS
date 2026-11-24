package main

import (
	"crypto/rand"
	"path/filepath"
	"runtime"
	"testing"
)

// A fetched file is read once — to copy it in — and its pin is recorded, not walked: bitswap (or the local blocks)
// verified every block as it came, and every leaf was referenced into the file just before the pin. Walking the pin
// read the whole file back and hashed it again: on a library's replication, ~900 GB read twice. Teeth: pin the root
// with pinner.Pin (the recursive walk) again.
func TestAFetchedFileIsNotReadAgainToPinIt(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("reads counted from /proc")
	}
	n := offlineNode(t)
	dir := t.TempDir()
	src := filepath.Join(dir, "src.bin")
	content := make([]byte, 32<<20)
	if _, err := rand.Read(content); err != nil {
		t.Fatal(err)
	}
	writeFile(t, src, content)
	c, err := n.addNoCopy(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.unpin(c); err != nil { // the add pinned it: the pin checked below must be the fetch's
		t.Fatal(err)
	}
	before := readBytes(t)
	if err := n.fetchToPath(c.String(), filepath.Join(dir, "out", "fetched.bin"), nil, nil); err != nil {
		t.Fatalf("fetchToPath: %v", err)
	}
	read := readBytes(t) - before
	if read > int64(len(content))*16/10 {
		t.Fatalf("fetching %d MB read %d MB: the file was read again after it was written", len(content)>>20, read>>20)
	}
	if n.cidMissing(c) {
		t.Fatal("the fetched file's content reads as missing")
	}
	pinned, err := n.pinnedDirectly(c)
	if err != nil || !pinned {
		t.Fatalf("the fetched file is not pinned (%v)", err)
	}
}
