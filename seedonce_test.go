package main

import (
	"bufio"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	posinfo "github.com/ipfs/boxo/filestore/posinfo"
	merkledag "github.com/ipfs/boxo/ipld/merkledag"
	ipfspinner "github.com/ipfs/boxo/pinning/pinner"
)

// readBytes: what this process has read so far through read syscalls (/proc/self/io rchar) — page cache or not.
func readBytes(t *testing.T) int64 {
	f, err := os.Open("/proc/self/io")
	if err != nil {
		t.Skip("no /proc/self/io here")
	}
	defer f.Close()
	for s := bufio.NewScanner(f); s.Scan(); {
		if v, ok := strings.CutPrefix(s.Text(), "rchar: "); ok {
			n, _ := strconv.ParseInt(v, 10, 64)
			return n
		}
	}
	t.Skip("no rchar in /proc/self/io")
	return 0
}

// A file added by reference is read once: the check that it is the CID asked for, the references and the pin all come
// from that one read. Before, a seed read it three times (computeCid, the build, the recursive pin's walk of every leaf).
func TestAddNoCopyIfReadsTheFileOnce(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("counts reads through /proc/self/io")
	}
	n := offlineNode(t)
	f := filepath.Join(t.TempDir(), "disc.iso")
	b := make([]byte, 24<<20)
	rand.New(rand.NewSource(7)).Read(b)
	writeFile(t, f, b)
	want, err := computeCid(f)
	if err != nil {
		t.Fatal(err)
	}

	before := readBytes(t)
	got, err := n.addNoCopyIf(f, want, true)
	read := readBytes(t) - before
	if err != nil || !got.Equals(want) {
		t.Fatalf("addNoCopyIf = %s, %v; want %s", got, err, want)
	}
	if read > int64(len(b))*3/2 {
		t.Errorf("adding a %d MiB file read %d MiB — it is read more than once", len(b)>>20, read>>20)
	}
	if err := n.heldWhole(want); err != nil {
		t.Errorf("not held whole after the add: %v", err)
	}
	if _, pinned, err := n.pinner.IsPinnedWithType(n.ctx, want, ipfspinner.Recursive); err != nil || !pinned {
		t.Errorf("not pinned recursively after the add (%v)", err)
	}
	if bad := n.cidUnservable(want); bad != "" {
		t.Errorf("does not serve after the add: %s", bad)
	}
}

// A file that is not the CID its node records is never referenced, pinned or announced: the add answers what it is
// and keeps nothing of it.
func TestAddNoCopyIfKeepsNothingOfAnotherFile(t *testing.T) {
	n := offlineNode(t)
	dir := t.TempDir()
	recorded := filepath.Join(dir, "recorded.bin")
	writeFile(t, recorded, sampleBytes())
	want, err := computeCid(recorded)
	if err != nil {
		t.Fatal(err)
	}
	changed := filepath.Join(dir, "changed.bin")
	other := append(sampleBytes(), 'x')
	writeFile(t, changed, other)

	got, err := n.addNoCopyIf(changed, want, false)
	if err != nil {
		t.Fatal(err)
	}
	if got.Equals(want) {
		t.Fatalf("a changed file answered the recorded CID %s", want)
	}
	if is, _ := computeCid(changed); !got.Equals(is) {
		t.Errorf("answered %s; the file is %s", got, is)
	}
	if refs, plain := blockCounts(n); refs != 0 || plain != 0 {
		t.Errorf("kept %d reference(s) and %d block(s) of a file that is not the CID asked for", refs, plain)
	}
	if n.hasLocal(got) {
		t.Errorf("holds %s, which nobody asked to add", got)
	}
}

// The orphan probe runs every 90 s: it lists the references' files once and, while no reference is written or
// dropped, only stats them again — yet a file deleted since is still reported, and a reference added or dropped is
// seen at once.
func TestOrphanedRefPathsListsTheIndexOnlyWhenItChanged(t *testing.T) {
	n := offlineNode(t)
	dir := t.TempDir()
	first := filepath.Join(dir, "first.bin")
	writeFile(t, first, sampleBytes())
	c1, err := n.addNoCopy(first)
	if err != nil {
		t.Fatal(err)
	}
	if gone := n.orphanedRefPaths(); len(gone) != 0 {
		t.Fatalf("orphans with every file present: %v", gone)
	}
	scans := refScans.Load()
	if gone := n.orphanedRefPaths(); len(gone) != 0 || refScans.Load() != scans {
		t.Fatalf("asked again with nothing changed: %v, %d scan(s) more", gone, refScans.Load()-scans)
	}

	if err := os.Remove(first); err != nil {
		t.Fatal(err)
	}
	if gone := n.orphanedRefPaths(); len(gone) != 1 || gone[0] != first || refScans.Load() != scans {
		t.Fatalf("a deleted file: %v (%d scan(s) more); want [%s] from the paths already listed", gone, refScans.Load()-scans, first)
	}

	// A reference written since: listed again, so its file going away is seen.
	second := filepath.Join(dir, "second.bin")
	writeFile(t, second, append(sampleBytes(), 'y'))
	if _, err := n.addNoCopy(second); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(second); err != nil {
		t.Fatal(err)
	}
	if gone := n.orphanedRefPaths(); len(gone) != 2 {
		t.Fatalf("after a new reference whose file then went: %v, want both files", gone)
	}

	// A reference written on its own (a fetch puts its leaves one by one), not in a batch.
	third := filepath.Join(dir, "third.bin")
	data := []byte("one leaf")
	writeFile(t, third, data)
	leaf := &posinfo.FilestoreNode{Node: merkledag.NewRawNode(data), PosInfo: &posinfo.PosInfo{FullPath: third}}
	if err := n.fstore.Put(n.ctx, leaf); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(third); err != nil {
		t.Fatal(err)
	}
	if gone := n.orphanedRefPaths(); len(gone) != 3 {
		t.Fatalf("after a leaf put on its own whose file then went: %v, want all three files", gone)
	}

	// A reference dropped since (one batch): its file is no longer reported.
	n.dropRef(c1)
	if gone := n.orphanedRefPaths(); len(gone) != 2 || gone[0] != second || gone[1] != third {
		t.Fatalf("after dropping %s's references: %v, want only [%s %s]", c1, gone, second, third)
	}
}

// A leaf the new file shares with one seeded before (a common run of bytes) is referenced into the new file, over the
// old reference: the old file may be gone, or changed in place, and a recorded pin over the old reference would announce
// content no peer can be served. Teeth: keep a held leaf's reference (the filestore's Has-skip) and the new content
// reads its shared leaf from the gone file.
func TestAddNoCopyIfRepointsLeavesSharedWithAnotherFile(t *testing.T) {
	chunk := func(seed int64) []byte {
		b := make([]byte, chunkSize)
		rand.New(rand.NewSource(seed)).Read(b)
		return b
	}
	for _, gone := range []bool{true, false} {
		n := offlineNode(t)
		dir := t.TempDir()
		first := filepath.Join(dir, "v1.bin")
		writeFile(t, first, append(chunk(1), chunk(2)...))
		if _, err := n.addNoCopy(first); err != nil {
			t.Fatal(err)
		}
		if gone {
			if err := os.Remove(first); err != nil {
				t.Fatal(err)
			}
		} else { // changed in place, same length: only a read finds it
			writeFile(t, first, append(chunk(3), chunk(2)...))
		}
		second := filepath.Join(dir, "v2.bin")
		writeFile(t, second, append(chunk(1), chunk(4)...)) // shares v1's first leaf
		want, err := computeCid(second)
		if err != nil {
			t.Fatal(err)
		}
		if got, err := n.addNoCopyIf(second, want, false); err != nil || !got.Equals(want) {
			t.Fatalf("addNoCopyIf = %s, %v", got, err)
		}
		if bad := n.cidUnservable(want); bad != "" {
			t.Errorf("(old file gone: %v) the new file does not serve: %s", gone, bad)
		}
	}
}

// Held whole asks where each leaf's reference points: a file cut short since no longer holds the leaves past its end,
// though it is still there; one that grew still holds every published byte. Teeth: check only that the file exists.
func TestHeldWholeSeesAFileCutShort(t *testing.T) {
	n := offlineNode(t)
	f := filepath.Join(t.TempDir(), "disc.iso")
	writeFile(t, f, sampleBytes())
	c, err := n.addNoCopy(f)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(f, int64(len(sampleBytes()))+4096); err != nil {
		t.Fatal(err)
	}
	if err := n.heldWhole(c); err != nil {
		t.Fatalf("a file that grew still holds every published byte: %v", err)
	}
	if err := os.Truncate(f, 300000); err != nil {
		t.Fatal(err)
	}
	if err := n.heldWhole(c); err == nil {
		t.Fatal("a file cut short is held whole")
	}
}
