package main

import (
	"bufio"
	"errors"
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
	cid "github.com/ipfs/go-cid"
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

// Held whole, asked of many CIDs together, answers each in its order, and a backing file shared by several of them is
// stat'd once. Teeth: answer in completion order (a worker writing its own slot wrongly); a memo per walk (the file
// stat'd for each CID that references into it).
func TestHeldWholeManyAnswersEachInOrder(t *testing.T) {
	n := offlineNode(t)
	dir := t.TempDir()
	whole := filepath.Join(dir, "whole.iso")
	short := filepath.Join(dir, "short.iso")
	gone := filepath.Join(dir, "gone.iso")
	for i, f := range []string{whole, short, gone} { // no leaf in common: a shared leaf references one file only
		b := make([]byte, len(sampleBytes()))
		rand.New(rand.NewSource(int64(i + 7))).Read(b)
		writeFile(t, f, b)
	}
	var cs []cid.Cid
	for _, f := range []string{whole, short, gone} {
		c, err := n.addNoCopy(f)
		if err != nil {
			t.Fatal(err)
		}
		cs = append(cs, c)
	}
	if err := os.Truncate(short, 300000); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(gone); err != nil {
		t.Fatal(err)
	}
	nd, err := n.localDserv.Get(n.ctx, cs[0])
	if err != nil || len(nd.Links()) == 0 {
		t.Fatalf("the whole file has no leaves: %v", err)
	}
	leaf := nd.Links()[0].Cid // references into the whole file too
	never, _ := cid.Decode("bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy")
	ask := []cid.Cid{cs[1], cs[0], never, cs[2], leaf, cs[0]}
	want := []bool{false, true, false, false, true, true}
	before := memoStats.Load()
	got := n.heldWholeMany(ask)
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("answer %d = %v, want %v (all: %v)", i, got[i], want[i], got)
		}
	}
	if stats := memoStats.Load() - before; stats != 3 { // whole, short, gone: once each
		t.Fatalf("%d stats for 3 backing files", stats)
	}
}

// Held whole, with the files: the files a CID's leaves are referenced into, each once. Teeth: list a file per leaf;
// answer whole for a file cut short.
func TestHeldWholeFilesNamesTheBackingFilesOnce(t *testing.T) {
	n := offlineNode(t)
	f := filepath.Join(t.TempDir(), "disc.iso")
	writeFile(t, f, sampleBytes())
	c, err := n.addNoCopy(f)
	if err != nil {
		t.Fatal(err)
	}
	files, err := n.heldWholeFiles(c)
	if err != nil || len(files) != 1 || filepath.Base(files[0]) != "disc.iso" {
		t.Fatalf("files = %v, %v", files, err)
	}
	if err := os.Truncate(f, 300000); err != nil {
		t.Fatal(err)
	}
	if _, err := n.heldWholeFiles(c); err == nil {
		t.Fatal("a file cut short is held whole")
	}
}

// The IPFS tab's check of every pin, in one pass: each CID's verdict is what cidServeStatus and cidMissing say of it
// alone, and a block reached from several pins (a file, and the folder holding it) is walked once. Teeth: drop the
// subtree memo (the folder walks its files again); stop at a child's first problem (gone, behind a truncated file, is
// not seen).
func TestServeStatusManyAgreesAndWalksABlockOnce(t *testing.T) {
	n := offlineNode(t)
	dir := t.TempDir()
	var files []cid.Cid
	for i, name := range []string{"whole.iso", "short.iso", "gone.iso"} {
		b := make([]byte, len(sampleBytes()))
		rand.New(rand.NewSource(int64(i + 31))).Read(b)
		f := filepath.Join(dir, name)
		writeFile(t, f, b)
		c, err := n.addNoCopy(f)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, c)
	}
	folder, err := n.makeDir(map[string]string{"a": files[1].String(), "b": files[2].String(), "c": files[0].String()})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(filepath.Join(dir, "short.iso"), 300000); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "gone.iso")); err != nil {
		t.Fatal(err)
	}
	ask := append([]cid.Cid{folder}, files...)
	before := serveWalks.Load()
	got := n.serveStatusMany(ask)
	walked := serveWalks.Load() - before
	for i, c := range ask {
		if (got[i].Err == "") != (n.cidServeStatus(c) == "") || got[i].Gone != n.cidMissing(c) {
			t.Fatalf("%d: %+v, alone: %q gone=%v", i, got[i], n.cidServeStatus(c), n.cidMissing(c))
		}
	}
	if !got[0].Gone || got[0].Err == "" {
		t.Fatalf("the folder: %+v", got[0])
	}
	// The folder's blocks, then nothing again for the files it holds.
	again := serveWalks.Load()
	n.serveStatusMany([]cid.Cid{folder})
	if alone := serveWalks.Load() - again; walked != alone {
		t.Fatalf("%d blocks walked for the folder and its files, %d for the folder alone", walked, alone)
	}
}

// References written while the listing is kept (every block a download lands) add their files to it without reading
// the index again: during a download every 90 s probe read all of it. A drop reads it again; a move of references is
// looked for again (a gone file with writes since is). Teeth: read the index again at any write (a scan per write);
// ignore the added files (a file written since, then gone, is never reported); look nothing up again after a move
// (its old file, renamed, reported gone).
func TestOrphanedRefPathsGrowWithWritesWithoutAScan(t *testing.T) {
	n := offlineNode(t)
	dir := t.TempDir()
	add := func(name string, seed int64) (string, cid.Cid) {
		b := make([]byte, len(sampleBytes()))
		rand.New(rand.NewSource(seed)).Read(b)
		f := filepath.Join(dir, name)
		writeFile(t, f, b)
		c, err := n.addNoCopy(f)
		if err != nil {
			t.Fatal(err)
		}
		return f, c
	}
	add("a.bin", 51)
	add("c.bin", 53) // listed by the scan below, not learned from a write
	if gone := n.orphanedRefPaths(); len(gone) != 0 {
		t.Fatalf("orphans: %v", gone)
	}
	scans := refScans.Load()
	b, _ := add("b.bin", 52)
	if gone := n.orphanedRefPaths(); len(gone) != 0 || refScans.Load() != scans {
		t.Fatalf("after writes: %v, %d scan(s)", gone, refScans.Load()-scans)
	}
	if err := os.Remove(b); err != nil {
		t.Fatal(err)
	}
	if gone := n.orphanedRefPaths(); len(gone) != 1 || gone[0] != b {
		t.Fatalf("a file written since, then gone: %v", gone)
	}
	// A move: the old file renamed away is not an orphan — its references follow it.
	moved := filepath.Join(dir, "moved")
	if err := os.MkdirAll(moved, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(dir, "c.bin"), filepath.Join(moved, "c.bin")); err != nil {
		t.Fatal(err)
	}
	if _, err := n.moveRefs(n.ctx, refMove{filepath.Join(dir, "c.bin"), filepath.Join(moved, "c.bin")}); err != nil {
		t.Fatal(err)
	}
	for _, p := range n.orphanedRefPaths() {
		if filepath.Base(p) == "c.bin" {
			t.Fatalf("a moved file reported gone: %s", p)
		}
	}
}

// A file whose every reference was written over to another file (the same block in both) and is then deleted is not an
// orphan: nothing names it any more. With references written since the listing, a file that looks gone is looked for
// in the index once more before it is reported. Teeth: report it from the listing as it is.
func TestAFileNoReferenceNamesAnyMoreIsNotAnOrphan(t *testing.T) {
	n := offlineNode(t)
	dir := t.TempDir()
	data := []byte("the same block in two files")
	x, y := filepath.Join(dir, "x.bin"), filepath.Join(dir, "y.bin")
	writeFile(t, x, data)
	writeFile(t, y, data)
	if _, err := n.addNoCopy(x); err != nil {
		t.Fatal(err)
	}
	if gone := n.orphanedRefPaths(); len(gone) != 0 {
		t.Fatalf("orphans: %v", gone)
	}
	leaf := &posinfo.FilestoreNode{Node: merkledag.NewRawNode(data), PosInfo: &posinfo.PosInfo{FullPath: y}}
	if err := n.fstore.FileManager().Put(n.ctx, leaf); err != nil { // written over: the block now read from y
		t.Fatal(err)
	}
	if err := os.Remove(x); err != nil {
		t.Fatal(err)
	}
	if gone := n.orphanedRefPaths(); len(gone) != 0 {
		t.Fatalf("a file no reference names reported gone: %v", gone)
	}
}

// A gone file the index confirmed still referenced does not make every probe read the index again while references
// are written (a download landing blocks, a lasting orphan the heal cannot fix): a 5.8 M-entry read every 90 s.
// Teeth: re-check every gone file at every probe with writes since (a scan per probe).
func TestALastingOrphanIsConfirmedOnce(t *testing.T) {
	n := offlineNode(t)
	dir := t.TempDir()
	gone := filepath.Join(dir, "gone.bin")
	b := make([]byte, len(sampleBytes()))
	rand.New(rand.NewSource(91)).Read(b)
	writeFile(t, gone, b)
	if _, err := n.addNoCopy(gone); err != nil {
		t.Fatal(err)
	}
	n.orphanedRefPaths()
	if err := os.Remove(gone); err != nil {
		t.Fatal(err)
	}
	if g := n.orphanedRefPaths(); len(g) != 1 {
		t.Fatalf("gone: %v", g)
	}
	scans := refScans.Load()
	for i := 0; i < 3; i++ { // a download lands blocks between probes
		f := filepath.Join(dir, "landed"+strconv.Itoa(i))
		rand.New(rand.NewSource(int64(100 + i))).Read(b)
		writeFile(t, f, b)
		if _, err := n.addNoCopy(f); err != nil {
			t.Fatal(err)
		}
		if g := n.orphanedRefPaths(); len(g) != 1 || g[0] != gone {
			t.Fatalf("probe %d: %v", i, g)
		}
	}
	if s := refScans.Load() - scans; s > 1 {
		t.Fatalf("%d reads of the index for one lasting orphan", s)
	}
}

// A gone file the index confirmed still referenced is looked for again once its blocks are re-pointed to another file
// (the heal's re-add of a copy): it was reported orphaned at every probe until an unrelated drop. Teeth: no re-point
// flag in addNoCopyIf.
func TestAHealedOrphanIsNotReportedAgain(t *testing.T) {
	n := offlineNode(t)
	dir := t.TempDir()
	b := make([]byte, len(sampleBytes()))
	rand.New(rand.NewSource(77)).Read(b)
	x, y, z := filepath.Join(dir, "x.bin"), filepath.Join(dir, "y.bin"), filepath.Join(dir, "z.bin")
	writeFile(t, x, b)
	if _, err := n.addNoCopy(x); err != nil {
		t.Fatal(err)
	}
	n.orphanedRefPaths()
	if err := os.Remove(x); err != nil {
		t.Fatal(err)
	}
	other := make([]byte, len(b))
	rand.New(rand.NewSource(78)).Read(other)
	writeFile(t, z, other)
	if _, err := n.addNoCopy(z); err != nil { // a write since: the gone file is confirmed by a read of the index
		t.Fatal(err)
	}
	if g := n.orphanedRefPaths(); len(g) != 1 || g[0] != x {
		t.Fatalf("gone: %v", g)
	}
	writeFile(t, y, b) // the heal: a copy of x's bytes, added over x's references
	if _, err := n.addNoCopy(y); err != nil {
		t.Fatal(err)
	}
	if g := n.orphanedRefPaths(); len(g) != 0 {
		t.Fatalf("a healed file still reported gone: %v", g)
	}
}

// A repair whose file went away before it was read is the file's failure, not the store's (discard and fetch again).
// Teeth: return the stat error unwrapped.
func TestAVanishedRepairFileIsTheFilesFailure(t *testing.T) {
	n := offlineNode(t)
	_, err := n.addNoCopyIf(filepath.Join(t.TempDir(), "never.bin"), cid.Undef, false)
	var fe *fileReadError
	if !errors.As(err, &fe) {
		t.Fatalf("err = %v, want a fileReadError", err)
	}
}

// A gone file the index confirmed, whose references are then moved to where it went (a rename the app made, its
// references following a moment later), is not reported again. Teeth: no re-point flag in moveRefs.
func TestAConfirmedOrphanMovedAwayIsNotReportedAgain(t *testing.T) {
	n := offlineNode(t)
	dir := t.TempDir()
	b := make([]byte, len(sampleBytes()))
	rand.New(rand.NewSource(81)).Read(b)
	oldPath, newPath, z := filepath.Join(dir, "old.bin"), filepath.Join(dir, "new.bin"), filepath.Join(dir, "z.bin")
	writeFile(t, oldPath, b)
	if _, err := n.addNoCopy(oldPath); err != nil {
		t.Fatal(err)
	}
	n.orphanedRefPaths()
	if err := os.Rename(oldPath, newPath); err != nil {
		t.Fatal(err)
	}
	other := make([]byte, len(b))
	rand.New(rand.NewSource(82)).Read(other)
	writeFile(t, z, other)
	if _, err := n.addNoCopy(z); err != nil {
		t.Fatal(err)
	}
	if g := n.orphanedRefPaths(); len(g) != 1 || g[0] != oldPath {
		t.Fatalf("gone: %v", g)
	}
	if _, err := n.moveRefs(n.ctx, refMove{oldPath, newPath}); err != nil {
		t.Fatal(err)
	}
	if g := n.orphanedRefPaths(); len(g) != 0 {
		t.Fatalf("a file whose references moved still reported gone: %v", g)
	}
}
