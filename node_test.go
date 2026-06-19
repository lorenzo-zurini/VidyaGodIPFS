package main

// Offline tests for the node's storage + fetch mechanics (no network — VIDYAGOD_IPFS_OFFLINE). They exercise the
// internal Go API directly. CID parity against Kubo's `ipfs add --nocopy` is covered by a fixed-content regression
// (the same importer settings that reproduced real recorded CIDs).

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// offlineNode opens a fresh purely-local node in a temp repo and registers teardown.
func offlineNode(t *testing.T) *node {
	t.Helper()
	t.Setenv("VIDYAGOD_IPFS_OFFLINE", "1")
	if err := openNode(t.TempDir()); err != nil {
		t.Fatalf("openNode: %v", err)
	}
	t.Cleanup(closeNode)
	n := get()
	if n == nil {
		t.Fatal("node not started")
	}
	return n
}

// sampleBytes is deterministic multi-chunk content: 600000 bytes → 3 leaves at the 262144 chunk size.
func sampleBytes() []byte {
	b := make([]byte, 600000)
	for i := range b {
		b[i] = byte(i*7 + 3)
	}
	return b
}

func writeFile(t *testing.T, path string, b []byte) {
	t.Helper()
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
}

// blockCounts reports (filestore references, plain-blockstore blocks) — the logical no-duplication view.
func blockCounts(n *node) (fsRefs, mainBlocks int) {
	if ch, err := n.fstore.FileManager().AllKeysChan(n.ctx); err == nil {
		for range ch {
			fsRefs++
		}
	}
	if ch, err := n.fstore.MainBlockstore().AllKeysChan(n.ctx); err == nil {
		for range ch {
			mainBlocks++
		}
	}
	return
}

// The CID for sampleBytes() under Kubo's `ipfs add --nocopy` settings (raw leaves, 256 KiB chunker, dag-pb v0
// root). Pins this so a change in importer parameters — which would silently break public-network compatibility —
// fails the build.
// (Verified equal to `ipfs add --nocopy -Q` on the identical bytes.)
const sampleCID = "QmQERknUMpAyepiUavR9smoKE9CbxtAVoRxViYbSYFYZVx"

func TestAddNoCopyDeterministicAndParity(t *testing.T) {
	n := offlineNode(t)
	dir := t.TempDir()
	f := filepath.Join(dir, "data.bin")
	writeFile(t, f, sampleBytes())

	c1, err := n.addNoCopy(f)
	if err != nil {
		t.Fatalf("addNoCopy: %v", err)
	}
	c2, err := n.addNoCopy(f)
	if err != nil {
		t.Fatalf("addNoCopy (2nd): %v", err)
	}
	if c1 != c2 {
		t.Fatalf("non-deterministic CID: %s vs %s", c1, c2)
	}
	if c1.Version() != 0 {
		t.Errorf("expected a CIDv0 dag-pb root, got v%d (%s)", c1.Version(), c1)
	}
	if c1.String() != sampleCID {
		t.Errorf("CID regression — importer params changed?\n got  %s\n want %s", c1, sampleCID)
	}
}

func TestAddNoCopyNoDuplication(t *testing.T) {
	n := offlineNode(t)
	dir := t.TempDir()
	f := filepath.Join(dir, "data.bin")
	writeFile(t, f, sampleBytes())

	if _, err := n.addNoCopy(f); err != nil {
		t.Fatal(err)
	}
	fsRefs, mainBlocks := blockCounts(n)
	if fsRefs < 1 {
		t.Errorf("expected leaf data stored as filestore references, got fsRefs=%d", fsRefs)
	}
	if mainBlocks > 2 {
		t.Errorf("expected only the dag-pb root in the blockstore, got mainBlocks=%d (leaf data duplicated?)", mainBlocks)
	}
}

func TestAddNoCopyDirectoryRejected(t *testing.T) {
	n := offlineNode(t)
	if _, err := n.addNoCopy(t.TempDir()); err == nil {
		t.Error("expected directory add to be rejected (M1: files only)")
	}
}

func TestPinLsAndCidSize(t *testing.T) {
	n := offlineNode(t)
	dir := t.TempDir()
	f := filepath.Join(dir, "data.bin")
	writeFile(t, f, sampleBytes())

	c, err := n.addNoCopy(f)
	if err != nil {
		t.Fatal(err)
	}
	pins, err := n.pinLs()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, p := range pins {
		if p == c {
			found = true
		}
	}
	if !found {
		t.Errorf("added CID %s not in pin set %v", c, pins)
	}
	// CumulativeSize is the DAG total — at least the file size, a little more for the dag-pb framing.
	if sz := n.cidSize(c); sz < int64(len(sampleBytes())) {
		t.Errorf("cidSize=%d, expected >= %d", sz, len(sampleBytes()))
	}

	if err := n.unpin(c); err != nil {
		t.Fatal(err)
	}
	if pins, _ := n.pinLs(); len(pins) != 0 {
		t.Errorf("expected no pins after unpin, got %v", pins)
	}
}

func TestFetchOfflineRoundTrip(t *testing.T) {
	n := offlineNode(t)
	dir := t.TempDir()
	src := filepath.Join(dir, "src.bin")
	content := sampleBytes()
	writeFile(t, src, content)

	c, err := n.addNoCopy(src)
	if err != nil {
		t.Fatal(err)
	}

	dst := filepath.Join(dir, "out", "fetched.bin")
	if err := n.fetchToPath(c.String(), dst, nil); err != nil {
		t.Fatalf("fetchToPath: %v", err)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("fetched content differs: %d bytes vs %d", len(got), len(content))
	}

	// Fetching an existing destination is a no-op (and must not error).
	if err := n.fetchToPath(c.String(), dst, nil); err != nil {
		t.Errorf("re-fetch of existing dest should be a no-op, got %v", err)
	}

	// No blockstore duplication: leaves are filestore references, only the root is a plain block.
	if fsRefs, mainBlocks := blockCounts(n); fsRefs < 1 || mainBlocks > 2 {
		t.Errorf("unexpected dedup state after fetch: fsRefs=%d mainBlocks=%d", fsRefs, mainBlocks)
	}
}

func TestFetchProgressReported(t *testing.T) {
	n := offlineNode(t)
	dir := t.TempDir()
	src := filepath.Join(dir, "src.bin")
	writeFile(t, src, sampleBytes())
	c, err := n.addNoCopy(src)
	if err != nil {
		t.Fatal(err)
	}

	var last float64 = -1
	saw := false
	err = n.fetchToPath(c.String(), filepath.Join(dir, "out.bin"), func(pct float64) {
		saw = true
		if pct < last {
			t.Errorf("progress went backwards: %f after %f", pct, last)
		}
		last = pct
	})
	if err != nil {
		t.Fatal(err)
	}
	if !saw {
		t.Error("no progress callbacks fired")
	}
}

func TestFetchCancellation(t *testing.T) {
	n := offlineNode(t)
	dir := t.TempDir()
	src := filepath.Join(dir, "src.bin")
	writeFile(t, src, sampleBytes())
	c, err := n.addNoCopy(src)
	if err != nil {
		t.Fatal(err)
	}

	requestCancel(c.String())
	defer clearCancel(c.String())
	dst := filepath.Join(dir, "cancelled.bin")
	if err := n.fetchToPath(c.String(), dst, nil); err == nil {
		t.Error("expected cancellation to abort the fetch")
	}
	if _, err := os.Stat(dst); !os.IsNotExist(err) {
		t.Error("cancelled fetch should not leave a destination file")
	}
}

func TestOrphanedRefDetectedAndFetchErrors(t *testing.T) {
	n := offlineNode(t)
	dir := t.TempDir()
	src := filepath.Join(dir, "src.bin")
	writeFile(t, src, sampleBytes())
	c, err := n.addNoCopy(src)
	if err != nil {
		t.Fatal(err)
	}

	// Intact: not missing, fetch works.
	if n.cidMissing(c) {
		t.Fatal("freshly-added content reported as missing")
	}

	// Delete the backing file out from under the filestore reference (simulates a deleted package).
	if err := os.Remove(src); err != nil {
		t.Fatal(err)
	}

	if !n.cidMissing(c) {
		t.Error("orphaned CID (backing file deleted) not detected as missing")
	}
	// Fetching it now yields the clean "missing files" error (→ "Errored: missing files" in the UI), not a
	// cryptic open() error and not a silent success.
	if err := n.fetchToPath(c.String(), filepath.Join(dir, "out.bin"), nil); err != errMissingFiles {
		t.Errorf("expected errMissingFiles fetching an orphaned CID, got %v", err)
	}
}

func TestStartedOfflineNotOnline(t *testing.T) {
	n := offlineNode(t)
	if !n.online && get() == nil {
		t.Fatal("node should be started")
	}
	if n.online {
		t.Error("VIDYAGOD_IPFS_OFFLINE node should not be online")
	}
}
