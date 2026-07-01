package main

// Offline tests for the node's storage + fetch mechanics (no network — VIDYAGOD_IPFS_OFFLINE). They exercise the
// internal Go API directly. CID parity against Kubo's `ipfs add --nocopy` is covered by a fixed-content regression
// (the same importer settings that reproduced real recorded CIDs).

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	chunker "github.com/ipfs/boxo/chunker"
	merkledag "github.com/ipfs/boxo/ipld/merkledag"
	balanced "github.com/ipfs/boxo/ipld/unixfs/importer/balanced"
	uih "github.com/ipfs/boxo/ipld/unixfs/importer/helpers"
	ufsio "github.com/ipfs/boxo/ipld/unixfs/io"
	ipld "github.com/ipfs/go-ipld-format"
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
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
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

// Adding a DIRECTORY recursively references its files and returns a folder CID; fetching that CID back materializes the
// same tree — the publish/add-by-CID round trip for a folder of (dehydrated) packages.
func TestAddDirNoCopyRoundTrip(t *testing.T) {
	n := offlineNode(t)
	src := t.TempDir()
	writeFile(t, filepath.Join(src, "pkgA", "a.json"), []byte(`{"NODE_ID":"a"}`))
	writeFile(t, filepath.Join(src, "pkgA", "cover.png"), sampleBytes())
	writeFile(t, filepath.Join(src, "pkgB", "nested", "b.json"), []byte(`{"NODE_ID":"b"}`))

	c, err := n.addDirNoCopy(src)
	if err != nil {
		t.Fatalf("addDirNoCopy: %v", err)
	}

	dest := filepath.Join(t.TempDir(), "out")
	if err := n.fetchDirToPath(c.String(), dest); err != nil {
		t.Fatalf("fetchDirToPath: %v", err)
	}
	if got, _ := os.ReadFile(filepath.Join(dest, "pkgA", "a.json")); string(got) != `{"NODE_ID":"a"}` {
		t.Errorf("pkgA/a.json = %q", got)
	}
	if got, _ := os.ReadFile(filepath.Join(dest, "pkgB", "nested", "b.json")); string(got) != `{"NODE_ID":"b"}` {
		t.Errorf("pkgB/nested/b.json = %q", got)
	}
	if got, _ := os.ReadFile(filepath.Join(dest, "pkgA", "cover.png")); !bytes.Equal(got, sampleBytes()) {
		t.Errorf("pkgA/cover.png bytes mismatch (%d)", len(got))
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
	if err := n.fetchToPath(c.String(), dst, nil, nil); err != nil {
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
	if err := n.fetchToPath(c.String(), dst, nil, nil); err != nil {
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
	}, nil)
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
	if err := n.fetchToPath(c.String(), dst, nil, nil); err == nil {
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
	// A download must NOT be blocked by a stale local reference: fetchToPath drops the orphaned reference and
	// retries over the network. This node is offline, so the retry can't succeed — but the point is it does NOT
	// return errMissingFiles (it re-fetches instead), and the stale reference is cleared afterwards.
	if err := n.fetchToPath(c.String(), filepath.Join(dir, "out.bin"), nil, nil); err == errMissingFiles {
		t.Error("fetchToPath returned errMissingFiles instead of dropping the orphaned ref + retrying")
	}
	if n.hasLocal(c) {
		t.Error("orphaned reference was not cleared by fetchToPath's drop-and-retry")
	}
}

// Write-through references must be correct: after fetching to a new path, the content must still be readable via
// its CID even once the ORIGINAL source is gone — i.e. the references genuinely point into (and reconstruct) the
// new destination file. This is what lets the fetcher re-seed what it downloaded.
func TestWriteThroughReferencesAreValid(t *testing.T) {
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
	if err := n.fetchToPath(c.String(), dst, nil, nil); err != nil { // write-through (sampleBytes is multi-chunk raw)
		t.Fatalf("fetchToPath: %v", err)
	}

	// Remove the original — now the content can ONLY be served from the write-through references into dst.
	if err := os.Remove(src); err != nil {
		t.Fatal(err)
	}
	if n.cidMissing(c) {
		t.Fatal("content reported missing, but dst exists — write-through references are wrong")
	}

	// Read the whole DAG back via its CID (through the filestore references) and confirm it reconstructs.
	rootNode, err := n.localDserv.Get(n.ctx, c)
	if err != nil {
		t.Fatalf("get root: %v", err)
	}
	rdr, err := ufsio.NewDagReader(n.ctx, rootNode, n.localDserv)
	if err != nil {
		t.Fatalf("dag reader: %v", err)
	}
	got, err := io.ReadAll(rdr)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("content read back via write-through refs differs: %d vs %d bytes", len(got), len(content))
	}

	// And the leaves must be stored as references (not duplicated as plain blocks in the blockstore).
	fsRefs, mainBlocks := blockCounts(n)
	if fsRefs < 1 || mainBlocks > 2 {
		t.Errorf("unexpected dedup state: fsRefs=%d mainBlocks=%d (want leaves as refs, only the root as a block)", fsRefs, mainBlocks)
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

// fileNodeFromBytes builds a UnixFS file DAG from bytes (added to n.dserv) and returns its root node.
func fileNodeFromBytes(t *testing.T, n *node, b []byte) ipld.Node {
	t.Helper()
	spl := chunker.NewSizeSplitter(bytes.NewReader(b), chunkSize)
	dbp := uih.DagBuilderParams{
		Maxlinks:   uih.DefaultLinksPerBlock,
		RawLeaves:  true,
		CidBuilder: merkledag.V0CidPrefix(),
		Dagserv:    n.dserv,
	}
	db, err := dbp.New(spl)
	if err != nil {
		t.Fatalf("dag builder: %v", err)
	}
	root, err := balanced.Layout(db)
	if err != nil {
		t.Fatalf("layout: %v", err)
	}
	return root
}

// TestFetchDirToPath: build a UnixFS directory (two files + a nested subdir with a file), fetch its CID recursively,
// and assert the whole tree materializes to disk with matching contents — the "add a package folder by CID" path.
func TestFetchDirToPath(t *testing.T) {
	n := offlineNode(t)

	// nested/ subdir with one file
	sub, err := ufsio.NewDirectory(n.dserv)
	if err != nil {
		t.Fatal(err)
	}
	if err := sub.AddChild(n.ctx, "inner.json", fileNodeFromBytes(t, n, []byte(`{"NODE_ID":"inner"}`))); err != nil {
		t.Fatal(err)
	}
	subNode, err := sub.GetNode()
	if err != nil {
		t.Fatal(err)
	}
	if err := n.dserv.Add(n.ctx, subNode); err != nil {
		t.Fatal(err)
	}

	// root dir: two files + the subdir
	dir, err := ufsio.NewDirectory(n.dserv)
	if err != nil {
		t.Fatal(err)
	}
	if err := dir.AddChild(n.ctx, "pkg.json", fileNodeFromBytes(t, n, []byte(`{"NODE_ID":"pkg"}`))); err != nil {
		t.Fatal(err)
	}
	if err := dir.AddChild(n.ctx, "cover.png", fileNodeFromBytes(t, n, sampleBytes())); err != nil {
		t.Fatal(err)
	}
	if err := dir.AddChild(n.ctx, "nested", subNode); err != nil {
		t.Fatal(err)
	}
	dirNode, err := dir.GetNode()
	if err != nil {
		t.Fatal(err)
	}
	if err := n.dserv.Add(n.ctx, dirNode); err != nil {
		t.Fatal(err)
	}

	dest := filepath.Join(t.TempDir(), "out")
	if err := n.fetchDirToPath(dirNode.Cid().String(), dest); err != nil {
		t.Fatalf("fetchDirToPath: %v", err)
	}

	if got, _ := os.ReadFile(filepath.Join(dest, "pkg.json")); string(got) != `{"NODE_ID":"pkg"}` {
		t.Errorf("pkg.json = %q", got)
	}
	if got, _ := os.ReadFile(filepath.Join(dest, "nested", "inner.json")); string(got) != `{"NODE_ID":"inner"}` {
		t.Errorf("nested/inner.json = %q", got)
	}
	if got, _ := os.ReadFile(filepath.Join(dest, "cover.png")); !bytes.Equal(got, sampleBytes()) {
		t.Errorf("cover.png bytes mismatch (%d)", len(got))
	}
}
