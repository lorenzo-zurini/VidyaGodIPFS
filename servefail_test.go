package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	ipld "github.com/ipfs/go-ipld-format"
)

// A block a peer asks for that we cannot deliver must be RECORDED, not merely logged. This is the uploader-side
// corruption signal BitTorrent has no equivalent for: there only the downloader hashes a piece, so a seeder with
// rotted data serves garbage indefinitely and just gets banned. Here the filestore verifies every read against
// the reference's hash, so a failed serve is proof — at the exact moment it matters — that we advertised
// something we cannot produce.
func TestServeFailureIsRecordedWhenBackingBytesChange(t *testing.T) {
	n := offlineNode(t)

	path := filepath.Join(t.TempDir(), "a.bin")
	if err := os.WriteFile(path, sampleBytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	root, err := n.addNoCopy(path)
	if err != nil {
		t.Fatalf("addNoCopy: %v", err)
	}

	// Healthy: reading every block works and records nothing.
	if e := n.cidUnservable(root); e != "" {
		t.Fatalf("freshly added content should be servable, got %q", e)
	}
	if got := n.serveFails.drain(); len(got) != 0 {
		t.Fatalf("no failures expected on a healthy read, got %v", got)
	}

	// Corrupt IN PLACE at the same length — the case no cheap size/stat check can catch, and the one that
	// silently produced 933 undeliverable references in the real library.
	f, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte("XXXXXXXX"), 1024); err != nil {
		t.Fatal(err)
	}
	f.Close()

	// Serve it the way bitswap would: read the blocks out of the node's blockstore.
	if e := n.cidUnservable(root); e == "" {
		t.Fatal("corrupted backing bytes should make the DAG unservable")
	}
	fails := n.serveFails.drain()
	if len(fails) == 0 {
		t.Fatal("a failed serve read must be recorded")
	}
	if !strings.Contains(strings.ToLower(fails[0].Err), "did not match") {
		t.Fatalf("expected a hash-mismatch reason, got %q", fails[0].Err)
	}
	if fails[0].When == 0 {
		t.Error("failure should carry a timestamp")
	}

	// drain() empties: the UI consumes each failure once rather than re-reporting it forever.
	if got := n.serveFails.drain(); len(got) != 0 {
		t.Fatalf("drain should empty the log, got %v", got)
	}
}

// A block we simply do not host is NOT a serve failure — it is the ordinary answer "we don't have that", and
// recording it would bury the real failures under noise from every routine want-list probe.
func TestMissingBlockIsNotAServeFailure(t *testing.T) {
	n := offlineNode(t)
	path := filepath.Join(t.TempDir(), "b.bin")
	if err := os.WriteFile(path, sampleBytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	root, err := n.addNoCopy(path)
	if err != nil {
		t.Fatal(err)
	}
	n.serveFails.drain()

	// Ask for something we never added.
	var notFound ipld.ErrNotFound
	_, err = n.bstore.Get(context.Background(), root) // sanity: this one IS held
	if err != nil {
		t.Fatalf("held block should read: %v", err)
	}
	other := sampleBytes()
	other[0] ^= 0xFF
	otherPath := filepath.Join(t.TempDir(), "c.bin")
	if err := os.WriteFile(otherPath, other, 0o644); err != nil {
		t.Fatal(err)
	}
	// Compute a CID we do not hold, then request it.
	missing, err := computeCid(otherPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := n.bstore.Get(context.Background(), missing); err == nil {
		t.Fatal("expected a not-found error for a block we never added")
	} else if !errorAs(err, &notFound) {
		t.Logf("not-found error was %T (%v)", err, err)
	}
	if got := n.serveFails.drain(); len(got) != 0 {
		t.Fatalf("a not-held block must not count as a serve failure, got %v", got)
	}
}

func errorAs(err error, target *ipld.ErrNotFound) bool {
	e, ok := err.(ipld.ErrNotFound)
	if ok {
		*target = e
	}
	return ok
}
