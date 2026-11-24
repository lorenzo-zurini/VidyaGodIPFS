package main

import (
	"bytes"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"
)

// A resumed fetch does not trust its earlier bytes blindly: a bitmap that claims every leaf over a file that holds
// garbage (a sync that failed, a card that lost an acknowledged write, then power gone) is read back at finalize and
// refused, and the next attempt fetches it again — never pinned and served as it is. Teeth: record the pin of a
// resumed file without reading it back.
func TestAResumedFetchChecksTheBytesItInherited(t *testing.T) {
	n := offlineNode(t)
	dir := t.TempDir()
	src := filepath.Join(dir, "src.bin")
	content := make([]byte, 4<<20)
	if _, err := rand.Read(content); err != nil {
		t.Fatal(err)
	}
	writeFile(t, src, content)
	c, err := n.addNoCopy(src)
	if err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(dir, "out", "fetched.bin")
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		t.Fatal(err)
	}
	garbage := make([]byte, len(content))
	if _, err := rand.Read(garbage); err != nil {
		t.Fatal(err)
	}
	writeFile(t, tmpPath(dest), garbage)
	count := (len(content) + int(chunkSize) - 1) / int(chunkSize)
	p := newPartBits(c.String(), int64(len(content)), count)
	for i := 0; i < count; i++ {
		p.set(i)
	}
	if err := savePart(dest, p); err != nil {
		t.Fatal(err)
	}
	// Offline here, so the fetch cannot get the bytes again (online it would): what matters is that no attempt ends
	// with the garbage accepted — pinned, its sidecar gone, served as the file.
	for attempt := 0; attempt < 3; attempt++ {
		err := n.fetchToPath(c.String(), dest, nil, nil)
		got, rerr := os.ReadFile(dest)
		if err == nil && (rerr != nil || !bytes.Equal(got, content)) {
			t.Fatalf("attempt %d reported success over the garbage the bitmap vouched for", attempt)
		}
		if attempt == 0 && err == nil {
			t.Fatal("the resumed garbage was accepted")
		}
		if rerr == nil && !bytes.Equal(got, content) && !partExists(dest) {
			t.Fatalf("attempt %d left the garbage as a finished file", attempt)
		}
	}
}
