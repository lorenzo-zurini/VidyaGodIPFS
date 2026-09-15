package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	crypto "github.com/libp2p/go-libp2p/core/crypto"
	peer "github.com/libp2p/go-libp2p/core/peer"
)

// genPeer makes a real Ed25519 peer ID (the peer ID embeds the pubkey — the same shape a friend code has).
func genPeer(t *testing.T) peer.ID {
	t.Helper()
	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		t.Fatal(err)
	}
	id, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// friendProviderPeers is what makes "the friend ID is everything" reach bitswap: accepted friends become providers
// for every CID. Teeth: it must return exactly the ACCEPTED friends, never a pending/blocked one, and never self.
func TestFriendProviderPeersOffersAcceptedFriendsNotSelfNotPending(t *testing.T) {
	s := newSocialState(filepath.Join(t.TempDir(), "sub"))
	self := genPeer(t)
	acceptedA, acceptedB := genPeer(t), genPeer(t)
	pending := genPeer(t)
	blocked := genPeer(t)

	s.upsert(acceptedA.String(), func(c *contact) { c.State = stAccepted })
	s.upsert(acceptedB.String(), func(c *contact) { c.State = stAccepted })
	s.upsert(pending.String(), func(c *contact) { c.State = stPending })
	s.upsert(blocked.String(), func(c *contact) { c.State = stBlocked })
	s.upsert(self.String(), func(c *contact) { c.State = stAccepted }) // self befriended somehow — must NOT be offered

	got := map[peer.ID]bool{}
	for _, p := range friendProviderPeers(s, self) {
		got[p] = true
	}
	if !got[acceptedA] || !got[acceptedB] {
		t.Errorf("accepted friends must be offered as providers; got %v", got)
	}
	if got[self] {
		t.Error("self must never be offered as a provider (would make bitswap ask itself)")
	}
	if got[pending] {
		t.Error("a PENDING contact is not a friend and must not be offered")
	}
	if got[blocked] {
		t.Error("a BLOCKED contact must not be offered")
	}
	if len(got) != 2 {
		t.Errorf("expected exactly the 2 accepted non-self friends, got %d: %v", len(got), got)
	}
}

// The anti-hang invariant, negative half: an OFFLINE node must FAIL a fetch of absent content PROMPTLY, not retry
// forever. The retry-forever behaviour is only correct online (a provider may appear); offline there is no network,
// so retrying is an infinite spin. Teeth: remove the `n.dht == nil` guards in fetchToPathLoop/getRoot and this hangs.
func TestOfflineFetchOfAbsentContentReturnsPromptlyNotForever(t *testing.T) {
	n := offlineNode(t)
	// A well-formed CID for content this node does not have and cannot fetch (offline).
	src := filepath.Join(t.TempDir(), "x.bin")
	writeFile(t, src, sampleBytes())
	c, err := n.addNoCopy(src)
	if err != nil {
		t.Fatal(err)
	}
	n.dropRef(c) // now the node neither has it nor can fetch it (offline)

	done := make(chan error, 1)
	go func() { done <- n.fetchToPath(c.String(), filepath.Join(t.TempDir(), "out.bin"), nil, nil) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("offline fetch of absent content unexpectedly succeeded")
		}
		// any non-nil error is correct — the point is it RETURNED rather than spinning.
	case <-time.After(20 * time.Second):
		t.Fatal("offline fetch did not return within 20s — it is retrying forever (anti-hang guard missing)")
	}
}

// A fetch that crashed mid-finalize leaves dest on disk WITH a .part marker but the content unreferenced and
// unpinned (the tmp→dest rename happened; the reference+pin did not). The dest-exists path must REPAIR this
// (re-reference + re-pin from the existing file), not treat dest-exists as a blind no-op — otherwise the content
// is present but unseedable and GC-vulnerable forever. Teeth: revert the repair to a plain `return nil` and the
// post-conditions below (hasLocal true, .part cleared) fail.
func TestCrashedFinalizeIsRepairedOnNextFetch(t *testing.T) {
	n := offlineNode(t)
	dir := t.TempDir()
	dest := filepath.Join(dir, "out.bin")
	writeFile(t, dest, sampleBytes())

	// Reference it (as a completed fetch would), learn its CID, then simulate the crash: drop the reference/pin
	// but LEAVE the dest bytes, and drop a .part marker beside it — exactly the on-disk state of a finalize that
	// died after the rename.
	c, err := n.addNoCopy(dest)
	if err != nil {
		t.Fatal(err)
	}
	n.dropRef(c)
	if n.hasLocal(c) {
		t.Fatal("precondition: content should be unreferenced after dropRef")
	}
	if err := os.WriteFile(dest+".part", []byte("marker"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := n.fetchToPath(c.String(), dest, nil, nil); err != nil {
		t.Fatalf("repair fetch returned %v", err)
	}
	if !n.hasLocal(c) {
		t.Error("crashed finalize was NOT repaired: content still unreferenced after fetch (dest-exists was a blind no-op)")
	}
	if partExists(dest) {
		t.Error("the .part commit-marker was not cleared after a successful repair")
	}
}

// The want-token pool is the hard global cap on outstanding wants across ALL concurrent fetches: you can never
// hold more than wantBudget tokens at once, so the combined wants sent to any single peer stay under the stock
// 1024/peer cap by construction. Teeth: make the pool unbounded (or > 1024) and the cap assertions fail.
func TestWantPoolCapsOutstandingWants(t *testing.T) {
	if wantBudget >= 1024 {
		t.Fatalf("wantBudget %d must stay under the stock boxo 1024/peer cap", wantBudget)
	}
	p := newWantPool(wantBudget)
	if p.available() != wantBudget {
		t.Fatalf("fresh pool should have %d tokens, has %d", wantBudget, p.available())
	}
	ctx := context.Background()
	// Drain the whole budget — every acquire up to the cap must succeed.
	for i := 0; i < wantBudget; i++ {
		if !p.acquire(ctx) {
			t.Fatalf("acquire %d/%d failed on a non-empty pool", i, wantBudget)
		}
	}
	// The cap: one more must NOT be grantable (this is what bounds outstanding wants to a peer).
	if p.tryAcquire() {
		t.Fatal("acquired a token beyond wantBudget — the pool does not cap outstanding wants")
	}
	// A cancelled acquire returns false rather than blocking forever (teardown path).
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if p.acquire(cctx) {
		t.Fatal("acquire on an empty pool with a cancelled ctx must return false, not block")
	}
	// Release restores capacity, and never exceeds the cap even if over-released.
	p.release()
	if p.available() != 1 {
		t.Fatalf("after one release available should be 1, is %d", p.available())
	}
	for i := 0; i < wantBudget*2; i++ {
		p.release()
	}
	if p.available() != wantBudget {
		t.Fatalf("over-release must not exceed the cap; available %d != %d", p.available(), wantBudget)
	}
}

// The real thing, end to end, offline: a multi-leaf file already in the local store, fetched to a NEW dest, drives
// the FULL rolling-window path — producer acquires tokens + issues GetBlocks in refill batches, fans arrive, the
// receive loop writes them and returns tokens, the exit drains the rest. Asserts: (a) the bytes are correct, (b)
// it COMPLETES (a wedge would hang the test — the anti-head-of-line-block property), and (c) every token is
// returned to the global pool afterwards (no leak, no over-release — the accounting the adversary flagged as
// untested). Teeth: break the exit-drain or the per-block release and (c) fails; wedge the producer and (b) hangs.
func TestLargeOfflineFetchRunsRollingWindowAndReturnsAllTokens(t *testing.T) {
	n := offlineNode(t)
	dir := t.TempDir()
	src := filepath.Join(dir, "big.bin")
	// RANDOM data → every 256KiB leaf is unique (NO dedup), so a multi-MB file is genuinely many leaves and the
	// rolling window has real work. (A periodic pattern would dedup to ONE leaf and the window would never roll —
	// the degenerate fixture that made an earlier version of this test hollow.)
	big := make([]byte, 8<<20) // 8 MiB → 32 leaves at 256 KiB
	if _, err := rand.Read(big); err != nil {
		t.Fatal(err)
	}
	writeFile(t, src, big)
	c, err := n.addNoCopy(src)
	if err != nil {
		t.Fatal(err)
	}
	beforeTokens := globalWantPool.available()

	dest := filepath.Join(dir, "out", "big.bin")
	done := make(chan error, 1)
	go func() { done <- n.fetchToPath(c.String(), dest, nil, nil) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("large offline fetch failed: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("large offline fetch did not complete in 30s — the rolling window wedged")
	}
	if got := lastFetchLeaves.Load(); got < 20 {
		t.Fatalf("fixture is degenerate: the transfer needed only %d leaves — the window is not being exercised", got)
	}
	got, err := os.ReadFile(dest)
	if err != nil || !bytes.Equal(got, big) {
		t.Fatalf("fetched content differs (err=%v, len got=%d want=%d)", err, len(got), len(big))
	}
	if after := globalWantPool.available(); after != beforeTokens {
		t.Errorf("want-token pool not balanced after fetch: before=%d after=%d (leak/over-release)", beforeTokens, after)
	}
}

func TestRollingWindowRollsUnderATightBudget(t *testing.T) {
	saved := globalWantPool
	globalWantPool = newWantPool(8) // 8 tokens; the file below is ~32 leaves → the window MUST recycle to finish
	defer func() { globalWantPool = saved }()

	n := offlineNode(t)
	dir := t.TempDir()
	src := filepath.Join(dir, "big.bin")
	big := make([]byte, 8<<20) // 8 MiB → ~32 unique (random) leaves, 4× the 8-token budget
	if _, err := rand.Read(big); err != nil {
		t.Fatal(err)
	}
	writeFile(t, src, big)
	c, err := n.addNoCopy(src)
	if err != nil {
		t.Fatal(err)
	}

	dest := filepath.Join(dir, "out.bin")
	done := make(chan error, 1)
	go func() { done <- n.fetchToPath(c.String(), dest, nil, nil) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("tight-budget fetch failed: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("tight-budget fetch wedged — per-block token release is not recycling the window")
	}
	if got := lastFetchLeaves.Load(); got <= 8 {
		t.Fatalf("fixture leaves (%d) must exceed the 8-token budget for this test to prove recycling", got)
	}
	if got, _ := os.ReadFile(dest); !bytes.Equal(got, big) {
		t.Fatal("tight-budget fetch produced wrong bytes")
	}
	if globalWantPool.available() != 8 {
		t.Errorf("budget not restored after fetch: available=%d want 8", globalWantPool.available())
	}
}

// A bounded (deadline'd) fetch must STOP at its deadline even for content it could otherwise get — the in-loop
// deadline is what keeps a synchronous launch/cover from blocking a user action forever. Using a deadline in the
// past makes it deterministic: the loop must return a deadline error before it even attempts (so no dependence on
// network timing). Teeth: delete the deadline check in fetchToPathLoopUntil and this present-content fetch
// succeeds (returns nil) instead of erroring.
func TestBoundedFetchStopsAtItsDeadline(t *testing.T) {
	n := offlineNode(t)
	dir := t.TempDir()
	src := filepath.Join(dir, "x.bin")
	writeFile(t, src, sampleBytes())
	c, err := n.addNoCopy(src) // content IS present — only the deadline should stop the fetch
	if err != nil {
		t.Fatal(err)
	}
	err = n.fetchToPathLoopUntil(c.String(), filepath.Join(dir, "out.bin"), nil, nil, time.Now().Add(-time.Hour))
	if err == nil {
		t.Fatal("a past deadline must stop the fetch with an error, even for present content")
	}
	if !errors.Is(err, errFetchDeadline) {
		t.Errorf("expected a deadline error, got %v", err)
	}
}
