package main

// fetch.go — materialize a CID's content at a destination path and seed it from there BY REFERENCE.
//
// This is the no-duplication fetch: the DAG is read and written to `dest`, then `dest` is filestore-added so the
// only on-disk copy of the content IS the destination file. M2 validates the mechanics offline (content already in
// the local filestore); M3 swaps the offline exchange for online bitswap so blocks arrive from peers, at which point
// gcUnpinnedLeaves() reclaims the leaf blocks bitswap cached in the blockstore.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	blockservice "github.com/ipfs/boxo/blockservice"
	blockstore "github.com/ipfs/boxo/blockstore"
	dshelp "github.com/ipfs/boxo/datastore/dshelp"
	files "github.com/ipfs/boxo/files"
	filestore "github.com/ipfs/boxo/filestore"
	posinfo "github.com/ipfs/boxo/filestore/posinfo"
	unixfile "github.com/ipfs/boxo/ipld/unixfs/file"
	ufsio "github.com/ipfs/boxo/ipld/unixfs/io"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
)

// (Fetch de-duplication and leadership handoff were removed with the internal retry loop: the C++ DownloadQueue
// now guarantees exactly one active fetch per CID, so there is no same-dest race to singleflight.)

// errNotRawLeaves signals that a DAG isn't all-raw-leaves, so the write-through path can't reference it (filestore
// references require raw leaves) and the caller must fall back to the read + re-add path.
var errNotRawLeaves = errors.New("not all-raw-leaves")

// dbgFetch enables verbose stderr tracing of the fetch path when VG_FETCH_DEBUG is set — used to diagnose why a
// specific CID takes the slow fallback / errors. No-op in normal operation. Every line is wall-clock timestamped
// (HH:MM:SS.mmm) so a "stuck for a minute" gap is visible directly in the log.
var dbgFetch = os.Getenv("VG_FETCH_DEBUG") != ""

// wantBudget is the TOTAL outstanding block-wants this process may have across ALL concurrent fetches. It is a
// hard global cap enforced by a token pool (below), NOT a per-fetch number — so no matter how many downloads run
// at once, the combined wants we send to any single peer stay under the stock-boxo 1024/peer server cap. Kept
// under 1024 with margin. One fetch alone can hold the whole budget (fast); N fetches SHARE it fairly through the
// pool, which is the concrete meaning of "concurrent transfers must not collectively overload one peer".
const wantBudget = 768

// maxPerFetch caps how many tokens ONE fetch may hold, so a single download — especially a trickling one whose
// stragglers hold tokens until its stall watchdog fires — cannot starve every other fetch of the whole budget.
// Half the budget: one fetch alone still gets a 96MiB pipeline (plenty above any BDP), and two fetches always
// both make progress.
var maxPerFetch = wantBudget / 2 // var (not const) so tests can shrink it to prove it binds independently of the pool

// wantPool is a counting semaphore of outstanding-want tokens. A fetch's rolling window acquires a token per
// in-flight want and releases it when the block lands (or when the fetch ends), so len(tokens-in-use) — summed
// across every fetch — never exceeds wantBudget. This is provable by construction, unlike a per-fetch window
// that is fixed at session start (which, with staggered starts, can transiently exceed the cap).
type wantPool struct{ tokens chan struct{} }

func newWantPool(n int) *wantPool {
	p := &wantPool{tokens: make(chan struct{}, n)}
	for i := 0; i < n; i++ {
		p.tokens <- struct{}{}
	}
	return p
}

// acquire blocks for one token, returning false if ctx is cancelled first (teardown). tryAcquire never blocks.
func (p *wantPool) acquire(ctx context.Context) bool {
	select {
	case <-p.tokens:
		return true
	case <-ctx.Done():
		return false
	}
}
func (p *wantPool) tryAcquire() bool {
	select {
	case <-p.tokens:
		return true
	default:
		return false
	}
}
func (p *wantPool) release() {
	select {
	case p.tokens <- struct{}{}:
	default: // pool already full — never happens if acquire/release are balanced; guards against a double release
	}
}
func (p *wantPool) available() int { return len(p.tokens) }

var globalWantPool = newWantPool(wantBudget)

// lastFetchLeaves records the unique-leaf count of the most recent writeThrough transfer phase. Test-only
// observability: it lets a test assert the rolling window actually had many leaves to roll through (guarding
// against a degenerate fixture whose chunks all dedup to one block, which would make the window a no-op).
var lastFetchLeaves atomic.Int64

func fdbg(format string, a ...interface{}) {
	if dbgFetch {
		fmt.Fprintf(os.Stderr, "[fetchdbg %s] "+format+"\n", append([]interface{}{time.Now().Format("15:04:05.000")}, a...)...)
	}
}

// getRoot fetches a CID's root block over the network with a bounded timeout, and — if libp2p can't reach any provider
// in time (a hostile/captive network where the DHT finds providers but no transport connects, e.g. Pinata's ws:3000
// mangled by a proxy) — falls back to importing the whole DAG from an HTTPS trustless gateway (gateway.go) and reads
// the root from the now-local blockstore. On a normal network the root arrives fast and the gateway is never touched.
var rootLibp2pTimeout = 30 * time.Second // how long the libp2p/bitswap root fetch gets BEFORE the gateway fallback (var: tests shrink it)

// rootNoPeersGrace: how long phase 1 tolerates the node having NO connected peers at all before handing off to the
// gateway. Zero peers means no bitswap fetch can ever complete, so waiting the full rootLibp2pTimeout there is pure
// waste (the isolation matrix measured 30 of a 72 s gateway-only fetch spent exactly so). The grace covers the
// first seconds after startup while bootstrap connects; a bootstrapped node always has peers. (var: tests shrink it)
var rootNoPeersGrace = 5 * time.Second

func (n *node) getRoot(nctx context.Context, c cid.Cid, cidStr string, onProgress func(pct float64)) (ipld.Node, error) {
	// A single user-cancel poller for the whole call: it cancels whichever phase is running. Both phase contexts are
	// children of nctx, so a bounded caller's deadline (or node shutdown) still bounds the total.
	// A BOUNDED caller (a cover's 30 s, a launch's per-layer budget) splits its budget between the two phases: libp2p
	// gets at most half of what is left, so the gateway always gets a real share. Otherwise the libp2p phase alone
	// ate the whole budget on any network where the DHT/indexers turn up nothing (Pinata-only content), the deadline
	// fired the instant the gateway would have started, and covers "did not complete before its deadline" while the
	// unbounded game download beside them succeeded through that very gateway.
	libTimeout := rootLibp2pTimeout
	if dl, ok := nctx.Deadline(); ok {
		if half := time.Until(dl) / 2; half < libTimeout {
			libTimeout = half
		}
	}
	libCtx, libCancel := context.WithTimeout(nctx, libTimeout)
	defer libCancel()
	gwCtx, gwCancel := context.WithCancel(nctx)
	defer gwCancel()
	stopPoll := make(chan struct{})
	safeGo("fetch.getRootCancel", func() {
		t := time.NewTicker(250 * time.Millisecond)
		defer t.Stop()
		var zeroPeersSince time.Time
		for {
			select {
			case <-stopPoll:
				return
			case <-nctx.Done():
				return
			case <-t.C:
				if isCancelled(cidStr) {
					libCancel()
					gwCancel()
					return
				}
				// Phase-1 short-circuit (see rootNoPeersGrace): no connected peers for the grace period → end the
				// libp2p attempt now so the gateway runs; libCancel is idempotent and never touches phase 2.
				if n.host != nil && len(n.host.Network().Peers()) == 0 {
					if zeroPeersSince.IsZero() {
						zeroPeersSince = time.Now()
					} else if time.Since(zeroPeersSince) >= rootNoPeersGrace {
						libCancel()
					}
				} else {
					zeroPeersSince = time.Time{}
				}
			}
		}
	})
	defer close(stopPoll)

	// Phase 1 — libp2p/bitswap, bounded by ITS OWN timeout (a fair chance, NOT the whole budget).
	phase(cidStr, "locating providers (DHT + indexers)")
	root, err := n.dserv.Get(libCtx, c)
	if err == nil {
		return root, nil
	}
	if isMissingFile(err) {
		return nil, errMissingFiles
	}
	if n.dht == nil {
		return nil, err // offline (unit tests / pre-goOnline): no DHT means no network — do not touch real gateways
	}

	// Phase 2 — HTTPS trustless-gateway fallback for hostile/captive networks (DHT finds junk/loopback provider
	// addrs, no transport connects). CRITICAL: it runs under gwCtx, a FRESH context derived from nctx — NOT libCtx,
	// which phase 1 just exhausted. Sharing the spent libp2p deadline made every gateway request fail "context
	// deadline exceeded" the instant it started, so on exactly the networks the fallback exists for it never ran —
	// the field "will sync when online while the node is up" bug. The gateway has no fixed cap of its own (a big CAR
	// streams for minutes); its stall watchdog + nctx bound it.
	fdbg("getRoot: libp2p root fetch failed (%v) → HTTPS trustless-gateway fallback cid=%s", err, cidStr)
	phase(cidStr, "no p2p source answered — trying HTTPS gateways")
	gerr := n.fetchViaGateway(gwCtx, c, -1, func(read, total int64) {
		if onProgress != nil && total > 0 {
			onProgress(math.Min(99, 100.0*float64(read)/float64(total)))
		}
	})
	if gerr != nil {
		fdbg("getRoot: gateway fallback failed cid=%s: %v", cidStr, gerr)
		return nil, err // surface the original network error
	}
	return n.dserv.Get(nctx, c) // DAG now in the local blockstore
}

// userCancelCtx derives a context cancelled when the user cancels cidStr (250 ms poll), for a phase that has no
// session poller of its own. Call the returned stop when the phase ends.
func userCancelCtx(parent context.Context, cidStr string) (context.Context, func()) {
	ctx, cancel := context.WithCancel(parent)
	stop := make(chan struct{})
	safeGo("fetch.userCancel", func() {
		t := time.NewTicker(250 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				if isCancelled(cidStr) {
					cancel()
					return
				}
			}
		}
	})
	return ctx, func() { close(stop); cancel() }
}

// monotoneProgress wraps a progress callback so ONE attempt's bar never runs backwards. An attempt is one piece of
// work measured by two rulers in turn: the gateway phase reports CAR bytes over the wire, then the materialize phase
// reports file bytes on disk starting from what the .part bitmap says (nothing, right after a CAR import — every leaf
// is local and is written through in a blink). Unwrapped, the user saw the bar hit 100%, then a second, quick bar
// from 0 before "pinning". Held at its high-water mark, it is one bar. A RETRY gets a fresh wrapper: a resume really
// does start where the bitmap says.
func monotoneProgress(f func(pct float64)) func(pct float64) {
	if f == nil {
		return nil
	}
	var mu sync.Mutex
	hi := -1.0
	return func(pct float64) {
		mu.Lock()
		if pct < hi {
			pct = hi
		} else {
			hi = pct
		}
		mu.Unlock()
		f(pct)
	}
}

// shortCid trims a CID for readable logs (first 6 + last 4 of the base58/base32 string).
func shortCid(c cid.Cid) string {
	s := c.String()
	if len(s) <= 12 {
		return s
	}
	return s[:6] + ".." + s[len(s)-4:]
}

// stallTimeout: if no block arrives for this long the fetch session is torn down so the wrapper can back off and RESUME
// from the on-disk bitmap (a dead peer shouldn't hang the download). Longer than the UI's 6s "Stalled" hint.
var stallTimeout = 20 * time.Second // (var: tests shrink it)

// fetchWindow bounds how many leaves are requested from bitswap at once — and thus how many received blocks are buffered
// (in the channel + the plain blockstore) before they're written to disk and dropped. It caps a fetch's resident memory
// at ~fetchWindow × chunkSize (512 × 256 KiB = 128 MiB) REGARDLESS of file size, so a multi-GB runner (e.g. GE-Proton)
// can't grow the blockstore to the whole file and OOM-thrash a small-RAM machine. Below this, memory scaled with size.
const fetchWindow = 512

// zeroChunk backs refLeaf.RawData() — FileManager.Put only reads len(RawData()), never the bytes, so a shared
// read-only buffer of the max chunk size avoids allocating per leaf.
var zeroChunk = make([]byte, chunkSize)

// refLeaf is a minimal ipld.Node carrying just a CID + a byte length. It lets us create a filestore reference for an
// already-downloaded leaf WITHOUT holding or re-reading its data (FileManager.Put needs only Cid() + len(RawData())).
type refLeaf struct {
	c    cid.Cid
	size int
}

func (r *refLeaf) Cid() cid.Cid { return r.c }
func (r *refLeaf) RawData() []byte {
	if r.size <= len(zeroChunk) {
		return zeroChunk[:r.size]
	}
	return make([]byte, r.size)
}
func (r *refLeaf) String() string                                     { return r.c.String() }
func (r *refLeaf) Loggable() map[string]interface{}                   { return nil }
func (r *refLeaf) Resolve([]string) (interface{}, []string, error)    { return nil, nil, nil }
func (r *refLeaf) Tree(string, int) []string                          { return nil }
func (r *refLeaf) ResolveLink([]string) (*ipld.Link, []string, error) { return nil, nil, nil }
func (r *refLeaf) Copy() ipld.Node                                    { return r }
func (r *refLeaf) Links() []*ipld.Link                                { return nil }
func (r *refLeaf) Stat() (*ipld.NodeStat, error)                      { return &ipld.NodeStat{}, nil }
func (r *refLeaf) Size() (uint64, error)                              { return uint64(r.size), nil }

// errMissingFiles is returned when content the node believes it has (a filestore reference) can't be read because
// the backing file was deleted. Surfaced to the UI as "Errored: missing files" rather than a cryptic open() error.
var errMissingFiles = errors.New("missing files")

// errStr is err.Error() but nil-safe, for classifying an attempt outcome without a panic on a nil error.
func errStr(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// errLocalFatal marks an error that retrying can NEVER fix — a malformed CID, a disk error (ENOSPC/EROFS/EACCES),
// a datastore/pin failure. The retry loop treats it as terminal (and logs it loudly) instead of spinning forever;
// the network-transient errors are the only ones that retry. localFatal wraps a concrete error as this class.
var errLocalFatal = errors.New("local fatal")

func localFatal(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %v", errLocalFatal, err)
}

func isMissingFile(err error) bool {
	if err == nil {
		return false
	}
	var cre *filestore.CorruptReferenceError
	if errors.As(err, &cre) {
		return true
	}
	return strings.Contains(err.Error(), "no such file")
}

var (
	cancelMu  sync.Mutex
	cancelSet = map[string]bool{}
)

func requestCancel(c string) { cancelMu.Lock(); cancelSet[c] = true; cancelMu.Unlock() }
func clearCancel(c string)   { cancelMu.Lock(); delete(cancelSet, c); cancelMu.Unlock() }
func isCancelled(c string) bool {
	cancelMu.Lock()
	defer cancelMu.Unlock()
	return cancelSet[c]
}

// fetchToPath retrieves cidStr's file content to dest in ONE attempt (getRoot → writeThrough, with the 20 s stall
// watchdog and the HTTPS gateway leaf-resume inside). It does NOT loop: retry/backoff and stall-demotion now live in
// the C++ DownloadQueue's rolling dispatcher, which re-dispatches a Retryable outcome. Kept as a thin convenience for
// the node's own tests; VgFetchOnce is the production entry point. A stale LOCAL reference (errMissingFiles) is
// cleared here so the re-dispatch re-fetches over the network.
// destGuard serializes concurrent fetches to the SAME dest. The C++ DownloadQueue already guarantees one active
// fetch per CID (dest ↔ CID is 1:1), so this is a cheap primitive-level backstop — not the old leadership-handoff —
// against a silent dest.tmp/rename race if two callers ever collide. Refcounted so the map is bounded by live dests.
type destGuard struct {
	mu   sync.Mutex
	refs int
}

var (
	destGuardsMu sync.Mutex
	destGuards   = map[string]*destGuard{}
)

func lockDest(dest string) func() {
	destGuardsMu.Lock()
	g := destGuards[dest]
	if g == nil {
		g = &destGuard{}
		destGuards[dest] = g
	}
	g.refs++
	destGuardsMu.Unlock()
	g.mu.Lock()
	return func() {
		g.mu.Unlock()
		destGuardsMu.Lock()
		if g.refs--; g.refs == 0 {
			delete(destGuards, dest)
		}
		destGuardsMu.Unlock()
	}
}

func (n *node) fetchToPath(cidStr, dest string, onProgress, onFinalize func(pct float64)) error {
	unlock := lockDest(dest)
	defer unlock()
	err := n.fetchToPathOnce(n.ctx, cidStr, dest, onProgress, onFinalize)
	if err == errMissingFiles {
		if c, derr := cid.Decode(cidStr); derr == nil {
			n.dropRef(c) // clear the stale ref (+ cached blocks) so the next dispatch fetches over the network
		}
	}
	return err
}

// fetchOutcome classes, mirrored on the C++ side (downloadqueue.cpp RunJob): the rolling dispatcher reads these to
// decide Done / rotate-and-retry / fail.
const (
	fetchDone      = 0 // complete + seeded
	fetchRetryable = 1 // stalled, provider-exhausted, offline, or a cleared stale ref — re-dispatch later
	fetchTerminal  = 2 // cancelled, malformed CID, or a local/disk error retrying can never fix
)

// classifyFetchErr maps one attempt's error to a fetchOutcome. hard-stall / network / offline are Retryable (the
// queue backs off and rotates — retry-forever without a held slot). Only two things are TERMINAL: a USER cancel
// (isCancelled — the request surfaces as "cancelled", or as context.Canceled when it lands inside getRoot) and an
// errLocalFatal (bad CID, ENOSPC, datastore). getRoot's OWN internal libCtx timeout ALSO surfaces as context.Canceled
// but is a transient give-up → Retryable; classifying it terminal made every gated fetch die after one attempt.
func classifyFetchErr(cidStr string, err error) int {
	switch {
	case err == nil:
		return fetchDone
	case errors.Is(err, errLocalFatal):
		return fetchTerminal
	case isCancelled(cidStr):
		return fetchTerminal
	default:
		return fetchRetryable
	}
}

func (n *node) fetchDirToPath(cidStr, dest string, onProgress func(pct float64), onFinalize func(pct float64)) error {
	return n.fetchDirOnce(cidStr, dest, onProgress, onFinalize) // ONE attempt; the C++ rolling queue re-dispatches
}

// fetchDirOnce is one attempt: root fetch (bounded, with gateway fallback), then a stall-guarded tree materialize.
// The fetched tree is intentionally SMALL (node JSON + cover images, no content bytes — the folder is dehydrated);
// each package's large per-layer content is hydrated later, on demand, by fetchToPath. Writes to a temp dir then
// renames into place so a partial/cancelled fetch never leaves a half dir at dest.
// onProgress is called with 0..100 while the tree streams in (indeterminate until the first tick); onFinalize(-1)
// fires once the bytes are down and the pin/reference step runs. Both may be nil (e.g. the offline test path).
func (n *node) fetchDirOnce(cidStr, dest string, onProgress func(pct float64), onFinalize func(pct float64)) error {
	c, err := cid.Decode(cidStr)
	if err != nil {
		return err
	}
	onProgress = monotoneProgress(onProgress)
	// A meta/collection CID is a DIRECTORY, and fetching one on a hostile network needs the same proactive provider
	// warming that single-file fetches get (warm.go): a live DHT provider walk + connect in parallel with bitswap, so a
	// NAT'd provider is holepunched before getRoot's deadline instead of relying on bitswap's slower passive connect.
	n.warmProviders(c)
	// Directory (meta) fetches run under the sync worker with a bounded attempt count, not a wall-clock deadline, so
	// the attempt context is just n.ctx — getRoot still caps each root fetch at 30s.
	root, err := n.getRoot(n.ctx, c, cidStr, onProgress) // libp2p, or an HTTPS trustless-gateway CAR fallback on hostile nets
	if err != nil {
		if err == errMissingFiles || isMissingFile(err) {
			return errMissingFiles
		}
		return err
	}
	// Per-attempt context: the stall watchdog cancels it to abort a tree stream that has gone quiet, so a dead peer
	// mid-materialize surfaces as a retryable error instead of hanging the sync forever (observed: "stuck at 36%").
	actx, acancel := context.WithCancel(n.ctx)
	defer acancel()
	fnode, err := unixfile.NewUnixfsFile(actx, n.dserv, root)
	if err != nil {
		return err
	}
	defer fnode.Close()

	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}
	tmp := dest + ".tmp"
	_ = os.RemoveAll(tmp)

	// One poller drives both progress reporting (bytes-on-disk vs the folder's cumulative size) and the stall
	// watchdog (no growth for stallTimeout → cancel the attempt). Growth-based, so a slow-but-moving stream is
	// never killed; the root-fetch wait is already bounded by getRoot.
	var total int64
	if sz, serr := fnode.Size(); serr == nil {
		total = sz
	}
	stop := make(chan struct{})
	var stalled atomic.Bool
	safeGo("fetch.progress", func() {
		t := time.NewTicker(250 * time.Millisecond)
		defer t.Stop()
		last, lastGrow := int64(-1), time.Now()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				sz := dirSize(tmp)
				if sz != last {
					last, lastGrow = sz, time.Now()
				} else if time.Since(lastGrow) > stallTimeout {
					stalled.Store(true)
					acancel()
					return
				}
				if onProgress != nil && total > 0 {
					pct := 100.0 * float64(sz) / float64(total)
					if pct > 99 {
						pct = 99 // leave the last 1% for the pin/finalize step
					}
					onProgress(pct)
				}
			}
		}
	})
	werr := files.WriteTo(fnode, tmp)
	close(stop)
	if werr != nil {
		_ = os.RemoveAll(tmp)
		if stalled.Load() {
			return fmt.Errorf("stalled mid-transfer: %w", werr)
		}
		return werr
	}
	if err := os.RemoveAll(dest); err != nil {
		// dest cannot be replaced (EACCES and kin). Swallowing this made the NEXT line fail "file exists" forever —
		// the real cause was the one value discarded. Terminal: re-fetching cannot fix a permission problem.
		_ = os.RemoveAll(tmp)
		return localFatal(err)
	}
	if err := os.Rename(tmp, dest); err != nil {
		_ = os.RemoveAll(tmp) // don't leave the materialized tree behind on the failure path
		return localFatal(err)
	}
	// Pin the folder root recursively so the fetched source tree is SEEDED (reprovided to the DHT) and shows in VgPinLs
	// — mirrors addDirNoCopy. Best-effort: the files are already on disk, so a pin hiccup must not fail the fetch.
	if onFinalize != nil {
		onFinalize(-1)
	}
	if err := n.pinner.Pin(n.ctx, root, true, dest); err != nil {
		fmt.Fprintf(os.Stderr, "[fetchDir] pin %s failed: %v\n", cidStr, err)
	} else if err := n.pinner.Flush(n.ctx); err != nil {
		fmt.Fprintf(os.Stderr, "[fetchDir] pin flush %s failed: %v\n", cidStr, err)
	}
	n.announce(c) // publish now so a re-hosted source tree is immediately discoverable (not after the 22h reprovide)
	return nil
}

// onProgress is called with 0..100 during the transfer; onFinalize is called once the bytes are all down and the
// (slower) re-reference/"pinning" step runs — with 0..100 progress through that step on the write-through path, or -1
// (indeterminate) on the fallback re-add path (so the UI can show "Pinning… 42%" instead of looking stuck at 100%).
func (n *node) fetchToPathOnce(nctx context.Context, cidStr, dest string, onProgress func(pct float64), onFinalize func(pct float64)) error {
	if isCancelled(cidStr) {
		return errors.New("cancelled")
	}
	onProgress = monotoneProgress(onProgress)
	c, err := cid.Decode(cidStr)
	if err != nil {
		return localFatal(err) // malformed CID — no retry can fix it
	}
	// Proactively freshen provider addresses on EVERY attempt (moved here from the deleted retry loop): a live DHT
	// walk + connect in parallel with bitswap, so a provider that restarted (new ports) or a NAT'd seeder is
	// re-discovered/holepunched instead of the C++ rotation re-dialing a stale peerstore forever. All no-op offline.
	n.warmProviders(c)
	n.warmSeedLevelProviders() // fresh content may have no DHT record yet — whoever seeds our sources has it
	n.warmFriends()            // a friend is a guaranteed provider the DHT never surfaces
	if _, err := os.Stat(dest); err == nil {
		// dest already on disk. Two cases:
		//  * no .part sidecar → finalize completed fully (referenced + pinned + announced) → fast no-op.
		//  * .part still present → the fetch crashed AFTER the tmp→dest rename but BEFORE finalize finished
		//    (referencing/pinning). The bytes are correct and complete, but the content is unreferenced (can't be
		//    served) and unpinned (GC-vulnerable). addNoCopy is the idempotent finalize: it references the leaves in
		//    place from the existing file and (re)pins+announces, converging any crash landing to a seedable state.
		if !partExists(dest) {
			fdbg("fetchToPathOnce: dest present, no .part → finalized → no-op cid=%s", cidStr)
			return nil
		}
		fdbg("fetchToPathOnce: dest present WITH .part → finalize was interrupted, repairing cid=%s", cidStr)
		// VERIFY the on-disk bytes hash to the requested CID BEFORE referencing or pinning anything. computeCid has
		// NO side effects (throwaway in-memory store); addNoCopy would pin+announce FIRST, so on a content mismatch
		// it would leave a pinned, announced provider record for content whose only backing file we then delete —
		// an unserveable orphan. Hash first, commit second.
		got, cerr := computeCid(dest)
		switch {
		case cerr != nil:
			// The bytes on disk are unreadable/corrupt — discard and re-fetch clean (a fresh fetch fixes it).
			fdbg("fetchToPathOnce: dest unreadable for repair (%v) → discard + re-fetch cid=%s", cerr, cidStr)
			_ = os.Remove(dest)
			removePartial(dest)
		case got == c:
			// Content is correct and complete → finish the finalize idempotently (reference in place + pin).
			if _, aerr := n.addNoCopy(dest); aerr != nil {
				return localFatal(aerr) // referencing/pinning a present, correct file failed = local/datastore error
			}
			removePartial(dest)
			fdbg("fetchToPathOnce: repair OK cid=%s", cidStr)
			return nil
		default:
			// dest holds DIFFERENT content than requested — discard WITHOUT ever pinning it, then re-fetch.
			fdbg("fetchToPathOnce: dest content %s != requested %s → discard (unpinned) + re-fetch", got, cidStr)
			_ = os.Remove(dest)
			removePartial(dest)
		}
	}
	// Orphaned reference: the node "has" this CID via a filestore reference, but the backing file was deleted.
	// Surface it cleanly as "missing files" (→ "Errored: missing files" in the UI) instead of reading the gone
	// file. (cidMissing is local-only; for content we don't have it returns false, so normal fetches proceed.)
	if n.cidMissing(c) {
		fdbg("fetchToPathOnce: cidMissing=true (orphaned local ref) → errMissingFiles cid=%s", cidStr)
		return errMissingFiles
	}
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return localFatal(err)
	}

	fdbg("fetchToPathOnce: fetching root block cid=%s", cidStr)
	getStart := time.Now()
	root, err := n.getRoot(nctx, c, cidStr, onProgress)
	if err != nil {
		fdbg("fetchToPathOnce: root Get FAILED cid=%s err=%v (isMissingFile=%v)", cidStr, err, isMissingFile(err))
		if err == errMissingFiles || isMissingFile(err) {
			return errMissingFiles // node has a filestore ref but the backing file is gone
		}
		return err
	}
	fdbg("fetchToPathOnce: got root block codec=%d in %s cid=%s", root.Cid().Prefix().Codec, time.Since(getStart).Round(time.Millisecond), cidStr)
	fdbg("fetchToPathOnce: %s", n.connsDump()) // RELAYED vs DIRECT to the seeder — the throughput ceiling

	// Fast path: stream the fetched leaf blocks straight to dest AND reference them in place — no re-chunk/re-hash
	// (the old "stuck at 100%" delay). Falls back to read + re-add for DAGs that aren't all raw leaves.
	wtErr := n.writeThrough(nctx, c, root, dest, cidStr, onProgress, onFinalize)
	fdbg("writeThrough(%s) -> %v", cidStr, wtErr)
	if err := wtErr; err == nil {
		n.scheduleCompaction() // reclaim tombstone disk from the leaves we dropped from the blockstore
		return nil
	} else if err == errIncomplete {
		return errIncomplete // stalled/dropped mid-transfer — partial kept; the wrapper backs off + resumes
	} else if err != errNotRawLeaves {
		if isMissingFile(err) {
			fdbg("writeThrough(%s) classified as MISSING FILES (isMissingFile=true): %v", cidStr, err)
			return errMissingFiles
		}
		return err // cancelled / network / io
	}
	fdbg("writeThrough(%s) fell through to SLOW read+addNoCopy fallback", cidStr)

	rdr, err := ufsio.NewDagReader(nctx, root, n.dserv)
	if err != nil {
		return err
	}
	total := int64(rdr.Size())
	fdbg("fallback: DagReader size=%d cid=%s — reading whole DAG over the network", total, cidStr)

	tmp := dest + ".tmp"
	_ = os.RemoveAll(tmp)
	out, err := os.Create(tmp)
	if err != nil {
		return localFatal(err)
	}

	buf := make([]byte, 1<<20)
	var written int64
	nextMark := int64(10)
	for {
		if isCancelled(cidStr) {
			_ = out.Close()
			_ = os.Remove(tmp)
			return errors.New("cancelled")
		}
		nr, rerr := rdr.Read(buf)
		if nr > 0 {
			if _, werr := out.Write(buf[:nr]); werr != nil {
				_ = out.Close()
				_ = os.Remove(tmp)
				return localFatal(werr)
			}
			written += int64(nr)
			if total > 0 && onProgress != nil {
				pct := 100.0 * float64(written) / float64(total)
				onProgress(pct)
				if int64(pct) >= nextMark { // log every ~10% so a stall inside the read is visible
					fdbg("fallback: read %d%% (%d/%d) cid=%s", int64(pct), written, total, cidStr)
					nextMark = int64(pct) + 10
				}
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			_ = out.Close()
			_ = os.Remove(tmp)
			fdbg("fallback: read error cid=%s err=%v (isMissingFile=%v)", cidStr, rerr, isMissingFile(rerr))
			if isMissingFile(rerr) {
				return errMissingFiles
			}
			return rerr
		}
	}
	if err := out.Close(); err != nil {
		return localFatal(err)
	}
	fdbg("fallback: read complete (%d bytes) → rename + addNoCopy re-hash cid=%s", written, cidStr)

	// Publish atomically, then seed from the destination by reference (filestore) so dest IS the seed source.
	_ = os.RemoveAll(dest)
	if err := os.Rename(tmp, dest); err != nil {
		return localFatal(err)
	}

	// Online bitswap cached the fetched blocks in the plain blockstore. Drop that whole closure first: Filestore.Put
	// skips any block that already exists, so without clearing them addNoCopy would NOT create the filestore
	// references and the content would stay duplicated in the blockstore. After dropping, addNoCopy re-chunks dest
	// from disk and stores the leaves as references into it — leaving the destination file as the only on-disk copy.
	// All bytes are down; the remaining re-chunk/re-reference step ("pinning") can take a while for large files.
	// addNoCopy re-hashes the whole file opaquely, so we can't report granular progress here — signal indeterminate.
	if onFinalize != nil {
		onFinalize(-1)
	}
	fdbg("fallback: dropClosure + addNoCopy re-hash START cid=%s", cidStr)
	addStart := time.Now()
	n.dropClosure(c)
	if _, err := n.addNoCopy(dest); err != nil {
		fdbg("fallback: addNoCopy FAILED cid=%s err=%v", cidStr, err)
		return localFatal(err) // referencing/pinning a present file failed — local/datastore, terminal
	}
	fdbg("fallback: addNoCopy DONE in %s cid=%s", time.Since(addStart).Round(time.Millisecond), cidStr)
	n.scheduleCompaction() // reclaim tombstone disk from the dropped bitswap-cached blocks
	return nil
}

// writeThrough materializes a CID's file at dest by streaming each fetched leaf block straight to disk in DAG order,
// then recording each leaf as a filestore reference into dest — WITHOUT re-chunking or re-hashing the file (which is
// the slow "pinning" step the old path did via addNoCopy). Only the small dag-pb root/intermediate nodes remain as
// plain blocks. Returns errNotRawLeaves if the DAG isn't all raw leaves (filestore refs require raw leaves; the
// caller then falls back to read + re-add).
// rollingGetBlocks streams every cid in `need` from a bitswap session as a ROLLING, bounded want-window and
// delivers the blocks on the returned channel (arrival order). It is the fix for the head-of-line wedge: instead
// of chunk slots that free only when a whole chunk arrives, it keeps a bounded number of wants outstanding and
// admits a new one for every block that lands, so one straggler never stops the rest.
//
// The bound is the GLOBAL want-token pool (shared across all concurrent fetches, so their combined outstanding
// wants stay under the stock-boxo 1024/peer server cap) plus a per-fetch cap (so one download can't monopolise
// the budget). Token lifecycle is entirely LOCAL to each batch's fan goroutine — one token per requested key,
// released when its block arrives OR when its GetBlocks channel closes without it (a straggler / ctx-cancel).
// So tokens are always returned exactly, with no separate exit-drain and no cross-goroutine race. The channel
// closes when every block has been delivered or ctx is cancelled (the caller's stall watchdog / user-cancel).
func rollingGetBlocks(ctx context.Context, sess *blockservice.Session, need []cid.Cid, refillBatch int) <-chan blocks.Block {
	out := make(chan blocks.Block, 64)
	safeGo("fetch.producer", func() {
		defer close(out)
		var held atomic.Int64 // want-tokens this fetch holds right now (producer +1 on acquire, fan -1 on release)
		var fans sync.WaitGroup
		// fan drains ONE GetBlocks batch channel, releasing exactly `batch` tokens (one per delivered block, the
		// rest for keys the batch never delivered) so the pool is always balanced regardless of how it ends.
		fan := func(ch <-chan blocks.Block, batch int) {
			defer fans.Done()
			// Release EXACTLY `batch` tokens no matter how this fan exits (all delivered, batch channel closed
			// early on ctx-cancel, or blocked-on-send at cancel). A single deferred accounting guarantees it, so
			// there is no exit path that can leak or double-release a token — the property the whole global budget
			// depends on. released counts what we've already returned; the defer returns the remainder.
			released := 0
			relOne := func() {
				released++
				held.Add(-1)
				globalWantPool.release()
			}
			defer func() {
				for released < batch {
					relOne()
				}
			}()
			for b := range ch {
				relOne() // this block's token is done the moment it arrives
				select {
				case out <- b:
				case <-ctx.Done():
					return // the defer returns the rest of the batch's tokens
				}
			}
		}
		requested := 0
		for requested < len(need) {
			if ctx.Err() != nil {
				break
			}
			// Per-fetch cap: if we already hold maxPerFetch, wait for our own blocks to land (fans release tokens)
			// rather than taking more of the shared budget. Cheap poll; a fan freeing room unblocks us.
			if held.Load() >= int64(maxPerFetch) {
				select {
				case <-time.After(50 * time.Millisecond):
				case <-ctx.Done():
				}
				continue
			}
			// One blocking acquire (real backpressure — wait until the global budget has room), then top up without
			// blocking to refillBatch, the per-fetch cap, or a momentarily-empty pool.
			if !globalWantPool.acquire(ctx) {
				break
			}
			held.Add(1)
			got := 1
			for got < refillBatch && requested+got < len(need) && held.Load() < int64(maxPerFetch) && globalWantPool.tryAcquire() {
				held.Add(1)
				got++
			}
			end := requested + got
			ch := sess.GetBlocks(ctx, need[requested:end])
			batch := got
			fans.Add(1)
			safeGo("fetch.blockConsumer", func() { fan(ch, batch) })
			requested = end
		}
		fans.Wait()
	})
	return out
}

func (n *node) writeThrough(nctx context.Context, root cid.Cid, rootNode ipld.Node, dest, cidStr string,
	onProgress func(pct float64), onFinalize func(pct float64)) error {
	fdbg("writeThrough ENTER cid=%s dest=%s", cidStr, dest)
	// File size (for progress + preallocation) — cheap, reads the root's UnixFS metadata.
	rdr, err := ufsio.NewDagReader(nctx, rootNode, n.dserv)
	if err != nil {
		return err
	}
	total := int64(rdr.Size())

	// Enumerate the file's leaves IN ORDER from the dag-pb spine BEFORE touching any tmp (so errNotRawLeaves is a clean
	// fall-through). A raw-codec link is a leaf — CID/size/offset come from the link (no fetch); only the few
	// intermediate dag-pb nodes are read. A dag-pb node with no links = a dag-pb leaf → errNotRawLeaves. A chunk can
	// repeat (dedup), so a CID maps to every offset AND every leaf-INDEX it occupies (leaf-index drives the resume bitmap).
	type leafRef struct {
		c   cid.Cid
		off uint64
		sz  int
	}
	var leaves []leafRef
	offsets := map[cid.Cid][]int64{}
	idxOf := map[cid.Cid][]int{}
	var uniq []cid.Cid
	uniqSeen := cid.NewSet()
	var off int64
	addLeaf := func(c cid.Cid, sz int) {
		idxOf[c] = append(idxOf[c], len(leaves))
		leaves = append(leaves, leafRef{c, uint64(off), sz})
		offsets[c] = append(offsets[c], off)
		off += int64(sz)
		if uniqSeen.Visit(c) {
			uniq = append(uniq, c)
		}
	}
	if root.Prefix().Codec == cid.Raw {
		addLeaf(root, len(rootNode.RawData())) // single-block file: the root IS the only leaf
	} else {
		seenN := cid.NewSet()
		var spine func(nd ipld.Node) error
		spine = func(nd ipld.Node) error {
			if len(nd.Links()) == 0 {
				fdbg("spine: dag-pb node %s has NO links -> errNotRawLeaves", nd.Cid())
				return errNotRawLeaves
			}
			for _, l := range nd.Links() {
				if l.Cid.Prefix().Codec == cid.Raw {
					addLeaf(l.Cid, int(l.Size))
				} else if seenN.Visit(l.Cid) {
					child, gerr := n.dserv.Get(nctx, l.Cid)
					if gerr != nil {
						return gerr
					}
					if serr := spine(child); serr != nil {
						return serr
					}
				}
			}
			return nil
		}
		if serr := spine(rootNode); serr != nil {
			return serr
		}
	}
	fdbg("enumerated %s: total(unixfs)=%d off(sum-leaf-sizes)=%d leaves=%d uniq=%d match=%v", root, total, off, len(leaves), len(uniq), total == off)

	// Open the partial: RESUME if a matching dest.tmp + dest.part exist, else start fresh (preallocated + empty bitmap).
	tmp := tmpPath(dest)
	var out *os.File
	var bits *partBits
	if pb, ok := loadPart(dest, root.String(), total, len(leaves)); ok {
		if f, oerr := os.OpenFile(tmp, os.O_RDWR, 0o644); oerr == nil {
			out, bits = f, pb // resume where the last attempt left off
			fdbg("writeThrough RESUME from partial: %d/%d leaves already on disk cid=%s", pb.nset, pb.count, cidStr)
		} else {
			fdbg("writeThrough: .part loaded but tmp open FAILED (%v) → starting fresh cid=%s", oerr, cidStr)
		}
	}
	if out == nil {
		_ = os.RemoveAll(tmp)
		f, cerr := os.Create(tmp)
		if cerr != nil {
			return localFatal(cerr)
		}
		if terr := f.Truncate(off); terr != nil { // preallocate so out-of-order WriteAt lands correctly
			_ = f.Close()
			_ = os.Remove(tmp)
			return localFatal(terr)
		}
		out, bits = f, newPartBits(root.String(), total, len(leaves))
		_ = savePart(dest, bits)
	}

	// Bytes already on disk from prior attempts → resume the progress bar from there, not 0.
	var written int64
	for i, lf := range leaves {
		if bits.get(i) {
			written += int64(lf.sz)
		}
	}
	if total > 0 && onProgress != nil {
		onProgress(math.Min(99, 100.0*float64(written)/float64(total)))
	}

	// Only re-request the leaves not yet durable on disk.
	var need []cid.Cid
	for _, c := range uniq {
		for _, i := range idxOf[c] {
			if !bits.get(i) {
				need = append(need, c)
				break
			}
		}
	}
	fdbg("writeThrough: %d/%d unique leaves still needed (written so far=%d/%d) cid=%s", len(need), len(uniq), written, total, cidStr)

	if len(need) > 0 {
		// Cancel + STALL watcher: cancel the fetch on user-cancel, OR when no block has arrived for stallTimeout (a dead
		// peer), so the wrapper can back off + resume from the bitmap instead of hanging.
		fctx, fcancel := context.WithCancel(nctx)
		defer fcancel()
		var lastBlk atomic.Int64
		lastBlk.Store(time.Now().UnixNano())
		var stalled atomic.Bool
		safeGo("fetch.worker", func() {
			t := time.NewTicker(200 * time.Millisecond)
			defer t.Stop()
			for {
				select {
				case <-fctx.Done():
					return
				case <-t.C:
					if isCancelled(cidStr) {
						fdbg("writeThrough: user-cancel detected → tearing down session cid=%s", cidStr)
						fcancel()
						return
					}
					if time.Since(time.Unix(0, lastBlk.Load())) > stallTimeout {
						stalled.Store(true)
						fdbg("writeThrough: STALL — no block for >%s → tearing down session cid=%s", stallTimeout, cidStr)
						phase(cidStr, fmt.Sprintf("stalled — no data for %s", stallTimeout))
						fcancel()
						return
					}
				}
			}
		})

		// One long-lived bitswap SESSION with a SINGLE continuous want-list for EVERY missing leaf — the session pipelines
		// to link speed with its own bounded in-flight window, and blocks stream in out of order. THROUGHPUT-CRITICAL: the
		// receive loop only does WriteAt (page cache — cheap, non-blocking) + a bitmap set; the expensive durability work
		// (fsync + bitmap sidecar) and memory reclaim (dropping the redundant blockstore copies) run in a BACKGROUND
		// flusher so they never stall the pipeline. The previous design fetched in windows and fsync'd on the hot path
		// every 2s, which parked the want-list during each multi-hundred-ms fsync → the download visibly PULSED. Resident
		// memory stays bounded because the flusher drops blocks as soon as ~dropBatch accumulate (independent of file
		// size — no windowing needed). Durability invariant preserved: each sync flushes the FILE (out.Sync) BEFORE
		// persisting the bitmap snapshot, so a crash never marks a leaf done whose bytes aren't on disk (it re-fetches
		// via the .part bitmap). Dropping a block without an fsync is safe — it's redundant with the file bytes, and an
		// un-synced leaf isn't in the saved bitmap, so resume re-fetches it.
		fdbg("writeThrough: opening bitswap session, %d leaves, ONE continuous want-list cid=%s", len(need), cidStr)
		phase(cidStr, fmt.Sprintf("downloading %d block(s) over p2p", len(need)))
		sess := blockservice.NewSession(fctx, n.bserv)
		recv := 0
		sessStart := time.Now()

		// VG_FETCH_RATE=1: per-second receive trace — bytes/s, blocks/s and the LARGEST inter-block gap inside each
		// second — the ground-truth for diagnosing "pulsing" download speed (is the silence on the wire, and how long).
		var rateBytes, rateBlocks atomic.Int64
		var rateMaxGapMs atomic.Int64
		if os.Getenv("VG_FETCH_RATE") != "" {
			safeGo("fetch.rateLogger", func() {
				t := time.NewTicker(1 * time.Second)
				defer t.Stop()
				sec := 0
				for {
					select {
					case <-fctx.Done():
						return
					case <-t.C:
						sec++
						b := rateBytes.Swap(0)
						k := rateBlocks.Swap(0)
						g := rateMaxGapMs.Swap(0)
						fmt.Fprintf(os.Stderr, "[rate %s] t=%03ds %8.3f MB/s blk=%3d maxgap=%4dms\n",
							shortCid(root), sec, float64(b)/1e6, k, g)
						wire.report("[wire " + shortCid(root) + "]") // per-peer wants/cancels/blocks/haves since last tick
					}
				}
			})
		}

		const dropBatch = 256 // ~64 MiB of received blocks before the flusher reclaims them → bounded residency
		var mu sync.Mutex     // guards bits (set + snapshot) and dropQ
		var dropQ []cid.Cid
		doDrop := func() {
			mu.Lock()
			q := dropQ
			dropQ = nil
			mu.Unlock()
			if len(q) == 0 {
				return
			}
			if batch, berr := n.ds.Batch(n.ctx); berr == nil {
				for _, c := range q {
					_ = batch.Delete(n.ctx, blockstore.BlockPrefix.Child(dshelp.MultihashToDsKey(c.Hash())))
				}
				_ = batch.Commit(n.ctx)
			}
		}
		doSync := func() { // fsync the file THEN persist a bitmap snapshot — order matters for crash-consistency
			mu.Lock()
			snap := &partBits{root: bits.root, total: bits.total, count: bits.count, nset: bits.nset,
				bits: append([]byte(nil), bits.bits...)}
			mu.Unlock()
			_ = out.Sync()
			_ = savePart(dest, snap)
		}
		flushStop := make(chan struct{})
		dropSig := make(chan struct{}, 1)
		var flusher sync.WaitGroup
		flusher.Add(1)
		safeGo("fetch.flusher", func() {
			defer flusher.Done()
			t := time.NewTicker(2 * time.Second)
			defer t.Stop()
			for {
				select {
				case <-flushStop:
					return
				case <-dropSig:
					doDrop()
				case <-t.C:
					doSync()
					doDrop()
				}
			}
		})

		// ROLLING WANT-LIST. bitswap SERVERS truncate each peer's queued wantlist (boxo default: 1024 entries,
		// silently dropping the overflow), and the cap is PER PEER across ALL sessions — so a big file, or several
		// concurrent downloads to one peer, overflow it and the tail crawls in on 10s rebroadcasts. The FIX is to
		// keep only a bounded window of wants outstanding and to REFILL it by RECEIVED COUNT, not by chunk boundary.
		//
		// The previous design chunked the wants into `wantChunks` slots of `wantChunk` keys, freeing a slot only when
		// a WHOLE chunk arrived. That head-of-line-blocks: one straggler (a want the peer dropped) holds its chunk's
		// slot open forever, and once every slot is held by a straggler NO new keys are ever requested — the session
		// only knows the ≤ window keys handed so far, of which a few never come, and keys beyond the window are never
		// asked for at all. Against a single stock-cap peer (a friend on a dead-DHT network reaching only Pinata) the
		// stragglers never arrive, so the fetch wedges permanently. Measured: 3015/4037 then dead; 24→56 in minutes.
		//
		// Rolling window: request an initial `wantWindow` keys, then admit ONE new key for every block received, so
		// ~wantWindow wants stay outstanding continuously with no chunk boundary to wedge. A straggler delays only
		// itself; the window keeps advancing through every other key, and the session cancels each want as its block
		// lands (draining the peer's ledger). wantWindow×256KiB is the pipeline depth — 256 = 64MiB, far above any
		// link's BDP, so throughput is unaffected while staying well under a stock 1024 cap even with a few concurrent
		// fetches to the same peer. refillBatch bounds how many GetBlocks channels exist at once (fan-in goroutines).
		refillBatch := 32
		if v, _ := strconv.Atoi(os.Getenv("VG_WANT_REFILL")); v > 0 {
			refillBatch = v
		}
		lastFetchLeaves.Store(int64(len(need)))
		fdbg("writeThrough: rolling window (global budget=%d, pool avail=%d) refill=%d leaves=%d cid=%s", wantBudget, globalWantPool.available(), refillBatch, len(need), cidStr)
		var writeErr error
		for blk := range rollingGetBlocks(fctx, sess, need, refillBatch) {
			now := time.Now().UnixNano()
			if gap := (now - lastBlk.Load()) / 1e6; gap > rateMaxGapMs.Load() {
				rateMaxGapMs.Store(gap) // benign race with the reporter's Swap — diagnostic only
			}
			lastBlk.Store(now)
			recv++
			data := blk.RawData()
			rateBytes.Add(int64(len(data)))
			rateBlocks.Add(1)
			c := blk.Cid()
			for _, o := range offsets[c] {
				if _, werr := out.WriteAt(data, o); werr != nil {
					fdbg("writeThrough: WriteAt IO error cid=%s err=%v", cidStr, werr)
					writeErr = localFatal(werr) // disk error (ENOSPC/EIO) — terminal
					fcancel()                   // stop the producer + fans; we drain + clean up after the loop
					break
				}
				written += int64(len(data))
				if total > 0 && onProgress != nil {
					onProgress(math.Min(99, 100.0*float64(written)/float64(total)))
				}
			}
			if writeErr != nil {
				break
			}
			mu.Lock()
			for _, i := range idxOf[c] {
				bits.set(i)
			}
			dropQ = append(dropQ, c)
			qlen := len(dropQ)
			mu.Unlock()
			if qlen >= dropBatch { // nudge the flusher to reclaim memory (non-blocking — it coalesces)
				select {
				case dropSig <- struct{}{}:
				default:
				}
			}
			if recv%512 == 0 {
				fdbg("writeThrough: recv %d/%d leaves, written=%d/%d (%.1f%%) cid=%s", recv, len(need), written, total, 100.0*float64(written)/float64(total), cidStr)
			}
		}
		// Session ended (all received, or torn down by stall/cancel/write-error). rollingGetBlocks has drained the
		// channel and returned every want-token by the time its channel closed, so there is nothing to join or
		// drain here — token lifecycle is entirely inside it.
		if writeErr != nil {
			fcancel()
			close(flushStop)
			flusher.Wait()
			_ = out.Sync()
			_ = savePart(dest, bits) // keep the partial for a later resume
			_ = out.Close()
			return writeErr
		}
		// Stop the flusher, then do a final durability + reclaim pass on the main goroutine so the returned state
		// is fully persisted.
		close(flushStop)
		flusher.Wait()
		doSync()
		doDrop()
		fdbg("writeThrough: session ended cid=%s recv=%d/%d elapsed=%s stalled=%v cancelled=%v allSet=%v", cidStr, recv, len(need), time.Since(sessStart).Round(time.Millisecond), stalled.Load(), isCancelled(cidStr), bits.allSet())
		wire.report("[wire-final " + shortCid(root) + "]")

		if isCancelled(cidStr) {
			_ = out.Close()
			removePartial(dest) // explicit cancel discards the partial
			return errors.New("cancelled")
		}
		// The session STALLED with leaves missing while the node is online: before giving the attempt up, ask the HTTPS
		// gateways for exactly what is missing — an entity-bytes CAR from the first missing leaf's offset on. This is
		// the second transport for LEAVES, the same way getRoot has it for the root. Without it a DAG whose root had
		// landed (a hedge winner that died mid-stream; a CAR cut by the stall watchdog) was stuck for good on a
		// gateway-only network: every later attempt found the root locally, never re-entered the gateway, and looped
		// on a bitswap session with nobody to ask (isolation matrix run-4: seven identical "incomplete" attempts).
		// The resume is USER-CANCELLABLE on its own: fetch.worker (the session's cancel poller) has already exited
		// on the stall, and nctx is n.ctx for a background download, so without userCancelCtx a Cancel pressed
		// during a multi-GB CAR would stream on for minutes and then finalize + pin the file. COST where no gateway
		// has the content (a friend's own package): gatewayHedgeDelay + gatewayHeaderTimeout (~55 s) per attempt
		// whose session stalled — and while that friend stays offline the loop cycles stall → probe → backoff,
		// ~80 s per lap, on every lap. No cheaper definitive negative exists: see gatewayHeaderTimeout.
		if !bits.allSet() && stalled.Load() && n.dht != nil && nctx.Err() == nil {
			from := int64(-1)
			for i, lf := range leaves {
				if !bits.get(i) && (from < 0 || int64(lf.off) < from) {
					from = int64(lf.off)
				}
			}
			base := written
			fdbg("writeThrough: bitswap stalled with %d/%d leaves missing → HTTPS gateway resume from byte %d cid=%s", bits.count-bits.nset, bits.count, from, cidStr)
			phase(cidStr, fmt.Sprintf("resuming the missing %d block(s) over HTTPS gateway", bits.count-bits.nset))
			rctx, rstop := userCancelCtx(nctx, cidStr)
			gerr := n.fetchViaGateway(rctx, root, from, func(read, _ int64) {
				if total > 0 && onProgress != nil {
					onProgress(math.Min(99, 100.0*float64(base+read)/float64(total)))
				}
			})
			rstop()
			if gerr != nil {
				fdbg("writeThrough: gateway resume failed cid=%s: %v", cidStr, gerr)
			} else {
				// The CAR landed in the blockstore; write every still-missing leaf through from there.
				for i, lf := range leaves {
					if bits.get(i) {
						continue
					}
					blk, berr := n.bstore.Get(nctx, lf.c)
					if berr != nil {
						fdbg("writeThrough: leaf %d (%s) still missing after the gateway resume cid=%s", i, lf.c, cidStr)
						continue
					}
					if _, werr := out.WriteAt(blk.RawData(), int64(lf.off)); werr != nil {
						_ = out.Sync()
						_ = savePart(dest, bits)
						_ = out.Close()
						return localFatal(werr)
					}
					written += int64(len(blk.RawData()))
					bits.set(i)
					if total > 0 && onProgress != nil {
						onProgress(math.Min(99, 100.0*float64(written)/float64(total)))
					}
				}
				fdbg("writeThrough: gateway resume done, %d/%d leaves on disk cid=%s", bits.nset, bits.count, cidStr)
			}
			if isCancelled(cidStr) { // a cancel during the resume must never reach finalize
				_ = out.Close()
				removePartial(dest)
				return errors.New("cancelled")
			}
		}
		if !bits.allSet() { // stalled / session dropped incomplete → persist progress, keep the partial, resume later
			_ = out.Sync()
			_ = savePart(dest, bits)
			_ = out.Close()
			fdbg("writeThrough: INCOMPLETE — %d/%d leaves, keeping partial, returning errIncomplete cid=%s", bits.nset, bits.count, cidStr)
			return errIncomplete
		}
	}

	// Complete: flush, publish atomically, reference every leaf into dest, drop the redundant blockstore copies, pin.
	fdbg("writeThrough: all %d leaves present → FINALIZE START (fsync+rename) cid=%s", len(leaves), cidStr)
	finStart := time.Now()
	if err := out.Sync(); err != nil {
		_ = out.Close()
		return localFatal(err)
	}
	if err := out.Close(); err != nil {
		return localFatal(err)
	}
	_ = os.RemoveAll(dest)
	if err := os.Rename(tmp, dest); err != nil {
		return localFatal(err)
	}
	fdbg("finalize: fsync+rename done in %s cid=%s", time.Since(finStart).Round(time.Millisecond), cidStr)

	// Reference each leaf into dest, then drop its plain blockstore copy — TWO batched datastore commits (one for refs,
	// one for blockstore deletes) so a large file's thousands of leaves finalize in ~a couple of commits (near-instant).
	if onFinalize != nil {
		onFinalize(0)
	}
	st, err := os.Stat(dest)
	if err != nil {
		return localFatal(err)
	}
	fm := n.fstore.FileManager()
	fsns := make([]*posinfo.FilestoreNode, 0, len(leaves))
	for _, lf := range leaves {
		fsns = append(fsns, &posinfo.FilestoreNode{
			Node:    &refLeaf{c: lf.c, size: lf.sz},
			PosInfo: &posinfo.PosInfo{Offset: lf.off, FullPath: dest, Stat: st},
		})
	}
	fdbg("finalize %s: dest size=%d, referencing %d leaves via PutMany (validates each leaf hash vs on-disk bytes)", root, st.Size(), len(fsns))
	putStart := time.Now()
	if err := fm.PutMany(n.ctx, fsns); err != nil { // one batched commit of all references
		fdbg("PutMany(%s) FAILED after %s: %v (isMissingFile=%v)", root, time.Since(putStart).Round(time.Millisecond), err, isMissingFile(err))
		// A CorruptReferenceError here means the bytes we just wrote to dest DON'T hash to their leaf CIDs — a
		// corrupt/incomplete transfer, NOT a stale local reference. Returning errMissingFiles would send the wrapper
		// down dropRef (wrong medicine) AND leave the bad dest + partial in place, so every retry re-fails identically
		// (the "errored missing files that keeps re-downloading" symptom). Instead DISCARD the bad output and the
		// resume sidecar and report errIncomplete so the wrapper backs off and re-fetches from scratch.
		if isMissingFile(err) {
			_ = os.Remove(dest)
			removePartial(dest)
			return errIncomplete
		}
		return localFatal(err) // datastore write failure — terminal
	}
	fdbg("finalize: PutMany OK in %s cid=%s", time.Since(putStart).Round(time.Millisecond), cidStr)
	// Drop the bitswap-cached leaf blocks from the plain blockstore in one batched commit (they're now referenced in
	// place, so the blockstore copies are redundant). Keys follow boxo's blockstore scheme: BlockPrefix + multihash.
	delStart := time.Now()
	if batch, berr := n.ds.Batch(n.ctx); berr == nil {
		for _, lf := range leaves {
			_ = batch.Delete(n.ctx, blockstore.BlockPrefix.Child(dshelp.MultihashToDsKey(lf.c.Hash())))
		}
		_ = batch.Commit(n.ctx)
	}
	fdbg("finalize: dropped %d blockstore copies in %s cid=%s", len(leaves), time.Since(delStart).Round(time.Millisecond), cidStr)
	if onFinalize != nil {
		onFinalize(100)
	}

	// Pin the root so it's seeded + reprovided (mirrors addNoCopy in the fallback path). Walks the DAG via the OFFLINE
	// pinner dagservice (captured pre-goOnline) so leaves resolve from the filestore/disk, never the network.
	fdbg("finalize: recursive PIN START cid=%s", cidStr)
	pinStart := time.Now()
	if err := n.pinner.Pin(n.ctx, rootNode, true, dest); err != nil {
		fdbg("finalize: PIN FAILED in %s cid=%s err=%v", time.Since(pinStart).Round(time.Millisecond), cidStr, err)
		return localFatal(err)
	}
	if err := n.pinner.Flush(n.ctx); err != nil {
		fdbg("finalize: pin Flush FAILED cid=%s err=%v", cidStr, err)
		return localFatal(err)
	}
	n.announce(root) // re-announce now so a re-hosted CID is immediately discoverable (not after the 22h reprovide)
	// LAST: clear the resume sidecar. The .part is the finalize COMMIT MARKER — removed only once the content is
	// fully referenced, pinned and announced, so a crash anywhere above leaves .part+dest and the dest-exists path
	// re-runs the idempotent repair on the next attempt. Never removed earlier.
	removePartial(dest)
	fdbg("finalize: PIN+flush done in %s → writeThrough SUCCESS cid=%s", time.Since(pinStart).Round(time.Millisecond), cidStr)
	return nil
}

// dropClosure removes a CID's block closure from the plain blockstore — used both to clear bitswap's cached copy
// before re-adding as filestore references (successful fetch), and to PURGE a cancelled download's partial cached
// blocks. Walks via the OFFLINE DAG service so absent leaves (a partial download) are skipped, never fetched over the
// network, and batches the deletes into one datastore commit (a large partial is thousands of blocks). Only touches
// the plain blockstore — a completed download's leaves are filestore references, so those are unaffected.
func (n *node) dropClosure(root cid.Cid) {
	seen := cid.NewSet()
	var cids []cid.Cid
	var walk func(c cid.Cid)
	walk = func(c cid.Cid) {
		if !seen.Visit(c) {
			return
		}
		if nd, err := n.localDserv.Get(n.ctx, c); err == nil { // only present (cached) nodes are readable offline
			for _, l := range nd.Links() {
				walk(l.Cid)
			}
		}
		cids = append(cids, c)
	}
	walk(root)
	if batch, err := n.ds.Batch(n.ctx); err == nil {
		for _, c := range cids {
			_ = batch.Delete(n.ctx, blockstore.BlockPrefix.Child(dshelp.MultihashToDsKey(c.Hash())))
		}
		_ = batch.Commit(n.ctx)
	} else {
		main := n.fstore.MainBlockstore()
		for _, c := range cids {
			_ = main.DeleteBlock(n.ctx, c)
		}
	}
}

// dropRef removes a CID's entire closure from the FILESTORE (the FileManager references AND any plain blockstore
// blocks), then unpins it. Filestore.Put skips a block it already has, so an existing reference — healthy or orphaned
// (backing file deleted/moved) — must be deleted before addNoCopy can re-point it at a new backing file. Uses the
// offline DAG service so an orphaned leaf (whose backing file is gone) is simply skipped, never fetched over the
// network. Only the small dag-pb intermediate nodes need reading to enumerate leaf CIDs; the leaves themselves are
// deleted by CID without reading their (possibly gone) content.
func (n *node) dropRef(root cid.Cid) {
	// Enumerate the closure first (reading only the small dag-pb intermediates), then delete every leaf in ONE
	// datastore batch. Per-leaf fstore.DeleteBlock was O(leaves) sync writes to leveldb — ~21 ms each, so a 763-leaf
	// file stalled ~16 s. A single batched commit is ~one sync. Each leaf may live as a filestore reference
	// (/filestore/<mh>) AND/OR a plain block (/blocks/<mh>); delete both keys so neither layout survives.
	seen := cid.NewSet()
	var cids []cid.Cid
	var walk func(c cid.Cid)
	walk = func(c cid.Cid) {
		if !seen.Visit(c) {
			return
		}
		if nd, err := n.localDserv.Get(n.ctx, c); err == nil {
			for _, l := range nd.Links() {
				walk(l.Cid)
			}
		}
		cids = append(cids, c)
	}
	walk(root)
	if batch, err := n.ds.Batch(n.ctx); err == nil {
		for _, c := range cids {
			k := dshelp.MultihashToDsKey(c.Hash())
			_ = batch.Delete(n.ctx, filestore.FilestorePrefix.Child(k))
			_ = batch.Delete(n.ctx, blockstore.BlockPrefix.Child(k))
		}
		if cerr := batch.Commit(n.ctx); cerr != nil { // batch failed → fall back to the per-block path (correctness over speed)
			for _, c := range cids {
				_ = n.fstore.DeleteBlock(n.ctx, c)
			}
		}
	} else {
		for _, c := range cids {
			_ = n.fstore.DeleteBlock(n.ctx, c)
		}
	}
	_ = n.unpin(root) // drop the stale recursive pin; the re-add re-pins against the new backing file
	n.scheduleCompaction()
}
