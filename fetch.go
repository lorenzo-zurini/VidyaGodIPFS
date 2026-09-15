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

// Fetch de-duplication by destination file. Two workers (a layer CID referenced from two nodes, an overlapping
// hydrate, or a startup auto-resume racing a manual download) writing the same dest.tmp/dest would stomp each
// other's rename/pin → spurious "no such file"/missing-files. Keyed by dest (a dest maps to exactly one CID), the
// FIRST caller (the leader) runs the fetch and drives ITS OWN onProgress/onFinalize callbacks; later callers
// (joiners) block on the leader's result instead of racing.
//
// Why not singleflight: a bounded (deadline) joiner must be able to STOP WAITING at its deadline without waiting
// for an UNBOUNDED leader to finish (the pass-4 hang). Abandoning the WAIT is UAF-safe precisely because a joiner's
// own callbacks are NEVER invoked — only the leader's fn runs — so no goroutine touches the joiner's C strings
// after it returns. The LEADER never abandons (its C wrapper owns the callback strings for the call's lifetime);
// instead the leader's deadline is threaded INTO the attempt (see fetchToPathLoopUntil), so it returns on its own
// at ~deadline. singleflight.DoChan can't express "leader waits, joiner may bail" because Result.Shared is known
// only after the result arrives — too late to decide who may abandon.
type fetchWait struct {
	done    chan struct{}
	err     error
	waiters int // joiners currently blocked on this leader (guarded by inflightMu) — see the handoff-emit note
}

var (
	inflightMu sync.Mutex
	inflight   = map[string]*fetchWait{}
)

// errFetchDeadline marks a give-up caused by a caller's WALL-CLOCK budget running out (as opposed to a terminal
// local error or the node shutting down). It is the signal for leadership handoff: a give-up under one caller's
// deadline is not a verdict on the content, so a still-in-budget (or unbounded) waiter takes over instead of
// inheriting it. errors.Is unwraps it whether it was produced here or wrapped with the CID for the message.
var errFetchDeadline = errors.New("did not complete before its deadline")

func deadlineErr(cidStr string) error { return fmt.Errorf("fetch of %s %w", cidStr, errFetchDeadline) }

// dedupFetch coordinates one fetch per dest. The FIRST caller (leader) runs `run(deadline)` and drives its own
// callbacks; later callers (joiners) wait on the leader's result. Two properties this must guarantee:
//
//  1. A joiner with a deadline stops WAITING at its deadline (returns a deadline error) instead of blocking on a
//     possibly-unbounded leader. Abandoning the wait is UAF-safe: a joiner's own callbacks are never invoked (only
//     the leader's run fn runs), so nothing touches the joiner's C strings after it returns.
//
//  2. LEADERSHIP HANDOFF. A give-up under the LEADER's deadline must not be inherited by a waiter that still has
//     budget — otherwise a bounded launch (120s leader) would kill an UNBOUNDED background download of the same
//     file the moment the transfer runs longer than 120s. When the leader finishes with errFetchDeadline and a
//     waiter is still in-budget (or unbounded), that waiter loops back, becomes the new leader, and CONTINUES the
//     fetch — it resumes from the .part bitmap, so nothing is refetched. Whoever has the longest budget ends up
//     leading; a truly unbounded waiter leads until the content lands.
//
// The returned `ran` is true iff THIS call executed the fetch (as the original leader, or as a joiner that took
// over via handoff). It is false for a pure joiner that only waited (and then inherited a result or abandoned).
// Callers that emit transfer events use it so that only the call actually driving the transfer reports its
// terminal event — a pure joiner must not stamp Errored/Finished onto a CID whose leader still owns the row.
func (n *node) dedupFetch(dest, cidStr string, deadline time.Time, run func(deadline time.Time) error) (err error, ran bool, handedOff bool) {
	for {
		inflightMu.Lock()
		fw, joining := inflight[dest]
		if !joining {
			lead := &fetchWait{done: make(chan struct{})}
			inflight[dest] = lead
			inflightMu.Unlock()
			err, handedOff = n.runFetchAsLeader(dest, lead, deadline, run)
			return err, true, handedOff // this call drove the fetch
		}
		fw.waiters++ // counted (under inflightMu) so the leader knows a successor is queued to take over
		inflightMu.Unlock()

		var timerC <-chan time.Time
		var timer *time.Timer
		if !deadline.IsZero() {
			timer = time.NewTimer(time.Until(deadline))
			timerC = timer.C
		}
		select {
		case <-fw.done:
			if timer != nil {
				timer.Stop()
			}
			lerr := fw.err
			inflightMu.Lock()
			fw.waiters--
			inflightMu.Unlock()
			// Handoff: the leader gave up under ITS deadline but we still have budget (or are unbounded) → take over.
			if errors.Is(lerr, errFetchDeadline) && (deadline.IsZero() || time.Now().Before(deadline)) {
				fdbg("dedupFetch: leader gave up on its deadline, taking over as new leader cid=%s dest=%s", cidStr, dest)
				continue
			}
			return lerr, false, false // pure joiner: inherited the leader's result, did not drive
		case <-timerC:
			inflightMu.Lock()
			fw.waiters--
			inflightMu.Unlock()
			fdbg("dedupFetch: joiner deadline exceeded, abandoning wait on leader cid=%s dest=%s", cidStr, dest)
			return deadlineErr(cidStr), false, false // pure joiner: abandoned the wait, did not drive
		case <-n.ctx.Done():
			if timer != nil {
				timer.Stop()
			}
			inflightMu.Lock()
			fw.waiters--
			inflightMu.Unlock()
			return n.ctx.Err(), false, false // pure joiner: node shutting down, did not drive
		}
	}
}

// runFetchAsLeader runs the fetch for one dest and publishes its result to waiters. Cleanup (unlock the dest + close
// the done channel) is deferred so that even a PANIC in run releases waiters — otherwise one panic would wedge that
// dest forever (every future fetch of it blocks on a done channel that never closes). On panic the published error
// is a real non-nil error (never a silent nil that a joiner would read as success) and the panic is re-raised.
func (n *node) runFetchAsLeader(dest string, lead *fetchWait, deadline time.Time, run func(deadline time.Time) error) (err error, handedOff bool) {
	defer func() {
		inflightMu.Lock()
		delete(inflight, dest)
		// A give-up on OUR deadline with a waiter queued will be TAKEN OVER by that waiter (it resumes from the .part
		// bitmap), so this call must not emit a terminal Errored - the successor owns the row. Read under the same lock
		// joiners bump waiters with. A sole give-up (no waiter) keeps handedOff=false, so it reports the error.
		//
		// Two known suppress-without-successor windows (both accepted): a counted waiter can still fail to take over
		// if, between this read and its own wake, (a) its timer fires and the runtime picks the timerC arm, or (b) it
		// wakes on fw.done but its deadline has since passed (the Before(deadline) check below returns instead of
		// continuing). In either case handedOff was true but nobody takes over → the row shows a wrong "Downloading"/
		// "Stalled" label (not a hang; the next Started heals it). Both require TWO bounded callers racing on the SAME
		// dest within scheduling latency — a topology the app never creates (launch dests are per-layer with no double
		// launch; covers are unique; every other caller is unbounded and never yields errFetchDeadline). Closing them
		// needs a "designated last waiter emits" handshake; not worth the added concurrency for an unreachable case.
		handedOff = errors.Is(err, errFetchDeadline) && lead.waiters > 0
		inflightMu.Unlock()
		if r := recover(); r != nil {
			lead.err = fmt.Errorf("fetch aborted (panic): %v", r)
			close(lead.done)
			panic(r) // preserve the crash; waiters already have a real error, not a false success
		}
		lead.err = err
		close(lead.done)
	}()
	err = run(deadline)
	return
}

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

// sleepOrCancel waits d, returning true if a user-cancel arrived during the wait (polled every 250ms so a cancel
// is honoured promptly instead of after a full 30s backoff — the slot is freed and the waiter unblocked quickly).
// Returns false if the full delay elapsed (or the node is shutting down — the caller then checks n.ctx).
func (n *node) sleepOrCancel(cidStr string, d time.Duration) bool {
	const step = 250 * time.Millisecond
	deadline := time.Now().Add(d)
	for {
		if isCancelled(cidStr) {
			return true
		}
		remain := time.Until(deadline)
		if remain <= 0 {
			return false
		}
		if remain > step {
			remain = step
		}
		select {
		case <-time.After(remain):
		case <-n.ctx.Done():
			return false
		}
	}
}

// isMissingFile detects a filestore reference whose backing file is gone (deleted package content).
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

// fetchToPath retrieves cidStr's file content to dest and seeds it from there — TORRENT-STYLE: it resumes and retries
// until the file is whole or the user cancels, never failing on a transient stall. Each attempt (fetchToPathOnce →
// writeThrough) resumes only the missing leaves from the on-disk partial (dest.tmp + dest.part). A stall/drop returns
// errIncomplete → back off (growing to a cap, reset once an attempt has clearly made progress) and resume. A stale
// LOCAL reference (removed/moved content) returns errMissingFiles → clear it once and re-fetch over the network.
// fetchToPath de-duplicates by dest (see dedupFetch) then runs the resumable retry loop. Concurrent callers for the
// same dest share ONE fetch: the leader drives the transfer callbacks; joiners wait for the result. A zero deadline
// means UNBOUNDED (retry-forever) — background downloads, which must never give up on a transient failure.
//
// fetchToPathDeadline is fetchToPath for SYNCHRONOUS callers that must not block a user action forever (launch layer
// materialization, cover fetches). The budget bounds this CALLER'S WAIT, not necessarily the shared fetch: the
// deadline is threaded INTO the attempt (getRoot/session ctxs) AND caps the between-attempt backoff, and — if this
// caller is a pure joiner — caps how long it waits on the leader. NOTE (behaviour, not a caveat): a bounded fetch
// that is the SOLE caller is torn down at its deadline and does NOT continue in the background — the loop returns
// and the goroutine exits, so the C callback string's lifetime is exactly the call (no use-after-free, no ghost
// fetch). What DOES continue is a fetch that an UNBOUNDED caller is also waiting on: when a bounded leader gives up
// on its deadline, dedupFetch hands leadership to the unbounded waiter, which continues from the .part bitmap. So a
// launch that collides with a background download of the same layer never kills that download.
//
// Returns (err, emitTerminal): emitTerminal is true only when this call drove the transfer AND did not hand its
// deadline give-up off to a waiting successor. False for a pure joiner (the leader owns the CID's row) and for a
// leader that handed off (the successor owns it) — so no caller stamps Errored onto a live download, while a SOLE
// bounded give-up still emits (emitTerminal true) so its row never hangs as a stuck spinner.
func (n *node) fetchToPathDeadline(cidStr, dest string, onProgress, onFinalize func(pct float64), budget time.Duration) (error, bool) {
	deadline := time.Time{}
	if budget > 0 {
		deadline = time.Now().Add(budget)
	}
	err, ran, handedOff := n.dedupFetch(dest, cidStr, deadline, func(dl time.Time) error {
		return n.fetchToPathLoopUntil(cidStr, dest, onProgress, onFinalize, dl)
	})
	if !ran {
		fdbg("fetchToPathDeadline JOINED an in-flight fetch (deduped, did not drive) cid=%s dest=%s err=%v", cidStr, dest, err)
	}
	// emitTerminal: emit a terminal transfer event only if this call drove the transfer AND did not hand the
	// give-up off to a successor (which now owns the row). A sole bounded give-up still emits so its row can't hang.
	return err, ran && !handedOff
}

func (n *node) fetchToPath(cidStr, dest string, onProgress, onFinalize func(pct float64)) error {
	err, ran, _ := n.dedupFetch(dest, cidStr, time.Time{}, func(dl time.Time) error {
		return n.fetchToPathLoopUntil(cidStr, dest, onProgress, onFinalize, dl)
	})
	if !ran {
		fdbg("fetchToPath JOINED an in-flight fetch of the same dest (deduped, did not drive) cid=%s dest=%s err=%v", cidStr, dest, err)
	}
	return err
}

// backoffWait sleeps for d between attempts, honouring both a user-cancel and the fetch deadline. It returns
// exit=true (with the error to return) when the loop must stop: the user cancelled, the node is shutting down, or
// the deadline is already exhausted (rem<=0). When the remaining budget is smaller than the backoff, the sleep is
// CLAMPED to the remaining budget (not zeroed — a fast-failing online attempt would busy-spin) so the sleep never
// runs past the deadline and the loop's top-of-iteration deadline check then ends the fetch. deadline zero =
// unbounded (background download): sleep the full backoff.
func (n *node) backoffWait(cidStr, dest string, d time.Duration, deadline time.Time) (bool, error) {
	if !deadline.IsZero() {
		rem := time.Until(deadline)
		if rem <= 0 {
			fdbg("fetchToPath deadline reached, no budget for another attempt cid=%s", cidStr)
			return true, deadlineErr(cidStr)
		}
		if d > rem {
			d = rem // sleep at most the remaining budget, then the loop-top deadline check returns. Never sleep past
			// the deadline, and never drop to a zero backoff (a fast-failing online attempt would then busy-spin).
		}
	}
	if d > 0 && n.sleepOrCancel(cidStr, d) {
		removePartial(dest)
		return true, errors.New("cancelled")
	}
	if n.ctx.Err() != nil {
		return true, n.ctx.Err()
	}
	return false, nil
}

func (n *node) fetchToPathLoopUntil(cidStr, dest string, onProgress, onFinalize func(pct float64), deadline time.Time) error {
	backoff := 2 * time.Second
	missTries := 0
	fdbg("fetchToPath ENTER cid=%s dest=%s", cidStr, dest)
	for attempt := 1; ; attempt++ {
		if isCancelled(cidStr) {
			fdbg("fetchToPath cancelled before attempt %d cid=%s", attempt, cidStr)
			removePartial(dest)
			return errors.New("cancelled")
		}
		if !deadline.IsZero() && !time.Now().Before(deadline) { // now >= deadline (>= not >, so no doomed extra attempt at equality)
			fdbg("fetchToPath deadline exceeded before attempt %d cid=%s", attempt, cidStr)
			return deadlineErr(cidStr)
		}
		// Proactively freshen provider addresses (warm.go) on EVERY attempt — not just the first: a live DHT walk +
		// connect in parallel with bitswap, so a provider that restarted (new ports) or came online mid-download is
		// re-discovered instead of the retry loop spinning forever on a dead peer set. No-op when already connected.
		// A fresh CONTENT CID may have no DHT record yet (bulk content announces drain through a slow batched queue),
		// but the 3-level schema blocking-announces the COLLECTION metas first — so also warm the collection providers:
		// whoever seeds our sources has this file, and bitswap's want-broadcast reaches it once we're connected.
		if c, derr := cid.Decode(cidStr); derr == nil {
			n.warmProviders(c)
			n.warmSeedLevelProviders()
		}
		n.warmFriends() // a friend is a guaranteed provider the DHT never surfaces — connect so bitswap can ask them
		start := time.Now()
		fdbg("fetchToPath attempt %d START cid=%s", attempt, cidStr)
		phase(cidStr, fmt.Sprintf("attempt %d — connecting to providers", attempt))
		// Bound the ATTEMPT ITSELF by the deadline, not just the loop top: a 30s getRoot, a gateway CAR pull, or a
		// session that trickles one block per <stallTimeout (never triggering the stall watchdog) must all be torn
		// down AT the deadline so a synchronous caller returns on time. attemptCtx carries the deadline into every
		// network read (getRoot, the fallback DagReader, the writeThrough session). n.ctx (shutdown) is the parent,
		// so a node stop still cancels. No deadline (background download) → attemptCtx == n.ctx, unbounded.
		attemptCtx := n.ctx
		var acancel context.CancelFunc
		if !deadline.IsZero() {
			attemptCtx, acancel = context.WithDeadline(n.ctx, deadline)
		}
		err := n.fetchToPathOnce(attemptCtx, cidStr, dest, onProgress, onFinalize)
		if acancel != nil {
			acancel()
		}
		fdbg("fetchToPath attempt %d DONE cid=%s err=%v elapsed=%s", attempt, cidStr, err, time.Since(start).Round(time.Millisecond))
		switch {
		case err == nil:
			fdbg("fetchToPath SUCCESS cid=%s after %d attempt(s)", cidStr, attempt)
			return nil
		case err == errIncomplete:
			if time.Since(start) > 5*time.Second { // the attempt fetched for a while before stalling → reset backoff
				backoff = 2 * time.Second
			} else if backoff < 30*time.Second {
				backoff *= 2
			}
			fdbg("fetchToPath incomplete → backoff %s then resume cid=%s", backoff, cidStr)
			phase(cidStr, fmt.Sprintf("transfer interrupted — resuming in %s", backoff.Round(time.Second)))
			if exit, e := n.backoffWait(cidStr, dest, backoff, deadline); exit {
				return e
			}
		case err == errMissingFiles:
			missTries++
			fdbg("fetchToPath missingFiles (try %d) → dropRef + refetch cid=%s", missTries, cidStr)
			if missTries > 2 { // shouldn't recur after dropRef; guard against a spin
				return err
			}
			if c, derr := cid.Decode(cidStr); derr == nil {
				tdr := time.Now()
				n.dropRef(c) // clear stale refs (+ cached blocks) so the retry fetches over the network
				fdbg("fetchToPath dropRef closure cleared in %s cid=%s", time.Since(tdr).Round(time.Millisecond), cidStr)
			}
		case isCancelled(cidStr) || errStr(err) == "cancelled":
			// The USER asked to stop (or a waiter cancelled us). Terminal.
			fdbg("fetchToPath cancelled cid=%s", cidStr)
			removePartial(dest)
			return errors.New("cancelled")
		case n.ctx.Err() != nil:
			return n.ctx.Err() // node shutting down — terminal
		case n.dht == nil:
			// OFFLINE (unit tests / pre-goOnline): no network to retry against — terminal.
			return err
		case errors.Is(err, errLocalFatal):
			// A LOCAL, permanent failure — malformed CID, disk full/read-only, datastore or pin error. Retrying
			// cannot fix it and an infinite silent spin is the worst kind of "stuck", so this is TERMINAL and is
			// logged at production level (not fdbg) so the failure is visible without VG_FETCH_DEBUG. "Never give
			// up" means never give up on a NETWORK problem — not on a broken disk or corrupt input.
			fmt.Fprintf(os.Stderr, "[fetch] FATAL (not retryable) cid=%s: %v\n", cidStr, err)
			return err
		default:
			// TRANSIENT / NETWORK: a root-fetch timeout, all-gateways failure, a dropped connection, a DHT that
			// found nobody this pass. None mean the content is unobtainable — only that this attempt reached no
			// provider. Back off and retry; a provider (our seeder, a friend, Pinata after a rate-limit, a peer
			// that comes online later) may appear at any time. Unbounded for background downloads (zero deadline);
			// bounded (deadline threaded into the attempt) for synchronous callers (a launch) and for a cover's
			// one-attempt-per-sweep queue job, so neither can hang — or hold a DownloadSlot — forever.
			if time.Since(start) > 5*time.Second { // made progress before failing → reset backoff
				backoff = 2 * time.Second
			} else if backoff < 30*time.Second {
				backoff *= 2
			}
			fdbg("fetchToPath attempt %d TRANSIENT (%v) → backoff %s then retry cid=%s", attempt, err, backoff, cidStr)
			if exit, e := n.backoffWait(cidStr, dest, backoff, deadline); exit {
				return e
			}
		}
	}
}

// dirFetchAttempts bounds the retry loop of one fetchDirToPath call. Unlike file fetches (torrent-like, retry forever
// in a background worker), a source sync runs INSIDE the app's sequential sync worker — an unbounded loop there would
// wedge every source behind one dead CID. The C++ side re-runs syncSources periodically while a source is missing, so
// bounded-here + periodic-there = indefinite retry overall with bounded workers.
const dirFetchAttempts = 4

// fetchDirToPath materializes a UnixFS DIRECTORY CID with the same resilience as file fetches: each attempt is
// stall-guarded (no byte written for stallTimeout tears the attempt down instead of hanging forever), failures back
// off and retry, and every fetched block persists in the blockstore between attempts — so a retry refills instantly
// to where the last attempt died and continues from there (natural resume, no sidecar needed for a small meta tree).
func (n *node) fetchDirToPath(cidStr, dest string, onProgress func(pct float64), onFinalize func(pct float64)) error {
	clearCancel(cidStr)
	backoff := 2 * time.Second
	var err error
	for attempt := 1; ; attempt++ {
		if isCancelled(cidStr) {
			return errors.New("cancelled")
		}
		err = n.fetchDirOnce(cidStr, dest, onProgress, onFinalize)
		if err == nil || err == errMissingFiles || n.ctx.Err() != nil {
			return err
		}
		// Offline (tests / local-only materialize): the error is not transient, retrying is pure delay.
		if n.dht == nil {
			return err
		}
		if attempt >= dirFetchAttempts {
			return err
		}
		fdbg("fetchDirToPath attempt %d failed (%v) → backoff %s then retry cid=%s", attempt, err, backoff, cidStr)
		select {
		case <-time.After(backoff):
		case <-n.ctx.Done():
			return n.ctx.Err()
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
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
