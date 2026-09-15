package main

// gateway.go — HTTPS trustless-gateway fallback. When libp2p can't reach a provider (hostile / captive networks that
// filter DNS + block or mangle non-443 transports — Pinata only offers plain ws:3000, which such networks break), fetch
// the CID's whole DAG as a CAR over HTTPS/443 from a trustless gateway and load it into the blockstore, so the normal
// materialize path (writeThrough / files.WriteTo) completes offline. HTTPS/443 is the one thing these networks always
// allow. Trustless: every block is verified against its CID before it's stored, so gateways are untrusted mirrors.

import (
	"context"
	"fmt"
	blocks "github.com/ipfs/go-block-format"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	cid "github.com/ipfs/go-cid"
	car "github.com/ipld/go-car/v2"
)

// trustlessGateways: Pinata's own gateway first (our content is pinned there), then reliable public path gateways.
var trustlessGateways = []string{
	"https://gateway.pinata.cloud", // PRIMARY: has our content natively; served every isolation-matrix row
	"https://ipfs.io",              // the public aliases are ONE backend (trustless-gateway.net) reached through CDN
	"https://w3s.link",             // routes that differ in health at the same moment — RACED, not tried in sequence.
	// gateway.ipfs.io is NOT listed: it 301s straight to ipfs.io (verified), so it was the same route twice.
}

// countingReader reports bytes read as they stream past — for download progress AND stall detection.
type countingReader struct {
	r      io.Reader
	onRead func(n int)
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	if n > 0 && c.onRead != nil {
		c.onRead(n)
	}
	return n, err
}

// headSize returns a URL's Content-Length via a bounded HEAD, or -1 if unknown.
func headSize(ctx context.Context, hc *http.Client, url string) int64 {
	hctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(hctx, http.MethodHead, url, nil)
	if err != nil {
		return -1
	}
	req.Header.Set("User-Agent", "vidyagod")
	resp, err := hc.Do(req)
	if err != nil {
		return -1
	}
	resp.Body.Close()
	if resp.StatusCode == http.StatusOK && resp.ContentLength > 0 {
		return resp.ContentLength
	}
	return -1
}

// gatewaySize asks the trustless gateways for a CID's file size over HTTPS (HEAD → Content-Length). Used as cidSize's
// fallback so the UI can show a size/speed even on a network where the DHT can't fetch the root block.
func (n *node) gatewaySize(ctx context.Context, root cid.Cid) int64 {
	hc := dohHTTPClient(newDoHResolver())
	for _, gw := range trustlessGateways {
		if sz := headSize(ctx, hc, gw+"/ipfs/"+root.String()); sz > 0 {
			return sz
		}
	}
	return -1
}

// gatewayHedgeDelay: how long the PRIMARY gateway gets on its own before the others are raced alongside it. It must
// sit ABOVE the primary's normal time-to-verified-first-block, or the race includes the flaky public backend even
// when Pinata is healthy — and the backend can WIN it: at 5 s (below Pinata's measured 6.0–7.1 s header floor,
// tools/gwprobe.2026-09-15.txt) run-7's gateway-any row saw w3s.link win, die mid-stream, and cost 32 s over the
// pinata-only row. 10 s clears every observed healthy-Pinata first block; when the primary IS dead, failing fast
// still fires the rest early. (var: tests shrink it.)
var gatewayHedgeDelay = 10 * time.Second

// carCandidate is one gateway's opened CAR stream that has ALREADY delivered a verified first block — the only
// evidence a route is actually serving. (The isolation matrix caught routes answering 200 with an empty body.)
type carCandidate struct {
	gw       string
	hc       *http.Client
	fromByte int64
	body     io.ReadCloser
	br       *car.BlockReader
	first    blocks.Block
	total    atomic.Int64 // Content-Length, else a HEAD probe filled in AFTER commit (0/-1 = indeterminate)
	lastRead *atomic.Int64
	cancel   context.CancelFunc
}

// gatewayStatusError is a non-200 answer from a route (typed: logs and tests read the code). There is NO cheap,
// definitive "content absent" on this ecosystem — measured: Pinata answers 404 for a CID nobody has after ~63 s,
// and `Cache-Control: only-if-cached` gets 412 even for PRESENT pinned content — so a probe for content no gateway
// has costs the header timeout (gatewayHeaderTimeout), bounded and paid per stalled attempt, never remembered.
type gatewayStatusError struct{ code int }

func (e gatewayStatusError) Error() string { return fmt.Sprintf("status %d", e.code) }

// fetchViaGateway pulls a CID's whole DAG as a verified CAR over HTTPS from the first gateway that PROVES it can serve
// it. The fetch is HEDGED (see gatewayHedgeDelay): the primary runs alone first; the rest are raced in only if it has
// not produced a verified first block in time (or failed fast). The first candidate with a verified first block wins
// and streams; every other candidate is cancelled the moment we commit. Why not sequential: the public trustless
// aliases are one flaky backend behind several CDN routes — sequential meant ~28 s of dead time per bad route.
//
// fromByte < 0 pulls the WHOLE DAG (dag-scope=all: the root fetch, files and directories alike). fromByte >= 0 pulls
// the entity's bytes from that offset to the end (IPIP-402 entity-bytes): the RESUME form writeThrough uses when a
// stream broke mid-file or bitswap stalled — the CAR then carries the root, the spine and only the leaves from the
// first missing one on, instead of re-sending the whole file. (Probed: Pinata honours it — 877 KB for the last 0.8 MB
// of a 9.8 MB file.) onRead reports (bytes read so far, total or -1) so each caller maps it onto ITS progress bar.
func (n *node) fetchViaGateway(ctx context.Context, root cid.Cid, fromByte int64, onRead func(read, total int64)) error {
	if len(trustlessGateways) == 0 {
		return fmt.Errorf("no trustless gateways configured")
	}
	hc := dohStreamingClient(newDoHResolver()) // DoH-resolving, NO whole-request timeout (large CARs stream for minutes)
	rctx, rcancel := context.WithCancel(ctx)
	defer rcancel()
	type report struct {
		gw   string
		cand *carCandidate
		err  error
	}
	ch := make(chan report, len(trustlessGateways))               // buffered: a late loser never blocks after we return
	cancels := make([]context.CancelFunc, len(trustlessGateways)) // per candidate, so losers die AT commit
	open := func(i int) {
		cctx, ccancel := context.WithCancel(rctx)
		cancels[i] = ccancel
		gw := trustlessGateways[i]
		safeGo("gateway.hedge", func() {
			// Aggregate open bound: the transport's ResponseHeaderTimeout is PER HOP and the public routes are
			// redirect chains, so a stalling chain could cost hops × the timeout. This caps the whole open —
			// resolve, dial, every redirect hop, headers, first block — at one gatewayHeaderTimeout. Stopped the
			// moment the open returns, so a winner's long stream is never touched.
			var timedOut atomic.Bool
			tm := time.AfterFunc(gatewayHeaderTimeout, func() { timedOut.Store(true); ccancel() })
			c, err := n.openCarCandidate(cctx, hc, gw, root, fromByte)
			tm.Stop()
			if err != nil && timedOut.Load() {
				err = fmt.Errorf("open exceeded gatewayHeaderTimeout (%s): %w", gatewayHeaderTimeout, err)
			}
			ch <- report{gw, c, err}
		})
	}
	open(0)
	fired := 1
	fireRest := func() {
		for ; fired < len(trustlessGateways); fired++ {
			open(fired)
		}
	}
	hedge := time.NewTimer(gatewayHedgeDelay)
	defer hedge.Stop()

	received := 0
	var lastErr error
	for received < len(trustlessGateways) {
		var r report
		select {
		case r = <-ch:
		case <-hedge.C:
			fireRest()
			continue
		case <-rctx.Done():
			return rctx.Err()
		}
		received++
		if r.err != nil {
			fdbg("gateway %s failed for %s: %v", r.gw, shortCid(root), r.err)
			lastErr = r.err
			if fired < len(trustlessGateways) { // the primary failed fast → don't wait for the hedge timer
				fireRest()
			}
			continue
		}
		// COMMIT: this route proved itself. Every other candidate is cancelled NOW (its own context — one still
		// waiting on headers dies here, not when the winner's stream ends minutes later) and drained as it reports.
		fdbg("gateway %s won the race for %s — streaming", r.gw, shortCid(root))
		for i, cf := range cancels {
			if cf != nil && trustlessGateways[i] != r.gw {
				cf()
			}
		}
		remaining := fired - received
		safeGo("gateway.drainLosers", func() {
			for i := 0; i < remaining; i++ {
				if l := <-ch; l.cand != nil {
					l.cand.cancel()
					_ = l.cand.body.Close()
				}
			}
		})
		if fired < len(trustlessGateways) { // never fired the rest → nothing to drain there; stop the timer path
			fired = len(trustlessGateways)
		}
		err := n.streamCarCandidate(r.cand, root, onRead)
		r.cand.cancel()
		_ = r.cand.body.Close()
		if err != nil {
			fdbg("gateway %s stream failed for %s after winning: %v", r.gw, shortCid(root), err)
			return err // the outer fetch loop retries the attempt; blocks already imported persist
		}
		fdbg("gateway %s served %s — DAG imported over HTTPS", r.gw, shortCid(root))
		return nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no gateway produced a verified block for %s", shortCid(root))
	}
	return lastErr
}

// openCarCandidate opens a gateway's CAR stream and reads + verifies its FIRST block. It returns a candidate only when
// that block is real; a non-200, a CAR header error, an EMPTY body ("200 with no bytes") or a CID mismatch all fail
// here, so the race can never commit to a route that isn't serving. Bounded by ctx + the transport's header timeout.
func (n *node) openCarCandidate(ctx context.Context, hc *http.Client, gw string, root cid.Cid, fromByte int64) (*carCandidate, error) {
	gctx, cancel := context.WithCancel(ctx)
	url := gw + "/ipfs/" + root.String() + "?format=car&car-order=dfs&" + carScopeQuery(fromByte)
	req, err := http.NewRequestWithContext(gctx, http.MethodGet, url, nil)
	if err != nil {
		cancel()
		return nil, err
	}
	req.Header.Set("Accept", "application/vnd.ipld.car")
	req.Header.Set("User-Agent", "vidyagod")
	resp, err := hc.Do(req)
	if err != nil {
		cancel()
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		cancel()
		return nil, gatewayStatusError{resp.StatusCode}
	}
	lastRead := &atomic.Int64{}
	lastRead.Store(time.Now().UnixNano())
	body := &countingReader{r: resp.Body, onRead: func(int) { lastRead.Store(time.Now().UnixNano()) }}
	br, err := car.NewBlockReader(body)
	if err != nil {
		_ = resp.Body.Close()
		cancel()
		return nil, fmt.Errorf("car header: %w", err)
	}
	first, err := br.Next()
	if err != nil {
		_ = resp.Body.Close()
		cancel()
		if err == io.EOF {
			return nil, fmt.Errorf("empty CAR")
		}
		return nil, fmt.Errorf("first block: %w", err)
	}
	if computed, cerr := first.Cid().Prefix().Sum(first.RawData()); cerr != nil || !computed.Equals(first.Cid()) {
		_ = resp.Body.Close()
		cancel()
		return nil, fmt.Errorf("block %s failed CID verification", first.Cid())
	}
	// Size: Content-Length if the route sent one; otherwise streamCarCandidate probes it AFTER commit, concurrently
	// with the stream (a HEAD here would sit between the verified first block and the report, delaying the commit
	// and holding the body unread for up to its 15 s cap).
	c := &carCandidate{gw: gw, hc: hc, fromByte: fromByte, body: resp.Body, br: br, first: first, lastRead: lastRead, cancel: cancel}
	c.total.Store(resp.ContentLength)
	return c, nil
}

// streamCarCandidate imports the winning candidate's CAR: its already-verified first block, then the rest, each block
// CID-verified (trustless), under a stall watchdog (no bytes for stallTimeout → abort so the outer loop retries).
// carScopeQuery is the trustless-gateway scope for a fetch: the whole DAG, or the entity's bytes from an offset on.
func carScopeQuery(fromByte int64) string {
	if fromByte < 0 {
		return "dag-scope=all"
	}
	return fmt.Sprintf("dag-scope=entity&entity-bytes=%d:*", fromByte)
}

func (n *node) streamCarCandidate(c *carCandidate, root cid.Cid, onRead func(read, total int64)) error {
	sctx, scancel := context.WithCancel(context.Background())
	defer scancel()
	safeGo("gateway.fetchWatchdog", func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-sctx.Done():
				return
			case <-t.C:
				if time.Since(time.Unix(0, c.lastRead.Load())) > stallTimeout {
					c.cancel()
					return
				}
			}
		}
	})
	if c.total.Load() <= 0 && c.fromByte < 0 { // whole-file fetch with no Content-Length: probe the size alongside
		safeGo("gateway.sizeProbe", func() {
			if sz := headSize(sctx, c.hc, c.gw+"/ipfs/"+root.String()); sz > 0 {
				c.total.Store(sz)
				fdbg("gateway CAR %s: size probe=%d → progress ON", shortCid(root), sz)
			}
		})
	}
	fdbg("gateway CAR %s: size=%d (progress %s)", shortCid(root), c.total.Load(), map[bool]string{true: "ON", false: "indeterminate until the size probe answers"}[c.total.Load() > 0])
	var read int64
	put := func(blk blocks.Block) error {
		sz := int64(len(blk.RawData()))
		read += sz
		if n.bwc != nil {
			n.bwc.LogRecvMessage(sz) // gateway bytes are downloads too: they count on the global ↓ speedometer
		}
		if onRead != nil {
			onRead(read, c.total.Load())
		}
		return n.bstore.Put(sctx, blk)
	}
	if err := put(c.first); err != nil {
		return err
	}
	got := 1
	for {
		blk, err := c.br.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		// Trustless verification: the block bytes must hash to the CID that names them.
		if computed, cerr := blk.Cid().Prefix().Sum(blk.RawData()); cerr != nil || !computed.Equals(blk.Cid()) {
			return fmt.Errorf("block %s failed CID verification", blk.Cid())
		}
		if err := put(blk); err != nil {
			return err
		}
		got++
	}
	if has, herr := n.bstore.Has(sctx, root); herr != nil || !has {
		return fmt.Errorf("CAR (%d blocks) did not include the requested root", got)
	}
	return nil
}
