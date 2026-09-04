package main

// gateway.go — HTTPS trustless-gateway fallback. When libp2p can't reach a provider (hostile / captive networks that
// filter DNS + block or mangle non-443 transports — Pinata only offers plain ws:3000, which such networks break), fetch
// the CID's whole DAG as a CAR over HTTPS/443 from a trustless gateway and load it into the blockstore, so the normal
// materialize path (writeThrough / files.WriteTo) completes offline. HTTPS/443 is the one thing these networks always
// allow. Trustless: every block is verified against its CID before it's stored, so gateways are untrusted mirrors.

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"sync/atomic"
	"time"

	cid "github.com/ipfs/go-cid"
	car "github.com/ipld/go-car/v2"
)

// trustlessGateways: Pinata's own gateway first (our content is pinned there), then reliable public path gateways.
var trustlessGateways = []string{
	"https://gateway.pinata.cloud",
	"https://ipfs.io",
	"https://trustless-gateway.link",
	"https://dweb.link",
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

// fetchViaGateway pulls a CID's whole DAG as a verified CAR over HTTPS from the first working trustless gateway and
// stores the blocks locally. onProgress (0..99) tracks the CAR download. Returns nil once a gateway delivers the DAG.
func (n *node) fetchViaGateway(ctx context.Context, root cid.Cid, onProgress func(pct float64)) error {
	hc := dohStreamingClient(newDoHResolver()) // DoH-resolving, NO whole-request timeout (large CARs stream for minutes)
	var lastErr error
	for _, gw := range trustlessGateways {
		if err := n.fetchCarFromGateway(ctx, hc, gw, root, onProgress); err != nil {
			fdbg("gateway %s failed for %s: %v", gw, shortCid(root), err)
			lastErr = err
			continue
		}
		fdbg("gateway %s served %s — DAG imported over HTTPS", gw, shortCid(root))
		return nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no trustless gateways configured")
	}
	return lastErr
}

func (n *node) fetchCarFromGateway(ctx context.Context, hc *http.Client, gw string, root cid.Cid, onProgress func(pct float64)) error {
	// No fixed duration cap — a big CAR streams for minutes. Bound it by a STALL watchdog (abort only if bytes stop),
	// like the bitswap fetcher, so a slow-but-progressing large download keeps going instead of dying at a deadline.
	gctx, cancel := context.WithCancel(ctx)
	defer cancel()
	url := gw + "/ipfs/" + root.String() + "?format=car&dag-scope=all&car-order=dfs"
	req, err := http.NewRequestWithContext(gctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/vnd.ipld.car")
	req.Header.Set("User-Agent", "vidyagod")
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", resp.StatusCode)
	}

	// Stall watchdog: tear the transfer down if no bytes arrive for stallTimeout (a dead/hung gateway), so we move on
	// to the next one instead of hanging. Reset on every read.
	var lastRead atomic.Int64
	lastRead.Store(time.Now().UnixNano())
	safeGo("gateway.fetchWatchdog", func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-gctx.Done():
				return
			case <-t.C:
				if time.Since(time.Unix(0, lastRead.Load())) > stallTimeout {
					cancel()
					return
				}
			}
		}
	})

	// The CAR stream carries no Content-Length (chunked), so probe the file size with a HEAD for a real progress bar.
	// The CAR is a touch larger than the file (block framing) so cap the ratio at 99; the finalize step lands it at 100.
	total := resp.ContentLength
	if total <= 0 {
		total = headSize(gctx, hc, gw+"/ipfs/"+root.String())
	}
	fdbg("gateway CAR %s: size probe=%d (progress %s)", shortCid(root), total, map[bool]string{true: "ON", false: "indeterminate"}[total > 0])
	var read int64
	body := &countingReader{r: resp.Body, onRead: func(nr int) {
		lastRead.Store(time.Now().UnixNano())
		read += int64(nr)
		if onProgress != nil && total > 0 {
			onProgress(math.Min(99, 100.0*float64(read)/float64(total)))
		}
	}}

	br, err := car.NewBlockReader(body)
	if err != nil {
		return err
	}
	got := 0
	for {
		blk, err := br.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		// Trustless verification: the block bytes must hash to the CID that names them.
		computed, cerr := blk.Cid().Prefix().Sum(blk.RawData())
		if cerr != nil || !computed.Equals(blk.Cid()) {
			return fmt.Errorf("block %s failed CID verification", blk.Cid())
		}
		if err := n.bstore.Put(gctx, blk); err != nil {
			return err
		}
		got++
	}
	if got == 0 {
		return fmt.Errorf("empty CAR")
	}
	if has, herr := n.bstore.Has(gctx, root); herr != nil || !has {
		return fmt.Errorf("CAR did not include the requested root")
	}
	return nil
}
