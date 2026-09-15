package main

// netpath_test.go — the two things that make the network-path matrix (tools/netpaths.sh) trustworthy:
//
//  1. The HTTPS-gateway fallback in getRoot must actually RUN when libp2p gives up. For months it could not: it was
//     handed the context the libp2p attempt had just exhausted, so every gateway request died "context deadline
//     exceeded" before a byte was sent — the field "will sync when online, node is up" bug. The oracle here is an
//     httptest gateway that only COUNTS requests: under the fix it is reached, under the bug it never is.
//  2. VG_BENCH_BLOCK_HOSTS is the external DNS-layer block the matrix relies on to switch a path off; its suffix
//     match and hard refusal (no OS fallback) are pinned so a row can't silently pass through a "blocked" host.

import (
	"context"
	"crypto/rand"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	merkledag "github.com/ipfs/boxo/ipld/merkledag"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	carblockstore "github.com/ipld/go-car/v2/blockstore"
	libp2p "github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	crypto "github.com/libp2p/go-libp2p/core/crypto"
	peer "github.com/libp2p/go-libp2p/core/peer"
	routing "github.com/libp2p/go-libp2p/core/routing"
	swarm "github.com/libp2p/go-libp2p/p2p/net/swarm"
	ma "github.com/multiformats/go-multiaddr"
	"net"
)

// The gateway fallback must be handed a LIVE context after libp2p exhausts its own. Teeth: in getRoot, pass libCtx
// (the spent libp2p context) to fetchViaGateway instead of gwCtx and the fake gateway records ZERO requests.
func TestGatewayFallbackRunsAfterLibp2pExhaustsItsOwnDeadline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var hits atomic.Int32
	gw := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusInternalServerError) // reaching us is the whole proof; no CAR needed
	}))
	defer gw.Close()

	origGWs, origTimeout := trustlessGateways, rootLibp2pTimeout
	trustlessGateways = []string{gw.URL}
	rootLibp2pTimeout = 300 * time.Millisecond // libp2p phase: a peerless bitswap Get blocks until THIS expires
	defer func() { trustlessGateways, rootLibp2pTimeout = origGWs, origTimeout }()

	_, bs, stop := peerlessBitswapDserv(t, ctx)
	defer stop()
	// dht non-nil selects the online branch that reaches the gateway fallback; getRoot never dereferences it.
	n := &node{ctx: ctx, dserv: merkledag.NewDAGService(bs), dht: &dht.IpfsDHT{}}
	absent := blocks.NewBlock([]byte("nobody provides this over libp2p")).Cid()

	start := time.Now()
	_, err := n.getRoot(ctx, absent, absent.String(), nil)
	if err == nil {
		t.Fatal("getRoot must fail: libp2p has no provider and the gateway answers 500")
	}
	if got := hits.Load(); got < 1 {
		t.Fatalf("the gateway fallback never sent a request after libp2p timed out (%s) — it was run under the spent libp2p context", time.Since(start))
	}
}

// VG_BENCH_BLOCK_HOSTS must refuse the host AND its subdomains (a dnsaddr bootstrap lookup is _dnsaddr.<host>), and
// must not bleed into unrelated names. Teeth: drop the HasSuffix arm → the subdomain assertion fails; match on
// Contains instead → the unrelated-name assertion fails.
func TestBenchBlockedHostsSuffixMatchAndRefusal(t *testing.T) {
	t.Setenv("VG_BENCH_BLOCK_HOSTS", "bootstrap.libp2p.io, Gateway.Pinata.Cloud.")
	for _, h := range []string{"bootstrap.libp2p.io", "_dnsaddr.bootstrap.libp2p.io", "gateway.pinata.cloud", "GATEWAY.PINATA.CLOUD."} {
		if !benchHostBlocked(h) {
			t.Errorf("%q should be blocked", h)
		}
	}
	for _, h := range []string{"libp2p.io", "notbootstrap.libp2p.io.evil", "delegated-ipfs.dev", "pinata.cloud"} {
		if benchHostBlocked(h) {
			t.Errorf("%q must NOT be blocked", h)
		}
	}
	// The resolver refuses outright — no DoH query, no OS fallback — so a blocked path fails fast and deterministically.
	d := newDoHResolver()
	if _, err := d.LookupIPAddr(context.Background(), "gateway.pinata.cloud"); err == nil {
		t.Error("LookupIPAddr of a blocked host must return an error, not fall back to the OS resolver")
	}
	if _, err := d.LookupTXT(context.Background(), "_dnsaddr.bootstrap.libp2p.io"); err == nil {
		t.Error("LookupTXT of a blocked dnsaddr host must return an error")
	}
	os.Unsetenv("VG_BENCH_BLOCK_HOSTS")
}

// The HTTP transport must REFUSE a blocked host outright. Its DoH-failure fallback re-dials the bare hostname via the
// OS resolver — right when DoH is down, but it punched straight through a deliberate block (the dht-only matrix row
// saw the "blocked" delegated indexer still answering). Hermetic: the host is an RFC 2606 .invalid name, so a mutant
// that falls through gets a fast NXDOMAIN error, not the "blocked" one this asserts. Teeth: delete the
// benchHostBlocked refusal in dohTransport's DialContext.
func TestDohTransportRefusesBlockedHostInsteadOfFallingBack(t *testing.T) {
	t.Setenv("VG_BENCH_BLOCK_HOSTS", "indexer.example.invalid")
	hc := dohHTTPClient(newDoHResolver())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "https://indexer.example.invalid/routing/v1/providers/x", nil)
	_, err := hc.Do(req)
	if err == nil || !strings.Contains(err.Error(), "blocked (VG_BENCH_BLOCK_HOSTS)") {
		t.Fatalf("a blocked host must be refused by the transport, not re-dialed via the OS resolver; got: %v", err)
	}
}

// H-C: the DoH lookup must RACE its endpoints — a dead first endpoint must cost nothing, and a good later one must
// win promptly. Teeth: make query try endpoints sequentially (or only the first) and this waits on the 2 s dead
// endpoint per record type (A + AAAA = 4 s) and trips the 1.5 s bound — or never reaches the good one at all.
func TestDoHRacesEndpointsSoADeadResolverCostsNothing(t *testing.T) {

	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done(): // cancelled by the winner — exactly what should happen
		case <-time.After(2 * time.Second):
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer dead.Close()
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/dns-json")
		fmt.Fprint(w, `{"Answer":[{"type":1,"data":"203.0.113.7"}]}`)
	}))
	defer good.Close()

	d := &dohResolver{endpoints: []string{dead.URL, good.URL}, hc: &http.Client{Timeout: 5 * time.Second}}
	start := time.Now()
	ips, err := d.LookupIPAddr(context.Background(), "hedge.example.invalid")
	el := time.Since(start)
	if err != nil || len(ips) == 0 || ips[0].IP.String() != "203.0.113.7" {
		t.Fatalf("expected the good endpoint's answer 203.0.113.7, got ips=%v err=%v", ips, err)
	}
	if el > 1500*time.Millisecond {
		t.Fatalf("lookup took %s — the dead endpoint was waited on instead of raced", el)
	}
}

// fakeRouter: a ContentDiscovery that (optionally) sits on the lookup for `delay`, then (optionally) yields one provider.
type fakeRouter struct {
	delay   time.Duration
	provide bool
}

func (f fakeRouter) FindProvidersAsync(ctx context.Context, _ cid.Cid, _ int) <-chan peer.AddrInfo {
	ch := make(chan peer.AddrInfo)
	go func() {
		defer close(ch)
		if f.delay > 0 {
			select {
			case <-time.After(f.delay):
			case <-ctx.Done():
				return
			}
		}
		if f.provide {
			select {
			case ch <- peer.AddrInfo{}:
			case <-ctx.Done():
			}
		}
	}()
	return ch
}

// H-D rests on this: a dead/slow router must NOT delay a live one, or racing a second indexer would cost rather than
// save. Teeth: drain combinedFinder's routers sequentially instead of fanning out and the dead first router holds
// the live one's answer for its full 3 s → the 1 s bound trips.
func TestCombinedFinderALiveRouterIsNotDelayedByADeadOne(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cf := combinedFinder{routers: []routing.ContentDiscovery{fakeRouter{delay: 3 * time.Second}, fakeRouter{provide: true}}}
	start := time.Now()
	got := 0
	for range cf.FindProvidersAsync(ctx, cid.Cid{}, 10) {
		got++
		break // first answer is all we need
	}
	if got == 0 {
		t.Fatal("the live router's provider never arrived")
	}
	if el := time.Since(start); el > time.Second {
		t.Fatalf("first provider took %s — the dead router delayed the live one (no fan-out)", el)
	}
}

// H-E: the gateway fetch is HEDGED and commits only to a route that delivers a VERIFIED first block. Primary = a
// dead route (sleeps, then 520); second = the empty-200 trap the matrix caught; third = a real CAR. Two independent
// teeth: (timing) a sequential fetch waits out the dead primary → the bound trips; (commit rule) committing on "200"
// instead of "verified first block" picks the empty route → "empty CAR" → the success assertion fails.
func TestGatewayFetchHedgesAndCommitsOnlyToAVerifiedFirstBlock(t *testing.T) {
	n := offlineNode(t) // real bstore for the import + the root-present check
	ctx := context.Background()

	// a real one-block DAG + its CAR bytes (go-car v2 blockstore writes a valid CARv1)
	blk := blocks.NewBlock([]byte("the real content behind the CID"))
	carPath := t.TempDir() + "/one.car"
	rw, err := carblockstore.OpenReadWrite(carPath, []cid.Cid{blk.Cid()}, carblockstore.WriteAsCarV1(true))
	if err != nil {
		t.Fatal(err)
	}
	if err := rw.Put(ctx, blk); err != nil {
		t.Fatal(err)
	}
	if err := rw.Finalize(); err != nil {
		t.Fatal(err)
	}
	carBytes, err := os.ReadFile(carPath)
	if err != nil {
		t.Fatal(err)
	}

	// a HEADER-ONLY CAR (valid header, zero blocks): a 0-byte body dies at the header parse, but this one reaches
	// the first-block rule — the trap that actually pins "commit only on a verified block".
	hdrPath := t.TempDir() + "/hdr.car"
	hw, err := carblockstore.OpenReadWrite(hdrPath, []cid.Cid{blk.Cid()}, carblockstore.WriteAsCarV1(true))
	if err != nil {
		t.Fatal(err)
	}
	if err := hw.Finalize(); err != nil {
		t.Fatal(err)
	}
	hdrBytes, err := os.ReadFile(hdrPath)
	if err != nil {
		t.Fatal(err)
	}
	headerOnly := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.ipld.car")
		_, _ = w.Write(hdrBytes)
	}))
	defer headerOnly.Close()

	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done(): // cancelled by the winner — correct
		case <-time.After(3 * time.Second):
			w.WriteHeader(520)
		}
	}))
	defer dead.Close()
	empty := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.ipld.car")
		w.WriteHeader(http.StatusOK) // 200 with NO body — the trap
	}))
	defer empty.Close()
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)                         // deterministically SLOWER than the empty route: if an empty body were ever
		w.Header().Set("Content-Type", "application/vnd.ipld.car") // accepted as a candidate, it would always win
		_, _ = w.Write(carBytes)
	}))
	defer good.Close()

	origGWs, origHedge := trustlessGateways, gatewayHedgeDelay
	trustlessGateways = []string{dead.URL, empty.URL, headerOnly.URL, good.URL} // every trap is faster than good
	gatewayHedgeDelay = 150 * time.Millisecond
	defer func() { trustlessGateways, gatewayHedgeDelay = origGWs, origHedge }()

	start := time.Now()
	err = n.fetchViaGateway(ctx, blk.Cid(), -1, nil)
	el := time.Since(start)
	if err != nil {
		t.Fatalf("the hedged fetch must succeed via the good route, got: %v", err)
	}
	if el > 1500*time.Millisecond {
		t.Fatalf("took %s — the dead primary was waited on instead of hedged past", el)
	}
	if has, _ := n.bstore.Has(ctx, blk.Cid()); !has {
		t.Fatal("the winning route's block was not imported")
	}
}

// H-F: libp2p BACKS A PEER OFF after a failed dial (seconds, growing), so a provider that refused ONE dial is silently
// not re-dialed on the next fetch attempt(s) — the probe measured two dead 30 s attempts against Pinata's wss peer
// after a single transient failure. Every fetch attempt is a deliberate retry, so it must get a FRESH dial. Teeth:
// make clearDialBackoff a no-op and the backoff survives the call.
func TestClearDialBackoffLetsARetryRedialAProviderThatRefusedOnce(t *testing.T) {
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"), libp2p.DisableRelay())
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	sw, ok := h.Network().(*swarm.Swarm)
	if !ok {
		t.Fatal("test premise: a basic host's network is a *swarm.Swarm")
	}
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	pid, _ := peer.IDFromPrivateKey(priv)
	l, err := net.Listen("tcp", "127.0.0.1:0") // grab a port, then close it → a peer that REFUSES
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	addr := ma.StringCast(fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", port))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := h.Connect(ctx, peer.AddrInfo{ID: pid, Addrs: []ma.Multiaddr{addr}}); err == nil {
		t.Fatal("a dial to a closed port must fail")
	}
	if !sw.Backoff().Backoff(pid, addr) {
		t.Fatal("test premise: libp2p backs the peer off after the failed dial")
	}
	n := &node{host: h}
	n.clearDialBackoff(pid)
	if sw.Backoff().Backoff(pid, addr) {
		t.Fatal("clearDialBackoff must lift the dial backoff so the next fetch attempt actually re-dials the provider")
	}
}
