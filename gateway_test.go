package main

import (
	"context"
	"testing"
	"time"

	cid "github.com/ipfs/go-cid"
)

// TestGatewayFetchImports checks the HTTPS trustless-gateway fallback pulls a CID's DAG as a verified CAR and lands it
// in the local blockstore. Uses a tiny, universally-pinned CID (the empty UnixFS directory) so it's fast; the first
// gateway that doesn't have it (Pinata) 404s and it falls through to a public one. Network-gated: skips if offline, so
// it never flakes the suite. Runs on an OFFLINE node — fetchViaGateway uses HTTPS, not the libp2p network.
func TestGatewayFetchImports(t *testing.T) {
	n := offlineNode(t)
	c, err := cid.Decode("QmUNLLsPACCz1vLxQVkXqqLX5R1X345qqfHbsf67hvA3Nn") // empty UnixFS dir
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	if err := n.fetchViaGateway(ctx, c, nil); err != nil {
		t.Skipf("no trustless gateway reachable (offline?): %v", err)
	}
	has, err := n.bstore.Has(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	if !has {
		t.Error("root block not in the blockstore after a successful gateway import")
	}
}
