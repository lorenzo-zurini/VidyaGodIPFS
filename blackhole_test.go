package main

// blackhole_test.go — a provider that says it has the content and never sends it (a public pinning node that
// advertises a file and does not answer: the replication's Dino Crisis stall) must not strand the wants a session
// happens to route to it while a peer that DOES serve is connected.

import (
	"context"
	"io"
	"testing"
	"time"

	"crypto/rand"
	bitswap "github.com/ipfs/boxo/bitswap"
	bsnet "github.com/ipfs/boxo/bitswap/network/bsnet"
	blockservice "github.com/ipfs/boxo/blockservice"
	blockstore "github.com/ipfs/boxo/blockstore"
	offline "github.com/ipfs/boxo/exchange/offline"
	merkledag "github.com/ipfs/boxo/ipld/merkledag"
	cid "github.com/ipfs/go-cid"
	datastore "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	network "github.com/libp2p/go-libp2p/core/network"
	peer "github.com/libp2p/go-libp2p/core/peer"
	protocol "github.com/libp2p/go-libp2p/core/protocol"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
)

// fixedFinder names the same providers for every CID.
type fixedFinder []peer.AddrInfo

func (f fixedFinder) FindProvidersAsync(ctx context.Context, _ cid.Cid, _ int) <-chan peer.AddrInfo {
	ch := make(chan peer.AddrInfo, len(f))
	for _, p := range f {
		ch <- p
	}
	close(ch)
	return ch
}

type blackholeRig struct {
	sess   *blockservice.Session
	leaves []cid.Cid
	stop   func()
}

// setupBlackhole: seeder A, provider H, client B whose finder names H first for every CID. H speaks bitswap and never
// answers (speaks=true), or does not speak bitswap at all (speaks=false).
func setupBlackhole(t *testing.T, ctx context.Context, payloadSize int, speaks bool) blackholeRig {
	t.Helper()
	mn, err := mocknet.FullMeshLinked(3)
	if err != nil {
		t.Fatal(err)
	}
	mn.SetLinkDefaults(mocknet.LinkOptions{Latency: 5 * time.Millisecond})
	hosts := mn.Hosts()
	hostA, hostH, hostB := hosts[0], hosts[1], hosts[2]

	bstoreA := blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore()))
	dservA := merkledag.NewDAGService(blockservice.New(bstoreA, offline.Exchange(bstoreA)))
	payload := make([]byte, payloadSize)
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}
	_, leaves := buildLeafDAG(t, dservA, payload)
	bswapA := bitswap.New(ctx, bsnet.NewFromIpfsHost(hostA), nilFinder{}, bstoreA, bitswapOptions(nil)...) // the seeder as configured

	if speaks { // it accepts every stream and reads every message, and never answers
		for _, p := range []protocol.ID{bsnet.ProtocolBitswap, bsnet.ProtocolBitswapOneOne, bsnet.ProtocolBitswapOneZero, bsnet.ProtocolBitswapNoVers} {
			hostH.SetStreamHandler(p, func(s network.Stream) { _, _ = io.Copy(io.Discard, s) })
		}
	}

	bstoreB := blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore()))
	finder := fixedFinder{{ID: hostH.ID()}, {ID: hostA.ID()}}
	bswapB := bitswap.New(ctx, bsnet.NewFromIpfsHost(hostB), finder, bstoreB, bitswapOptions(nil)...)
	if err := mn.ConnectAllButSelf(); err != nil {
		t.Fatal(err)
	}
	// A busy seeder: its first answer comes after the session's first idle tick (1 s), so the session asks the finder
	// and takes on the other provider — as the replication's receiver, busy with other files, did.
	for _, l := range mn.LinksBetweenPeers(hostA.ID(), hostB.ID()) {
		l.SetOptions(mocknet.LinkOptions{Latency: 600 * time.Millisecond})
	}
	sess := blockservice.NewSession(ctx, blockservice.New(bstoreB, bswapB))
	return blackholeRig{sess: sess, leaves: leaves, stop: func() { bswapB.Close(); bswapA.Close(); _ = mn.Close() }}
}

// Every leaf arrives from the seeder, inside the fetch stall watchdog, although the finder names another provider
// first for every CID — one that never answers, or one that does not speak bitswap at all (both seen on the
// replication: a public pinning node over wss, and indexer hits). The session routes some want-blocks there; the
// DONT_HAVE timeout (silent) or the unresponsive-peer disconnect (no bitswap) must hand them to the seeder. Teeth:
// bitswap.SetSimulateDontHavesOnTimeout(false) in bitswapOptions and the silent provider strands its wants.
func TestAProviderThatNeverAnswersDoesNotStrandWants(t *testing.T) {
	for _, speaks := range []bool{true, false} {
		t.Run(map[bool]string{true: "silent", false: "no-bitswap"}[speaks], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			rig := setupBlackhole(t, ctx, 32<<20, speaks) // 128 leaves
			defer rig.stop()
			fctx, fcancel := context.WithTimeout(ctx, stallTimeout)
			defer fcancel()
			got := map[cid.Cid]bool{}
			for b := range rollingGetBlocks(fctx, rig.sess, rig.leaves, 32) {
				got[b.Cid()] = true
			}
			if len(got) != len(rig.leaves) {
				t.Fatalf("%d of %d leaves arrived within %s (the rest stranded on the other provider)", len(got), len(rig.leaves), stallTimeout)
			}
		})
	}
}
