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
	bsnetwork "github.com/ipfs/boxo/bitswap/network"
	bsnet "github.com/ipfs/boxo/bitswap/network/bsnet"
	blockservice "github.com/ipfs/boxo/blockservice"
	blockstore "github.com/ipfs/boxo/blockstore"
	offline "github.com/ipfs/boxo/exchange/offline"
	merkledag "github.com/ipfs/boxo/ipld/merkledag"
	blocks "github.com/ipfs/go-block-format"
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

// Kinds of other provider.
const (
	providerSilent    = iota // speaks bitswap, never answers
	providerNoBitswap        // does not speak bitswap
	providerPromiser         // answers HAVE for everything the seeder has, never delivers (bitswap.pinata.cloud)
)

// promiseStore says it has what inner has, and never hands a block over.
type promiseStore struct{ blockstore.Blockstore }

func (s promiseStore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// setupBlackhole: seeder A, provider H of the given kind, client B whose finder names H first for every CID; B's
// network is wrapped in the quarantine when quarantine is set.
func setupBlackhole(t *testing.T, ctx context.Context, payloadSize int, kind int, quarantine bool) blackholeRig {
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

	var bswapH *bitswap.Bitswap
	switch kind {
	case providerSilent: // it accepts every stream and reads every message, and never answers
		for _, p := range []protocol.ID{bsnet.ProtocolBitswap, bsnet.ProtocolBitswapOneOne, bsnet.ProtocolBitswapOneZero, bsnet.ProtocolBitswapNoVers} {
			hostH.SetStreamHandler(p, func(s network.Stream) { _, _ = io.Copy(io.Discard, s) })
		}
	case providerPromiser:
		bswapH = bitswap.New(ctx, bsnet.NewFromIpfsHost(hostH), nilFinder{}, promiseStore{bstoreA}, bitswapOptions(nil)...)
	}

	bstoreB := blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore()))
	finder := fixedFinder{{ID: hostH.ID()}, {ID: hostA.ID()}}
	var netB bsnetwork.BitSwapNetwork = bsnet.NewFromIpfsHost(hostB)
	if quarantine {
		q := newQuarantineNet(netB)
		go q.run(ctx)
		netB = q
	}
	bswapB := bitswap.New(ctx, netB, finder, bstoreB, bitswapOptions(nil)...)
	if err := mn.ConnectAllButSelf(); err != nil {
		t.Fatal(err)
	}
	// A busy seeder: its first answer comes after the session's first idle tick (1 s), so the session asks the finder
	// and takes on the other provider — as the replication's receiver, busy with other files, did.
	for _, l := range mn.LinksBetweenPeers(hostA.ID(), hostB.ID()) {
		l.SetOptions(mocknet.LinkOptions{Latency: 600 * time.Millisecond})
	}
	sess := blockservice.NewSession(ctx, blockservice.New(bstoreB, bswapB))
	return blackholeRig{sess: sess, leaves: leaves, stop: func() {
		bswapB.Close()
		if bswapH != nil {
			bswapH.Close()
		}
		bswapA.Close()
		_ = mn.Close()
	}}
}

// Every leaf arrives from the seeder, inside the fetch stall watchdog, although the finder names another provider
// first for every CID — one that never answers, or one that does not speak bitswap at all (both seen on the
// replication: a public pinning node over wss, and indexer hits). The session routes some want-blocks there; the
// DONT_HAVE timeout (silent) or the unresponsive-peer disconnect (no bitswap) must hand them to the seeder. Teeth:
// bitswap.SetSimulateDontHavesOnTimeout(false) in bitswapOptions and the silent provider strands its wants.
func TestAProviderThatNeverAnswersDoesNotStrandWants(t *testing.T) {
	for _, kind := range []int{providerSilent, providerNoBitswap} {
		t.Run(map[int]string{providerSilent: "silent", providerNoBitswap: "no-bitswap"}[kind], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			rig := setupBlackhole(t, ctx, 32<<20, kind, false) // 128 leaves
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

// A provider that answers HAVE for every block and never delivers (bitswap.pinata.cloud on the replication) strands
// the wants a session gives it: its HAVE outranks the busy seeder's, the want-block goes to it, the DONT_HAVE timeout
// does not undo its HAVE, and the want-block sent again is dropped as already sent — no new timeout. The quarantine
// takes it out of the client's rotation once it has sat on want-blocks for quarantineAfter, and boxo re-routes every
// want it held to the seeder. Teeth: build the client without the quarantine (quarantine=false) and leaves strand.
func TestAProviderThatPromisesAndNeverDeliversIsQuarantined(t *testing.T) {
	a, c, tk := quarantineAfter, quarantineCool, quarantineTick
	quarantineAfter, quarantineCool, quarantineTick = 2*time.Second, time.Minute, 200*time.Millisecond
	defer func() { quarantineAfter, quarantineCool, quarantineTick = a, c, tk }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rig := setupBlackhole(t, ctx, 32<<20, providerPromiser, true)
	defer rig.stop()
	fctx, fcancel := context.WithTimeout(ctx, stallTimeout)
	defer fcancel()
	got := map[cid.Cid]bool{}
	for b := range rollingGetBlocks(fctx, rig.sess, rig.leaves, 32) {
		got[b.Cid()] = true
	}
	if len(got) != len(rig.leaves) {
		t.Fatalf("%d of %d leaves arrived within %s (the rest stranded on the provider that never delivers)", len(got), len(rig.leaves), stallTimeout)
	}
}
