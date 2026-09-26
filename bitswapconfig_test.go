package main

// bitswapconfig_test.go — the node's resource-manager configuration (online.go resourceLimits), exercised as
// configured: a seeder streaming blocks to us must not be refused the streams it sends them on.

import (
	"testing"

	bsnet "github.com/ipfs/boxo/bitswap/network/bsnet"
	network "github.com/libp2p/go-libp2p/core/network"
	peer "github.com/libp2p/go-libp2p/core/peer"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
)

// A seeder sends every blocks message on a new stream, so at link speed one peer holds many inbound bitswap streams
// at once. All of them must be admitted up to the PEER's allowance (256+), not refused at the per-protocol default
// (64), where each refusal was a lost block. Teeth: drop the AddProtocolPeerLimit loop in resourceLimits and the
// 65th stream is refused.
func TestBitswapStreamsGetThePeersWholeAllowance(t *testing.T) {
	rm, err := rcmgr.NewResourceManager(rcmgr.NewFixedLimiter(resourceLimits()))
	if err != nil {
		t.Fatal(err)
	}
	defer rm.Close()
	p := peer.ID("seeder")
	var open []network.StreamManagementScope
	defer func() {
		for _, s := range open {
			s.Done()
		}
	}()
	for i := 0; i < 200; i++ {
		s, err := rm.OpenStream(p, network.DirInbound)
		if err != nil {
			t.Fatalf("inbound stream %d refused at the peer scope: %v", i+1, err)
		}
		open = append(open, s)
		if err := s.SetProtocol(bsnet.ProtocolBitswap); err != nil {
			t.Fatalf("inbound bitswap stream %d refused: %v", i+1, err)
		}
	}
}
