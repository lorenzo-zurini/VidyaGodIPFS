package main

import (
	"context"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/net/swarm"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

// The local network is the subnet of the interface the machine sends internet traffic from — this PC's interfaces as
// they are: Ethernet (the LAN, v4 and v6), WireGuard, ZeroTier, a Docker bridge. Only the LAN is local; with a
// full-tunnel VPN (the source on the tunnel) nothing is. Teeth: count every interface's subnet; count none; count a
// tunnel the OS sends from.
func TestTheLocalNetworkIsTheOneTheMachineIsOutOn(t *testing.T) {
	ip := func(s string) net.Addr { a, n, _ := net.ParseCIDR(s); n.IP = a; return n }
	mac := net.HardwareAddr{0xf0, 0x2f, 0x74, 0x22, 0x6f, 0xf6}
	up := net.FlagUp | net.FlagBroadcast | net.FlagMulticast
	ifs := []iface{
		{flags: net.FlagUp | net.FlagLoopback, addrs: []net.Addr{ip("127.0.0.1/8")}},
		{flags: up, hw: mac, addrs: []net.Addr{ip("192.168.8.178/24"), ip("2a01:4f9:c015:42ae::5/64"), ip("fe80::1/64")}},
		{flags: net.FlagUp | net.FlagPointToPoint, addrs: []net.Addr{ip("10.10.0.2/24")}},               // WireGuard
		{flags: up, hw: net.HardwareAddr{2, 0, 0, 0, 0, 1}, addrs: []net.Addr{ip("172.25.119.206/16")}}, // ZeroTier
		{flags: up, hw: net.HardwareAddr{2, 0, 0, 0, 0, 2}, addrs: []net.Addr{ip("172.17.0.1/16")}},     // Docker
	}
	got := localNetsFor([]net.IP{net.ParseIP("192.168.8.178"), net.ParseIP("2a01:4f9:c015:42ae::5")}, ifs)
	if len(got) != 2 || got[0].String() != "192.168.8.0/24" || got[1].String() != "2a01:4f9:c015:42ae::/64" {
		t.Fatalf("local networks %v, want 192.168.8.0/24 and 2a01:4f9:c015:42ae::/64", got)
	}
	if !onLocalNet(ma.StringCast("/ip4/192.168.8.235/udp/55571/quic-v1"), got) {
		t.Fatal("the Deck's address on the home network is not local")
	}
	for _, a := range []string{"/ip4/10.10.0.6/udp/55571/quic-v1", "/ip4/172.25.30.157/tcp/4001", "/ip4/172.17.0.1/tcp/4001",
		"/ip4/144.76.30.57/tcp/4001/p2p/12D3KooWRwrs3M34JNnRwGqhhocQs5xSYQk8kmci59fmYjhg9q4v/p2p-circuit"} {
		if onLocalNet(ma.StringCast(a), got) {
			t.Fatalf("%s counted as local", a)
		}
	}
	if n := localNetsFor([]net.IP{net.ParseIP("10.10.0.2")}, ifs); len(n) != 0 {
		t.Fatalf("a full-tunnel VPN (the OS sending from the tunnel) made %v local", n)
	}
	if len(localNetsFor(nil, ifs)) != 0 {
		t.Fatal("a machine not out on any network has a local one")
	}
}

// Local addresses are dialed first, the rest only after them; a peer with none is ranked as libp2p ranks it. Teeth:
// rank as libp2p does (a VPN's address at once); drop the rest (a peer whose LAN address fails must still be reached).
func TestLocalAddressesAreDialedFirst(t *testing.T) {
	_, lan, _ := net.ParseCIDR("192.168.8.0/24")
	rank := localFirstRanker(func() []*net.IPNet { return []*net.IPNet{lan} })
	vpn := ma.StringCast("/ip4/10.10.0.6/udp/55571/quic-v1")
	home := ma.StringCast("/ip4/192.168.8.235/udp/55571/quic-v1")
	ds := rank([]ma.Multiaddr{vpn, home})
	if len(ds) != 2 {
		t.Fatalf("ranked %v, want both", ds)
	}
	var homeAt, vpnAt time.Duration = -1, -1
	for _, d := range ds {
		if d.Addr.Equal(home) {
			homeAt = d.Delay
		} else if d.Addr.Equal(vpn) {
			vpnAt = d.Delay
		}
	}
	if homeAt != 0 || vpnAt < localFirstDelay {
		t.Fatalf("home network at %v, VPN at %v: want the home network at once, the VPN after %v", homeAt, vpnAt, localFirstDelay)
	}
	if got := rank([]ma.Multiaddr{vpn}); len(got) != 1 || got[0].Delay != 0 {
		t.Fatalf("a peer with no local address ranked %v, want libp2p's own", got)
	}
}

func lanTestHost(t *testing.T, nets func() []*net.IPNet, listen ...string) host.Host {
	t.Helper()
	h, err := libp2p.New(libp2p.ListenAddrStrings(listen...), libp2p.SwarmOpts(swarm.WithDialRanker(localFirstRanker(nets))))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	return h
}

// A peer with an address on the local network and one around it (here two loopback addresses, one standing for
// each — the one libp2p itself would dial first standing for the VPN) is connected to across the local network.
// Teeth: rank as libp2p does (it dials the VPN's first, and that answers).
func TestAPeerBesideUsIsConnectedToAcrossTheLocalNetwork(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("127.0.0.2 is only there by default on Linux")
	}
	b := lanTestHost(t, func() []*net.IPNet { return nil }, "/ip4/127.0.0.1/tcp/0", "/ip4/127.0.0.2/tcp/0")
	port := func(a ma.Multiaddr) int {
		na, err := manet.ToNetAddr(a)
		if err != nil {
			t.Fatal(err)
		}
		return na.(*net.TCPAddr).Port
	}
	addrs := b.Addrs()
	if len(addrs) != 2 {
		t.Fatalf("b listens on %v", addrs)
	}
	//libp2p dials the lowest port first: that one stands for the VPN, so local-first is what changes the outcome.
	around, local := addrs[0], addrs[1]
	if port(around) > port(local) {
		around, local = local, around
	}
	ip, _ := manet.ToIP(local)
	home := []*net.IPNet{{IP: ip, Mask: net.CIDRMask(len(ip)*8, len(ip)*8)}}
	a := lanTestHost(t, func() []*net.IPNet { return home }, "/ip4/127.0.0.1/tcp/0")
	if err := a.Connect(context.Background(), peer.AddrInfo{ID: b.ID(), Addrs: []ma.Multiaddr{around, local}}); err != nil {
		t.Fatal(err)
	}
	for _, c := range a.Network().ConnsToPeer(b.ID()) {
		if !onLocalNet(c.RemoteMultiaddr(), home) {
			t.Fatalf("connected around the local network: %v", c.RemoteMultiaddr())
		}
	}
}
