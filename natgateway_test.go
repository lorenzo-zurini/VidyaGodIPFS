package main

// natgateway_test.go — the whole userspace NAT exercised END-TO-END, no TUN and no privileges: a second gvisor
// stack plays the GAME (address = a vIP, default route into a pipe), the pipe feeds natGateway.inject, and the
// gateway's egress feeds back into the game stack. Dial + DNS are injected, so "the internet" is a local listener.

import (
	"context"
	"net"
	"testing"
	"time"

	"golang.org/x/net/dns/dnsmessage"

	"gvisor.dev/gvisor/pkg/buffer"
	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/header"
	"gvisor.dev/gvisor/pkg/tcpip/link/channel"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
	"gvisor.dev/gvisor/pkg/tcpip/transport/tcp"
	"gvisor.dev/gvisor/pkg/tcpip/transport/udp"
)

// gameStack builds the "game side": a netstack with the vIP assigned and every packet leaving through ep.
func gameStack(t *testing.T, vip net.IP) (*stack.Stack, *channel.Endpoint) {
	t.Helper()
	st := stack.New(stack.Options{
		NetworkProtocols:   []stack.NetworkProtocolFactory{ipv4.NewProtocol},
		TransportProtocols: []stack.TransportProtocolFactory{tcp.NewProtocol, udp.NewProtocol},
	})
	ep := channel.New(512, overlayMTU, "")
	if err := st.CreateNIC(1, ep); err != nil {
		t.Fatalf("game NIC: %v", err)
	}
	addr := tcpip.ProtocolAddress{
		Protocol:          ipv4.ProtocolNumber,
		AddressWithPrefix: tcpip.AddrFrom4([4]byte(vip.To4())).WithPrefix(),
	}
	if err := st.AddProtocolAddress(1, addr, stack.AddressProperties{}); err != nil {
		t.Fatalf("game addr: %v", err)
	}
	st.SetRouteTable([]tcpip.Route{{Destination: header.IPv4EmptySubnet, NIC: 1}})
	t.Cleanup(func() { st.Close(); ep.Close() })
	return st, ep
}

// natPair wires game-stack ⟷ gateway and returns the game stack for gonet dials.
func natPair(t *testing.T, dial func(string, string) (net.Conn, error),
	resolve func(context.Context, string) ([]net.IP, error)) *stack.Stack {
	t.Helper()
	vip := net.IPv4(10, 66, 1, 2)
	gst, gep := gameStack(t, vip)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	gw, err := newNATGateway(ctx, func(pkt []byte) error {
		pb := stack.NewPacketBuffer(stack.PacketBufferOptions{Payload: buffer.MakeWithData(append([]byte(nil), pkt...))})
		gep.InjectInbound(ipv4.ProtocolNumber, pb)
		pb.DecRef()
		return nil
	}, dial, resolve)
	if err != nil {
		t.Fatalf("gateway: %v", err)
	}
	t.Cleanup(gw.close)

	go func() { // game egress → gateway
		for {
			pb := gep.ReadContext(ctx)
			if pb == nil {
				return
			}
			gw.inject(pb.ToView().AsSlice())
			pb.DecRef()
		}
	}()
	return gst
}

func TestNATGatewayTCP(t *testing.T) {
	// "The internet": a local TCP server the injectable dialer routes everything to.
	srv, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	go func() {
		for {
			c, aerr := srv.Accept()
			if aerr != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 64)
				n, _ := c.Read(buf)
				_, _ = c.Write(append([]byte("echo:"), buf[:n]...))
			}(c)
		}
	}()
	var dialedTo string
	gst := natPair(t, func(network, addr string) (net.Conn, error) {
		dialedTo = addr
		return net.Dial("tcp", srv.Addr().String())
	}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := gonet.DialContextTCP(ctx, gst,
		tcpip.FullAddress{NIC: 1, Addr: tcpip.AddrFrom4([4]byte{99, 88, 77, 66}), Port: 8080}, ipv4.ProtocolNumber)
	if err != nil {
		t.Fatalf("game dial through NAT: %v", err)
	}
	defer c.Close()
	if _, err := c.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 64)
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, err := c.Read(buf)
	if err != nil {
		t.Fatalf("read via NAT: %v", err)
	}
	if string(buf[:n]) != "echo:hello" {
		t.Fatalf("payload mismatch: %q", buf[:n])
	}
	if dialedTo != "99.88.77.66:8080" {
		t.Fatalf("gateway dialed %q, want the game's ORIGINAL destination", dialedTo)
	}
}

func TestNATGatewayUDPAndDNS(t *testing.T) {
	// UDP echo "internet" server.
	usrv, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer usrv.Close()
	go func() {
		buf := make([]byte, 1500)
		for {
			n, from, rerr := usrv.ReadFrom(buf)
			if rerr != nil {
				return
			}
			_, _ = usrv.WriteTo(append([]byte("u:"), buf[:n]...), from)
		}
	}()
	gst := natPair(t, func(network, addr string) (net.Conn, error) {
		return net.Dial("udp", usrv.LocalAddr().String())
	}, func(ctx context.Context, host string) ([]net.IP, error) {
		if host != "play.example" {
			t.Errorf("unexpected DNS host %q", host)
		}
		return []net.IP{net.IPv4(11, 22, 33, 44)}, nil
	})

	// Plain UDP flow through the NAT.
	uc, err := gonet.DialUDP(gst, nil,
		&tcpip.FullAddress{NIC: 1, Addr: tcpip.AddrFrom4([4]byte{50, 60, 70, 80}), Port: 7777}, ipv4.ProtocolNumber)
	if err != nil {
		t.Fatalf("game udp dial: %v", err)
	}
	defer uc.Close()
	if _, err := uc.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 128)
	_ = uc.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, err := uc.Read(buf)
	if err != nil {
		t.Fatalf("udp read via NAT: %v", err)
	}
	if string(buf[:n]) != "u:ping" {
		t.Fatalf("udp payload mismatch: %q", buf[:n])
	}

	// DNS: query ANY server on :53 — answered in-node by the injected resolver.
	dc, err := gonet.DialUDP(gst, nil,
		&tcpip.FullAddress{NIC: 1, Addr: tcpip.AddrFrom4([4]byte{8, 8, 8, 8}), Port: 53}, ipv4.ProtocolNumber)
	if err != nil {
		t.Fatalf("game dns dial: %v", err)
	}
	defer dc.Close()
	var qb dnsmessage.Builder
	qb = dnsmessage.NewBuilder(nil, dnsmessage.Header{ID: 42, RecursionDesired: true})
	_ = qb.StartQuestions()
	name := dnsmessage.MustNewName("play.example.")
	_ = qb.Question(dnsmessage.Question{Name: name, Type: dnsmessage.TypeA, Class: dnsmessage.ClassINET})
	q, _ := qb.Finish()
	if _, err := dc.Write(q); err != nil {
		t.Fatal(err)
	}
	_ = dc.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, err = dc.Read(buf)
	if err != nil {
		t.Fatalf("dns read: %v", err)
	}
	var p dnsmessage.Parser
	hdr, err := p.Start(buf[:n])
	if err != nil || hdr.ID != 42 || !hdr.Response {
		t.Fatalf("bad dns response: %v %+v", err, hdr)
	}
	_ = p.SkipAllQuestions()
	if _, err := p.AnswerHeader(); err != nil {
		t.Fatalf("no answer header: %v", err)
	}
	a, err := p.AResource()
	if err != nil {
		t.Fatalf("no A answer: %v", err)
	}
	if net.IP(a.A[:]).String() != "11.22.33.44" {
		t.Fatalf("wrong A answer: %v", a.A)
	}
}
