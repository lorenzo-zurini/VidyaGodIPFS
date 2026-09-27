package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	peer "github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

// TestRestartChild is not a test: it is the node TestARestartedNodeIsHeardAtOnce kills and restarts, in its own
// process (a killed process sends no QUIC close — that is the point).
func TestRestartChild(t *testing.T) {
	if os.Getenv("VG_RESTART_CHILD") == "" {
		t.Skip("the child node of TestARestartedNodeIsHeardAtOnce")
	}
	repo, mode := os.Getenv("VG_CHILD_REPO"), os.Getenv("VG_CHILD_MODE")
	ai, err := peer.AddrInfoFromString(os.Getenv("VG_CHILD_PEER"))
	if err != nil {
		t.Fatal(err)
	}
	priv, err := loadOrCreateIdentity(repo)
	if err != nil {
		t.Fatal(err)
	}
	h, err := libp2p.New(libp2p.Identity(priv), libp2p.NoListenAddrs) // as the node does
	if err != nil {
		t.Fatal(err)
	}
	if err := listenOn(h, repo); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := h.Connect(ctx, *ai); err != nil {
		t.Fatal(err)
	}
	switch mode {
	case "hold": // a live node's connection: long-lived streams (bitswap, friend, overlay)
		for i := 0; i < 6; i++ {
			s, err := h.NewStream(ctx, ai.ID, "/vgtest/hold/1")
			if err != nil {
				t.Fatal(err)
			}
			_, _ = s.Write([]byte("x\n"))
		}
		fmt.Println("READY")
		select {}
	case "ask": // ask until the peer's answer arrives (bitswap re-asks on its own timeouts)
		got := make(chan struct{})
		var once sync.Once
		h.SetStreamHandler("/vgtest/reply/1", func(s network.Stream) { once.Do(func() { close(got) }); _ = s.Close() })
		start := time.Now()
		for time.Since(start) < 20*time.Second {
			if s, err := h.NewStream(ctx, ai.ID, "/vgtest/req/1"); err == nil {
				_, _ = s.Write([]byte("hi\n"))
				_ = s.CloseWrite()
			}
			select {
			case <-got:
				fmt.Printf("REPLY %d\n", time.Since(start).Milliseconds())
				return
			case <-time.After(500 * time.Millisecond):
			}
		}
		fmt.Println("NOREPLY")
	}
}

// A node killed and started again under the same identity (an app restart, a crash) must be heard at once. Its peer
// still holds the dead process's QUIC connection, alive until the idle timeout, and sends every new stream down the
// connection with the most streams: that one. On the same UDP port, the new process answers the peer's next packet on
// it with a stateless reset and the peer drops it; on a fresh port the peer's replies vanish for ~30 s (replication
// 7: the seeder's answers for 38 s). Teeth: listen on fresh ports every start (listenAddrs ignoring the saved ports).
func TestARestartedNodeIsHeardAtOnce(t *testing.T) {
	if os.Getenv("VG_RESTART_CHILD") != "" {
		t.Skip()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/udp/0/quic-v1")) // QUIC: dead connections linger
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	a.SetStreamHandler("/vgtest/hold/1", func(s network.Stream) { _, _ = io.Copy(io.Discard, s) })
	a.SetStreamHandler("/vgtest/req/1", func(s network.Stream) {
		p := s.Conn().RemotePeer()
		_ = s.Close()
		if r, err := a.NewStream(ctx, p, "/vgtest/reply/1"); err == nil { // a new stream: the swarm picks the connection
			_, _ = r.Write([]byte("ok\n"))
			_ = r.Close()
		}
	})
	addr := a.Addrs()[0].Encapsulate(ma.StringCast("/p2p/" + a.ID().String())).String()
	repo := t.TempDir()
	child := func(mode string) (*exec.Cmd, *bufio.Scanner) {
		c := exec.Command(os.Args[0], "-test.run=^TestRestartChild$", "-test.v")
		c.Env = append(os.Environ(), "VG_RESTART_CHILD=1", "VG_CHILD_REPO="+repo, "VG_CHILD_MODE="+mode, "VG_CHILD_PEER="+addr)
		out, err := c.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		c.Stderr = io.Discard
		if err := c.Start(); err != nil {
			t.Fatal(err)
		}
		return c, bufio.NewScanner(out)
	}
	first, out := child("hold")
	for out.Scan() && out.Text() != "READY" {
	}
	if err := first.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	_ = first.Wait()
	if n := len(a.Network().ConnsToPeer(peerOf(t, repo))); n == 0 {
		t.Fatal("setup: the peer already dropped the dead connection — nothing to test")
	}
	second, out := child("ask")
	defer func() { _ = second.Wait() }()
	for out.Scan() {
		line := out.Text()
		if strings.HasPrefix(line, "REPLY ") {
			var ms int
			fmt.Sscan(strings.TrimPrefix(line, "REPLY "), &ms)
			t.Logf("the restarted node was answered after %d ms", ms)
			if ms > 5000 {
				t.Fatalf("the restarted node was answered after %d ms: its peer's replies went down the dead connection", ms)
			}
			return
		}
		if line == "NOREPLY" {
			t.Fatal("the restarted node was not answered in 20 s: its peer's replies went down the dead connection")
		}
	}
	t.Fatal("the restarted node exited without an answer")
}

func peerOf(t *testing.T, repo string) peer.ID {
	priv, err := loadOrCreateIdentity(repo)
	if err != nil {
		t.Fatal(err)
	}
	id, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// Saved ports that cannot be had — held by a live node, or not ports at all (a damaged file) — are replaced for the
// run: the node always listens on every transport, and saves what it uses. Each port is the same on IPv4 and IPv6.
// Before, a saved set that could not be bound kept the node offline for good (every retry read the same file).
// Teeth: no fresh port for a refused transport; accept an out-of-range saved port; skip the save; a different port
// per family.
func TestListenPortsReplaceWhatCannotBeHad(t *testing.T) {
	newHost := func() host.Host {
		h, err := libp2p.New(libp2p.NoListenAddrs)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = h.Close() })
		return h
	}
	all := func(p listenPorts) bool { return p.TCP != 0 && p.UDP != 0 && p.WS != 0 }
	v4 := func(h host.Host) listenPorts { // what listens on IPv4 (portsOf falls back to IPv6 ports)
		var out []ma.Multiaddr
		for _, a := range h.Network().ListenAddresses() {
			if _, err := a.ValueForProtocol(ma.P_IP4); err == nil {
				out = append(out, a)
			}
		}
		return portsOf(out)
	}
	repo := t.TempDir()
	h1 := newHost()
	if err := listenOn(h1, repo); err != nil {
		t.Fatal(err)
	}
	saved := loadListenPorts(repo)
	if !all(saved) || portsOf(h1.Network().ListenAddresses()) != saved {
		t.Fatalf("first start saved %+v, listens on %+v", saved, portsOf(h1.Network().ListenAddresses()))
	}
	if ipv6Available() {
		for _, a := range h1.Network().ListenAddresses() {
			if _, err := a.ValueForProtocol(ma.P_IP6); err != nil {
				continue
			}
			if p := portsOf([]ma.Multiaddr{a}); p.TCP+p.UDP+p.WS != 0 && p.TCP != saved.TCP && p.UDP != saved.UDP && p.WS != saved.WS {
				t.Fatalf("IPv6 listens on %s, not on the IPv4 ports %+v", a, saved)
			}
		}
	}

	h2 := newHost() // every saved port held by h1 (TCP may be shared: SO_REUSEPORT)
	if err := listenOn(h2, repo); err != nil {
		t.Fatalf("a node whose saved ports are taken did not come up: %v", err)
	}
	got := portsOf(h2.Network().ListenAddresses())
	if !all(v4(h2)) || got.UDP == saved.UDP {
		t.Fatalf("with its saved ports taken the node listens on %+v (saved %+v): a transport stayed deaf or shared UDP", got, saved)
	}
	if loadListenPorts(repo) != got {
		t.Fatalf("saved %+v, listening on %+v", loadListenPorts(repo), got)
	}

	if err := os.WriteFile(filepath.Join(repo, listenPortsFile), []byte(`{"tcp":70000,"udp":-1,"ws":0}`), 0o644); err != nil {
		t.Fatal(err)
	}
	h3 := newHost()
	if err := listenOn(h3, repo); err != nil {
		t.Fatalf("a damaged ports file kept the node from listening: %v", err)
	}
	if got := portsOf(h3.Network().ListenAddresses()); !all(v4(h3)) || loadListenPorts(repo) != got {
		t.Fatalf("after a damaged file: listening on %+v, saved %+v", got, loadListenPorts(repo))
	}
}
