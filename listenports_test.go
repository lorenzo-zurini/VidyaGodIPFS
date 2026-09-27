package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
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
	h, err := libp2p.New(libp2p.Identity(priv), libp2p.ListenAddrStrings(listenAddrs(repo)...)) // as the node does
	if err != nil {
		t.Fatal(err)
	}
	keepListenPorts(h, repo)
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

// A saved port that is taken is replaced by a fresh one, and the ports in use are saved. Teeth: skip the fresh
// listen (the transport stays deaf); skip the save.
func TestListenPortsSurviveATakenPort(t *testing.T) {
	repo := t.TempDir()
	h1, err := libp2p.New(libp2p.ListenAddrStrings(listenAddrs(repo)...))
	if err != nil {
		t.Fatal(err)
	}
	keepListenPorts(h1, repo)
	saved := loadListenPorts(repo)
	if saved.TCP == 0 || saved.UDP == 0 || saved.WS == 0 {
		t.Fatalf("first start saved %+v: every transport needs its port", saved)
	}
	// h1 still holds them: a second node on the same saved ports finds every one taken.
	h2, err := libp2p.New(libp2p.ListenAddrStrings(listenAddrs(repo)...))
	if err == nil {
		defer h2.Close()
		keepListenPorts(h2, repo)
	} else {
		h2, err = libp2p.New(libp2p.NoListenAddrs)
		if err != nil {
			t.Fatal(err)
		}
		defer h2.Close()
		keepListenPorts(h2, repo)
	}
	got := portsOf(h2.Network().ListenAddresses())
	if got.TCP == 0 || got.UDP == 0 || got.WS == 0 {
		t.Fatalf("with its saved ports taken the node listens on %+v: a transport stayed deaf", got)
	}
	if loadListenPorts(repo) != got {
		t.Fatalf("saved %+v, listening on %+v", loadListenPorts(repo), got)
	}
	_ = h1.Close()
}
