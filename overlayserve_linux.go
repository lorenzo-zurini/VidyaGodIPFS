//go:build linux

package main

// overlayserve_linux.go — the parent side of the nested-sandbox TUN handoff. The overlay forwarder runs in the
// VidyaGod process (it holds the libp2p host), but the game's TUN is created inside the bwrap sandbox's own network
// namespace. The sandbox-init creates + addresses the TUN there and sends its fd out to us over a bound unix socket
// (SCM_RIGHTS). This file listens on that socket, receives the fd, wraps it as a packetLink, and attaches it to the
// forwarder — so packets flow between the game (in the sandbox netns) and its session peers over libp2p.

import (
	"fmt"
	"net"
	"os"

	unix "golang.org/x/sys/unix"
)

// serve binds a unix listening socket at sockPath and, in the background, accepts one connection from the
// sandbox-init, receives the TUN fd, and attaches it. Routes must already be configured (o.configure) before the fd
// arrives. Non-blocking: returns once listening so the caller can spawn the sandbox.
func (o *overlayService) serve(sockPath string) error {
	_ = os.Remove(sockPath)
	l, err := net.ListenUnix("unix", &net.UnixAddr{Name: sockPath, Net: "unix"})
	if err != nil {
		return fmt.Errorf("listen %s: %w", sockPath, err)
	}
	o.mu.Lock()
	o.sockL = l
	o.sockPath = sockPath
	o.mu.Unlock()
	go func() {
		conn, err := l.AcceptUnix()
		if err != nil {
			return // listener closed (detach) before the sandbox connected
		}
		defer conn.Close()
		fd, err := recvFD(conn)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[overlay] TUN fd handoff failed: %v\n", err)
			return
		}
		o.attach(&fdLink{f: os.NewFile(uintptr(fd), "vgtun"), mtu: overlayMTU})
		fmt.Fprintf(os.Stderr, "[overlay] received sandbox TUN fd — forwarding attached\n")
	}()
	return nil
}

// recvFD reads a single file descriptor sent over a unix socket with SCM_RIGHTS.
func recvFD(conn *net.UnixConn) (int, error) {
	oob := make([]byte, unix.CmsgSpace(4))
	buf := make([]byte, 1)
	_, oobn, _, _, err := conn.ReadMsgUnix(buf, oob)
	if err != nil {
		return -1, err
	}
	scms, err := unix.ParseSocketControlMessage(oob[:oobn])
	if err != nil || len(scms) == 0 {
		return -1, fmt.Errorf("no control message: %w", err)
	}
	fds, err := unix.ParseUnixRights(&scms[0])
	if err != nil || len(fds) == 0 {
		return -1, fmt.Errorf("no fd in message: %w", err)
	}
	return fds[0], nil
}
