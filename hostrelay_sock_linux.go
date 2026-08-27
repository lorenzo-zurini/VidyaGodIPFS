//go:build linux

package main

import (
	"net"

	"golang.org/x/sys/unix"
)

// listenBroadcastUDP binds 0.0.0.0:port with SO_REUSEADDR (a game on the HOST may share the port) and
// SO_BROADCAST (we both send and receive limited broadcasts). All unprivileged.
func listenBroadcastUDP(port int) (*net.UDPConn, error) {
	c, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero, Port: port})
	if err != nil {
		return nil, err
	}
	if rc, rerr := c.SyscallConn(); rerr == nil {
		_ = rc.Control(func(fd uintptr) {
			_ = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_BROADCAST, 1)
			_ = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEADDR, 1)
		})
	}
	return c, nil
}
