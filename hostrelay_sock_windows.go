//go:build windows

package main

import (
	"net"

	"golang.org/x/sys/windows"
)

// listenBroadcastUDP — Windows twin of the Linux helper (SO_BROADCAST + SO_REUSEADDR on 0.0.0.0:port).
func listenBroadcastUDP(port int) (*net.UDPConn, error) {
	c, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero, Port: port})
	if err != nil {
		return nil, err
	}
	if rc, rerr := c.SyscallConn(); rerr == nil {
		_ = rc.Control(func(fd uintptr) {
			_ = windows.SetsockoptInt(windows.Handle(fd), windows.SOL_SOCKET, windows.SO_BROADCAST, 1)
			_ = windows.SetsockoptInt(windows.Handle(fd), windows.SOL_SOCKET, windows.SO_REUSEADDR, 1)
		})
	}
	return c, nil
}
