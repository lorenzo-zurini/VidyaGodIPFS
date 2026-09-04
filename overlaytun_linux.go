//go:build linux

package main

// overlaytun_linux.go — a real Linux TUN device as an overlay packetLink. Creates /dev/net/tun with TUNSETIFF (one
// IP packet per read/write, no packet-info header), then brings the interface up and assigns the local vIP + the
// session /24 route via the `ip` tool. The fd is held open for the interface's lifetime (a non-persistent TUN
// vanishes when its fd closes — which is exactly what we want on detach/crash).
//
// Requires CAP_NET_ADMIN. In production the interface lives inside the game's bubblewrap netns (where our uid maps to
// root, so the capability holds unprivileged); for a same-machine end-to-end test it can be created in the host netns
// with a setcap'd binary or root. This file only does the device + IP plumbing; routing packets is overlay.go.

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"unsafe"
)

const (
	iffTun    = 0x0001     // IFF_TUN — a layer-3 (IP) device
	iffNoPI   = 0x1000     // IFF_NO_PI — no 4-byte packet-info prefix; reads/writes are raw IP packets
	tunSetIff = 0x400454ca // TUNSETIFF
)

type tunLink struct {
	f    *os.File
	name string
	mtu  int
}

// newTUN opens a TUN device with the given name (<=15 chars; empty → kernel picks) and MTU.
func newTUN(name string, mtu int) (*tunLink, error) {
	f, err := os.OpenFile("/dev/net/tun", os.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("open /dev/net/tun: %w", err)
	}
	// struct ifreq: 16 bytes ifr_name, then a union whose first 2 bytes here are ifr_flags.
	var ifr [40]byte
	copy(ifr[:15], name)
	flags := uint16(iffTun | iffNoPI)
	ifr[16] = byte(flags)
	ifr[17] = byte(flags >> 8)
	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, f.Fd(), tunSetIff, uintptr(unsafe.Pointer(&ifr))); errno != 0 {
		_ = f.Close()
		return nil, fmt.Errorf("TUNSETIFF: %w", errno)
	}
	real := string(bytes.TrimRight(ifr[:16], "\x00"))
	return &tunLink{f: f, name: real, mtu: mtu}, nil
}

func (t *tunLink) ReadPacket() ([]byte, error) {
	buf := make([]byte, t.mtu+64)
	n, err := t.f.Read(buf)
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}

func (t *tunLink) WritePacket(p []byte) error {
	_, err := t.f.Write(p)
	return err
}

func (t *tunLink) Close() error { return t.f.Close() }

// configureIP brings the interface up with the given CIDR (e.g. "10.66.5.1/24"), which also installs the /24 route
// so packets to any session vIP enter this TUN.
func (t *tunLink) configureIP(cidr string) error {
	steps := [][]string{
		{"ip", "link", "set", "dev", t.name, "mtu", fmt.Sprint(t.mtu)},
		{"ip", "addr", "add", cidr, "dev", t.name},
		{"ip", "link", "set", "dev", t.name, "up"},
	}
	for _, s := range steps {
		if out, err := exec.Command(s[0], s[1:]...).CombinedOutput(); err != nil {
			return fmt.Errorf("%v: %w: %s", s, err, bytes.TrimSpace(out))
		}
	}
	return nil
}
