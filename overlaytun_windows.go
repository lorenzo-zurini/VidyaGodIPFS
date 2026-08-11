//go:build windows

package main

// overlaytun_windows.go — a real Windows TUN device (Wintun) as an overlay packetLink, the Windows analogue of
// overlaytun_linux.go. Creates a Wintun adapter, pumps one L3 (IP) packet per read/write through its ring-buffer
// session, and assigns the local vIP + /24 (which installs the on-link route) via netsh. The adapter is held for the
// service's lifetime and torn down on Close.
//
// Unlike Linux there is NO network namespace: the adapter is host-wide, created directly by the VidyaGod process, so
// the production path on Windows is the host-TUN mode (VgOverlayStart → newTUN/configureIP/attach), NOT the nested-
// sandbox fd-handoff (VgOverlayServe) that Linux uses to push the TUN into a bwrap netns. The game runs in a
// Sandboxie box that shares the host network, so it reaches session peers through this host adapter. `serve` is
// therefore a stub here.
//
// Requires the wintun.dll runtime (bundled beside the binary) and Administrator (adapter creation + netsh). The
// friend-LAN overlay is off by default (Settings.LanEnabled), so this only runs when the user opts in.

import (
	"errors"
	"fmt"
	"net"
	"os/exec"

	"golang.org/x/sys/windows"
	"golang.zx2c4.com/wintun"
)

type tunLink struct {
	adapter *wintun.Adapter
	session wintun.Session
	name    string
	mtu     int
}

// newTUN creates a Wintun adapter with the given name (empty → "VidyaGod") and starts a packet session.
func newTUN(name string, mtu int) (*tunLink, error) {
	if name == "" {
		name = "VidyaGod"
	}
	adapter, err := wintun.CreateAdapter(name, "VidyaGod", nil)
	if err != nil {
		return nil, fmt.Errorf("create wintun adapter %q: %w", name, err)
	}
	// Ring capacity: power of two in [128 KiB, 64 MiB]. 4 MiB is plenty for a single game's overlay traffic.
	session, err := adapter.StartSession(0x400000)
	if err != nil {
		_ = adapter.Close()
		return nil, fmt.Errorf("start wintun session: %w", err)
	}
	return &tunLink{adapter: adapter, session: session, name: name, mtu: mtu}, nil
}

// ReadPacket blocks for the next outbound IP packet from the OS/game side. Wintun hands back a slice into its ring,
// so we copy it out before releasing (the ring memory is reused).
func (t *tunLink) ReadPacket() ([]byte, error) {
	for {
		p, err := t.session.ReceivePacket()
		if err == nil {
			out := make([]byte, len(p))
			copy(out, p)
			t.session.ReleaseReceivePacket(p)
			return out, nil
		}
		if errors.Is(err, windows.ERROR_NO_MORE_ITEMS) {
			// Ring empty — wait until Wintun signals more data, then retry.
			windows.WaitForSingleObject(t.session.ReadWaitEvent(), windows.INFINITE)
			continue
		}
		return nil, err
	}
}

// WritePacket injects an inbound IP packet (from a session peer) into the adapter.
func (t *tunLink) WritePacket(p []byte) error {
	packet, err := t.session.AllocateSendPacket(len(p))
	if err != nil {
		return err
	}
	copy(packet, p)
	t.session.SendPacket(packet)
	return nil
}

func (t *tunLink) Close() error {
	t.session.End()
	return t.adapter.Close()
}

// configureIP assigns the CIDR (e.g. "10.66.5.1/24") to the adapter and sets its MTU via netsh. The static address
// with its mask installs the on-link /24 route, so packets to any session vIP enter this adapter.
func (t *tunLink) configureIP(cidr string) error {
	ip, ipnet, err := net.ParseCIDR(cidr)
	if err != nil {
		return fmt.Errorf("parse cidr %q: %w", cidr, err)
	}
	mask := net.IP(ipnet.Mask).String() // dotted-decimal, what netsh expects
	steps := [][]string{
		{"netsh", "interface", "ip", "set", "address", "name=" + t.name, "static", ip.String(), mask},
		{"netsh", "interface", "ipv4", "set", "subinterface", t.name, "mtu=" + fmt.Sprint(t.mtu), "store=active"},
	}
	for _, s := range steps {
		if out, err := exec.Command(s[0], s[1:]...).CombinedOutput(); err != nil {
			return fmt.Errorf("%v: %w: %s", s, err, out)
		}
	}
	return nil
}

// serve (the nested-sandbox TUN fd handoff) is Linux-only; Windows uses the host-TUN path (VgOverlayStart).
func (o *overlayService) serve(string) error {
	return errors.New("windows overlay uses host-TUN mode (VgOverlayStart), not sandbox fd-passing")
}
