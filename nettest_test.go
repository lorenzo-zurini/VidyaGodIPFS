package main

import "testing"

// isPublicish gates the inbound-reachability verdict; a private addr misread as public would report a NATed
// user as world-reachable and hide the relay-only warning.
func TestIsPublicishClassification(t *testing.T) {
	private := []string{
		"/ip4/10.66.235.0/udp/1/quic-v1", "/ip4/127.0.0.1/tcp/4001", "/ip4/172.19.0.1/tcp/1",
		"/ip4/192.168.1.131/udp/2/quic-v1", "/ip6/::1/tcp/1", "/ip6/fc36:74a5::1/tcp/1",
		"/ip6/fddb:6485::1/tcp/1", "/ip6/fe80::1/tcp/1",
	}
	public := []string{"/ip4/81.180.250.152/tcp/4001", "/ip6/2a02:2f0a::1/udp/4001/quic-v1", "/dns4/x.example/tcp/443"}
	for _, a := range private {
		if isPublicish(a) {
			t.Fatalf("%s misclassified as public", a)
		}
	}
	for _, a := range public {
		if !isPublicish(a) {
			t.Fatalf("%s misclassified as private", a)
		}
	}
}

// rootErr powers every failure sentence; a mangled chain would bury the actionable cause.
func TestRootErrTrimsChains(t *testing.T) {
	if got := rootErr(errFmt("dial tcp: lookup x: no such host")); got != "no such host" {
		t.Fatalf("got %q", got)
	}
	if got := rootErr(nil); got != "unknown" {
		t.Fatalf("nil → %q", got)
	}
}

type errFmt string

func (e errFmt) Error() string { return string(e) }
