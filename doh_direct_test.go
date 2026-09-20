package main

import "testing"

func TestLibp2pDirectDecodesLocally(t *testing.T) {
	// Forge names decode to the IP in the leading label — ZERO DNS. Real observed names + go-libp2p's own test vector.
	ok := map[string]string{
		"31-204-136-139.k51qzi5uqu5dhqqipontlec6lapd61yde1hb46dluro3l5bo6nsexnkpaygmcg.libp2p.direct": "31.204.136.139",
		"192-0-2-1.k51qzi5uqu5dgutdk6i1ynyzgkqngpha5xpgia3a5qqp4jsh0u4csozksxel3r.libp2p.direct":       "192.0.2.1",
		"31-204-136-139.k51.libp2p.direct.":                                                            "31.204.136.139", // trailing root dot
		"2001-db8--1.somepeer.libp2p.direct":                                                           "2001:db8::1",    // IPv6 (':' as '-')
	}
	for name, want := range ok {
		ip, got := libp2pDirectIP(name)
		if !got || ip == nil || ip.String() != want {
			t.Errorf("libp2pDirectIP(%q) = (%v, %v), want (%s, true)", name, ip, got, want)
		}
	}
	// These MUST fall through to real resolution (ok=false) — a decoder that intercepts a real lookup would send the
	// dial to a bogus address. Covers non-forge hosts AND a forge suffix whose leading label is not an IP.
	for _, name := range []string{
		"bootstrap.libp2p.io", "bitswap.pinata.cloud", "delegated-ipfs.dev", "example.com",
		"libp2p.direct", "notanip.somepeer.libp2p.direct", "",
		// IP-like leading label but NOT under libp2p.direct: MUST fall through — the suffix guard is what stops us
		// hijacking a real host that merely happens to start with a dash-IP label (drop the guard and these intercept).
		"1-2-3-4.example.com", "10-0-0-1.attacker.net", "31-204-136-139.libp2p.direct.evil.com",
	} {
		if ip, got := libp2pDirectIP(name); got {
			t.Errorf("libp2pDirectIP(%q) = (%v, true), must fall through", name, ip)
		}
	}
}
