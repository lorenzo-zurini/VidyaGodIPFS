package main

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestDoHResolves checks the DoH resolver returns real answers for an A record and a dnsaddr TXT (the two lookup kinds
// libp2p needs). Network-gated: skips if DoH is unreachable (offline CI), so it never flakes the suite.
func TestDoHResolves(t *testing.T) {
	d := newDoHResolver()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	ips, err := d.LookupIPAddr(ctx, "one.one.one.one")
	if err != nil || len(ips) == 0 {
		t.Skipf("DoH unavailable (no network?): %v", err)
	}
	t.Logf("A one.one.one.one -> %v", ips)

	// The libp2p bootstrap peers + Pinata are addressed via dnsaddr TXT records; make sure DoH returns them.
	txt, err := d.LookupTXT(ctx, "_dnsaddr.bootstrap.libp2p.io")
	if err != nil {
		t.Skipf("DoH TXT unavailable: %v", err)
	}
	found := false
	for _, r := range txt {
		if strings.HasPrefix(r, "dnsaddr=") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected dnsaddr= TXT records via DoH, got %v", txt)
	}
}
