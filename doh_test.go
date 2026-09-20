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

// TestDoHFallbackBoundedAndCached: with DoH dead, the OS fallback runs ONCE per name per TTL — a DoH outage (the
// filtered-net case the resolver exists for) must never stampede the OS resolver / router with per-call flows again
// (the conntrack incident). Teeth: drop the cache and fbCalls doubles; drop the negative cache and the second
// failing lookup re-queries.
func TestDoHFallbackBoundedAndCached(t *testing.T) {
	d := newDoHResolver()
	d.endpoints = []string{"https://127.0.0.1:1/dns-query"} // dead DoH → every lookup would fall back
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	ips, err := d.LookupIPAddr(ctx, "localhost")
	if err != nil || len(ips) == 0 {
		t.Skipf("OS resolver can't resolve localhost here: %v", err)
	}
	if got := d.fbCalls.Load(); got != 1 {
		t.Fatalf("first lookup: fallback calls = %d, want 1", got)
	}
	ips2, err2 := d.LookupIPAddr(ctx, "localhost")
	if err2 != nil || len(ips2) != len(ips) {
		t.Fatalf("cached lookup diverged: %v / %v", ips2, err2)
	}
	if got := d.fbCalls.Load(); got != 1 {
		t.Fatalf("second lookup hit the OS resolver again (fallback calls = %d) — result cache is dead", got)
	}

	// Negative caching: a failing name is re-asked at most once per TTL.
	if _, err := d.LookupIPAddr(ctx, "does-not-exist.invalid"); err == nil {
		t.Skip("resolver unexpectedly answered .invalid")
	}
	after := d.fbCalls.Load()
	_, _ = d.LookupIPAddr(ctx, "does-not-exist.invalid")
	if got := d.fbCalls.Load(); got != after {
		t.Fatalf("failing lookup re-queried the OS resolver (fallback calls %d → %d) — negative cache is dead", after, got)
	}
}
