package main

// doh.go — DNS over HTTPS. On hostile / captive networks (hospital, hotel, corporate wifi) plain DNS (UDP/TCP 53) is
// often filtered or hijacked, which silently breaks EVERYTHING libp2p reaches by name: the DHT bootstrap peers
// (/dnsaddr/bootstrap.libp2p.io…), the delegated routing indexer (delegated-ipfs.dev), and content providers whose
// only advertised address is a DNS name — Pinata is /dnsaddr/bitswap.pinata.cloud. The node then DISCOVERS providers
// via the DHT but can't DIAL them, so a fetch just times out. DoH rides HTTPS/443 (which such networks allow, since
// the box can still browse), so name resolution works through the filter. The DoH endpoint is addressed by its IP
// LITERAL (1.1.1.1), so reaching the resolver needs no DNS itself — no bootstrap paradox.

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	madns "github.com/multiformats/go-multiaddr-dns"
)

// dohResolver implements madns.BasicResolver (LookupIPAddr + LookupTXT — all libp2p needs for /dns{,4,6} and /dnsaddr)
// by querying Cloudflare's DoH JSON API. Falls back to the OS resolver if DoH is unreachable, so it's a safe no-op on
// normal networks.
type dohResolver struct {
	hc        *http.Client
	endpoints []string // IP-literal DoH JSON endpoints, RACED (see query) — none needs DNS to reach
}

// dohEndpoints: every entry is an IP literal (no bootstrap paradox) whose TLS cert carries the IP SAN, and every one
// speaks the same JSON shape (Answer[].type/data). Cloudflare + Google, so a network that filters one provider still
// resolves through the other. Overridable in tests.
var dohEndpoints = []string{
	"https://1.1.1.1/dns-query",
	"https://1.0.0.1/dns-query",
	"https://8.8.8.8/resolve",
	"https://8.8.4.4/resolve",
}

func newDoHResolver() *dohResolver {
	return &dohResolver{
		endpoints: dohEndpoints,
		hc:        &http.Client{Timeout: 6 * time.Second}, // per-endpoint cap; the race makes it the WORST case, not the sum
	}
}

type dohAnswer struct {
	Type int    `json:"type"` // 1=A, 28=AAAA, 16=TXT
	Data string `json:"data"`
}

// query HEDGES the lookup across every endpoint at once: the first good answer wins and the rest are cancelled. A
// single blocked or dead resolver (a filtered 1.1.1.1 at work) then costs nothing instead of everything, and the
// worst case is ONE endpoint timeout, not their sum — the sequential form would have made a DNS-dead network take
// 4x longer to reach the OS fallback. Every DNS-named path (bootstrap, indexer, gateways) sits on this.
func (d *dohResolver) query(ctx context.Context, name, qtype string) ([]dohAnswer, error) {
	qctx, cancel := context.WithCancel(ctx)
	defer cancel() // first success cancels the losers
	type res struct {
		ans []dohAnswer
		err error
	}
	ch := make(chan res, len(d.endpoints)) // buffered: a late loser never blocks after we return
	for _, ep := range d.endpoints {
		ep := ep
		safeGo("doh.hedge", func() {
			a, e := d.queryOne(qctx, ep, name, qtype)
			ch <- res{a, e}
		})
	}
	var lastErr error
	for range d.endpoints {
		r := <-ch
		if r.err == nil {
			return r.ans, nil
		}
		lastErr = r.err
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("doh %s/%s: no endpoints configured", name, qtype)
	}
	return nil, lastErr
}

func (d *dohResolver) queryOne(ctx context.Context, endpoint, name, qtype string) ([]dohAnswer, error) {
	u := endpoint + "?name=" + url.QueryEscape(name) + "&type=" + qtype
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/dns-json")
	req.Header.Set("User-Agent", "vidyagod")
	resp, err := d.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("doh %s/%s: status %d", name, qtype, resp.StatusCode)
	}
	var out struct {
		Answer []dohAnswer `json:"Answer"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out.Answer, nil
}

// libp2pDirectIP decodes an AutoTLS / p2p-forge "*.libp2p.direct" name locally. The leading DNS label IS the peer's IP
// with the separator replaced by '-' (IPv4 "31-204-136-139" == 31.204.136.139; IPv6 encodes ':' as '-'). The
// libp2p.direct zone is a wildcard that just echoes that IP back, so the lookup carries nothing the name doesn't already
// hold — and since every peer's name is unique it NEVER caches, making these the dominant DNS load (measured: ~113k
// queries in 6h, overwhelmingly unique *.libp2p.direct). Decoding locally returns the identical address with ZERO
// network DNS; TLS still validates against the "*.libp2p.direct" name, not how it was resolved. Anything that doesn't
// decode to a valid IP returns ok=false → normal DoH/OS resolution (never guess an address for a name we can't decode).
func libp2pDirectIP(host string) (net.IP, bool) {
	const suffix = ".libp2p.direct"
	trimmed := strings.TrimSuffix(host, ".") // tolerate a trailing root dot
	h := strings.TrimSuffix(trimmed, suffix)
	if h == trimmed { // suffix wasn't present → not a forge name
		return nil, false
	}
	label := h
	if i := strings.IndexByte(label, '.'); i >= 0 {
		label = label[:i] // leading label = the encoded IP; the rest is the peer id
	}
	if ip := net.ParseIP(strings.ReplaceAll(label, "-", ".")); ip != nil && ip.To4() != nil {
		return ip, true // IPv4: a-b-c-d
	}
	if ip := net.ParseIP(strings.ReplaceAll(label, "-", ":")); ip != nil {
		return ip, true // IPv6: ':' encoded as '-'
	}
	return nil, false
}

func (d *dohResolver) LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error) {
	if benchHostBlocked(host) { // external DNS-layer block (bench.go) - refused outright, no OS fallback
		return nil, fmt.Errorf("bench: host %q blocked (VG_BENCH_BLOCK_HOSTS)", host)
	}
	if ip, ok := libp2pDirectIP(host); ok { // AutoTLS name carries the IP → decode locally, ZERO DNS (see libp2pDirectIP)
		return []net.IPAddr{{IP: ip}}, nil
	}
	var out []net.IPAddr
	for _, qt := range []string{"A", "AAAA"} {
		ans, err := d.query(ctx, host, qt)
		if err != nil {
			continue
		}
		for _, a := range ans {
			if ip := net.ParseIP(strings.TrimSpace(a.Data)); ip != nil {
				out = append(out, net.IPAddr{IP: ip})
			}
		}
	}
	if len(out) > 0 {
		return out, nil
	}
	return net.DefaultResolver.LookupIPAddr(ctx, host) // DoH unreachable → fall back to the OS resolver
}

func (d *dohResolver) LookupTXT(ctx context.Context, name string) ([]string, error) {
	if benchHostBlocked(name) { // covers _dnsaddr.<bootstrap host> TXT lookups too
		return nil, fmt.Errorf("bench: host %q blocked (VG_BENCH_BLOCK_HOSTS)", name)
	}
	ans, err := d.query(ctx, name, "TXT")
	if err != nil {
		return net.DefaultResolver.LookupTXT(ctx, name)
	}
	var out []string
	for _, a := range ans {
		out = append(out, strings.Trim(strings.TrimSpace(a.Data), `"`)) // Cloudflare wraps TXT data in quotes
	}
	return out, nil
}

// dohMultiaddrResolver builds a madns resolver that resolves every domain via DoH — pass it to libp2p (wrapped in
// swarm.ResolverFromMaDNS) so /dnsaddr + /dns dials (bootstrap peers AND Pinata providers) work through a DNS filter.
func dohMultiaddrResolver() (*madns.Resolver, error) {
	return madns.NewResolver(madns.WithDefaultResolver(newDoHResolver()))
}

// dohHTTPClient returns an http.Client whose connections resolve hostnames via DoH before dialing — for the delegated
// routing indexer (delegated-ipfs.dev) so provider discovery works on a DNS-filtered network too. TLS still uses the
// original hostname for SNI/verification (http.Transport sets it from the request), so dialing the resolved IP is safe.
func dohHTTPClient(d *dohResolver) *http.Client {
	return &http.Client{
		Timeout:   60 * time.Second, // whole-request cap — fine for small routing responses, NOT for large downloads
		Transport: dohTransport(d),
	}
}

// dohStreamingClient is like dohHTTPClient but with NO overall request timeout — for streaming large downloads (a
// gateway CAR of a big file takes minutes). A whole-request Timeout would abort mid-download ("context deadline
// exceeded"). Instead the caller bounds it with the request context + a stall watchdog on the body; the transport's
// ResponseHeaderTimeout still catches a gateway that never starts responding.
//
// gatewayHeaderTimeout bounds how long a route gets to produce response headers — per hop in the transport, and in
// aggregate per candidate (fetchViaGateway arms a timer over the whole open, because the public routes are redirect
// chains). It is also the whole cost of probing a gateway for content it does not have: no cheap definitive negative
// exists (tools/gwprobe.sh + dated output: Pinata 404s an absent CID only at ~62 s; only-if-cached answered 412 for
// present content in one manual probe and 200 in the committed one — inconsistent, so not usable). 45 s = ~2× margin
// over the worst observed public-backend success (a 20.3 s first block, run-6; that run's log was overwritten —
// results now survive one generation as .prev). Cost where NO gateway has the content (a friend's own package):
// each stalled attempt pays hedge delay + this in the resume, so the offline-friend cycle is stall 20 s + ~55 s +
// backoff ≈ 80 s per lap, for as long as the friend stays offline. (var: tests shrink it)
var gatewayHeaderTimeout = 45 * time.Second

func dohStreamingClient(d *dohResolver) *http.Client {
	t := dohTransport(d)
	t.ResponseHeaderTimeout = gatewayHeaderTimeout
	t.IdleConnTimeout = 90 * time.Second
	return &http.Client{Transport: t} // Timeout: 0 — no whole-request cap; the context + stall watchdog bound it
}

// dohTransport builds an http.Transport whose connections resolve hostnames via DoH before dialing (TLS SNI/Host stay
// the hostname, so it works through DNS filters AND behind Cloudflare).
func dohTransport(d *dohResolver) *http.Transport {
	dialer := &net.Dialer{Timeout: 15 * time.Second}
	return &http.Transport{
		ForceAttemptHTTP2: true,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(addr)
			if err != nil || net.ParseIP(host) != nil {
				return dialer.DialContext(ctx, network, addr) // already an IP (or unparseable) → dial as-is
			}
			// A DELIBERATE block (VG_BENCH_BLOCK_HOSTS) is refused HERE, before the lookup: the fallback below
			// re-dials the bare hostname through the OS resolver when DoH fails — right for an unreachable DoH,
			// but it silently punched through the block (the path matrix saw the "blocked" indexer answering).
			if benchHostBlocked(host) {
				return nil, fmt.Errorf("bench: host %q blocked (VG_BENCH_BLOCK_HOSTS)", host)
			}
			ips, err := d.LookupIPAddr(ctx, host)
			if err != nil || len(ips) == 0 {
				return dialer.DialContext(ctx, network, addr)
			}
			var lastErr error
			for _, ip := range ips {
				if conn, e := dialer.DialContext(ctx, network, net.JoinHostPort(ip.IP.String(), port)); e == nil {
					return conn, nil
				} else {
					lastErr = e
				}
			}
			return nil, lastErr
		},
	}
}
