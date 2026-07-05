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
	hc       *http.Client
	endpoint string // https://1.1.1.1/dns-query — IP literal, needs no DNS to reach
}

func newDoHResolver() *dohResolver {
	return &dohResolver{
		endpoint: "https://1.1.1.1/dns-query",
		hc:       &http.Client{Timeout: 10 * time.Second}, // dials the IP literal directly; TLS SNI covers 1.1.1.1
	}
}

type dohAnswer struct {
	Type int    `json:"type"` // 1=A, 28=AAAA, 16=TXT
	Data string `json:"data"`
}

func (d *dohResolver) query(ctx context.Context, name, qtype string) ([]dohAnswer, error) {
	u := d.endpoint + "?name=" + url.QueryEscape(name) + "&type=" + qtype
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

func (d *dohResolver) LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error) {
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
func dohStreamingClient(d *dohResolver) *http.Client {
	t := dohTransport(d)
	t.ResponseHeaderTimeout = 45 * time.Second
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
