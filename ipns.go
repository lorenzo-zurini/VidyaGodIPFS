package main

// ipns.go — the mutable layer: a peer's Ed25519 identity key (its friend code / peer ID) signs a tiny IPNS record
// pointing the peer's NAME at the current library-index CID. Everything under that name stays content-addressed and
// verifiable exactly as before; only this one record moves when the publisher hits "Verify/Publish".
//
// Publish: namesys over the DHT (the datastore carries the sequence number across restarts, so a republish always
// supersedes). Resolve: DHT first, then an HTTPS-gateway fallback that fetches the raw signed record and verifies it
// against the NAME — no key lookup needed because Ed25519 pubkeys inline into the peer ID — so it works on
// DNS/DHT-filtered networks, the same reason the block fetch has a gateway fallback (project_gateway_fallback_dead_ctx).

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ipfs/boxo/ipns"
	bpath "github.com/ipfs/boxo/path"
	cid "github.com/ipfs/go-cid"
	ci "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	// A LONG EOL so a subscriber's cached index stays valid across our downtime — expiry costs only UPDATES here (the
	// package CIDs inside still resolve from Pinata/any seeder indefinitely). The republish loop refreshes well before.
	ipnsDefaultEOL = 30 * 24 * time.Hour
	ipnsDefaultTTL = 1 * time.Hour
	ipnsRepublish  = 6 * time.Hour // ≪ EOL, so several refreshes happen before a record could ever expire
	ipnsRecordMIME = "application/vnd.ipfs.ipns-record"
)

// ipnsSelfFile persists the CID we last published under our OWN name, so the republish loop can refresh it after a
// restart without the app re-issuing the publish.
func (n *node) ipnsSelfFile() string { return filepath.Join(n.repoPath, "ipns_self.cid") }

// ipnsSeqFile persists the highest IPNS sequence seen per name: for OUR name so a publish ALWAYS supersedes even after
// a repo wipe / identity restore (never resetting below a live record the DHT still serves), and for RESOLVED names so
// a rolled-back (lower-seq) record from a stale cache or hostile gateway is rejected. Keyed by name.String().
func (n *node) ipnsSeqFile() string { return filepath.Join(n.repoPath, "ipns_seq.json") }

func (n *node) ipnsLoadSelf() {
	if b, err := os.ReadFile(n.ipnsSelfFile()); err == nil {
		if s := strings.TrimSpace(string(b)); s != "" {
			n.ipnsMu.Lock()
			n.ipnsCurrent = s
			n.ipnsMu.Unlock()
		}
	}
}

func (n *node) ipnsStoreSelf(cidStr string) {
	n.ipnsMu.Lock()
	n.ipnsCurrent = cidStr
	n.ipnsMu.Unlock()
	if err := os.WriteFile(n.ipnsSelfFile(), []byte(cidStr+"\n"), 0o600); err != nil {
		// Surfaced, not swallowed: a failed persist silently kills post-restart republish and the address dies at EOL.
		fmt.Fprintf(os.Stderr, "[ipns] WARNING: could not persist self value: %v — republish across restart may fail\n", err)
	}
}

// ---- sequence high-water map (guarded by ipnsMu; small JSON in the repo) ----

func (n *node) ipnsLoadSeq() map[string]uint64 {
	m := map[string]uint64{}
	if b, err := os.ReadFile(n.ipnsSeqFile()); err == nil {
		_ = json.Unmarshal(b, &m)
	}
	return m
}
func (n *node) ipnsSeenSeq(nameStr string) uint64 {
	n.ipnsMu.Lock()
	defer n.ipnsMu.Unlock()
	return n.ipnsLoadSeq()[nameStr]
}
func (n *node) ipnsRecordSeq(nameStr string, seq uint64) {
	n.ipnsMu.Lock()
	defer n.ipnsMu.Unlock()
	m := n.ipnsLoadSeq()
	if seq > m[nameStr] {
		m[nameStr] = seq
		if b, err := json.Marshal(m); err == nil {
			if werr := os.WriteFile(n.ipnsSeqFile(), b, 0o600); werr != nil {
				// Surfaced, not swallowed: a failed persist silently drops rollback protection across a restart.
				fmt.Fprintf(os.Stderr, "[ipns] WARNING: could not persist seq high-water: %v\n", werr)
			}
		}
	}
}
// ipnsAcceptSeq enforces monotonicity: accept iff seq >= the highest seen for this name, then record the new high-water.
func (n *node) ipnsAcceptSeq(nameStr string, seq uint64) bool {
	if seq < n.ipnsSeenSeq(nameStr) {
		return false
	}
	n.ipnsRecordSeq(nameStr, seq)
	return true
}

func (n *node) ipnsName() (ipns.Name, error) {
	pid, err := peer.IDFromPrivateKey(n.priv)
	if err != nil {
		return ipns.Name{}, err
	}
	return ipns.NameFromPeer(pid), nil
}

// liveRecordSeq best-effort reads the CURRENT record's seq for a name (DHT then gateway), so a publish never picks a
// seq the live network already beat (the wipe/restore case). (0,false) if none is reachable.
func (n *node) liveRecordSeq(ctx context.Context, nm ipns.Name) (uint64, bool) {
	if n.dht != nil {
		if data, err := n.dht.GetValue(ctx, string(nm.RoutingKey())); err == nil {
			if rec, err := ipns.UnmarshalRecord(data); err == nil && ipns.ValidateWithName(rec, nm) == nil {
				if s, err := rec.Sequence(); err == nil {
					return s, true
				}
			}
		}
	}
	if _, s, err := ipnsRecordOverHTTP(ctx, dohHTTPClient(newDoHResolver()), trustlessGateways, nm); err == nil {
		return s, true
	}
	return 0, false
}

// ipnsPublish signs+publishes an IPNS record for OUR name pointing at /ipfs/<cid>, with a long EOL and a sequence that
// supersedes BOTH our last publish and any live record — so a publish after a repo wipe / identity restore is not
// silently shadowed by an older-but-higher-seq record still on the DHT.
func (n *node) ipnsPublish(ctx context.Context, cidStr string, ttl time.Duration) error {
	if n.dht == nil || n.priv == nil {
		return fmt.Errorf("IPNS not ready (node offline)")
	}
	c, err := cid.Decode(strings.TrimSpace(cidStr))
	if err != nil {
		return fmt.Errorf("bad CID %q: %w", cidStr, err)
	}
	if ttl <= 0 {
		ttl = ipnsDefaultTTL
	}
	nm, err := n.ipnsName()
	if err != nil {
		return err
	}
	nameStr := nm.String()
	seq := n.ipnsSeenSeq(nameStr)
	if live, ok := n.liveRecordSeq(ctx, nm); ok && live > seq {
		seq = live
	}
	seq++
	rec, err := ipns.NewRecord(n.priv, bpath.FromCid(c), seq, time.Now().Add(ipnsDefaultEOL), ttl)
	if err != nil {
		return err
	}
	data, err := ipns.MarshalRecord(rec)
	if err != nil {
		return err
	}
	if err := n.dht.PutValue(ctx, string(nm.RoutingKey()), data); err != nil {
		return err
	}
	n.ipnsRecordSeq(nameStr, seq)
	n.ipnsStoreSelf(c.String())
	return nil
}

// ipnsRepublishLoop refreshes our own record before its EOL for as long as the node is online. Own record only.
func (n *node) ipnsRepublishLoop() {
	n.ipnsLoadSelf()
	// An early refresh once the routing table is warm (the value may have been set before we had peers).
	select {
	case <-time.After(90 * time.Second):
	case <-n.ctx.Done():
		return
	}
	for {
		n.ipnsMu.Lock()
		cur := n.ipnsCurrent
		n.ipnsMu.Unlock()
		if cur != "" {
			ctx, cancel := context.WithTimeout(n.ctx, 60*time.Second)
			if err := n.ipnsPublish(ctx, cur, ipnsDefaultTTL); err != nil {
				fmt.Fprintf(os.Stderr, "[ipns] republish %s failed: %v\n", shortCid(cidForLog(cur)), err)
			}
			cancel()
		}
		select {
		case <-time.After(ipnsRepublish):
		case <-n.ctx.Done():
			return
		}
	}
}

// ipnsResolve resolves /ipns/<name> to its current /ipfs/<cid> path string. DHT (raw record) first, then the HTTPS
// gateway fallback with a FRESH context — never the exhausted one (project_gateway_fallback_dead_ctx). Both verify the
// signature against the name AND reject a record whose sequence is LOWER than the highest already seen for that name
// (a rollback from a stale cache or hostile gateway).
func (n *node) ipnsResolve(ctx context.Context, name string) (string, error) {
	nm, err := ipns.NameFromString(name)
	if err != nil {
		return "", fmt.Errorf("bad IPNS name %q: %w", name, err)
	}
	nameStr := nm.String()
	if n.dht != nil {
		dctx, cancel := context.WithTimeout(ctx, 20*time.Second)
		data, gerr := n.dht.GetValue(dctx, string(nm.RoutingKey()))
		cancel()
		if gerr == nil {
			if rec, uerr := ipns.UnmarshalRecord(data); uerr == nil && ipns.ValidateWithName(rec, nm) == nil {
				if val, verr := rec.Value(); verr == nil {
					seq, _ := rec.Sequence()
					if !n.ipnsAcceptSeq(nameStr, seq) {
						return "", fmt.Errorf("ipns %s: DHT record seq %d is a rollback — rejected", nameStr, seq)
					}
					return val.String(), nil
				}
			}
		}
		fdbg("ipns: DHT resolve of %s failed (%v) — trying gateways", nameStr, gerr)
	}
	// Fresh context DERIVED FROM the node ctx (so it dies at VgStop, not context.Background which would outlive it).
	gwctx, gwcancel := context.WithTimeout(n.ctx, 30*time.Second)
	defer gwcancel()
	val, seq, herr := ipnsRecordOverHTTP(gwctx, dohHTTPClient(newDoHResolver()), trustlessGateways, nm)
	if herr != nil {
		return "", herr
	}
	if !n.ipnsAcceptSeq(nameStr, seq) {
		return "", fmt.Errorf("ipns %s: gateway record seq %d is a rollback — rejected", nameStr, seq)
	}
	return val, nil
}

// ipnsRecordOverHTTP fetches the raw signed IPNS record over HTTPS from each trustless gateway (DoH-resolved), validates
// it against the NAME locally, and returns its value path + sequence — so it needs neither the DHT nor DNS (the
// filtered-net path). The http client and gateway list are injected so tests can drive it against an httptest server.
func ipnsRecordOverHTTP(ctx context.Context, hc *http.Client, gateways []string, nm ipns.Name) (string, uint64, error) {
	var lastErr error
	for _, gw := range gateways {
		url := strings.TrimRight(gw, "/") + "/ipns/" + nm.String()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			lastErr = err
			continue
		}
		req.Header.Set("Accept", ipnsRecordMIME)
		resp, err := hc.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		body, rerr := io.ReadAll(io.LimitReader(resp.Body, 1<<20)) // an IPNS record is tiny; cap to be safe
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK || rerr != nil {
			lastErr = fmt.Errorf("gateway %s: status %d", gw, resp.StatusCode)
			continue
		}
		rec, err := ipns.UnmarshalRecord(body)
		if err != nil {
			lastErr = fmt.Errorf("gateway %s: bad record: %w", gw, err)
			continue
		}
		if err := ipns.ValidateWithName(rec, nm); err != nil { // signature + name binding — a forged record dies here
			lastErr = fmt.Errorf("gateway %s: record failed validation: %w", gw, err)
			continue
		}
		val, err := rec.Value()
		if err != nil {
			lastErr = fmt.Errorf("gateway %s: record has no value: %w", gw, err)
			continue
		}
		seq, _ := rec.Sequence()
		return val.String(), seq, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no gateways configured")
	}
	return "", 0, fmt.Errorf("IPNS resolve of %s failed over all gateways: %w", nm.String(), lastErr)
}

// cidForLog is a logging helper — a bad string yields cid.Undef, which shortCid renders harmlessly.
func cidForLog(s string) cid.Cid { c, _ := cid.Decode(s); return c }

// exportIdentity copies the Ed25519 private key (identity.key = the friend code + library address) to dest. Losing it
// is permanent (a new key = a new peer ID = a new address nobody has), so this is the "back up your identity" path.
func (n *node) exportIdentity(dest string) error {
	src := filepath.Join(n.repoPath, "identity.key")
	b, err := os.ReadFile(src)
	if err != nil {
		return fmt.Errorf("no identity key yet (bring the node online once): %w", err)
	}
	if _, err := ci.UnmarshalPrivateKey(b); err != nil { // sanity: never export a corrupt file as a "backup"
		return fmt.Errorf("stored identity key is not a valid key: %w", err)
	}
	if err := os.WriteFile(dest, b, 0o600); err != nil {
		return fmt.Errorf("write backup %q: %w", dest, err)
	}
	return nil
}

// importIdentity validates src is a real Ed25519 key and installs it as the node's identity. The RUNNING node keeps
// the old key until it is restarted (libp2p.Identity is fixed at goOnline) — the caller MUST warn + restart.
// Safety: (1) the old key is copied aside FIRST so a crash mid-install never loses both identities; (2) the new key is
// written to a temp file and atomically renamed (never a torn truncate over identity.key); (3) the stale
// ipns_self.cid / ipns_seq.json are removed so the republish loop can't re-sign the PREVIOUS identity's index under
// the restored name after restart (they belong to the old key).
func (n *node) importIdentity(src string) error {
	b, err := os.ReadFile(src)
	if err != nil {
		return fmt.Errorf("read %q: %w", src, err)
	}
	if _, err := ci.UnmarshalPrivateKey(b); err != nil {
		return fmt.Errorf("not a valid identity key: %w", err)
	}
	dst := filepath.Join(n.repoPath, "identity.key")
	// 1. Back up the current key (if any) beside it — recoverable if the user restored the wrong file.
	if old, rerr := os.ReadFile(dst); rerr == nil {
		_ = os.WriteFile(dst+".prev", old, 0o600)
	}
	// 2. Atomic install: temp file + rename.
	tmp := dst + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return fmt.Errorf("stage identity: %w", err)
	}
	if err := os.Rename(tmp, dst); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("install identity: %w", err)
	}
	// 3. Drop the previous identity's published VALUE so the republish loop can't re-sign the OLD identity's index
	//    under the restored name. Keep ipns_seq.json: the new identity has a different name (a fresh entry), and the
	//    map also holds FRIENDS' rollback high-waters, which must survive an identity restore.
	_ = os.Remove(n.ipnsSelfFile())
	n.ipnsMu.Lock()
	n.ipnsCurrent = ""
	n.ipnsMu.Unlock()
	return nil
}
