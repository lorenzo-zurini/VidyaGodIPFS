package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ipfs/boxo/ipns"
	bpath "github.com/ipfs/boxo/path"
	cid "github.com/ipfs/go-cid"
	ci "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

// A stable, valid CIDv1 to point records at (content-irrelevant for these tests).
const ipnsTestCid = "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi"

func mkKeyName(t *testing.T) (ci.PrivKey, ipns.Name) {
	t.Helper()
	priv, _, err := ci.GenerateKeyPair(ci.Ed25519, -1)
	if err != nil {
		t.Fatalf("keygen: %v", err)
	}
	pid, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatalf("peer id: %v", err)
	}
	return priv, ipns.NameFromPeer(pid)
}

func signRecord(t *testing.T, priv ci.PrivKey, cidStr string, seq uint64) []byte {
	t.Helper()
	c, err := cid.Decode(cidStr)
	if err != nil {
		t.Fatalf("cid: %v", err)
	}
	rec, err := ipns.NewRecord(priv, bpath.FromCid(c), seq, time.Now().Add(time.Hour), time.Hour)
	if err != nil {
		t.Fatalf("NewRecord: %v", err)
	}
	data, err := ipns.MarshalRecord(rec)
	if err != nil {
		t.Fatalf("MarshalRecord: %v", err)
	}
	return data
}

// The security spine: a record must verify from the NAME alone (Ed25519 pubkey inlines into the peer ID), and any
// tampering or a name mismatch must be REJECTED — this is exactly the check the gateway-fallback resolve leans on to
// trust an untrusted mirror. Mutation: drop ValidateWithName in ipnsResolveOverHTTP and the forgery half stops failing.
func TestIpnsRecordVerifiesFromNameAloneAndRejectsTampering(t *testing.T) {
	priv, nm := mkKeyName(t)
	data := signRecord(t, priv, ipnsTestCid, 1)

	rec, err := ipns.UnmarshalRecord(data)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if err := ipns.ValidateWithName(rec, nm); err != nil {
		t.Fatalf("a genuine record must validate against its own name: %v", err)
	}
	val, err := rec.Value()
	if err != nil || val.String() != "/ipfs/"+ipnsTestCid {
		t.Fatalf("value = %v (%v), want /ipfs/%s", val, err, ipnsTestCid)
	}

	// Wrong name (another key's) must be rejected — a record is bound to exactly one name.
	_, other := mkKeyName(t)
	if err := ipns.ValidateWithName(rec, other); err == nil {
		t.Fatal("a record validated against a DIFFERENT name — signatures are not name-bound")
	}

	// A flipped byte must fail signature verification.
	bad := make([]byte, len(data))
	copy(bad, data)
	bad[len(bad)/2] ^= 0xFF
	if brec, err := ipns.UnmarshalRecord(bad); err == nil {
		if err := ipns.ValidateWithName(brec, nm); err == nil {
			t.Fatal("a tampered record validated — the signature check is not effective")
		}
	}
}

// The filtered-net path: resolve a name by fetching the raw signed record over HTTP and validating it locally, with
// NO DHT and NO DNS. A gateway that serves a record signed by the WRONG key (a forgery for that name) must be
// rejected, not trusted. Mutation: make ipnsResolveOverHTTP skip ValidateWithName → the forgery case returns a CID.
func TestIpnsResolveOverHTTPRoundTripAndForgeryRejected(t *testing.T) {
	priv, nm := mkKeyName(t)
	genuine := signRecord(t, priv, ipnsTestCid, 1)

	serve := func(body []byte) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// The client requests /ipns/<name> with the ipns-record Accept header.
			if r.URL.Path != "/ipns/"+nm.String() {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", ipnsRecordMIME)
			w.Write(body)
		}))
	}

	// Genuine record → resolves to the pointed-at path (and reports its sequence).
	srv := serve(genuine)
	defer srv.Close()
	got, gotSeq, err := ipnsRecordOverHTTP(context.Background(), srv.Client(), []string{srv.URL}, nm)
	if err != nil {
		t.Fatalf("resolve of a genuine record failed: %v", err)
	}
	if got != "/ipfs/"+ipnsTestCid {
		t.Fatalf("resolved %q, want /ipfs/%s", got, ipnsTestCid)
	}
	if gotSeq != 1 {
		t.Fatalf("seq = %d, want 1", gotSeq)
	}

	// A record signed by a DIFFERENT key, served for OUR name → must be rejected (forgery).
	otherPriv, _ := mkKeyName(t)
	forged := signRecord(t, otherPriv, ipnsTestCid, 9)
	fsrv := serve(forged)
	defer fsrv.Close()
	if _, _, err := ipnsRecordOverHTTP(context.Background(), fsrv.Client(), []string{fsrv.URL}, nm); err == nil {
		t.Fatal("a forged record (wrong signing key) resolved — the gateway path trusts unsigned mirrors")
	}
}

// Sequence monotonicity (project code, persisted to the repo): a name's highest-seen seq is remembered, and a record
// whose seq REGRESSES (a rollback from a stale cache or a hostile gateway) is rejected while an equal-or-higher one is
// accepted. Mutation: make ipnsAcceptSeq always return true → the rollback assertion fails.
func TestIpnsAcceptSeqRejectsRollback(t *testing.T) {
	n := offlineNode(t)
	_, nm := mkKeyName(t)
	name := nm.String()

	if !n.ipnsAcceptSeq(name, 5) {
		t.Fatal("first sighting (seq 5) must be accepted")
	}
	if !n.ipnsAcceptSeq(name, 5) {
		t.Fatal("an equal seq (a plain republish) must still be accepted")
	}
	if !n.ipnsAcceptSeq(name, 7) {
		t.Fatal("a higher seq (a genuine update) must be accepted")
	}
	if n.ipnsAcceptSeq(name, 6) {
		t.Fatal("a LOWER seq (a rollback) must be REJECTED")
	}
	if n.ipnsSeenSeq(name) != 7 {
		t.Fatalf("high-water = %d, want 7 (a rejected rollback must not lower it)", n.ipnsSeenSeq(name))
	}
	// And it PERSISTS: a fresh read of the seq file still bars the rollback.
	if got := n.ipnsLoadSeq()[name]; got != 7 {
		t.Fatalf("persisted high-water = %d, want 7", got)
	}
}
