package main

// health_test.go — the health report must be truthful at every node state: no node at all, an offline node
// (repo open, network deliberately off), and it must never panic or drain state it reports on.

import (
	"encoding/json"
	"os"
	"testing"

	cid "github.com/ipfs/go-cid"
)

func mustCid(t *testing.T) cid.Cid {
	t.Helper()
	c, err := cid.Decode("QmUNLLsPACCz1vLxQVkXqqLX5R1X345qqfHbsf67hvA3Nn")
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestHealthReportNoNode(t *testing.T) {
	closeNode() // ensure no global node
	r := healthReport()
	if len(r) != 1 || r[0].Name != "Node" || r[0].Status != "down" {
		t.Fatalf("no-node report should be exactly one down Node row, got %+v", r)
	}
	var parsed []healthEntry
	if err := json.Unmarshal([]byte(healthJSON()), &parsed); err != nil {
		t.Fatalf("healthJSON not valid JSON: %v", err)
	}
}

func TestHealthReportOfflineNode(t *testing.T) {
	t.Setenv("VIDYAGOD_IPFS_OFFLINE", "1")
	dir := t.TempDir()
	if err := openNode(dir); err != nil {
		t.Fatalf("openNode: %v", err)
	}
	t.Cleanup(closeNode)
	r := healthReport()
	if len(r) < 2 {
		t.Fatalf("offline report too short: %+v", r)
	}
	if r[0].Name != "Node" || r[0].Status != "ok" {
		t.Fatalf("repo is open — Node must be ok, got %+v", r[0])
	}
	if r[1].Name != "Network" || r[1].Status != "down" {
		t.Fatalf("offline node — Network must be down, got %+v", r[1])
	}
}

func TestServeFailureCountDoesNotDrain(t *testing.T) {
	f := newFailureLog()
	f.record(mustCid(t), os.ErrNotExist)
	if f.count() != 1 || f.count() != 1 {
		t.Fatal("count must be repeatable (non-draining)")
	}
	if len(f.drain()) != 1 {
		t.Fatal("drain must still see the entry after count()")
	}
}
