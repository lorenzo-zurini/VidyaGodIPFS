package main

// session_test.go — the session/lobby layer over two real libp2p hosts in-process. Proves host-authoritative roster
// convergence and distinct overlay vIP assignment: the host creates a session, the joiner joins, and both ends see a
// two-member roster with different vIPs in the same /24.

import (
	"context"
	"testing"
)

func TestSessionJoinRosterAndVIPs(t *testing.T) {
	hA, hB := testHost(t), testHost(t) // host helpers from friend_test.go
	connectHosts(t, hA, hB)

	sA := newSocialState(t.TempDir())
	sB := newSocialState(t.TempDir())
	sA.setProfile("host", "")
	sB.setProfile("joiner", "")
	// The session layer only surfaces invites from accepted friends; make them mutual friends up front.
	sA.upsert(hB.ID().String(), func(c *contact) { c.State = stAccepted })
	sB.upsert(hA.ID().String(), func(c *contact) { c.State = stAccepted })

	ctx := context.Background()
	ssA := newSessionService(ctx, hA, nil, sA, nil)
	ssB := newSessionService(ctx, hB, nil, sB, nil)
	ssA.start()
	ssB.start()

	// Host creates a session; it should immediately hold itself as member .1.
	s, err := ssA.createSession("QmGameCID")
	if err != nil {
		t.Fatalf("createSession: %v", err)
	}
	if m := s.Members[hA.ID().String()]; m == nil || m.VIP == "" {
		t.Fatalf("host not seeded as a member with a vIP: %+v", s.Members)
	}

	// Joiner joins by session id + host peer id.
	if err := ssB.join(s.ID, hA.ID().String()); err != nil {
		t.Fatalf("join: %v", err)
	}

	// Both ends converge on a 2-member roster.
	waitFor(t, "host sees 2 members", func() bool {
		snap, ok := ssA.snapshot(s.ID)
		return ok && len(snap["members"].([]member)) == 2
	})
	waitFor(t, "joiner receives the roster", func() bool {
		snap, ok := ssB.snapshot(s.ID)
		return ok && len(snap["members"].([]member)) == 2
	})

	// vIPs must be distinct and in the same /24.
	ma, _ := ssA.getSession(s.ID)
	va := ma.Members[hA.ID().String()].VIP
	vb := ma.Members[hB.ID().String()].VIP
	if va == "" || vb == "" || va == vb {
		t.Fatalf("bad vIP assignment: host=%q joiner=%q", va, vb)
	}

	// The joiner's replica agrees on its own vIP and knows it is NOT the host.
	mb, _ := ssB.getSession(s.ID)
	if mb.amHost {
		t.Fatalf("joiner should not think it is the host")
	}
	if got := mb.Members[hB.ID().String()].VIP; got != vb {
		t.Fatalf("joiner's replica vIP %q != host's assignment %q", got, vb)
	}

	// Ready-state propagates host-ward and back into the roster (checked via the locked snapshot).
	if err := ssB.setReady(s.ID, true); err != nil {
		t.Fatalf("setReady: %v", err)
	}
	joinerReady := func(ss *sessionService) bool {
		snap, ok := ss.snapshot(s.ID)
		if !ok {
			return false
		}
		for _, m := range snap["members"].([]member) {
			if m.PeerID == hB.ID().String() {
				return m.Ready
			}
		}
		return false
	}
	waitFor(t, "host sees joiner ready", func() bool { return joinerReady(ssA) })
	waitFor(t, "joiner's replica shows ready", func() bool { return joinerReady(ssB) })

	// launchVars maps the session into the CustomVars a game launch (Goldberg) consumes: our own vIP + the peer's vIP.
	vars, okv := ssA.launchVars(s.ID)
	if !okv {
		t.Fatalf("launchVars failed")
	}
	if vars["VIDYAGOD_SANDBOX"] != "on" || vars["VIDYAGOD_SELF_VIP"] != va || vars["VIDYAGOD_PEER_VIPS"] != vb {
		t.Fatalf("bad launch vars: %+v (self=%s peer=%s)", vars, va, vb)
	}
}
