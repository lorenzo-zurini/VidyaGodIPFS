package main

// session.go — the multiplayer session / lobby layer that sits on top of the friends layer. A session is a group of
// friends who agree to play one game together over a private overlay network. This file implements the SIGNALLING:
// who is in the session, and which private "virtual LAN" IP each member is assigned. The actual packet tunnel (the
// bubblewrap-scoped overlay that carries game traffic between those vIPs) is a later phase — this layer produces the
// roster + vIP map that the tunnel and the LAN emulator (Goldberg et al.) are configured from.
//
// The host of a session is authoritative: it assigns vIPs and broadcasts the roster. Members hold a replica updated
// from the host's roster broadcasts. Coordination rides a dedicated /vidyagod/session/1.0.0 protocol. Like
// friendService, sessionService is decoupled from the node singleton so it can be driven by two real hosts in-process
// (session_test.go).

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	host "github.com/libp2p/go-libp2p/core/host"
	network "github.com/libp2p/go-libp2p/core/network"
	peer "github.com/libp2p/go-libp2p/core/peer"
	protocol "github.com/libp2p/go-libp2p/core/protocol"
)

const sessionProtoID = protocol.ID("/vidyagod/session/1.0.0")

// Inbound session event kinds emitted to C++ (mirror IpfsWrapper::SessionEvent::Kind).
const (
	evSessionInvite = 0 // a friend invited us to a session (UI can offer to join)
	evSessionRoster = 1 // the roster changed (members / vIPs / ready state)
	evSessionEnded  = 2 // the session ended or we left / were removed
)

// member is one participant in a session: their peer ID, nickname, assigned overlay vIP, and ready state.
type member struct {
	PeerID string `json:"peer"`
	Nick   string `json:"nick,omitempty"`
	VIP    string `json:"vip"` // overlay virtual IP, e.g. 10.66.42.2
	Ready  bool   `json:"ready"`
}

// session is one lobby. The host holds the authoritative members map + the vIP allocator; a member holds a replica.
type session struct {
	ID      string             `json:"id"`
	Host    string             `json:"host"` // host peer ID
	Game    string             `json:"game"` // game package CID both sides must hold
	Subnet  string             `json:"subnet"`
	Members map[string]*member `json:"members"` // peerID → member
	amHost  bool
	net     byte // host-only: the N in the 10.66.N.0/24 subnet (assigns vIPs)
	nextIP  byte // host-only: next vIP host octet to hand out (1=host, then 2,3,…)
}

// roster returns the members as a stable slice (for events / the C ABI).
func (s *session) roster() []member {
	out := make([]member, 0, len(s.Members))
	for _, m := range s.Members {
		out = append(out, *m)
	}
	return out
}

// sessionMsg is the framed wire message for /vidyagod/session.
type sessionMsg struct {
	Type    string   `json:"t"`             // invite | join | roster | ready | leave
	SID     string   `json:"sid"`           // session id
	Game    string   `json:"game,omitempty"`
	Subnet  string   `json:"subnet,omitempty"`
	Nick    string   `json:"nick,omitempty"` // sender nickname (join)
	Ready   bool     `json:"ready,omitempty"`
	Members []member `json:"members,omitempty"` // host→members authoritative roster
}

// sessionService binds the host + peer router + address book (for nicknames) + event sink.
type sessionService struct {
	ctx    context.Context
	host   host.Host
	router peerRouter
	social *socialState
	emit   func(kind int, jsonPayload string)

	mu       sync.Mutex
	sessions map[string]*session
}

func newSessionService(ctx context.Context, h host.Host, r peerRouter, s *socialState, emit func(int, string)) *sessionService {
	return &sessionService{ctx: ctx, host: h, router: r, social: s, emit: emit, sessions: map[string]*session{}}
}

func (ss *sessionService) start() {
	ss.host.SetStreamHandler(sessionProtoID, ss.handleStream)
}

func (ss *sessionService) myNick() string {
	if ss.social == nil {
		return ""
	}
	return ss.social.getProfile().Nick
}

func (ss *sessionService) emitSession(kind int, payload any) {
	if ss.emit == nil {
		return
	}
	b, _ := json.Marshal(payload)
	ss.emit(kind, string(b))
}

// emitRoster pushes the current roster of a session up to the UI.
func (ss *sessionService) emitRoster(s *session) {
	ss.emitSession(evSessionRoster, map[string]any{
		"id": s.ID, "host": s.Host, "game": s.Game, "subnet": s.Subnet, "members": s.roster(),
	})
}

// newSubnet derives a per-session /24 overlay subnet 10.66.N.0/24 from the session id (deterministic, collision-rare).
func newSubnet(sid string) (string, byte) {
	var n byte = 1
	if len(sid) >= 2 {
		if b, err := hex.DecodeString(sid[:2]); err == nil && len(b) == 1 {
			n = b[0]
		}
	}
	if n == 0 {
		n = 1
	}
	return fmt.Sprintf("10.66.%d.0/24", n), n
}

// vipFor builds a vIP in a session's subnet for a given host octet (10.66.N.<octet>). Host-only (uses s.net).
func (s *session) vipFor(octet byte) string {
	return fmt.Sprintf("10.66.%d.%d", s.net, octet)
}

// createSession makes a new session we host, assigning ourselves vIP .1.
func (ss *sessionService) createSession(gameCID string) (*session, error) {
	var raw [8]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return nil, err
	}
	sid := hex.EncodeToString(raw[:])
	subnet, n := newSubnet(sid)
	me := ss.host.ID().String()
	s := &session{
		ID: sid, Host: me, Game: gameCID, Subnet: subnet,
		Members: map[string]*member{}, amHost: true, net: n, nextIP: 1,
	}
	s.Members[me] = &member{PeerID: me, Nick: ss.myNick(), VIP: s.vipFor(1), Ready: true}
	s.nextIP = 2
	ss.mu.Lock()
	ss.sessions[sid] = s
	ss.mu.Unlock()
	ss.emitRoster(s)
	return s, nil
}

func (ss *sessionService) getSession(sid string) (*session, bool) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	s, ok := ss.sessions[sid]
	return s, ok
}

// dialPeer ensures a connection to pid (resolving via the router/DHT if not already connected).
func dialPeer(ctx context.Context, h host.Host, router peerRouter, pid peer.ID) error {
	if h.Network().Connectedness(pid) == network.Connected {
		return nil
	}
	if router != nil {
		ai, err := router.FindPeer(ctx, pid)
		if err != nil {
			return fmt.Errorf("locate peer: %w", err)
		}
		return h.Connect(ctx, ai)
	}
	return h.Connect(ctx, peer.AddrInfo{ID: pid})
}

// send opens a one-shot /vidyagod/session stream to a peer and writes one message.
func (ss *sessionService) send(pidStr string, m sessionMsg) error {
	pid, err := peer.Decode(pidStr)
	if err != nil {
		return fmt.Errorf("bad peer id: %w", err)
	}
	ctx, cancel := context.WithTimeout(ss.ctx, 30*time.Second)
	defer cancel()
	if err := dialPeer(ctx, ss.host, ss.router, pid); err != nil {
		return err
	}
	s, err := ss.host.NewStream(ctx, pid, sessionProtoID)
	if err != nil {
		return fmt.Errorf("open stream: %w", err)
	}
	defer s.Close()
	return json.NewEncoder(s).Encode(&m)
}

// invite asks a friend to join a session we host.
func (ss *sessionService) invite(sid, peerID string) error {
	s, ok := ss.getSession(sid)
	if !ok || !s.amHost {
		return fmt.Errorf("not the host of session %s", sid)
	}
	return ss.send(peerID, sessionMsg{Type: "invite", SID: sid, Game: s.Game, Subnet: s.Subnet})
}

// join requests to join a session hosted by hostPeer. The host replies (and broadcasts) an authoritative roster.
func (ss *sessionService) join(sid, hostPeer string) error {
	// Provisional local record so the UI shows "joining" until the roster lands.
	ss.mu.Lock()
	if _, ok := ss.sessions[sid]; !ok {
		ss.sessions[sid] = &session{ID: sid, Host: hostPeer, Members: map[string]*member{}}
	}
	ss.mu.Unlock()
	return ss.send(hostPeer, sessionMsg{Type: "join", SID: sid, Nick: ss.myNick()})
}

// leave removes us from a session locally and notifies the host (or, if we ARE the host, tells everyone it ended).
func (ss *sessionService) leave(sid string) error {
	s, ok := ss.getSession(sid)
	if !ok {
		return fmt.Errorf("no such session")
	}
	if s.amHost {
		for pid := range s.Members {
			if pid != s.Host {
				go func(p string) { _ = ss.send(p, sessionMsg{Type: "leave", SID: sid}) }(pid)
			}
		}
	} else {
		_ = ss.send(s.Host, sessionMsg{Type: "leave", SID: sid})
	}
	ss.mu.Lock()
	delete(ss.sessions, sid)
	ss.mu.Unlock()
	ss.emitSession(evSessionEnded, map[string]string{"id": sid})
	return nil
}

// setReady flips our ready flag and notifies the host (host applies + rebroadcasts).
func (ss *sessionService) setReady(sid string, ready bool) error {
	s, ok := ss.getSession(sid)
	if !ok {
		return fmt.Errorf("no such session")
	}
	if s.amHost {
		ss.applyReady(s, s.Host, ready)
		return nil
	}
	return ss.send(s.Host, sessionMsg{Type: "ready", SID: sid, Ready: ready})
}

func (ss *sessionService) applyReady(s *session, peerID string, ready bool) {
	ss.mu.Lock()
	if m := s.Members[peerID]; m != nil {
		m.Ready = ready
	}
	ss.mu.Unlock()
	if s.amHost {
		ss.broadcastRoster(s)
	}
	ss.emitRoster(s)
}

// broadcastRoster (host only) sends the authoritative member list to every non-host member.
func (ss *sessionService) broadcastRoster(s *session) {
	ss.mu.Lock()
	members := s.roster()
	targets := make([]string, 0, len(s.Members))
	for pid := range s.Members {
		if pid != s.Host {
			targets = append(targets, pid)
		}
	}
	ss.mu.Unlock()
	msg := sessionMsg{Type: "roster", SID: s.ID, Game: s.Game, Subnet: s.Subnet, Members: members}
	for _, pid := range targets {
		go func(p string) { _ = ss.send(p, msg) }(pid)
	}
}

// sessionMap builds the C-ABI JSON view of a session (members as an array). Caller holds ss.mu.
func sessionMap(s *session) map[string]any {
	return map[string]any{
		"id": s.ID, "host": s.Host, "game": s.Game, "subnet": s.Subnet,
		"amHost": s.amHost, "members": s.roster(),
	}
}

// overlayConfig derives the local overlay parameters for a session: our own vIP, the /24 subnet, and the vIP→peer
// routing table for every OTHER member. ok is false if we aren't in the session or have no vIP yet.
func (ss *sessionService) overlayConfig(sid string) (myVIP, subnet string, peerByVIP map[string]string, ok bool) {
	me := ss.host.ID().String()
	ss.mu.Lock()
	defer ss.mu.Unlock()
	s, exists := ss.sessions[sid]
	if !exists {
		return "", "", nil, false
	}
	peerByVIP = map[string]string{}
	for pid, m := range s.Members {
		if m.VIP == "" {
			continue
		}
		if pid == me {
			myVIP = m.VIP
		} else {
			peerByVIP[m.VIP] = pid
		}
	}
	if myVIP == "" {
		return "", "", nil, false
	}
	return myVIP, s.Subnet, peerByVIP, true
}

// snapshot returns the JSON view of one session (thread-safe).
func (ss *sessionService) snapshot(sid string) (map[string]any, bool) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	s, ok := ss.sessions[sid]
	if !ok {
		return nil, false
	}
	return sessionMap(s), true
}

// snapshotAll returns the JSON view of every session (thread-safe).
func (ss *sessionService) snapshotAll() []map[string]any {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	out := make([]map[string]any, 0, len(ss.sessions))
	for _, s := range ss.sessions {
		out = append(out, sessionMap(s))
	}
	return out
}

// handleStream reads framed session messages and dispatches them.
func (ss *sessionService) handleStream(s network.Stream) {
	defer s.Close()
	remote := s.Conn().RemotePeer().String()
	if ss.social != nil && ss.social.isBlocked(remote) {
		return
	}
	dec := json.NewDecoder(s)
	for {
		var m sessionMsg
		if err := dec.Decode(&m); err != nil {
			return
		}
		ss.dispatch(remote, m)
	}
}

func (ss *sessionService) dispatch(remote string, m sessionMsg) {
	switch m.Type {
	case "invite":
		// Only surface invites from accepted friends (ignore drive-by invites from strangers).
		if ss.social != nil {
			if c, ok := ss.social.get(remote); !ok || c.State != stAccepted {
				return
			}
		}
		ss.emitSession(evSessionInvite, map[string]string{"id": m.SID, "game": m.Game, "host": remote})
	case "join":
		// We are the host: assign the joiner a vIP, add them, and broadcast the new roster.
		s, ok := ss.getSession(m.SID)
		if !ok || !s.amHost {
			return
		}
		ss.mu.Lock()
		if _, exists := s.Members[remote]; !exists {
			octet := s.nextIP
			s.nextIP++
			s.Members[remote] = &member{PeerID: remote, Nick: m.Nick, VIP: s.vipFor(octet)}
		}
		ss.mu.Unlock()
		ss.broadcastRoster(s)
		ss.emitRoster(s)
	case "roster":
		// Authoritative roster from the host: replace our replica.
		if remote == "" {
			return
		}
		ss.mu.Lock()
		s := ss.sessions[m.SID]
		if s == nil {
			s = &session{ID: m.SID, Host: remote}
			ss.sessions[m.SID] = s
		}
		s.Host = remote
		s.Game = m.Game
		s.Subnet = m.Subnet
		s.amHost = false
		s.Members = map[string]*member{}
		for i := range m.Members {
			mc := m.Members[i]
			s.Members[mc.PeerID] = &mc
		}
		ss.mu.Unlock()
		ss.emitRoster(s)
	case "ready":
		if s, ok := ss.getSession(m.SID); ok && s.amHost {
			ss.applyReady(s, remote, m.Ready)
		}
	case "leave":
		s, ok := ss.getSession(m.SID)
		if !ok {
			return
		}
		if s.amHost {
			ss.mu.Lock()
			delete(s.Members, remote)
			ss.mu.Unlock()
			ss.broadcastRoster(s)
			ss.emitRoster(s)
		} else {
			// The host tore the session down.
			ss.mu.Lock()
			delete(ss.sessions, m.SID)
			ss.mu.Unlock()
			ss.emitSession(evSessionEnded, map[string]string{"id": m.SID})
		}
	}
}
