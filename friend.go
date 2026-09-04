package main

// friend.go — the live friends protocol on top of the libp2p host. Registers a stream handler for
// /vidyagod/friend/1.0.0 and drives mutual-consent friend requests, identity (nickname + profile-pic CID) exchange,
// and presence, using the DHT to locate a friend by peer ID and libp2p's authenticated encrypted streams as the
// handshake (no bespoke crypto: an Ed25519 peer ID already binds the connection to the friend's public key).
//
// The friendService is intentionally decoupled from the process-wide node singleton so it can be exercised with two
// real libp2p hosts in-process (see friend_test.go): it needs only a host, an optional peer router (the DHT), the
// socialState, and an emit sink for inbound events.

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	host "github.com/libp2p/go-libp2p/core/host"
	network "github.com/libp2p/go-libp2p/core/network"
	peer "github.com/libp2p/go-libp2p/core/peer"
	protocol "github.com/libp2p/go-libp2p/core/protocol"
)

const friendProtoID = protocol.ID("/vidyagod/friend/1.0.0")

// Inbound event kinds emitted to the C++ side (mirror IpfsWrapper::FriendEvent::Kind). The payload is the JSON of the
// affected contact (peer/nick/pic/state/online), except Removed which carries just {"peer":...}.
const (
	evFriendRequest  = 0 // someone asked to be our friend (state=incoming)
	evFriendAccept   = 1 // a peer we'd requested accepted us (state=accepted)
	evFriendDecline  = 2 // a peer declined / unfriended us
	evFriendPresence = 3 // a friend's online state changed
	evFriendProfile  = 4 // a friend updated their nickname / picture
	evFriendRemoved  = 5 // local removal (echoed for UI symmetry)
)

// friendMsg is the framed wire message. One JSON value per message; a stream may carry several (a decoder loop reads
// until EOF), so a single connection can, e.g., send request then profile.
//
// Adding fields is backward compatible in both directions: an old peer ignores unknown keys, and a new peer decoding
// an old message gets zero values. In particular a peer on an OLDER build still sends a "play" block — we simply
// ignore those keys now (no lobby/join/what-are-you-playing model anymore; see social.go).
type friendMsg struct {
	Type   string `json:"t"`              // request | accept | decline | profile | presence | ping
	Nick   string `json:"nick,omitempty"` // sender's nickname (on request/accept/profile)
	PicCID string `json:"pic,omitempty"`  // sender's profile-picture content CID
	Note   string `json:"note,omitempty"` // optional greeting on a request
}

// peerRouter is the subset of the DHT the friend service needs: resolve a peer ID to its current addresses. Optional
// (nil in tests where hosts are pre-connected) — dialing then relies on the peerstore / an existing connection.
type peerRouter interface {
	FindPeer(ctx context.Context, id peer.ID) (peer.AddrInfo, error)
}

// friendService binds the host + router + address book + event sink.
type friendService struct {
	ctx    context.Context
	host   host.Host
	router peerRouter
	social *socialState
	emit   func(kind int, jsonPayload string) // may be nil
}

func newFriendService(ctx context.Context, h host.Host, r peerRouter, s *socialState, emit func(int, string)) *friendService {
	return &friendService{ctx: ctx, host: h, router: r, social: s, emit: emit}
}

// start registers the inbound stream handler. Call once the host exists.
func (f *friendService) start() {
	f.host.SetStreamHandler(friendProtoID, f.handleStream)
}

func (f *friendService) emitContact(kind int, c contact) {
	if f.emit == nil {
		return
	}
	// Keep these keys in lockstep with VgFriendList (api_social.go): FriendsTab re-reads the whole list on every
	// event, so any field present only here would be wiped by the very refresh this event triggers.
	b, _ := json.Marshal(map[string]any{
		"peer": c.PeerID, "nick": c.Nick, "pic": c.PicCID,
		"state": string(c.State), "online": c.online, "seen": c.LastSeen,
	})
	f.emit(kind, string(b))
}

// handleStream reads framed messages off an inbound stream until EOF and dispatches each.
func (f *friendService) handleStream(s network.Stream) {
	defer s.Close()
	remote := s.Conn().RemotePeer().String()
	if f.social.isBlocked(remote) {
		return // silently drop everything from a blocked peer
	}
	dec := json.NewDecoder(s)
	for {
		var m friendMsg
		if err := dec.Decode(&m); err != nil {
			return // EOF or malformed → done with this stream
		}
		f.dispatch(remote, m)
	}
}

// dispatch applies one inbound message from remote to the address book + emits the UI event.
func (f *friendService) dispatch(remote string, m friendMsg) {
	vlog("friend", "RECV %-8s from %s (nick=%q)", m.Type, shortPeer(remote), m.Nick)
	switch m.Type {
	case "request":
		// A friend request: record as incoming unless we already accepted/blocked them. Store the profile they sent so
		// the UI can show who's asking. If we had ALREADY sent them a request (pending), their request crossing ours
		// completes the handshake → accepted.
		var kind = evFriendRequest
		c := f.social.upsert(remote, func(c *contact) {
			if m.Nick != "" {
				c.Nick = m.Nick
			}
			if m.PicCID != "" {
				c.PicCID = m.PicCID
			}
			if c.State == stPending { // mutual crossing → friends
				c.State = stAccepted
				kind = evFriendAccept
			} else if c.State != stAccepted && c.State != stBlocked {
				c.State = stIncoming
			}
		})
		f.emitContact(kind, c)
		// Mutual crossing: we just flipped THEM to accepted off their inbound request because WE were already
		// pending. They, however, only know they sent a request — our matching request may never have reached them
		// (common: whoever started second couldn't yet resolve the other, so their initial send failed). Tell them
		// explicitly so both sides converge to accepted instead of the initiator hanging in "pending" forever.
		if kind == evFriendAccept {
			vlog("friend", "crossing with %s → sending accept back", shortPeer(remote))
			safeGo("friend.crossAccept", func() { _ = f.send(remote, f.helloMsg("accept")) })
		}
	case "accept":
		// The peer we'd requested accepted. Mark accepted + absorb their profile.
		c := f.social.upsert(remote, func(c *contact) {
			c.State = stAccepted
			if m.Nick != "" {
				c.Nick = m.Nick
			}
			if m.PicCID != "" {
				c.PicCID = m.PicCID
			}
		})
		f.emitContact(evFriendAccept, c)
	case "decline":
		if c, ok := f.social.get(remote); ok {
			f.social.remove(remote)
			f.emitContact(evFriendDecline, c)
		}
	case "profile":
		if _, ok := f.social.get(remote); ok {
			c := f.social.upsert(remote, func(c *contact) {
				c.Nick = m.Nick
				c.PicCID = m.PicCID
			})
			f.emitContact(evFriendProfile, c)
		}
	case "ping", "presence":
		// Liveness only: a successful inbound ping/presence marks the friend online. No play payload anymore.
		if changed, c := f.social.setPresence(remote, true); changed {
			f.emitContact(evFriendPresence, c)
		}
	}
}

// dial ensures we have a connection to pid, resolving addresses via the router (DHT) if we're not already connected.
func (f *friendService) dial(ctx context.Context, pid peer.ID) error {
	if f.host.Network().Connectedness(pid) == network.Connected {
		return nil
	}
	if f.router != nil {
		ai, err := f.router.FindPeer(ctx, pid)
		if err != nil {
			return fmt.Errorf("locate peer: %w", err)
		}
		if err := f.host.Connect(ctx, ai); err != nil {
			return fmt.Errorf("connect: %w", err)
		}
		return nil
	}
	// No router: rely on the peerstore already holding an address (tests pre-connect).
	return f.host.Connect(ctx, peer.AddrInfo{ID: pid})
}

// send opens a one-shot stream to pid and writes msgs in order (used for request/accept/profile/ping).
func (f *friendService) send(pidStr string, msgs ...friendMsg) error {
	pid, err := peer.Decode(pidStr)
	if err != nil {
		return fmt.Errorf("bad peer id: %w", err)
	}
	if pid == f.host.ID() {
		return fmt.Errorf("cannot befriend yourself")
	}
	ctx, cancel := context.WithTimeout(f.ctx, 30*time.Second)
	defer cancel()
	if err := f.dial(ctx, pid); err != nil {
		vlog("friend", "SEND %-8s to %s: dial failed: %v", msgTypes(msgs), shortPeer(pidStr), err)
		return err
	}
	// WithAllowLimitedConn: a friend request is a tiny JSON message that must go through even when the only path to
	// the peer is a RELAYED (circuit-v2, "limited") connection — the common case when hole-punching hasn't succeeded
	// yet (strict NAT). Without this, NewStream returns network.ErrLimitedConn and the request silently fails ("open
	// stream: limited connection to peer") even though we're "connected" and bitswap (which opts in) downloads fine.
	s, err := f.host.NewStream(network.WithAllowLimitedConn(ctx, "vidyagod-friend"), pid, friendProtoID)
	if err != nil {
		vlog("friend", "SEND %-8s to %s: open stream failed: %v", msgTypes(msgs), shortPeer(pidStr), err)
		return fmt.Errorf("open stream: %w", err)
	}
	defer s.Close()
	enc := json.NewEncoder(s)
	for _, m := range msgs {
		if err := enc.Encode(&m); err != nil {
			vlog("friend", "SEND %-8s to %s: encode failed: %v", m.Type, shortPeer(pidStr), err)
			return fmt.Errorf("send: %w", err)
		}
	}
	vlog("friend", "SEND %-8s to %s: ok", msgTypes(msgs), shortPeer(pidStr))
	return nil
}

// msgTypes joins the message types in a batch for a log line (usually one).
func msgTypes(msgs []friendMsg) string {
	if len(msgs) == 1 {
		return msgs[0].Type
	}
	out := ""
	for i, m := range msgs {
		if i > 0 {
			out += "+"
		}
		out += m.Type
	}
	return out
}

// helloMsg builds a message of the given type carrying our current profile.
func (f *friendService) helloMsg(t string) friendMsg {
	p := f.social.getProfile()
	return friendMsg{Type: t, Nick: p.Nick, PicCID: p.PicCID}
}

// addFriend records an outgoing request and sends it (with our profile) to the peer.
func (f *friendService) addFriend(pidStr, note string) error {
	if _, err := peer.Decode(pidStr); err != nil {
		return fmt.Errorf("bad peer id: %w", err)
	}
	c := f.social.upsert(pidStr, func(c *contact) {
		if c.State != stAccepted { // don't downgrade an existing friendship
			c.State = stPending
		}
	})
	f.emitContact(evFriendRequest, c)
	m := f.helloMsg("request")
	m.Note = note
	return f.send(pidStr, m)
}

// acceptFriend accepts an incoming request: mark accepted locally and notify the peer (with our profile).
func (f *friendService) acceptFriend(pidStr string) error {
	c := f.social.upsert(pidStr, func(c *contact) { c.State = stAccepted })
	f.emitContact(evFriendAccept, c)
	return f.send(pidStr, f.helloMsg("accept"))
}

// declineFriend rejects/removes a contact and best-effort notifies the peer.
func (f *friendService) declineFriend(pidStr string) error {
	_ = f.send(pidStr, friendMsg{Type: "decline"}) // best-effort; peer may be offline
	f.social.remove(pidStr)
	f.emit(evFriendRemoved, fmt.Sprintf(`{"peer":%q}`, pidStr))
	return nil
}

// blockFriend marks a peer blocked (drops all their traffic) without notifying them.
func (f *friendService) blockFriend(pidStr string) error {
	c := f.social.upsert(pidStr, func(c *contact) { c.State = stBlocked; c.online = false })
	f.emitContact(evFriendDecline, c)
	return nil
}

// broadcastProfile pushes our updated profile to every accepted friend (best-effort, async).
func (f *friendService) broadcastProfile() {
	for _, pid := range f.social.acceptedPeers() {
		safeGo("friend.helloProfile", func() { _ = f.send(pid, f.helloMsg("profile")) })
	}
}

// pingPresence pings one friend; success marks them online (and refreshes their view of us). Returns reachability.
func (f *friendService) pingPresence(pidStr string) bool {
	err := f.send(pidStr, f.helloMsg("ping"))
	online := err == nil
	if changed, c := f.social.setPresence(pidStr, online); changed {
		f.emitContact(evFriendPresence, c)
	}
	return online
}

// startPresence runs a background loop that periodically pings accepted friends so the UI reflects who is reachable.
func (f *friendService) startPresence(interval time.Duration) {
	safeGo("friend.presenceLoop", func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-f.ctx.Done():
				return
			case <-t.C:
				for _, pid := range f.social.acceptedPeers() {
					safeGo("friend.pingPresence", func() { f.pingPresence(pid) })
				}
			}
		}
	})
}
