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
	"io"
	"sync"
	"time"
	"unicode/utf8"

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
	evFriendLibrary  = 6 // a friend sent their COMPLETE shared set (snapshot): payload {peer, libs:{name:[shareItem]}, seq}
)

// shareItem is one shared node in a library snapshot: the node-block CID plus the metadata the RECEIVER needs to
// place the block at its FINAL working-tree path BEFORE fetching it (LIBRARY/<nick> - <lib>/[uid] <title>/<node>.json)
// — so a received share rides the ordinary rolling fetch queue straight into the library, with no intermediary dir
// and no post-fetch materialize step. Tile* names the node's LIBRARYITEM tile block (fetched alongside, same dir).
// Go relays these opaquely (bounds-checked only); C++ produces them at publish time and consumes them on receipt.
type shareItem struct {
	Cid      string `json:"cid"`
	Node     string `json:"node,omitempty"`     // NODE_ID — the receiver's on-disk filename
	Uid      string `json:"uid,omitempty"`      // PACKAGEUID — package-dir name, first half
	Title    string `json:"title,omitempty"`    // display title — package-dir name, second half
	TileCid  string `json:"tilecid,omitempty"`  // LIBRARYITEM tile block CID (empty = no tile)
	TileNode string `json:"tilenode,omitempty"` // tile's NODE_ID — its on-disk filename
}

// friendMsg is the framed wire message. One JSON value per message; a stream may carry several (a decoder loop reads
// until EOF), so a single connection can, e.g., send request then profile.
//
// Adding fields is backward compatible in both directions: an old peer ignores unknown keys, and a new peer decoding
// an old message gets zero values. In particular a peer on an OLDER build still sends a "play" block — we simply
// ignore those keys now (no lobby/join/what-are-you-playing model anymore; see social.go).
type friendMsg struct {
	Type   string `json:"t"`              // request | accept | decline | profile | presence | ping | library_req | library
	Nick   string `json:"nick,omitempty"` // sender's nickname (on request/accept/profile)
	PicCID string `json:"pic,omitempty"`  // sender's profile-picture content CID
	Note   string `json:"note,omitempty"` // optional greeting on a request
	// AllLibs (on a "library" message) is the sender's COMPLETE set of libraries shared with the recipient, as
	// {libName: [shareItem]}. It is a full SNAPSHOT, replaced wholesale by the receiver — so a withdrawn library
	// (absent from the map) is unambiguous and a dropped push self-heals on the next one. Empty map = nothing shared.
	AllLibs map[string][]shareItem `json:"libs,omitempty"`
	// LibSeq (on a "library" message) is a monotonic per-sender stamp. Each snapshot rides its own libp2p stream and the
	// receiver handles streams concurrently, so two rapid share changes can arrive/process out of order — the receiver
	// keeps only the HIGHEST seq it has seen from a peer and drops any older one, so a stale snapshot can never win.
	// Seeded from the sender's wall clock at service start, so it also stays monotonic across a sender restart.
	LibSeq uint64 `json:"libseq,omitempty"`
}

// Wire-decode caps for the library snapshot (defence against a hostile/broken peer). A snapshot exceeding any of these
// is rejected before it touches memory-resident state or config.
const (
	maxFriendMsgBytes = 8 << 20 // 8 MiB per inbound message — bounds the pre-auth decode buffer
	maxSharedLibs     = 4096    // libraries per snapshot
	maxLibCids        = 100000  // shared items per library
	maxLibNameLen     = 256     // library-name length
	maxCidLen         = 128     // CID length (cid / tilecid)
	maxShareIdLen     = 256     // node / uid / tilenode length
	maxShareTitleLen  = 512     // title length
	maxNickLen        = 64      // nickname bytes — it becomes a receiver-side directory segment
)

// capUtf8 bounds s to max BYTES, trimming further to the nearest valid-UTF-8 boundary (empty if s wasn't UTF-8).
func capUtf8(s string, max int) string {
	if len(s) <= max {
		if utf8.ValidString(s) {
			return s
		}
		return ""
	}
	s = s[:max]
	for len(s) > 0 && !utf8.ValidString(s) {
		s = s[:len(s)-1]
	}
	return s
}

// libSnapshotOK bounds a decoded snapshot; false ⇒ reject (drop, do not store/serve).
func libSnapshotOK(m map[string][]shareItem) bool {
	if len(m) > maxSharedLibs {
		return false
	}
	for name, items := range m {
		if len(name) == 0 || len(name) > maxLibNameLen || len(items) > maxLibCids {
			return false
		}
		for _, it := range items {
			if len(it.Cid) == 0 || len(it.Cid) > maxCidLen || len(it.TileCid) > maxCidLen ||
				len(it.Node) > maxShareIdLen || len(it.Uid) > maxShareIdLen || len(it.TileNode) > maxShareIdLen ||
				len(it.Title) > maxShareTitleLen {
				return false
			}
		}
	}
	return true
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

	// What library lists we serve each friend (peerID → libName → shared items). Set from C++ per the seeder's
	// per-(friend,library) "share" toggles; consulted when a friend requests our libraries. The BILATERAL gate's
	// seeder half — we only ever hand a friend a library we deliberately put here.
	//
	// sendSeq stamps each outbound snapshot (monotonic, clock-seeded) so the RECEIVER can order concurrently-delivered
	// snapshots (last-writer-wins by stamp lives in C++, where the persisted record is — a Go-side receive gate can't
	// work: the emit happens off-lock, so a big stale snapshot can still emit after a small newer one). pushPending
	// coalesces per peer: at most one in-flight push each, which reads the LATEST snapshot at send time — this both
	// keeps the newest state winning and bounds goroutines against a flood of library_req messages.
	shareMu     sync.Mutex
	shareLibs   map[string]map[string][]shareItem
	sendSeq     uint64
	pushPending map[string]bool

	// presenceDeny gates OUTBOUND liveness per peer: a peer in this set is never sent our profile broadcast and is
	// never auto-pinged, so we don't advertise that we're online to them (the Network tab's per-peer "Presence" toggle,
	// off). Replaced wholesale from config at node-ready. Guarded by denyMu.
	denyMu       sync.Mutex
	presenceDeny map[string]bool
}

func newFriendService(ctx context.Context, h host.Host, r peerRouter, s *socialState, emit func(int, string)) *friendService {
	// Seed the outbound snapshot counter from the wall clock so stamps keep rising across a restart (an in-memory
	// counter would reset to 0 and a fresh snapshot could then lose to one the receiver saw before we restarted).
	return &friendService{ctx: ctx, host: h, router: r, social: s, emit: emit,
		sendSeq: uint64(time.Now().UnixNano()), pushPending: map[string]bool{}, presenceDeny: map[string]bool{}}
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
	// Bound the whole stream so a hostile/broken peer can't stream gigabytes (a library snapshot with a giant Libs)
	// before the stAccepted/size gates in dispatch ever run. send() opens a one-shot stream per burst, so this cap
	// comfortably covers a real message set while killing the pre-auth memory-DoS.
	dec := json.NewDecoder(io.LimitReader(s, maxFriendMsgBytes))
	for {
		var m friendMsg
		if err := dec.Decode(&m); err != nil {
			return // EOF, over-cap, or malformed → done with this stream
		}
		f.dispatch(remote, m)
	}
}

// dispatch applies one inbound message from remote to the address book + emits the UI event.
func (f *friendService) dispatch(remote string, m friendMsg) {
	// Inbound identity fields are attacker-controlled, and the nick is consumed as more than display text — the
	// receiver derives an on-disk DIRECTORY segment from it for received shares — so bound them before any branch
	// stores them. An over-long / non-UTF-8 nick degrades to empty (downstream falls back to the peer-id suffix).
	m.Nick = capUtf8(m.Nick, maxNickLen)
	if len(m.PicCID) > maxCidLen {
		m.PicCID = ""
	}
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
		// If we hide our presence from this peer, don't record or reflect their liveness probe either (the Network
		// tab's per-peer "Presence" toggle, off) — at minimum we never answer/track a denied peer's ping.
		if !f.presenceDenied(remote) {
			if changed, c := f.social.setPresence(remote, true); changed {
				f.emitContact(evFriendPresence, c)
			}
		}
	case "library_req":
		// A friend asks for what we share with them. Reply with the FULL snapshot (the seeder half of the bilateral
		// gate — only what C++ deliberately put in shareLibs[remote]). Accepted friends only.
		if c, ok := f.social.get(remote); !ok || c.State != stAccepted {
			return
		}
		f.pushSnapshot(remote)
	case "library":
		// A friend sent us their COMPLETE shared set (a snapshot to replace wholesale; empty = nothing). Accepted
		// friends only, and bounded — a hostile snapshot is dropped, never stored. C++ replaces its record + applies
		// the leecher's receive gate.
		if c, ok := f.social.get(remote); !ok || c.State != stAccepted {
			return
		}
		if !libSnapshotOK(m.AllLibs) {
			vlog("friend", "rejecting oversized/invalid library snapshot from %s", shortPeer(remote))
			return
		}
		if f.emit != nil {
			// Carry the stamp through so C++ (which holds the persisted record) can do last-writer-wins across
			// concurrently-delivered snapshots. Send an explicit empty object for "share nothing" (a nil map would
			// marshal as null and read as malformed downstream).
			libs := m.AllLibs
			if libs == nil {
				libs = map[string][]shareItem{}
			}
			b, _ := json.Marshal(map[string]any{"peer": remote, "libs": libs, "seq": m.LibSeq})
			f.emit(evFriendLibrary, string(b))
		}
	}
}

// pushSnapshot sends a friend our COMPLETE current shared set — the one message type, so a change (add/withdraw) or a
// request all converge to "replace wholesale", immune to lost/out-of-order deltas. Only ACCEPTED friends are served
// (the seeder half of the bilateral gate — a blocked/pending/removed peer is never pushed to). Pushes COALESCE per
// peer: if one is already in flight we don't spawn another — the in-flight goroutine reads the latest snapshot at send
// time, so the newest state still wins and a flood of requests can't spawn unbounded goroutines/streams.
func (f *friendService) pushSnapshot(pidStr string) {
	if c, ok := f.social.get(pidStr); !ok || c.State != stAccepted {
		return
	}
	f.shareMu.Lock()
	if f.pushPending[pidStr] { // a push is already scheduled; it will pick up whatever the map holds when it runs
		f.shareMu.Unlock()
		return
	}
	f.pushPending[pidStr] = true
	f.shareMu.Unlock()
	safeGo("friend.libSnap", func() {
		// Clear the pending flag and read the snapshot under one lock: a mutation AFTER this read sees pending=false
		// and correctly schedules a fresh push, so no update is ever coalesced away.
		f.shareMu.Lock()
		f.pushPending[pidStr] = false
		out := map[string][]shareItem{}
		for k, v := range f.shareLibs[pidStr] {
			out[k] = append([]shareItem(nil), v...)
		}
		f.sendSeq++
		seq := f.sendSeq
		f.shareMu.Unlock()
		_ = f.send(pidStr, friendMsg{Type: "library", AllLibs: out, LibSeq: seq})
	})
}

// setShareLib / removeShareLib are the seeder's per-(friend,library) toggles; each re-pushes the full snapshot.
func (f *friendService) setShareLib(pidStr, lib string, items []shareItem) {
	f.shareMu.Lock()
	if f.shareLibs == nil {
		f.shareLibs = map[string]map[string][]shareItem{}
	}
	if f.shareLibs[pidStr] == nil {
		f.shareLibs[pidStr] = map[string][]shareItem{}
	}
	f.shareLibs[pidStr][lib] = items
	f.shareMu.Unlock()
	f.pushSnapshot(pidStr)
}

func (f *friendService) removeShareLib(pidStr, lib string) {
	f.shareMu.Lock()
	if m := f.shareLibs[pidStr]; m != nil {
		delete(m, lib)
		if len(m) == 0 {
			delete(f.shareLibs, pidStr)
		}
	}
	f.shareMu.Unlock()
	f.pushSnapshot(pidStr) // full snapshot now WITHOUT lib → receiver drops it (no lost-withdraw)
}

func (f *friendService) requestLibraries(pidStr string) {
	safeGo("friend.libReq", func() { _ = f.send(pidStr, friendMsg{Type: "library_req"}) })
}

// purgeShares forgets everything we shared with a peer — called when the friendship ends (remove/decline/block) so a
// later re-add cannot silently resume serving without fresh consent.
func (f *friendService) purgeShares(pidStr string) {
	f.shareMu.Lock()
	delete(f.shareLibs, pidStr)
	delete(f.pushPending, pidStr)
	f.shareMu.Unlock()
}

// dial ensures we have a connection to pid, resolving addresses via the router (DHT) if we're not already connected.
func (f *friendService) dial(ctx context.Context, pid peer.ID) error {
	if c := f.host.Network().Connectedness(pid); c == network.Connected || c == network.Limited {
		return nil // Limited (relayed) suffices: friend streams opt into limited conns (WithAllowLimitedConn)
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
	nick := p.Nick
	if nick == "" { // friends must see a real name, not "" — default to the hostname (the stored nick stays empty)
		nick = defaultNick()
	}
	return friendMsg{Type: t, Nick: nick, PicCID: p.PicCID}
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
	f.purgeShares(pidStr) // consent ends → stop serving them, so a re-add can't silently resume
	f.emit(evFriendRemoved, fmt.Sprintf(`{"peer":%q}`, pidStr))
	return nil
}

// blockFriend marks a peer blocked (drops all their traffic) without notifying them.
func (f *friendService) blockFriend(pidStr string) error {
	c := f.social.upsert(pidStr, func(c *contact) { c.State = stBlocked; c.online = false })
	f.purgeShares(pidStr)
	f.emitContact(evFriendDecline, c)
	return nil
}

// broadcastProfile pushes our updated profile to every accepted friend (best-effort, async).
// presenceDenied reports whether we suppress OUTBOUND liveness to a peer (see presenceDeny).
func (f *friendService) presenceDenied(pidStr string) bool {
	f.denyMu.Lock()
	defer f.denyMu.Unlock()
	return f.presenceDeny[pidStr]
}

// setPresenceDeny replaces the whole presence-deny set (pushed from config at node-ready).
func (f *friendService) setPresenceDeny(peers []string) {
	m := make(map[string]bool, len(peers))
	for _, p := range peers {
		m[p] = true
	}
	f.denyMu.Lock()
	f.presenceDeny = m
	f.denyMu.Unlock()
}

func (f *friendService) broadcastProfile() {
	for _, pid := range f.social.acceptedPeers() {
		if f.presenceDenied(pid) { // presence hidden from this peer → don't push our profile
			continue
		}
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
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-f.ctx.Done():
				return
			case <-t.C:
				// Per-tick guard: a panic must cost one tick, never the loop — a dead presence loop means every
				// friend drifts to "offline" forever with nothing reporting it (adversarial H5/C1 class).
				guard("friend.presenceTick", func() {
					for _, pid := range f.social.acceptedPeers() {
						if f.presenceDenied(pid) { // don't reveal we're online to a presence-denied peer
							continue
						}
						safeGo("friend.pingPresence", func() { f.pingPresence(pid) })
					}
				})
			}
		}
	}()
}
