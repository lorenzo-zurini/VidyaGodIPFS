package main

// social.go — the friends/contacts layer: a persistent address book keyed by libp2p peer ID (for an Ed25519 key the
// peer ID *embeds the public key*, so the peer ID IS the shareable "friend code" and libp2p authenticates a friend
// against it for free) plus this node's own shareable profile (nickname + profile-picture CID).
//
// Persistence is deliberately independent of the network: the contact list lives in <repo>/social.json and loads at
// openNode, so it survives while offline. The live wire protocol (friend.go) attaches to it once the host is up.

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// friendState is the lifecycle of a contact. A friendship is mutual-consent: a request goes out (pending) / comes in
// (incoming), and only becomes accepted once the other side agrees. blocked drops all traffic from a peer.
type friendState string

const (
	stPending  friendState = "pending"  // we sent a request; awaiting their accept
	stIncoming friendState = "incoming" // they requested us; awaiting our accept
	stAccepted friendState = "accepted" // mutual friends
	stBlocked  friendState = "blocked"  // we reject everything from this peer
)

// contact is one address-book entry. The exported fields persist; online and the play block are transient
// (recomputed by presence).
type contact struct {
	PeerID   string      `json:"peer"`
	Nick     string      `json:"nick,omitempty"`
	PicCID   string      `json:"pic,omitempty"`
	State    friendState `json:"state"`
	AddedAt  int64       `json:"added"`          // unix-ms
	LastSeen int64       `json:"seen,omitempty"` // unix-ms of last successful contact; 0 = never
	online   bool        // transient — not persisted
	// What this friend is playing, learned from presence. Transient ON PURPOSE: persisting it would resurrect a
	// dead session across a restart, leaving a Join button aimed at a game nobody is in.
	play playState
}

// profile is this node's own shareable identity, sent to friends on handshake.
type profile struct {
	Nick   string `json:"nick,omitempty"`
	PicCID string `json:"pic,omitempty"`
	// Invisible suppresses the play block in everything we send (see playBlock in friend.go). Persisted, because
	// "nobody can see me" must survive a restart — being silently re-exposed by an update is the wrong failure.
	Invisible bool `json:"invisible,omitempty"`
}

// playState is what a peer is currently running: launch FACTS only. We never claim to know in-game state (menu
// vs match, free slots) because we cannot see inside the process; Open is a flag the user sets by hand.
type playState struct {
	NodeID string `json:"node"` // no omitempty: "" is the meaningful "stopped playing" — see friendMsg
	Label  string `json:"label,omitempty"`
	Ident  string `json:"ident,omitempty"` // "v1:<uid>@<hash>" content identity, for a MISMATCH WARNING only
	Since  int64  `json:"since,omitempty"` // unix-ms
	Open   bool   `json:"open"`            // no omitempty: advisory "open to join", set manually
}

// suggestion is a stranger we were told about because we are in the same game as a mutual friend. Transient and
// never persisted; it must NEVER enter contacts, which is what keeps acceptedPeers/lanConfigFrom — and therefore
// the overlay routing table — untouched by hearsay.
type suggestion struct {
	Peer string `json:"peer"`
	Nick string `json:"nick,omitempty"`
	Via  string `json:"via"`  // the friend who told us
	Game string `json:"game"` // the node id we share
	At   int64  `json:"at"`   // unix-ms
}

const maxSuggestions = 128

// socialState is the persistent address book + self profile. Guarded by mu; all mutators persist through save().
type socialState struct {
	mu       sync.Mutex
	path     string // <repo>/social.json
	self     profile
	contacts map[string]*contact // peerID → contact
	// Transient: what WE are playing, and who we have been told about. Not in diskSocial — a crash must not leave
	// us permanently "in a game".
	play    playState
	suggest map[string]suggestion // peerID → suggestion
}

// diskSocial is the on-disk shape of social.json (profile + contacts in one atomically-rewritten file).
type diskSocial struct {
	Profile  profile    `json:"profile"`
	Contacts []*contact `json:"contacts"`
}

// newSocialState loads (or starts) the address book under repoPath.
func newSocialState(repoPath string) *socialState {
	s := &socialState{
		path:     filepath.Join(repoPath, "social.json"),
		contacts: map[string]*contact{},
		suggest:  map[string]suggestion{},
	}
	s.load()
	return s
}

func (s *socialState) load() {
	b, err := os.ReadFile(s.path)
	if err != nil {
		return // no file yet → empty book
	}
	var d diskSocial
	if json.Unmarshal(b, &d) != nil {
		return
	}
	s.self = d.Profile
	for _, c := range d.Contacts {
		if c != nil && c.PeerID != "" {
			s.contacts[c.PeerID] = c
		}
	}
}

// save atomically rewrites social.json. Caller holds mu.
func (s *socialState) saveLocked() {
	d := diskSocial{Profile: s.self}
	for _, c := range s.contacts {
		d.Contacts = append(d.Contacts, c)
	}
	b, err := json.MarshalIndent(&d, "", "  ")
	if err != nil {
		return
	}
	tmp := s.path + ".tmp"
	if os.WriteFile(tmp, b, 0o600) == nil {
		_ = os.Rename(tmp, s.path)
	}
}

// setProfile updates this node's own nickname + picture CID.
func (s *socialState) setProfile(nick, picCID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Preserve Invisible: this replaces the whole struct, so saving a nickname would otherwise un-hide the user.
	s.self = profile{Nick: nick, PicCID: picCID, Invisible: s.self.Invisible}
	s.saveLocked()
}

func (s *socialState) getProfile() profile {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.self
}

// setInvisible hides (or reveals) what we are playing. Persisted.
func (s *socialState) setInvisible(on bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.self.Invisible = on
	s.saveLocked()
}

func (s *socialState) getInvisible() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.self.Invisible
}

// setPlaying records what we just launched. Open is deliberately left alone — it is a manual user setting, not a
// property of the launch.
func (s *socialState) setPlaying(nodeID, label, ident string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.play.NodeID = nodeID
	s.play.Label = label
	s.play.Ident = ident
	s.play.Since = time.Now().UnixMilli()
}

// clearPlaying marks us as no longer in a game and drops every suggestion — those are scoped to a shared game, so
// they expire with it.
func (s *socialState) clearPlaying() {
	s.mu.Lock()
	defer s.mu.Unlock()
	open := s.play.Open
	s.play = playState{Open: open}
	s.suggest = map[string]suggestion{}
}

func (s *socialState) setOpenToJoin(on bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.play.Open = on
}

func (s *socialState) getPlay() playState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.play
}

// setPeerPlay records what a friend told us they are playing. Returns (changed, snapshot); no-op if unknown.
func (s *socialState) setPeerPlay(peerID string, p playState) (bool, contact) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.contacts[peerID]
	if !ok {
		return false, contact{}
	}
	changed := c.play != p
	c.play = p
	return changed, *c
}

// addSuggestion records a stranger we were told about. Rejects anyone we already know in ANY state (including
// blocked) so hearsay can never resurrect a contact, and caps the store so a chatty peer cannot grow it forever.
func (s *socialState) addSuggestion(sg suggestion) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if sg.Peer == "" {
		return false
	}
	if _, known := s.contacts[sg.Peer]; known {
		return false
	}
	if _, dup := s.suggest[sg.Peer]; dup {
		return false
	}
	if len(s.suggest) >= maxSuggestions {
		return false
	}
	sg.At = time.Now().UnixMilli()
	s.suggest[sg.Peer] = sg
	return true
}

func (s *socialState) listSuggestions() []suggestion {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]suggestion, 0, len(s.suggest))
	for _, sg := range s.suggest {
		out = append(out, sg)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Peer < out[j].Peer })
	return out
}

func (s *socialState) dismissSuggestion(peerID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.suggest, peerID)
}

// upsert creates-or-updates a contact under mu and returns a copy for event emission. mutate runs with the entry.
func (s *socialState) upsert(peerID string, mutate func(c *contact)) contact {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.contacts[peerID]
	if c == nil {
		// Stateless on creation: each caller sets the intended state explicitly (addFriend→pending,
		// acceptFriend→accepted, the request handler→incoming). A default of pending would make the request
		// handler mistake a brand-new inbound contact for a mutual crossing.
		c = &contact{PeerID: peerID, AddedAt: time.Now().UnixMilli()}
		s.contacts[peerID] = c
	}
	mutate(c)
	s.saveLocked()
	return *c
}

// get returns a copy of a contact and whether it exists.
func (s *socialState) get(peerID string) (contact, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.contacts[peerID]
	if !ok {
		return contact{}, false
	}
	return *c, true
}

// remove deletes a contact entirely.
func (s *socialState) remove(peerID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.contacts[peerID]; !ok {
		return false
	}
	delete(s.contacts, peerID)
	s.saveLocked()
	return true
}

// list returns copies of all contacts (unordered).
func (s *socialState) list() []contact {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]contact, 0, len(s.contacts))
	for _, c := range s.contacts {
		out = append(out, *c)
	}
	return out
}

// isBlocked reports whether we reject traffic from a peer.
func (s *socialState) isBlocked(peerID string) bool {
	c, ok := s.get(peerID)
	return ok && c.State == stBlocked
}

// setPresence records a contact's online state + last-seen; returns (changed, snapshot). No-op if unknown.
func (s *socialState) setPresence(peerID string, online bool) (bool, contact) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.contacts[peerID]
	if !ok {
		return false, contact{}
	}
	changed := c.online != online
	c.online = online
	if online {
		c.LastSeen = time.Now().UnixMilli()
		s.saveLocked()
	} else if c.play.NodeID != "" || c.play.Open {
		// An unreachable friend is not playing. Without this a friend who drops off keeps a live-looking Join
		// button pointing at a game we can no longer reach.
		c.play = playState{}
		changed = true
	}
	return changed, *c
}

// acceptedPeers returns the peer IDs of mutual friends (for the presence loop).
func (s *socialState) acceptedPeers() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for id, c := range s.contacts {
		if c.State == stAccepted {
			out = append(out, id)
		}
	}
	return out
}
