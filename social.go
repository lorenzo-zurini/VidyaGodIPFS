package main

// social.go — the friends/contacts layer: a persistent address book keyed by libp2p peer ID (for an Ed25519 key the
// peer ID *embeds the public key*, so the peer ID IS the shareable "friend code" and libp2p authenticates a friend
// against it for free) plus this node's own shareable profile (nickname + profile-picture CID).
//
// Persistence is deliberately independent of the network: the contact list lives in <repo>/social.json and loads at
// openNode, so it survives while offline. The live wire protocol (friend.go) attaches to it once the host is up.
//
// Scope: identity + mutual-consent friendship + online/offline liveness. There is intentionally NO "what are you
// playing" presence, no join-a-specific-friend targeting and no co-play suggestions — the multiplayer model is a
// flat always-on virtual LAN of accepted friends (friendlan.go), not a lobby/host/join system.

import (
	"encoding/json"
	"os"
	"path/filepath"
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

// contact is one address-book entry. The exported fields persist; online is transient (recomputed by presence).
type contact struct {
	PeerID   string      `json:"peer"`
	Nick     string      `json:"nick,omitempty"`
	PicCID   string      `json:"pic,omitempty"`
	State    friendState `json:"state"`
	AddedAt  int64       `json:"added"`          // unix-ms
	LastSeen int64       `json:"seen,omitempty"` // unix-ms of last successful contact; 0 = never
	online   bool        // transient — not persisted
}

// profile is this node's own shareable identity, sent to friends on handshake.
type profile struct {
	Nick   string `json:"nick,omitempty"`
	PicCID string `json:"pic,omitempty"`
}

// socialState is the persistent address book + self profile. Guarded by mu; all mutators persist through save().
type socialState struct {
	mu       sync.Mutex
	path     string // <repo>/social.json
	self     profile
	contacts map[string]*contact // peerID → contact
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
	s.self = profile{Nick: nick, PicCID: picCID}
	s.saveLocked()
}

func (s *socialState) getProfile() profile {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.self
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
