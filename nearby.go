package main

// nearby.go — "VidyaGod devices on your network": the LAN peers (found by mDNS, netgate.go lanPeers) that run
// VidyaGod, by the name a person knows them by, so pairing is one press instead of typing a peer code.
//
// The node already finds and connects to every libp2p node on the local network (online.go, mDNS). What it lacked
// is a name: a peer ID means nothing to a person, and the peer protocol carries the nickname only with a request.
// /vidyagod/nearby answers our nickname (the one peers see; it defaults to the hostname) — and ONLY to a peer on our
// own network: anyone across the internet gets the stream reset. Someone on the same network (a café's Wi-Fi) does
// learn the nickname; the machine's hostname is never sent beside a nickname chosen instead of it. Pairing itself is the ordinary peer request
// (peer.go): mutual consent, nothing new is trusted here.

import (
	"context"
	"encoding/json"
	"io"
	"sort"
	"sync"
	"time"

	host "github.com/libp2p/go-libp2p/core/host"
	network "github.com/libp2p/go-libp2p/core/network"
	peer "github.com/libp2p/go-libp2p/core/peer"
	protocol "github.com/libp2p/go-libp2p/core/protocol"
)

const (
	nearbyProtoID   = protocol.ID("/vidyagod/nearby/1.0.0")
	maxNearbyReply  = 1024             // {nick}: one capped string
	nearbyTimeout   = 4 * time.Second  // one ask, dial included
	nearbyFresh     = 30 * time.Second // a name is asked again after this
	maxNearbyAsking = 16               // asks in flight at once (a network full of libp2p nodes asks in waves)
)

// nearbyInfo is what a nearby device says about itself.
type nearbyInfo struct {
	Nick string `json:"nick"`
}

// nearbyPeer is one row of the list: who, and what we are to each other.
type nearbyPeer struct {
	Peer  string `json:"peer"`
	Nick  string `json:"nick"`
	State string `json:"state"` // the peer state ("" = not in the address book): pending | incoming | accepted
}

type nearbyEntry struct {
	info   nearbyInfo
	ok     bool      // it has answered (a libp2p node that is not VidyaGod, or refuses, never does); kept through a missed re-ask
	at     time.Time // when it was last asked
	asking bool
}

type nearbyService struct {
	ctx    context.Context
	host   host.Host
	social *socialState
	isLan  func(peer.ID) bool // on our own network (mDNS / connected to by address)
	self   func() nearbyInfo  // our own name, read at each answer (the nickname can change)

	mu    sync.Mutex
	known map[peer.ID]*nearbyEntry
	slots chan struct{}
}

func newNearbyService(ctx context.Context, h host.Host, s *socialState, isLan func(peer.ID) bool, self func() nearbyInfo) *nearbyService {
	return &nearbyService{ctx: ctx, host: h, social: s, isLan: isLan, self: self,
		known: map[peer.ID]*nearbyEntry{}, slots: make(chan struct{}, maxNearbyAsking)}
}

func (s *nearbyService) start() { s.host.SetStreamHandler(nearbyProtoID, s.handle) }

// handle answers a nearby device with our name; anyone not on our network gets nothing.
func (s *nearbyService) handle(st network.Stream) {
	remote := st.Conn().RemotePeer()
	if !s.isLan(remote) || (s.social != nil && s.social.isBlocked(remote.String())) {
		_ = st.Reset()
		return
	}
	defer st.Close()
	_ = st.SetDeadline(time.Now().Add(nearbyTimeout))
	me := s.self()
	_ = json.NewEncoder(st).Encode(nearbyInfo{Nick: capUtf8(me.Nick, maxNickLen)})
}

// ask asks one peer its name and records the answer (or that it gave none).
func (s *nearbyService) ask(p peer.ID) {
	defer func() { <-s.slots }()
	var info nearbyInfo
	ok := func() bool {
		ctx, cancel := context.WithTimeout(s.ctx, nearbyTimeout)
		defer cancel()
		st, err := s.host.NewStream(ctx, p, nearbyProtoID)
		if err != nil {
			return false
		}
		defer st.Close()
		_ = st.SetDeadline(time.Now().Add(nearbyTimeout))
		if err := json.NewDecoder(io.LimitReader(st, maxNearbyReply)).Decode(&info); err != nil {
			return false
		}
		// Attacker-controlled on the far end: bounded like a peer request's nickname.
		info.Nick = capUtf8(info.Nick, maxNickLen)
		return info.Nick != ""
	}()
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.known[p]
	if e == nil {
		return
	}
	e.asking, e.at = false, time.Now()
	if ok { // a missed re-ask (a slow moment) keeps the last answer: the row would vanish and come back otherwise
		e.ok, e.info = true, info
	}
}

// list is the nearby devices that run VidyaGod, by name. It never waits on the network: a device not asked yet (or
// asked too long ago) is asked in the background and shows from the next call on. Blocked peers are left out.
func (s *nearbyService) list() []nearbyPeer {
	now := time.Now()
	here := map[peer.ID]bool{}
	for _, p := range s.host.Network().Peers() {
		if p == s.host.ID() || !s.isLan(p) || s.host.Network().Connectedness(p) != network.Connected {
			continue
		}
		// A libp2p node that does not speak nearby (another IPFS node on the LAN) is not VidyaGod. Identify fills
		// this in a moment after the connection; until then it is skipped and comes on a later call.
		if ps, err := s.host.Peerstore().SupportsProtocols(p, nearbyProtoID); err != nil || len(ps) == 0 {
			continue
		}
		here[p] = true
	}

	s.mu.Lock()
	var out []nearbyPeer
	for p := range here {
		e := s.known[p]
		if e == nil {
			e = &nearbyEntry{}
			s.known[p] = e
		}
		if !e.asking && now.Sub(e.at) >= nearbyFresh {
			select {
			case s.slots <- struct{}{}:
				e.asking = true
				pp := p
				safeGo("nearby.ask", func() { s.ask(pp) })
			default: // every slot busy: asked on a later call
			}
		}
		if e.ok {
			out = append(out, nearbyPeer{Peer: p.String(), Nick: e.info.Nick})
		}
	}
	for p, e := range s.known { // gone from the network: forgotten (an ask in flight finds no entry and drops its answer)
		if !here[p] && !e.asking {
			delete(s.known, p)
		}
	}
	s.mu.Unlock()

	kept := out[:0]
	for _, r := range out {
		if s.social != nil {
			if c, ok := s.social.get(r.Peer); ok {
				if c.State == stBlocked {
					continue
				}
				r.State = string(c.State)
			}
		}
		kept = append(kept, r)
	}
	sort.Slice(kept, func(i, j int) bool {
		if kept[i].Nick != kept[j].Nick {
			return kept[i].Nick < kept[j].Nick
		}
		return kept[i].Peer < kept[j].Peer
	})
	return kept
}
