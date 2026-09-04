# The VidyaGod network stack — architecture, invariants, failure modes

*Written 2026-09-04 after the LAN-reliability incident (datagram-refactor regression, whole-node crashes,
one-way datagram loss). This is the reference for how the stack is layered, what each layer promises, and the
invariants any future change must preserve.*

## Layers (bottom-up)

| Layer | Files | Job |
|---|---|---|
| **Substrate** | `online.go`, `doh.go`, `peers.go` | libp2p host (TCP/QUIC/WS/WebTransport), Kademlia DHT + delegated HTTP routing, AutoRelay + DCUtR hole-punching, mDNS same-LAN discovery, bitswap (tuned), DoH for filtered networks. |
| **Social** | `friend.go`, `social.go` | `/vidyagod/friend/1.0.0`: mutual-consent friendship, profiles, presence (45 s) + play-state, co-play suggestions. Address book survives offline (`social.json`). |
| **Link** | `overlaylink.go` | ALWAYS-ON per-friend heartbeat/state machine: proves + repairs each friend link for the app's lifetime. The trust oracle for the datagram fast path. |
| **Datapath** | `overlay.go` | `/vidyagod/overlay/1.0.0`: L3 forwarder between a TUN (in the game's netns) and friends. TX = proven-QUIC-datagram fast path over reliable-stream fallback. |
| **Tri-plane** | `natgateway.go`, `hostrelay.go` | Plane 2: userspace NAT (gVisor-style, in-process) for internet + host-LAN unicast. Plane 3: broadcast reflector bridging the real LAN. Both attach at overlay serve time. |
| **Sandbox plumbing** | `overlayserve_linux.go`, C++ `sandboxinit` | TUN created inside the game's rootless netns; fd handed to the node over a unix socket. |
| **Panic boundary** | `safego.go` | `guard`/`safeGo`/`guardErr` around every long-lived goroutine: a panic anywhere degrades to a logged skip, never node death. |

## The invariants (violate these and you re-introduce a field incident)

1. **Never tear down a working connection.** One libp2p connection per friend multiplexes bitswap downloads,
   the friend/presence protocol AND the overlay. `Network().ClosePeer` from maintenance logic once killed live
   downloads on a zombie *verdict* (not a zombie *fact*) — the "downloading at direct 5 ms, then node down"
   incident. The maintainer observes, dials and routes. It never closes.

2. **Datagrams must prove themselves.** A direct QUIC conn can be half-dead (one-way hole punch, asymmetric
   firewall, idled-out NAT mapping): sends report success while every packet vanishes — measured live as 100%
   one-way loss with the link showing "direct". The datagram fast path is an *upgrade*, never a prerequisite:
   TX consults `linkMaintainer.datagramsTrusted()` and rides the reliable stream until pongs prove the path,
   demoting back the moment they stop. Delivery must never depend on an unverified path.

3. **The stream fallback must work over ANY connection.** `NewStream` refuses relayed (circuit-v2 "limited")
   conns unless the ctx carries `network.WithAllowLimitedConn`. Both custom protocols (`friend.go`,
   `overlay.go sendStream`) opt in — without it, a relay-only pair has *no* path at all.

4. **No panic may cross a goroutine boundary unguarded.** The node is a c-shared library: an unrecovered panic
   in any goroutine kills the whole process (node + bitswap + GUI) — the "whole node just went off" incident.
   Every `go` in node code goes through `safeGo`/`guard`; fallible steps that may panic use `guardErr` so the
   caller's existing error path (e.g. the goOnline retry) handles it. `retryOnline` specifically must never
   hold `gMu` across a panic (deferred-unlock closure).

5. **Every link state has an exit.** down →dial(backoff 2 s→30 s)→ connecting → relayed (usable via stream)
   →force-direct nudge (30 s cadence)→ direct-unproven (stream) →first pong→ direct-proven (datagrams)
   →misses/send-error→ demoted-unproven (stream) → re-proven on next pong. Friend add/remove takes effect
   within one 4 s beat. No state is terminal except "not friends anymore".

6. **Upgrade dials must force-direct and must have addresses.** `dialPeer` short-circuits when *any* conn
   exists, so upgrade nudges use `dialPeerDirect` (`network.WithForceDirectDial`). mDNS-discovered same-LAN
   addrs are pinned in the peerstore for 1 h (`mdnsNotifee`) so those dials still have them — this is what
   turns "two PCs on one LAN" into a 2 ms local link instead of a hairpin punch (measured: WG hairpin 60 ms,
   local path 2 ms).

7. **Receive loops must be re-kicked, not assumed.** A conn established *before* the peer became a friend
   never got a datagram receive loop, and the `dgRecv` dedupe made the miss permanent (pongs land in the void
   forever). The maintainer calls `ensureRecvLoops` every evaluate — idempotent, cheap.

## Known failure modes and where they're handled

| Failure | Detection | Response |
|---|---|---|
| NAT mapping idles out (~30 s) | heartbeat every 4 s keeps it warm; misses if dead | demote to stream + force-direct re-dial |
| One-way hole punch | pongs never arrive → path never trusted | traffic on stream from the start; periodic fresh punch |
| Proven path dies (path change, sleep) | `linkBeatMiss` misses or a send error (`suspect`) | instant distrust → stream; nudge new direct path |
| Hole punch impossible (symmetric NAT) | no direct conn at all | relayed state; limited-conn stream carries the LAN |
| Peer on a build without heartbeats | `everPonged` never set | stream forever (correct, not fast); no demote loop |
| Friend offline | connectedness lost | down + dial with backoff; conn re-protected on return |
| Panic in any network goroutine | `guard` recover + stack log | that iteration/goroutine dies; node lives |
| goOnline fails/panics at startup | `guardErr` | offline node + background retry with backoff |

## Diagnostics

- `VG_OVERLAY_DEBUG=1` — per-tick link state lines.
- `VG_OVERLAY_FORCE_STREAM=1` — disable the datagram path entirely (A/B lever).
- `--net-test` — headless network/firewall sweep.
- `--lan --overlay-exec "<cmd>"` — bring the overlay TUN up headless and run a command inside it (the
  standard 2-machine datapath test: `ping -c 15 <friend vIP>`).
- State-change log lines carry counters: `dgTx/dgRx/streamTx/demoted` — a healthy proven link shows dgRx>0;
  a link riding the fallback shows streamTx climbing; `demoted` counts trust losses (flapping datagram path).
