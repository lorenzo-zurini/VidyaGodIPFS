# Playing over the virtual LAN — UX evaluation

*Written 2026-09-04 from the two-machine open-internet campaign (PC on home LAN, laptop on work WiFi,
`VG_BENCH_NO_TUNNEL=1` so nothing rode the WireGuard tunnel). Numbers below are measured, not estimated.*

## The model the user experiences

There is no lobby, no host, no join button. **Accepted friends are simply present on one shared LAN**
(10.66.0.0/16), always, on every machine, with stable per-friend addresses. The user experience is meant to be
indistinguishable from two machines plugged into the same switch in 1999: start the game, open its own LAN
browser, see the friend's game, join it. Every game brings its own netcode; VidyaGod's job ends at delivering
IP packets between friends as if a cable connected them.

## Measured: does the illusion hold?

| The gameplay moment | Mechanism | Measured (open internet, cross-NAT) |
|---|---|---|
| "I see my friend's game in the server browser" | game's UDP broadcast → overlay fan-out → friend's TUN → host's unicast reply | **5/5 discovery replies**, first reply < 1 s |
| "I click join / lobby feels snappy" | TCP over the overlay | echo RTT **6.2 ms** |
| in-race feel | ICMP/UDP both directions | **15/15 + 15/15, 0 % loss, ~5–13 ms** (2–8 ms same-LAN) |
| level/asset transfer inside a game | TCP bulk over the overlay | **~8 MB/s** |
| friend's app dies mid-game and comes back | link maintainer: down → dial(backoff) → relayed → force-direct → direct | reconnect **5 s** after restart, full direct+proven at **+7 s**; a 36 s outage under slow ping ended **150/150, 0 % loss** — packets sent while down sat in the drop-oldest stream queue (≤64 deep) and flushed on reconnect. High-rate game UDP would instead drop-oldest through the outage and resume fresh — the right semantics for both. |
| game needs the internet too (CD-key, master server) | tri-plane NAT gateway | HTTP through tunnel in **6–9 ms** |

Verdict: for the games this targets (LAN-era, designed around 100 ms modems), the illusion holds completely.
The overlay's ~10 ms cross-internet is *better* than the real LANs some of these games were played on.

Path quality is adaptive and invisible: the datagram fast path carries traffic only after heartbeat pongs
PROVE it (one-way punches can't silently eat packets); otherwise the reliable stream carries everything at
~8 ms — the fallback was measured lossless under both ping and bulk load. Which transport the hole-punch
lands (QUIC → datagrams; TCP → stream) varies per punch; both deliver.

## The friend flow, honestly

- **Warm path (the normal case):** paste code → request → auto/one-click accept → both sides "accepted" in
  **< 1 s**, friend online, vIPs assigned, LAN ready. Excellent.
- **Cold start (both apps just launched):** the first request can fail with "friend request failed" because
  neither node has announced to the DHT yet (~60–90 s to become discoverable; deliberate: no bootstrap
  infrastructure). The mutual-crossing fix means if BOTH users paste each other's codes, the pair converges
  as soon as EITHER side's request lands — so the practical UX guidance is simply "you and your friend add
  each other, give it a minute." Friction: the error says *failed* when it means *not yet* — a retry-with-
  backoff on pending contacts (or "will keep trying…" wording) is the single highest-value UX improvement
  available here.
- **Once friends, forever frictionless:** the maintainer keeps links warm for the app's lifetime; a friend
  appearing online is enough to play — no per-session ritual of any kind.

## What the launch window tells the player

The pre-launch Virtual LAN panel: "Your vIP … — LAN ready", one row per friend (green ● direct + RTT, amber
● relayed, ○ offline), tick-boxes to exclude. "Relayed" is truthful-but-scary: it now also covers a healthy
direct-TCP path riding the stream at 8 ms — from the player's seat that is simply "good". A friendlier
labeling (e.g. ● connected · 8 ms, with the transport detail in a tooltip) would match the felt experience
better than the transport taxonomy.

## Residual honest limits

1. Cold-start discoverability (above) — by design; UX-copy/retry mitigations only.
2. Direct-QUIC (the 2 ms datagram path) is guaranteed same-LAN (mDNS) but a lottery cross-internet; the
   stream fallback at ~8 ms makes this a non-issue for the target library.
3. Symmetric-NAT pairs where punching fails entirely ride the public relay — playable for turn-based/slow
   games, degraded for twitch games. No infrastructure of ours exists to improve this, deliberately.
