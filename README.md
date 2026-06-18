# VidyaGodIPFS

An embedded IPFS node for [VidyaGod](https://github.com/lorenzo-zurini/VidyaGod), built on
[Boxo](https://github.com/ipfs/boxo) and compiled to an **in-process c-shared library** (`libvgipfs.so`) that
VidyaGod links and calls via cgo. It replaces shelling out to the external Kubo `ipfs` CLI.

## Why

- **No conflict with the user's Kubo.** The node runs against its own private repo
  (`~/.local/share/VidyaGod/ipfs`) and never touches `~/.ipfs` or the user's daemon/ports.
- **No disk duplication.** Content is fetched **write-through to a filestore**: leaf blocks are stored *by
  reference* into the on-disk destination file, so the seedable copy *is* the file — no separate blockstore copy
  (Kubo's `ipfs get` keeps a full second copy in `~/.ipfs/blocks`).
- **Public-IPFS compatible.** Joins the public DHT/swarm; existing CIDs keep resolving. `AddNoCopy` reproduces
  Kubo's `--nocopy` CIDs exactly (raw leaves, 256 KiB fixed chunker, balanced layout, dag-pb v0 root, sha2-256).

## C ABI

Exported via cgo (`buildmode=c-shared`); see the generated `libvgipfs.h`. Fallible calls return `0`/`-1` and pass
results/errors back through `char**` out-params allocated with C and freed via `VgFree`.

| Symbol | Purpose |
|--------|---------|
| `VgStart(repo, err)` / `VgStop()` | node lifecycle |
| `VgAddNoCopy(path, outCid, err)` | seed a local file by reference (filestore `--nocopy`) |
| `VgFetchToPath(cid, dest, err)` | fetch content write-through to a path *(M2)* |
| `VgPinLs` / `VgPinRm` | list / remove seeded pins |
| `VgCidSize(cid)` | CumulativeSize of a CID |
| `VgPeerCount` / `VgRepoStat` / `VgProviderCount` | IPFS-tab status *(M3)* |
| `VgRequestCancel` / `VgClearCancel` / `VgSetTransferCb` | fetch cancellation + progress *(M2)* |

## Build

```sh
go build -buildmode=c-shared -o libvgipfs.so .
```

Built automatically by VidyaGod's CMake (`external/VidyaGodIPFS` submodule) into the build root next to the
`VidyaGod` binary.

## Status

Complete and in use by VidyaGod (Kubo fully removed):

- **Storage + CID parity** — `VgAddNoCopy` reproduces Kubo's `--nocopy` CIDs exactly (regression-locked in tests).
- **Write-through fetch** — `VgFetchToPath` materializes content at the destination and seeds it by reference, with
  no blockstore duplication; progress + cancellation supported.
- **Public network** — libp2p + Kademlia DHT + Bitswap + reprovider; `VgPeerCount`/`VgRepoStat`/`VgProviderCount`.

## Tests

```sh
go test ./...
```

Offline tests (they force `VIDYAGOD_IPFS_OFFLINE`) covering CID parity, no-duplication, fetch round-trip, progress,
and cancellation. Also run in CI.
