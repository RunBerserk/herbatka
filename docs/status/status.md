# Project Status

Last updated: 2026-05-12

## Current Phase

External access (TCP) wire v1 baseline closed; remaining v1 items tracked under **Next Up**.

## Done

History extracted to [done.md](done.md).

- Cargo **workspace**: `herbatka` (broker), `herbatka-simulator`, `herbatka-ui`, shared **`herbatka-wire`** (TCP framing, commands, fleet protobuf). Repo root `Cargo.toml` is workspace-only; sources live under `crates/`.
- **UI fleet map** recovery when read cursor is past a shorter or reset log: `TopicBounds` on the wire, periodic clamp, **Resync read position**, reset when starting broker from UI (`herbatka-ui`).
- **UI local data**: clear on-disk `data/logs/<topic>` (default `events`) when embedded broker/sim are stopped; **Quick demo load** runs a fixed short simulator (burst / ramp / 5s / seed 42).
- wire: lossy UTF-8 display helper + docs
- **TCP wire v1 closure:** [tcp-wire-protocol.md](../reference/tcp-wire-protocol.md) — implementation notes (framed vs legacy errors), reference clients, minimal framed flow; integration tests — legacy `ERR` paths, CRLF handshake, framed unknown-op recovery, oversize first line / oversize `payload_len`.
- **v1 definition of done:** [v1.md](v1.md) — single-node scope, verification (CI-aligned), explicit not-in-v1.0 exclusions (clustering / leader / quorum / HA failover), pointers to open decisions.
- **`Message.timestamp` → `u64` epoch ms:** [roadmap.md](roadmap.md) — domain type aligned with segment encoding; [`now_epoch_millis`](../../crates/herbatka/src/time.rs).
- **v1 concurrency — (2) measure / reproduce:** Baseline harness + script + dated entry — [benchmarks.md — TCP concurrency baseline measurement](benchmarks.md#tcp-concurrency-baseline-measurement-pre-step-3).

## Next Up

**v1 concurrency** — criteria: [benchmarks.md — v1 TCP concurrency acceptance criteria](benchmarks.md#v1-tcp-concurrency-acceptance-criteria); baseline: [benchmarks.md — TCP concurrency baseline measurement](benchmarks.md#tcp-concurrency-baseline-measurement-pre-step-3) (see [Known Gaps / Risks](#known-gaps--risks); wire format unchanged—transport/runtime only unless you add wire v2 on purpose):

2. **Concurrent connections (minimal)** — e.g. `std::thread::spawn` per accepted `TcpStream`, keep sync `herbatka-wire` framing; integration test with **2+ simultaneous** clients.
3. **Tokio (optional)** — `#[tokio::main]`, accept loop + `spawn` per connection, async I/O; **Tokio** is a likely direction; same on-the-wire bytes as [tcp-wire-protocol.md](../reference/tcp-wire-protocol.md). May need async framing helpers.
4. **Broker lock / throughput** — After (2) or (3): e.g. `RwLock`, broker actor + channel, or `tokio::sync::Mutex`; prove with tests targeted at your bar.

- Cut v1 / tag — Changelog + version bump when you declare feature-complete (CHANGELOG.md + semver).


## Later (TODO, not now)

- **Protobuf on the wire** (replacing framed layout with protobuf RPC) — not the same as payload protobuf inside today’s frame body; only if a new protocol version is desired
- QUIC transport
- two additional testscenarios: (1. was carfleet,)  2. stock market, 3. logistics data
- versioning 
- better ui ux, maybe some information in tabs
- Bevy UI integration? (in seperated ui project, or cancel)
- ui dark mode/bright mode
- Real IoT client (Ox64)

## Known Gaps / Risks

- **Concurrency is a v1 problem:** today the broker is behind a **single shared** `Arc<Mutex<Broker>>` (`crates/herbatka/src/main.rs`, `crates/herbatka/src/tcp/server.rs`). v1 is **not** done while “many clients at once” is only theoretically correct—define an acceptable bar (e.g. N framed clients, produce/fetch mix), measure it, and close gaps (tests + code or documented limits you refuse to exceed). **Likely follow-up direction:** **Tokio** for async accept/read/write and structured tasks; the mutex on shared `Broker` state may still need a deliberate strategy (e.g. `tokio::sync::Mutex`, `RwLock`, or a single broker task + channels)—Tokio alone does not remove that design choice.

## Notes

- Startup replay summary log includes `closed_partial_replay_used` and `closed_partial_replay_fallback` (non-tail sparse seek) alongside existing `tail_partial_*` fields.
- Benchmark history: [benchmarks.md](benchmarks.md) — see short **Cadence** note there (re-baseline when performance-sensitive code paths change).
