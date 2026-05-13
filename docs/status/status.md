# Project Status

Last updated: 2026-05-13

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

## In Progress

## Next Up

- **v1 concurrency** — Multiple concurrent TCP clients must **work correctly** on v1 (no silent corruption; responsive enough for the intended demo / multi-client use). Baseline under load in [benchmarks.md](benchmarks.md); extend tests if gaps appear. **Implementation direction (not committed until designed):** a **Tokio**-based async TCP server and task model is a likely option to improve I/O concurrency while keeping broker invariants; finer-grained locking or an actor-style broker task are alternatives—see Known Gaps.
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
