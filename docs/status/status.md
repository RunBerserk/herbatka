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

## In Progress

## Next Up
- v1 risk decision — Benchmark or document acceptance of global Mutex + concurrent clients.
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

- Single shared broker lock (`Arc<Mutex<Broker>>`) may become a throughput bottleneck under concurrent clients. (should quantify under real load before redesigning — pattern unchanged in `crates/herbatka/src/main.rs` + `crates/herbatka/src/tcp/server.rs`.)

 

## Notes

- Startup replay summary log includes `closed_partial_replay_used` and `closed_partial_replay_fallback` (non-tail sparse seek) alongside existing `tail_partial_*` fields.
- Benchmark history: [benchmarks.md](benchmarks.md) — see short **Cadence** note there (re-baseline when performance-sensitive code paths change).
