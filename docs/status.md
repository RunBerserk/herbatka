# Project Status

Last updated: 2026-05-04

## Current Phase

Persistence and recovery baseline -> moving toward external access (TCP)

## Done

- In-memory log + produce/fetch
- Disk append format (`[len][message]`)
- Encode/decode (binary, minimal)
- Recovery on restart (per topic, via replay)
- Append after restart (no overwrite)
- Integration tests for persistence + recovery
- Minimal TCP interface (server + basic protocol)
- Define simple command format (PRODUCE / FETCH); **framed wire v1** with handshake (`HERBATKA WIRE/1`) + legacy first-line newline mode (`docs/tcp-wire-protocol.md`)
- Three logical channels (heartbeat / control / telemetry) as **topic naming only** — see [logical-channels.md](logical-channels.md); broker unchanged
- Canonical **Protobuf** shapes for those lanes (`proto/herbatka_fleet.proto`, `herbatka::generated_schemas`); payload bytes inside framed bodies; default demos stay JSON on topic `events`
- First manual end-to-end test (e.g. via netcat)
- Simple CLI producer (send messages over TCP)
- Simple CLI consumer (fetch loop)
- End-to-end pipeline: external client -> broker -> fetch
- Basic observability (logs / debug output)
- Topic auto-discovery on startup
- Segment files per topic
- Retention (`max_topic_bytes` global default)
- Per-topic retention overrides (`[per_topic_max_bytes]` in `herbatka.toml`; exact topic names only)
- Fsync policy tuning
- Corrupted-tail handling on startup replay (`UnexpectedEof` tail is truncated to last valid record boundary)
- Protocol command extraction to dedicated module (`tcp::command`) with compatibility shim in `tcp::protocol`
- Startup checkpoint manifest foundation (`.checkpoint`) with per-segment metadata (`base_offset`, `message_count`, `valid_len`)
- Hybrid checkpoint persistence cadence (on segment roll/retention and every N appends)
- Checkpoint fallback safety (stale/corrupt checkpoint falls back to safe replay path)
- Build the simulator (data producer)
  - [x] MVP simulator CLI
  - [x] Scenario engine
  - [x] Reliability/observability
  - [x] Load profiles
  - [x] Docs + test harness
- Startup sparse index foundation is in place (sidecar write/read + compatibility checks + safe fallback paths).
- Sparse-index startup skip for trusted closed segments is complete; tail segment replay path remains unchanged with corruption-truncation safety.
- Startup replay telemetry contract for fallback reasons is covered (`tail_segment`, `missing_checkpoint`, `missing_or_invalid_index`, `index_incompatible`).
- Non-tail `MustReplay` uses the same last-sparse-anchor seek as tail replay when checkpoint + index align (`closed_partial_replay_used` / `closed_partial_replay_fallback` in startup summary log); `can_metadata_skip` barrier for **trusted** closed skip after an earlier decode replay is unchanged (in-memory `fetch` consistency).
[x] MVP UI client (`egui`, map-first)
 - [x] UI app shell (window + panel layout)
 - [x] Broker connection state + basic fleet list (read-only)
 - [x] Selected vehicle telemetry panel
 - [x] Fleet stats panel (online/stale/avg speed/topic lag)
 - [x] Process output panel (stdout/stderr stream) 
 - [x] Process controls (start/stop broker + simulator) 
 - [x] Basic error/reconnect handling
 - [x] Minimal map pane (lat/lon points + vehicle selection) 
 - [x] lat/lon movement
 - refactoring core.rs
    [x] Create src/broker/core/ modules (topic_paths.rs, checkpoint_io.rs, retention.rs, startup.rs, api.rs) and wire mod ... in core.rs only
    [x] Move topic path helpers into core/topic_paths.rs and verify compile/tests
    [x] Move checkpoint/index I/O helpers into core/checkpoint_io.rs and verify compile/tests (next isolated slice)
    [x] Move retention logic (enforce_retention) into core/retention.rs and verify compile/tests
    [x] Move startup/discovery logic (discover_segments, load_topic_state, topic discovery helper) into core/startup.rs with no behavior change
    [x] Move public broker API methods (create_topic, discover_topics_on_startup, produce, fetch, fetch_batch) into core/api.rs
    [x] Keep shared types/constants in core.rs until all moves are complete (Broker, TopicState, SegmentMeta, BrokerError, constants)(no code changes needed)
    [x] Trim core.rs to minimal wiring/shared definitions after all extractions(no code changes needed)
    [x] extracting tests
    [x] Run validation after each step: cargo test --lib and cargo test --test broker_persistence
    [x] Final pass: small cleanup-only naming/comments, no protocol/storage/behavior changes
 - refactor simulator.rs
    [x]Extract CLI parsing module
    [x]Extract movement model module    
    [x]Extract broker transport/retry module
    [x]Keep run_simulation as orchestration    
    [x]Run cargo test --lib / relevant tests after each step
 - Tail-segment optimization  : evaluate index-assisted seek/partial replay without weakening corruption safety.
 - refactor startup_discovery
 - CI guardrails yet (`fmt`/`clippy`/`test` in pipeline), increasing regression risk.
 - Larger-scale startup: tail still decodes (safety); selective **trusted** skip of closed segments after a prior decode replay remains off (see `load_topic_state` comments). Optional follow-up: trusted tail skip / fetch-from-segment if history must stay visible without full RAM materialization.

## In Progress

## Next Up



## Later (TODO, not now)
 
- **Protobuf on the wire** (replacing framed layout with protobuf RPC) — not the same as payload protobuf inside today’s frame body; only if a new protocol version is desired
- QUIC transport
- Bevy UI integration?
- Real IoT client (Ox64)
-  skills folder
- ui dark mode/bright mode

## Known Gaps / Risks

- Single shared broker lock (`Arc<Mutex<Broker>>`) may become a throughput bottleneck under concurrent clients.
- CI: GitHub Actions workflow runs `fmt` / `clippy -D warnings` / `test` on push to `main`/`master`.
- Legacy `MSG` lines still go through lossy UTF‑8 for display; framed v1 returns raw message bytes.

## Notes

- Startup replay summary log includes `closed_partial_replay_used` and `closed_partial_replay_fallback` (non-tail sparse seek) alongside existing `tail_partial_*` fields.
- Startup replay benchmark (2026-05-01, `scripts/startup_replay_bench.ps1 -Iterations 1`): metadata-skip ~`0.371s`, fallback-decode ~`0.344s` (single runs; see `docs/benchmarks.md`).
- Startup replay benchmark (2026-04-27, `scripts/startup_replay_bench.ps1 -Iterations 3`):
  - metadata-skip-startup-path avg: `0.327s`
  - fallback-decode-startup-path avg: `0.333s`
- A/B benchmark (2026-04-27, `scripts/startup_ab_speed.ps1 -Iterations 3`, dataset: `80,000` messages, payload `128B`, `segment_max_bytes=4096`):
  - baseline (before sparse-index startup changes) full test avg: `25.467s`
  - current (with sparse-index startup changes) full test avg: `25.761s` (`+0.294s`, `+1.15%`)
  - restart phase markers (`restart_elapsed_ms`) improved on average: baseline `~215ms` -> current `~203ms` (`-12ms`, `~5.6%`)
  - interpretation: test runtime is dominated by data generation/write cost; restart-only marker is the better signal for startup benefit.
- Benchmark history: `docs/benchmarks.md`
- Tests passing (`cargo test`, `cargo test --test broker_persistence`) after 2026-05-01 startup seek extension
- Focus: keep core minimal, avoid premature features
- Philosophy: build only what is needed now
