# Project Status

Last updated: 2026-04-27

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
- Define simple command format (PRODUCE / FETCH)
- First manual end-to-end test (e.g. via netcat)
- Simple CLI producer (send messages over TCP)
- Simple CLI consumer (fetch loop)
- End-to-end pipeline: external client -> broker -> fetch
- Basic observability (logs / debug output)
- Topic auto-discovery on startup
- Segment files per topic
- Retention (max_topic_bytes)
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
## In Progress
refactor simulator.rs

[]Extract movement model module
[]Extract broker transport/retry module
[]Keep run_simulation as orchestration
[]Run cargo test --lib / relevant tests after each step

 
## Next Up
- Extend sparse-index startup skip for closed segments (keep tail replay path unchanged).
- Collect startup replay telemetry (`skipped/replayed/fallback reasons`) and verify with integration tests.
- Tail-segment optimization (optional): evaluate index-assisted seek/partial replay without weakening corruption safety.

## Later (TODO, not now)

- Protobuf encoding
- QUIC transport

- Bevy UI integration
- Real IoT client (Ox64)
- scripts, skills folder
- ui dark mode/bright mode

## Known Gaps / Risks


- Startup replay still decodes tail segment and fallback segments; sparse index helps for closed segments but deeper skip-paths are still needed at larger scales.
- Single shared broker lock (`Arc<Mutex<Broker>>`) may become a throughput bottleneck under concurrent clients.
- No CI guardrails yet (`fmt`/`clippy`/`test` in pipeline), increasing regression risk.
- TCP text protocol is MVP-only; no schema/framing guarantees for long-term interoperability.

## Notes

- Startup replay benchmark (2026-04-27, `scripts/startup_replay_bench.ps1 -Iterations 3`):
  - metadata-skip-startup-path avg: `0.327s`
  - fallback-decode-startup-path avg: `0.333s`
- A/B benchmark (2026-04-27, `scripts/startup_ab_speed.ps1 -Iterations 3`, dataset: `80,000` messages, payload `128B`, `segment_max_bytes=4096`):
  - baseline (before sparse-index startup changes) full test avg: `25.467s`
  - current (with sparse-index startup changes) full test avg: `25.761s` (`+0.294s`, `+1.15%`)
  - restart phase markers (`restart_elapsed_ms`) improved on average: baseline `~215ms` -> current `~203ms` (`-12ms`, `~5.6%`)
  - interpretation: test runtime is dominated by data generation/write cost; restart-only marker is the better signal for startup benefit.
- Benchmark history: `docs/benchmarks.md`
- Tests passing as of 2026-04-27 (`cargo test --test broker_persistence`)
- Focus: keep core minimal, avoid premature features
- Philosophy: build only what is needed now
