# Project Status

Last updated: 2026-05-08

## Current Phase

Persistence and recovery baseline -> moving toward external access (TCP)

## Done

History extracted to [done.md](done.md).

## In Progress

## Next Up
- Replace expect in load_topic.rs with Err
- Stub CHANGELOG.md

## Later (TODO, not now)
 
- **Protobuf on the wire** (replacing framed layout with protobuf RPC) — not the same as payload protobuf inside today’s frame body; only if a new protocol version is desired
- QUIC transport
- two additional testscenarios: (1. was carfleet,)  2. stock market, 3. logistics data
- seperate project to 3 projects, broker, simulation, ui
- Bevy UI integration? (in seperated ui project, or cancel)
- ui dark mode/bright mode
- Real IoT client (Ox64)

## Known Gaps / Risks

- Single shared broker lock (`Arc<Mutex<Broker>>`) may become a throughput bottleneck under concurrent clients. (should quantify under real load before redesigning — pattern unchanged in `src/main.rs` + `tcp/server.rs`.)

- Legacy `MSG` lines still go through lossy UTF‑8 for display; framed v1 returns raw message bytes. (should clarify: legacy text uses **`tcp/command::format_response`**; framed **wire** stays raw bytes, but **consumer** and **UI** `broker_client` still **`from_utf8_lossy`** for stdout / `String` payloads.)

## Notes

- Startup replay summary log includes `closed_partial_replay_used` and `closed_partial_replay_fallback` (non-tail sparse seek) alongside existing `tail_partial_*` fields.
- Startup replay benchmark (2026-05-01, `scripts/startup_replay_bench.ps1 -Iterations 1`): metadata-skip ~`0.371s`, fallback-decode ~`0.344s` (single runs; see `docs/status/benchmarks.md`).
- Startup replay benchmark (2026-04-27, `scripts/startup_replay_bench.ps1 -Iterations 3`):
  - metadata-skip-startup-path avg: `0.327s`
  - fallback-decode-startup-path avg: `0.333s`
- A/B benchmark (2026-04-27, `scripts/startup_ab_speed.ps1 -Iterations 3`, dataset: `80,000` messages, payload `128B`, `segment_max_bytes=4096`):
  - baseline (before sparse-index startup changes) full test avg: `25.467s`
  - current (with sparse-index startup changes) full test avg: `25.761s` (`+0.294s`, `+1.15%`)
  - restart phase markers (`restart_elapsed_ms`) improved on average: baseline `~215ms` -> current `~203ms` (`-12ms`, `~5.6%`)
  - interpretation: test runtime is dominated by data generation/write cost; restart-only marker is the better signal for startup benefit.
- Benchmark history: `docs/status/benchmarks.md`
- Tests passing (`cargo test`, `cargo test --test broker_persistence`) after 2026-05-01 startup seek extension
- Focus: keep core minimal, avoid premature features
- Philosophy: build only what is needed now
