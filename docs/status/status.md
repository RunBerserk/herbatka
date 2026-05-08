# Project Status

Last updated: 2026-05-08

## Current Phase

Persistence and recovery baseline -> moving toward external access (TCP)

## Done

History extracted to [done.md](done.md).

## In Progress

## Later (TODO, not now)
- versioning 
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
- Benchmark history: [benchmarks.md](benchmarks.md).
