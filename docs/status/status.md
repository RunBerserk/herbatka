# Project Status

Last updated: 2026-05-11

## Current Phase

Persistence and recovery baseline -> moving toward external access (TCP)

## Done

History extracted to [done.md](done.md).

- Cargo **workspace**: `herbatka` (broker), `herbatka-simulator`, `herbatka-ui`, shared **`herbatka-wire`** (TCP framing, commands, fleet protobuf). Repo root `Cargo.toml` is workspace-only; sources live under `crates/`.

## In Progress
-bugfix(UI fleet map stays empty (no vehicle markers) when the events topic has no messages on disk or the broker isn’t serving fresh fetches; recovery: clear data/logs/events if needed, start the broker, then reload the topic with the simulator (or producer).)
## Next Up



## Later (TODO, not now)
- versioning 
- **Protobuf on the wire** (replacing framed layout with protobuf RPC) — not the same as payload protobuf inside today’s frame body; only if a new protocol version is desired
- QUIC transport
- two additional testscenarios: (1. was carfleet,)  2. stock market, 3. logistics data
 
- Bevy UI integration? (in seperated ui project, or cancel)
- ui dark mode/bright mode
- Real IoT client (Ox64)

## Known Gaps / Risks

- Single shared broker lock (`Arc<Mutex<Broker>>`) may become a throughput bottleneck under concurrent clients. (should quantify under real load before redesigning — pattern unchanged in `crates/herbatka/src/main.rs` + `crates/herbatka/src/tcp/server.rs`.)

- Legacy `MSG` lines still go through lossy UTF‑8 for display; framed v1 returns raw message bytes. (should clarify: legacy text uses **`herbatka_wire::tcp::command::format_response`**; framed **wire** stays raw bytes, but **consumer** and **UI** `broker_client` still **`from_utf8_lossy`** for stdout / `String` payloads.)

## Notes

- Startup replay summary log includes `closed_partial_replay_used` and `closed_partial_replay_fallback` (non-tail sparse seek) alongside existing `tail_partial_*` fields.
- Benchmark history: [benchmarks.md](benchmarks.md).
