# Project Status

Last updated: 2026-04-20

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
- Extend startup replay performance improvements beyond checkpoint manifest (reduce decode work further; evaluate sparse index).
[ ] MVP UI client (`egui`, map-first)
 - [x] UI app shell (window + panel layout)
 - [x] Broker connection state + basic fleet list (read-only)
## In Progress
 - [ ] Selected vehicle telemetry panel
## Next Up
simple UI
 - [ ] Fleet stats panel (online/stale/avg speed/topic lag)
 - [ ] Process output panel (stdout/stderr stream)
 - [ ] Process controls (start/stop broker + simulator) 
 - [ ] Basic error/reconnect handling
 - [ ] Minimal map pane (lat/lon points + vehicle selection)
## Later (TODO, not now)

- Protobuf encoding
- QUIC transport

- Bevy UI integration
- Real IoT client (Ox64)
- scripts, skills folder

## Known Gaps / Risks


- Startup replay still decodes heavily at scale; checkpoint manifest helps, but sparse indexing or deeper skip-paths are still needed.
- Single shared broker lock (`Arc<Mutex<Broker>>`) may become a throughput bottleneck under concurrent clients.
- No CI guardrails yet (`fmt`/`clippy`/`test` in pipeline), increasing regression risk.
- TCP text protocol is MVP-only; no schema/framing guarantees for long-term interoperability.

## Notes

- Tests passing as of 2026-04-20 (`cargo test`)
- Focus: keep core minimal, avoid premature features
- Philosophy: build only what is needed now
