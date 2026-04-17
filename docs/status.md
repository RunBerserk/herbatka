# Project Status

Last updated: 2026-04-13

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

## In Progress

## Next Up
- Build the simulator (data producer)
  - [x] MVP simulator CLI
  - [x] Scenario engine
  - [x] Reliability/observability
  - [x] Load profiles
  - [ ] Docs + test harness

## Later (TODO, not now)

- Protobuf encoding
- QUIC transport
- simple UI
- Bevy UI integration
- Real IoT client (Ox64)
- scripts, skills folder

## Known Gaps / Risks

- No corrupted-tail handling (partial write)
- Full log replay on startup (no indexing)

## Notes

- Tests passing as of 2026-04-13 (`cargo test`)
- Focus: keep core minimal, avoid premature features
- Philosophy: build only what is needed now
