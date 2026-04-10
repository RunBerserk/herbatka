# Project Status

Last updated: 2026-04-10

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

## In Progress



## Next Up

- Simple CLI producer (send messages over TCP)
- Simple CLI consumer (fetch loop)
- End-to-end pipeline: external client -> broker -> fetch
- Basic observability (logs / debug output)

## Later (TODO, not now)

- QUIC transport
- Protobuf encoding
- Topic auto-discovery on startup
- Segment files per topic
- Retention (max_topic_bytes)
- Fsync policy tuning
- Real IoT client (Ox64)
- Bevy UI integration

## Known Gaps / Risks

- Topic must be manually created after restart
- No corrupted-tail handling (partial write)
- Full log replay on startup (no indexing)
- Single file per topic (no segmentation)

## Notes

- Tests passing as of 2026-04-10 (`cargo test`)
- Focus: keep core minimal, avoid premature features
- Philosophy: build only what is needed now
