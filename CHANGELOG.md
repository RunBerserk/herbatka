# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- **TCP server:** production binary uses **Tokio** for TCP bind/accept (`#[tokio::main]`, async [`run`](crates/herbatka/src/tcp/server.rs)), then **`std::thread`** per connection for sync [`handle_client`](crates/herbatka/src/tcp/server.rs) after `into_std()`. Integration tests use blocking [`serve`](crates/herbatka/src/tcp/server.rs). Shared broker state is [`SharedBroker`](crates/herbatka/src/tcp/server.rs) (`Arc<RwLock<Broker>>`): **Fetch** / **TopicBounds** use a read lock; **Produce** / topic creation use a write lock. **Breaking** for embedders: `serve`, `run`, and `handle_client` now take `SharedBroker` instead of `Arc<Mutex<Broker>>`. New integration test `tcp_framed_concurrent_fetch_same_topic` in `tcp_server_smoke`.

- **`herbatka` library:** `Message.timestamp` is now **`u64` Unix epoch milliseconds** (same as segment record encoding). **Breaking** for callers that used `std::time::SystemTime` on `Message`. Use `herbatka::time::now_epoch_millis` for current time at produce boundaries. Rationale: [roadmap.md](docs/status/roadmap.md) — Message Timestamp Representation.

### Added

- **v1 TCP concurrency acceptance criteria** in `docs/status/benchmarks.md` (checkable bar for multi-client framed TCP; links from `docs/status/status.md` and `docs/status/v1.md`).
- Maintainer **[complexity ledger](docs/decisions/complexity-ledger.md)** (`docs/decisions/`) for intentional tradeoffs; linked from [HOW.md — Decisions](docs/how.md#decisions).

- **tcp_concurrency_probe** `--profile default|fetch-heavy|max-pressure`: read-skew profiles burst-seed **512** produces per worker topic then a timed loop of **64** tail fetches + **1** produce per batch (`fetch-heavy` adds **1 ms** pacing per batch; `max-pressure` has no intentional sleeps). Prints **`probe_ops`** totals and `profile=` on **`probe_summary`**. Baseline scripts: **`-FetchHeavy`** / **`-MaxPressure`** (PowerShell) and **`--fetch-heavy`** / **`--max-pressure`** (bash). See [benchmarks.md](docs/status/benchmarks.md) — *tcp_concurrency_probe workload profiles*.
- Herbatka v1.0 definition of done (`docs/status/v1.md`): single-node scope, CI verification checklist, explicit not-in-v1.0 features, pointers to roadmap and risks.
- TCP protocol documentation: reference clients, minimal framed-client flow, and server implementation notes (`docs/reference/tcp-wire-protocol.md`).
- Integration tests for TCP legacy error lines, CRLF handshake, framed decode recovery, and oversize framing (`tcp_server_smoke`).

### Fixed

- Removed startup panic in `load_topic_state` when the trusted-skip invariant is violated; the broker now returns `BrokerError::Io(InvalidData)` instead of crashing during topic recovery.
