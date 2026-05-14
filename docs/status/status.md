# Project Status

Last updated: 2026-05-14

## Current Phase

**v1.0.0** shipped (annotated tag `v1.0.0`); optional TCP concurrency follow-ups remain under **Next Up**.

## Done

History extracted to [done.md](done.md).

- Cargo **workspace**: `herbatka` (broker), `herbatka-simulator`, `herbatka-ui`, shared **`herbatka-wire`** (TCP framing, commands, fleet protobuf). Repo root `Cargo.toml` is workspace-only; sources live under `crates/`.
- **UI fleet map** recovery when read cursor is past a shorter or reset log: `TopicBounds` on the wire, periodic clamp, **Resync read position**, reset when starting broker from UI (`herbatka-ui`).
- **UI local data**: clear on-disk `data/logs/<topic>` (default `events`) when embedded broker/sim are stopped; **Quick demo load** runs a fixed short simulator (burst / ramp / 5s / seed 42).
- wire: lossy UTF-8 display helper + docs
- **TCP wire v1 closure:** [tcp-wire-protocol.md](../reference/tcp-wire-protocol.md) — implementation notes (framed vs legacy errors), reference clients, minimal framed flow; integration tests — legacy `ERR` paths, CRLF handshake, framed unknown-op recovery, oversize first line / oversize `payload_len`.
- **v1 definition of done:** [v1.md](v1.md) — single-node scope, verification (CI-aligned), explicit not-in-v1.0 exclusions (clustering / leader / quorum / HA failover), pointers to open decisions.
- **`Message.timestamp` → `u64` epoch ms:** [roadmap.md](roadmap.md) — domain type aligned with segment encoding; [`now_epoch_millis`](../../crates/herbatka/src/time.rs).
- **v1 concurrency — (2) measure / reproduce:** Baseline harness + script + dated entry — [benchmarks.md — TCP concurrency baseline measurement](benchmarks.md#tcp-concurrency-baseline-measurement-pre-step-3).
- **v1 concurrency — minimal concurrent TCP accepts:** [`serve`](../../crates/herbatka/src/tcp/server.rs) (`std::thread::spawn` per accepted connection); integration test `tcp_framed_two_clients_concurrent_produce` in [`tcp_server_smoke.rs`](../../crates/herbatka/tests/tcp_server_smoke.rs).
- **v1 concurrency — Tokio TCP runtime (Phase A):** broker binary [`#[tokio::main]`](../../crates/herbatka/src/main.rs); production [`run`](../../crates/herbatka/src/tcp/server.rs) uses **`tokio::net::TcpListener`** for async **`accept`**, then **`into_std()`** + **`std::thread`** per connection for sync [`handle_client`](../../crates/herbatka/src/tcp/server.rs) (wire unchanged). Tests keep blocking [`serve`](../../crates/herbatka/src/tcp/server.rs).
- **v1 concurrency — broker `RwLock` ([`SharedBroker`](../../crates/herbatka/src/tcp/server.rs)):** **`Arc<RwLock<Broker>>`**; fetch / topic bounds use read lock, produce / `create_topic` write lock; integration test **`tcp_framed_concurrent_fetch_same_topic`** in [`tcp_server_smoke.rs`](../../crates/herbatka/tests/tcp_server_smoke.rs). Optional re-baseline: dated **RwLock** subsection in [benchmarks.md](benchmarks.md).
- **Cut v1 / tag:** [CHANGELOG.md](../../CHANGELOG.md) **1.0.0** (2026-05-14); workspace crates **1.0.0**; annotated tag **`v1.0.0`**.

## Next Up

**v1 concurrency (optional follow-ups)** — criteria: [benchmarks.md — v1 TCP concurrency acceptance criteria](benchmarks.md#v1-tcp-concurrency-acceptance-criteria); baseline: [benchmarks.md — TCP concurrency baseline measurement](benchmarks.md#tcp-concurrency-baseline-measurement-pre-step-3) (see [Known Gaps / Risks](#known-gaps--risks); wire format unchanged unless you add wire v2 on purpose):

2. **Further broker throughput** — e.g. broker actor + channel, per-topic locking, or **`tokio::sync::Mutex`** with async handlers; prove with the acceptance bar if you pursue it. Optional later: **async framing** / true async I/O (Phase B) around [`handle_client`](../../crates/herbatka/src/tcp/server.rs).


## Later (TODO, not now)
-update mermaid diagrams
- **TCP concurrency probe profiles** (`--profile fetch-heavy` / `max-pressure`, scripts `-FetchHeavy` / `-MaxPressure`) — see [benchmarks.md — tcp_concurrency_probe workload profiles](benchmarks.md#tcp_concurrency_probe-workload-profiles); extend further if you need hotter shared-topic or CI soak.
- **Protobuf on the wire** (replacing framed layout with protobuf RPC) — not the same as payload protobuf inside today’s frame body; only if a new protocol version is desired
- QUIC transport
- two additional testscenarios: (1. was carfleet,)  2. stock market, 3. logistics data
- versioning 
- better ui ux, maybe some information in tabs
- Bevy UI integration? (in seperated ui project, or cancel)
- ui dark mode/bright mode
- Real IoT client (Ox64)

## Known Gaps / Risks

- **Concurrency:** The **`herbatka`** binary uses **Tokio** for TCP **bind/accept** ([`run`](../../crates/herbatka/src/tcp/server.rs)); each accepted socket is handled on a **`std::thread`** running sync **`handle_client`**. The shared [`Broker`](../../crates/herbatka/src/broker/core.rs) is behind **[`SharedBroker`](../../crates/herbatka/src/tcp/server.rs)** (`Arc<RwLock<Broker>>`): **Fetch** / **TopicBounds** can run concurrently (read lock); **Produce** / topic creation still **serialize on the write lock** (global for the process). Integration tests use blocking [`serve`](../../crates/herbatka/src/tcp/server.rs). **Next direction (optional):** broker actor, finer-grained locking, or **async wire** (Phase B).

## Notes

- Startup replay summary log includes `closed_partial_replay_used` and `closed_partial_replay_fallback` (non-tail sparse seek) alongside existing `tail_partial_*` fields.
- Benchmark history: [benchmarks.md](benchmarks.md) — see short **Cadence** note there (re-baseline when performance-sensitive code paths change).
