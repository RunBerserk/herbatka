# Project Status

Last updated: 2026-05-14

## Current Phase

**v1.0.0** shipped (annotated tag `v1.0.0`); optional TCP concurrency follow-ups remain under **Next Up**.

## Done

History in [done.md](done.md).

## In Progress
Polishing toward v1.0.0 — hardening pass

## Next Up
more hardening pass

TCP concurrency: measure or explicitly accept — Run tcp_concurrency_probe once against benchmarks acceptance criteria, or write in v1.md / status that current SharedBroker behavior is accepted for this line with a one-line reason. Removes the biggest open “is v1 actually OK?” question.

Full v1 verification on a clean tree — cargo fmt --check, clippy -D warnings, doc with RUSTDOCFLAGS=-D warnings, cargo test (same as v1.md). Cheap gate; catches regressions before you bump version or tag again.

Align version + status narrative (docs only) — One pass: crates 0.7.9, tag/history, In Progress hardening bullets, what “done” means for the next semver step. Low code, high clarity; stops you and future-you from mixing “shipped 1.0.0” with “still polishing.”

**v1 concurrency (optional follow-ups)** — criteria: [benchmarks.md — v1 TCP concurrency acceptance criteria](benchmarks.md#v1-tcp-concurrency-acceptance-criteria); baseline: [benchmarks.md — TCP concurrency baseline measurement](benchmarks.md#tcp-concurrency-baseline-measurement-pre-step-3) (see [Known Gaps / Risks](#known-gaps--risks); wire format unchanged unless you add wire v2 on purpose):

2. **Further broker throughput** — e.g. broker actor + channel, per-topic locking, or **`tokio::sync::Mutex`** with async handlers; prove with the acceptance bar if you pursue it. Optional later: **async framing** / true async I/O (Phase B) around [`handle_client`](../../crates/herbatka/src/tcp/server.rs).


## Later (TODO, not now)

- **TCP concurrency probe profiles** (`--profile fetch-heavy` / `max-pressure`, scripts `-FetchHeavy` / `-MaxPressure`) — see [benchmarks.md — tcp_concurrency_probe workload profiles](benchmarks.md#tcp_concurrency_probe-workload-profiles); extend further if you need hotter shared-topic or CI soak.
- **Protobuf on the wire** (replacing framed layout with protobuf RPC) — not the same as payload protobuf inside today’s frame body; only if a new protocol version is desired
- QUIC transport

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
