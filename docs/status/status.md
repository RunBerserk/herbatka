# Project Status



Last updated: 2026-05-26



## Current Phase



**0.7.9** workspace line — polishing toward a declared stable **1.0.0** (scope in [v1.md](v1.md)). Hardening pass **complete**; per-topic broker locking **done** (2026-05-26). Early annotated tag **`v1.0.0`** exists on the remote; crate versions and this doc describe the current pre-final line.



## Done



History in [done.md](done.md).



## In Progress



_(none)_



## Next Up



**v1 concurrency (optional follow-ups)** — wire format unchanged unless you add wire v2 on purpose:



1. **Broker actor + channel** or **async framing** (Phase B) around [`handle_client`](../../crates/herbatka/src/tcp/server.rs) — only if you need further throughput or true async I/O; per-topic locking already allows overlapping produce on different topics.



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



- **Concurrency:** The **`herbatka`** binary uses **Tokio** for TCP **bind/accept** ([`run`](../../crates/herbatka/src/tcp/server.rs)); each accepted socket is handled on a **`std::thread`** running sync **`handle_client`**. The shared [`Broker`](../../crates/herbatka/src/broker/core.rs) is behind **[`SharedBroker`](../../crates/herbatka/src/tcp/server.rs)** (`Arc<Broker>`) with **per-topic** `RwLock`s: **Produce** / **Fetch** on **different** topics can overlap; **same-topic** produce still serializes on that topic’s lock. **v1 concurrent TCP bar verified (2026-05-18, re-checked 2026-05-26):** [benchmarks.md — per-topic locking re-check](benchmarks.md#2026-05-26--full-v1-re-check-after-per-topic-locking-860-default). **Optional (not a v1 blocker):** broker actor or **async wire** (Phase B).



## Notes



- Startup replay summary log includes `closed_partial_replay_used` and `closed_partial_replay_fallback` (non-tail sparse seek) alongside existing `tail_partial_*` fields.

- Benchmark history: [benchmarks.md](benchmarks.md) — see short **Cadence** note there (re-baseline when performance-sensitive code paths change).

- **Versioning:** Crates at **0.7.9**; next semver step (e.g. **0.8.0** or **1.0.0-rc.1**) when you declare polishing done — update [CHANGELOG.md](../../CHANGELOG.md) and tags then.

