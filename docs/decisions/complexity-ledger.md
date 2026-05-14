# Complexity ledger

**Purpose:** Explain **intentional** complexity for **future maintainers** (including yourself). Aligns with the README line *“Do nothing which is of no use.”*—if code is heavier than a toy, it should **buy** correctness, performance, time-to-ship, or compatibility.

**When to add a row:** You merge (or live with) a design that a newcomer might simplify “wrong.” **When to remove or shrink:** The simpler path becomes viable (e.g. full async I/O) or the feature is deleted.

| Area | What feels heavy | Simpler option (rejected for now) | Why this way | Revisit when |
|------|------------------|-----------------------------------|----------------|--------------|
| TCP runtime ([`run`](../../crates/herbatka/src/tcp/server.rs), [`serve`](../../crates/herbatka/src/tcp/server.rs)) | Tokio **`accept`**, then **`into_std()`**, **`set_nonblocking(false)`**, **`std::thread`** per client for sync [`handle_client`](../../crates/herbatka/src/tcp/server.rs) | Single **`std`** accept loop only; or full Tokio/async `handle_client` | Correct **blocking** framed I/O (notably on Windows); reuse one sync wire path; tests can use [`serve`](../../crates/herbatka/src/tcp/server.rs) without a Tokio harness | Phase B async framing / async I/O; or if platforms no longer need the handoff |
| Shared broker ([`SharedBroker`](../../crates/herbatka/src/tcp/server.rs)) | **`Arc<RwLock<Broker>>`** vs one mutex | **`Arc<Mutex<Broker>>`** only | Parallel **Fetch** / **TopicBounds** under read lock; writes still serialized | Broker actor, per-topic locks, or measured proof that `Mutex` is enough |
| Startup replay ([`load_topic.rs`](../../crates/herbatka/src/broker/core/startup_discovery/load_topic.rs), checkpoints, sparse index) | Trusted skip vs replay, tail partial replay, fallback counters | Always full decode of every segment | **Restart time** and I/O; optional sidecars stay safe when missing or invalid | v1 frozen and you delete optional fast paths; or new storage layout |
| Workspace split (`herbatka`, `herbatka-wire`, UI, simulator) | Multiple crates and repos of truth for framing | Single crate for everything | Shared wire types, UI/simulator reuse, clearer dependency edges | Wire churn hurts more than split buys |
| Concurrency probe ([`tcp_concurrency_probe`](../../crates/herbatka/src/bin/tcp_concurrency_probe.rs)) | Multiple **`--profile`** modes and scripts flags | One workload forever | **Default** profile preserves historical / v1 acceptance comparability; **fetch-heavy** / **max-pressure** stress read skew without changing the bar | Profiles multiply without insight; fold into one script with config file |

## Smells / open questions

Use bullets for things you are **unsure** are still pulling their weight—date when you last looked.

- *(Add entries here.)*

## Related docs

- [HOW.md — Architecture](../how.md#architecture) — source of truth, rebuild, diagrams  
- [TCP wire protocol](../reference/tcp-wire-protocol.md) — transport and server notes  
- [Benchmarks — workload profiles](../status/benchmarks.md#tcp_concurrency_probe-workload-profiles) — probe profiles (not v1 acceptance unless decided)
