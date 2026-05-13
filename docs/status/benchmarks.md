# Benchmarks

This document tracks benchmark history for Herbatka.

**Cadence:** upcoming tickets and post-v1 work (persistence, TCP, concurrency, retention, etc.) can shift numbers—re-run the relevant harnesses here when those areas change or before a release so baselines are not lost.

## v1 TCP concurrency acceptance criteria

Canonical bar for **feature-complete v1.0** concurrent TCP use (single broker process). Implementation and measured baselines are tracked in [status.md](status.md) **Next Up** steps (2)–(5); this section is the **pass/fail contract**. Revise only by explicit decision (update this file and [status.md](status.md)).

### Topology (in scope)

- **One** `herbatka` process, one `listen_addr`, one `data_dir` — matches [v1.md](v1.md) single-node scope.
- Clients may be separate OS processes or threads; broker is the only server under test.

### Client count and lifetime (checkable)

- **Minimum `N = 8`** simultaneous **framed v1** TCP connections after `HERBATKA WIRE/1` handshake ([tcp-wire-protocol.md](../reference/tcp-wire-protocol.md)).
- Each connection keeps the socket open and performs the workload below for at least **60 s** (not connect-disconnect-only).
- **Two dimensions** must both be exercised in the eventual harness (step 3+): (a) **overlapping connection lifetimes** (several clients active at the same wall time — today the accept loop in [`server::run`](../../crates/herbatka/src/tcp/server.rs) serializes `handle_client` until disconnect); (b) **`Arc<Mutex<Broker>>` contention** under concurrent produce/fetch from those connections.

### Wire mode

- **Primary bar:** **Framed v1** only (Produce / Fetch / TopicBounds as needed).
- **Legacy line mode** remains required to pass existing CI and [`tcp_server_smoke`](../../crates/herbatka/tests/tcp_server_smoke.rs); it is **not** part of the **8-connection concurrent soak** unless you later extend this bar.

### Workload (checkable)

- Per connection, on a **dedicated topic** per client (e.g. `v1cc-{client_id}`) to avoid artificial cross-client topic contention unless a separate scenario adds shared-topic traffic.
- **Mix:** at least **60%** of operations are **Produce** (small UTF-8 payload, **≤ 1 KiB** body); the remainder **Fetch** (tail or sequential read). At least **1 Produce/s** average per connection over the 60 s window (allows bursty patterns).
- Topics may be created implicitly by Produce as today.

### Correctness (must pass)

- **No deadlock:** broker process remains schedulable; a watchdog client can still connect and complete one framed round-trip within the **responsiveness** window below.
- **No silent corruption:** per-topic offsets monotonic; fetched payloads match produced bytes for the same offset; framed **Error** responses match [tcp-wire-protocol.md](../reference/tcp-wire-protocol.md) semantics where applicable.
- **Regression gate (CI-aligned minimum):** `cargo test -p herbatka --test tcp_server_smoke` and `cargo test -p herbatka --test broker_persistence` pass unchanged after concurrency work (extend only if new tests are added deliberately).

### Responsiveness (v1 qualitative SLO)

- With **8** clients running the workload above, a **9th** short-lived framed client must complete **handshake + Produce + Fetch** on a **fresh topic** within **10 s** wall clock on a typical developer laptop. (CI environments may record stricter numbers in a dated entry under this document when step 2 runs.)

### Out of scope for this bar

- Multi-node, HA, leader election, quorum, split-brain — see [v1.md — Explicitly not in v1.0](v1.md).
- Protobuf-on-wire, QUIC — see [status.md — Later](status.md).
- Throughput **champions** or datacenter-scale load testing — not required for v1; tighten numbers only if you add a product SLO later.

## Startup Replay Benchmarks

### 2026-05-01 - Lightweight replay sanity (1 iteration)

- Scope: sanity check after extending sparse seek to non-tail `MustReplay` paths
- Command: `pwsh ./scripts/startup_replay_bench.ps1 -Iterations 1`
- Tests: `restart_replays_multiple_segments_in_order` vs `corrupt_or_missing_sparse_index_falls_back_safely`

Results (Windows, single run each; expect noise):

- metadata-skip-startup-path: ~`0.371s`
- fallback-decode-startup-path: ~`0.344s`

### 2026-04-27 - Sparse index startup A/B

- Scope: startup replay performance (baseline vs current sparse-index startup changes)
- Command: `pwsh ./scripts/startup_ab_speed.ps1 -Iterations 3`
- Integration test: `startup_large_dataset_restart_profile`
- Dataset parameters:
  - messages: `80,000`
  - payload size: `128B`
  - `segment_max_bytes`: `4096`
  - `fsync_policy`: `never`

Results:

- Baseline full-test average: `25.467s`
- Current full-test average: `25.761s`
- Full-test delta: `+0.294s` (`+1.15%`)
- Restart marker average (`restart_elapsed_ms`):
  - Baseline: `~215ms`
  - Current: `~203ms`
  - Restart delta: `-12ms` (`~5.6%`)

Notes:

- Full test runtime is dominated by dataset generation/writes.
- Restart marker is a better signal for startup replay improvements.

## Template For Future Entries

```text
### YYYY-MM-DD - short title
- Scope:
- Command:
- Integration test / script:
- Dataset parameters:
  - messages:
  - payload size:
  - segment_max_bytes:
  - fsync_policy:

Results:
- Baseline full-test average:
- Current full-test average:
- Full-test delta:
- Restart marker average:
  - Baseline:
  - Current:
  - Delta:

Notes:
- ...
```
