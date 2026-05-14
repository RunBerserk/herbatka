# Benchmarks

This document tracks benchmark history for Herbatka.

**Cadence:** upcoming tickets and post-v1 work (persistence, TCP, concurrency, retention, etc.) can shift numbers—re-run the relevant harnesses here when those areas change or before a release so baselines are not lost.

## v1 TCP concurrency acceptance criteria

Canonical bar for **feature-complete v1.0** concurrent TCP use (single broker process). A **measured baseline** (pre–concurrent-accept work) is in [TCP concurrency baseline measurement](#tcp-concurrency-baseline-measurement-pre-step-3) below. Remaining implementation work is tracked in [status.md](status.md) **Next Up**. Revise the bar only by explicit decision (update this file and [status.md](status.md)).

### Topology (in scope)

- **One** `herbatka` process, one `listen_addr`, one `data_dir` — matches [v1.md](v1.md) single-node scope.
- Clients may be separate OS processes or threads; broker is the only server under test.

### Client count and lifetime (checkable)

- **Minimum `N = 8`** simultaneous **framed v1** TCP connections after `HERBATKA WIRE/1` handshake ([tcp-wire-protocol.md](../reference/tcp-wire-protocol.md)).
- Each connection keeps the socket open and performs the workload below for at least **60 s** (not connect-disconnect-only).
- **Two dimensions** for the full soak harness: (a) **overlapping framed connection lifetimes** (several clients doing produce/fetch at the same wall time — production binary: Tokio [`run`](../../crates/herbatka/src/tcp/server.rs) for accept + **`std::thread`** per client; tests: [`serve`](../../crates/herbatka/src/tcp/server.rs) with OS threads per `accept`); (b) **[`SharedBroker`](../../crates/herbatka/src/tcp/server.rs)** (`Arc<RwLock<Broker>>`) **contention** under concurrent produce/fetch (read lock for fetch/topic bounds; exclusive write lock for produce / topic creation — overlapping **writes** remain serialized).

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

- With **8** clients running the workload above, a **9th** short-lived framed client must complete **handshake + Produce + Fetch** on a **fresh topic** within **10 s** wall clock on a typical developer laptop. Baseline against that window: [TCP concurrency baseline measurement](#tcp-concurrency-baseline-measurement-pre-step-3) (watchdog row).

### Out of scope for this bar

- Multi-node, HA, leader election, quorum, split-brain — see [v1.md — Explicitly not in v1.0](v1.md).
- Protobuf-on-wire, QUIC — see [status.md — Later](status.md).
- Throughput **champions** or datacenter-scale load testing — not required for v1; tighten numbers only if you add a product SLO later.

## TCP concurrency baseline measurement (pre-step 3)

Dated **probe** runs using [`tcp_concurrency_probe`](../../crates/herbatka/src/bin/tcp_concurrency_probe.rs) and the baseline scripts. Rows may mix **before** and **after** transport changes (serial accept, per-connection OS threads on `serve`, **Tokio `run`** on the real binary); read each subsection’s **Scope**.

### 2026-05-12 — short / release (serial accept, historical)

**Scope (historical):** This row was captured when the broker still ran **`handle_client` inline in the accept loop** (no overlapping TCP sessions). Shared state is still [`Arc<Mutex<Broker>>`](../../crates/herbatka/src/main.rs). See the follow-up row for **per-connection threads** (`serve`).

**Harness:**

- Scripts (same flow): **Windows** — `powershell -NoProfile -ExecutionPolicy Bypass -File ./scripts/tcp_concurrency_baseline.ps1 -Short -Release`; **Unix / Git Bash** — `bash ./scripts/tcp_concurrency_baseline.sh --short --release` (requires `python3` for an ephemeral port; `nc` optional for readiness, otherwise bash `/dev/tcp`). Omit `-Short` / `--short` for **8 clients × 60 s** workload (long wall time on today’s server).
- Probe binary: `cargo run --release -p herbatka --bin tcp_concurrency_probe -- --addr HOST:PORT …` (see `--help` on the binary).

**Environment (first captured run):** Windows 10, **release** build, **short** profile (`-Short`: **4** clients, **3 s** workload each, framed v1, `fsync_policy = "never"` temp broker).

**Command (short / release):** `powershell -NoProfile -ExecutionPolicy Bypass -File ./scripts/tcp_concurrency_baseline.ps1 -Short -Release` **or** `bash ./scripts/tcp_concurrency_baseline.sh --short --release`

**Results (single run; expect noise):**

| Metric | Value |
|--------|-------|
| `probe_summary` `total_wall_s` | ~`15.94` |
| `probe_summary` `clients` / `duration_per_client_s` | `4` / `3.0` |
| `probe_watchdog_ok` `elapsed_ms` | ~`15_939` |
| Per-worker `total_worker_s` (printed one line per `client_id`) | ~`15.9`, `8.0`, `12.0`, `4.0` s (order depends which connection the OS schedules first) |

**Interpretation:** Total wall time is approximately **serial** service of each long-lived framed session (plus probe overhead) because **broker** work was still serialized on one mutex even though only one TCP handler ran at a time. Several workers show sub-millisecond `connect_ms` while `handshake_ms` is large: on this stack **`TcpStream::connect` can return before the application has `accept`ed**, so queue wait often appears under **`handshake_ms`** (time until `HERBATKA OK/1` and framed work begin), not under `connect_ms`. The **9th-client** watchdog starts after the first framed handshake completes elsewhere and then competes for `accept` behind the remaining workers; **~16 s** for handshake + one Produce + one Fetch is **far beyond** the **10 s** qualitative SLO in the acceptance criteria—expected for that historical build.

### 2026-05-12 — scripted short run after per-connection TCP threads (release)

**Scope:** Same harness and profile as the row above, after [`serve`](../../crates/herbatka/src/tcp/server.rs) spawns a **thread per accepted connection**; broker body still uses **`Arc<Mutex<Broker>>`**.

**Command:** `powershell -NoProfile -ExecutionPolicy Bypass -File ./scripts/tcp_concurrency_baseline.ps1 -Short -Release` (or the equivalent `.sh`).

**Results (single run; expect noise):**

| Metric | Value |
|--------|-------|
| `probe_summary` `total_wall_s` | ~`3.98` |
| `probe_summary` `clients` / `duration_per_client_s` | `4` / `3.0` |
| `probe_watchdog_ok` `elapsed_ms` | ~`16.5` |
| Per-worker `total_worker_s` | ~`4.0` s each |

**Interpretation:** With **concurrent TCP handlers**, the four workers overlap framed sessions; **`total_wall_s` drops to ~one workload window** (~4 s) instead of ~serial **N × duration**. The **9th-client** watchdog completes in **~17 ms** here (well under the **10 s** qualitative bar) because it no longer waits behind full per-client soak queues on `accept`. Broker **`Mutex`** still serializes produce/fetch; heavy lock contention is a separate follow-up ([status.md](status.md) **Next Up**).

### 2026-05-13 — short / release (Tokio accept + std thread per client, production binary)

**Scope:** Same harness and profile; temp broker is the **release `herbatka` binary** after [`run`](../../crates/herbatka/src/tcp/server.rs) uses **Tokio** for **`accept`** and **`std::thread`** per client for **`handle_client`** (after `into_std()`).

**Command:** `powershell -NoProfile -ExecutionPolicy Bypass -File ./scripts/tcp_concurrency_baseline.ps1 -Short -Release` (or the equivalent `.sh`).

**Results (single run; expect noise):**

| Metric | Value |
|--------|-------|
| `probe_summary` `total_wall_s` | ~`3.99` |
| `probe_summary` `clients` / `duration_per_client_s` | `4` / `3.0` |
| `probe_watchdog_ok` `elapsed_ms` | ~`16.6` |
| Per-worker `total_worker_s` | ~`4.0` s each |

**Interpretation:** In line with the **per-connection OS thread** row: Tokio here mainly **modernizes bind/accept**; **`handle_client`** and the broker **`Mutex`** are unchanged. **`set_nonblocking(false)`** after `into_std()` was required for correct framed reads under load on the capture host (Windows).

### 2026-05-13 — broker `RwLock` (`SharedBroker`)

**Scope:** Broker sharing changed from **`Arc<Mutex<Broker>>`** to **`Arc<RwLock<Broker>>`** ([`SharedBroker`](../../crates/herbatka/src/tcp/server.rs)): concurrent **Fetch** / **TopicBounds** can share a read lock; **Produce** / topic creation still take a write lock. TCP accept model unchanged from the row above (Tokio **`run`** + **`std::thread`** per client in production).

**Command:** `powershell -NoProfile -ExecutionPolicy Bypass -File ./scripts/tcp_concurrency_baseline.ps1 -Short -Release` (or the equivalent `.sh`).

#### 2026-05-14 — A/B on same host (short / release, single run each; expect noise)

**Method:** Run **A** with a clean working tree at **`deb98c0`** (`Arc<Mutex<Broker>>`); **`git stash`** local `SharedBroker` / `RwLock` edits, run the script, **`git stash pop`**; run **B** with the restored working tree (`SharedBroker` / `RwLock`). Same script, **Windows**, **4** clients, **3 s** workload per client, **`fsync_policy = "never"`** temp config from the script.

**A — `deb98c0` (Mutex broker), single run:**

| Metric | Value |
|--------|-------|
| `probe_summary` `total_wall_s` | `3.983` |
| `probe_summary` `clients` / `duration_per_client_s` | `4` / `3.0` |
| `probe_watchdog_ok` `elapsed_ms` | `7.9` |
| Per-worker `total_worker_s` (by `client_id`) | `4.0`, `4.0`, `4.0`, `4.0` s |

**B — same harness with uncommitted `SharedBroker` / `RwLock` tree, single run:**

| Metric | Value |
|--------|-------|
| `probe_summary` `total_wall_s` | `3.991` |
| `probe_summary` `clients` / `duration_per_client_s` | `4` / `3.0` |
| `probe_watchdog_ok` `elapsed_ms` | `15.8` |
| Per-worker `total_worker_s` (by `client_id`) | `4.0`, `4.0`, `4.0`, `4.0` s |

**Interpretation:** **`total_wall_s`** differs by **~8 ms** on a **~4 s** wall clock (negligible for one pair of runs). **Watchdog** stayed sub‑**20 ms** in both runs. The short probe is **produce‑heavy**; **`RwLock`** still **serializes writes**, so this harness is **not** expected to show a large gain over **`Mutex`**. For confidence, repeat **several** runs or use a **fetch‑heavy** scenario to exercise read‑lock overlap. Integration coverage: `tcp_framed_concurrent_fetch_same_topic` in [`tcp_server_smoke.rs`](../../crates/herbatka/tests/tcp_server_smoke.rs).

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
