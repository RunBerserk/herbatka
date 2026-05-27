# Benchmarks

This document tracks benchmark history for Herbatka.

**Cadence:** upcoming tickets and post-v1 work (persistence, TCP, concurrency, retention, etc.) can shift numbers—re-run the relevant harnesses here when those areas change or before a release so baselines are not lost.

## v1 TCP concurrency acceptance criteria

Canonical bar for **feature-complete v1.0** concurrent TCP use (single broker process). A **measured baseline** (pre–concurrent-accept work) is in [TCP concurrency baseline measurement](#tcp-concurrency-baseline-measurement-pre-step-3) below; **full 8×60 sign-off (2026-05-18)** is in [full v1 acceptance (8×60)](#2026-05-18--full-v1-acceptance-860-default-sharedbroker). Optional throughput work is tracked in [status.md](status.md) **Next Up**. Revise the bar only by explicit decision (update this file and [status.md](status.md)).

### Topology (in scope)

- **One** `herbatka` process, one `listen_addr`, one `data_dir` — matches [v1.md](v1.md) single-node scope.
- Clients may be separate OS processes or threads; broker is the only server under test.

### Client count and lifetime (checkable)

- **Minimum `N = 8`** simultaneous **framed v1** TCP connections after `HERBATKA WIRE/1` handshake ([tcp-wire-protocol.md](../reference/tcp-wire-protocol.md)).
- Each connection keeps the socket open and performs the workload below for at least **60 s** (not connect-disconnect-only).
- **Two dimensions** for the full soak harness: (a) **overlapping framed connection lifetimes** (several clients doing produce/fetch at the same wall time — production binary: Tokio [`run`](../../crates/herbatka/src/tcp/server.rs) for accept + **`std::thread`** per client; tests: [`serve`](../../crates/herbatka/src/tcp/server.rs) with OS threads per `accept`); (b) **[`SharedBroker`](../../crates/herbatka/src/tcp/server.rs)** (`Arc<Broker>`) **contention** — per-topic `RwLock`s (since 2026-05-26): overlapping **produce** on **different** topics can proceed in parallel; **same-topic** produce still serializes. The default v1 workload uses **one topic per client**, so this change targets that pattern.

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

## tcp_concurrency_probe workload profiles

The [`tcp_concurrency_probe`](../../crates/herbatka/src/bin/tcp_concurrency_probe.rs) binary supports **`--profile NAME`** (default: **`default`**). Baseline scripts [`tcp_concurrency_baseline.ps1`](../../scripts/tcp_concurrency_baseline.ps1) / [`tcp_concurrency_baseline.sh`](../../scripts/tcp_concurrency_baseline.sh) pass **`--profile`** only when you use **`-FetchHeavy` / `--fetch-heavy`** or **`-MaxPressure` / `--max-pressure`** (mutually exclusive).

| Profile | Behavior | When to use |
|---------|----------|----------------|
| **`default`** | ~66% Produce / Fetch with fixed sleeps; same shape as historical rows below. | **v1 acceptance criteria** workload and apples-to-apples baselines unless you explicitly change the bar. |
| **`fetch-heavy`** | Per worker: **512** burst `Produce` on topic `v1cc-{id}` (no sleeps), then for the timed window **64** tail `Fetch` + **1** `Produce` per batch, **`1 ms`** sleep after each batch. | Stress **`SharedBroker`** **read** paths and overlapping fetches; milder than max-pressure. |
| **`max-pressure`** | Same seed and **64** fetch + **1** produce batch as `fetch-heavy`, but **no** intentional sleeps in the timed loop. | Heavier load; can **saturate CPU** on a laptop—use short durations by default. |

**Output:** `probe_summary` includes `profile=…`. For **`fetch-heavy`** / **`max-pressure`**, each **`probe_worker`** line adds `produce_ok` / `fetch_msg` / `fetch_none`, and a **`probe_ops`** line aggregates counts and **`ops_per_s_wall`** (total ops / probe wall time).

**Note:** The **v1 TCP concurrency acceptance criteria** section above still describes the **`default`** profile unless you revise it by decision.

### 2026-05-14 — read-skew profiles measured (short / release, Windows)

**Scope:** **Release** `herbatka` binary with **[`SharedBroker`](../../crates/herbatka/src/tcp/server.rs)**; [`tcp_concurrency_baseline.ps1`](../../scripts/tcp_concurrency_baseline.ps1) **`-Short -Release`** plus **`-FetchHeavy`** or **`-MaxPressure`** (temp `data_dir`, **`fsync_policy = "never"`**, **4** clients, **3 s** timed workload after per-worker **512** seed produces). **Single run each; expect noise.** Row **re-captured** the same day after a quieter machine window (repeat when comparing A/B).

#### `fetch-heavy`

**Command:** `powershell -NoProfile -ExecutionPolicy Bypass -File ./scripts/tcp_concurrency_baseline.ps1 -Short -Release -FetchHeavy`

| Metric | Value |
|--------|-------|
| `probe_summary` `total_wall_s` | `3.555` |
| `probe_summary` `profile` | `fetch-heavy` |
| `probe_summary` `clients` / `duration_per_client_s` | `4` / `3.0` |
| `probe_watchdog_ok` `elapsed_ms` | `17.0` |
| `probe_ops` `produce_ok` | `4027` |
| `probe_ops` `fetch_msg` | `126656` |
| `probe_ops` `fetch_none` | `0` |
| `probe_ops` `total_ops` | `130683` |
| `probe_ops` `ops_per_s_wall` | `36764.7` |

Per-worker (`produce_ok` / `fetch_msg`): `0` → `979` / `29888`; `1` → `1058` / `34944`; `2` → `1008` / `31744`; `3` → `982` / `30080`.

#### `max-pressure`

**Command:** `powershell -NoProfile -ExecutionPolicy Bypass -File ./scripts/tcp_concurrency_baseline.ps1 -Short -Release -MaxPressure`

| Metric | Value |
|--------|-------|
| `probe_summary` `total_wall_s` | `3.811` |
| `probe_summary` `profile` | `max-pressure` |
| `probe_summary` `clients` / `duration_per_client_s` | `4` / `3.0` |
| `probe_watchdog_ok` `elapsed_ms` | `11.0` |
| `probe_ops` `produce_ok` | `4875` |
| `probe_ops` `fetch_msg` | `180928` |
| `probe_ops` `fetch_none` | `0` |
| `probe_ops` `total_ops` | `185803` |
| `probe_ops` `ops_per_s_wall` | `48749.8` |

Per-worker (`produce_ok` / `fetch_msg`): `0` → `1151` / `40896`; `1` → `1364` / `54528`; `2` → `1214` / `44928`; `3` → `1146` / `40576`.

**Interpretation:** **`max-pressure`** drops batch pacing, so **`ops_per_s_wall`** is higher than **`fetch-heavy`** on this pair of runs; **`total_wall_s`** can be slightly longer when clients spend more wall time in the timed loop. Watchdog stays **sub‑20 ms**. OS background load materially moves absolute **`ops_per_s_wall`** between captures—median over several runs is better for comparisons. Re-run after broker or probe changes; Unix numbers may differ.

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

### 2026-05-18 — full v1 acceptance (8×60, default, SharedBroker)

**Scope:** **Release** `herbatka` binary with Tokio [`run`](../../crates/herbatka/src/tcp/server.rs) for accept + **`std::thread`** per client, **[`SharedBroker`](../../crates/herbatka/src/tcp/server.rs)** (`Arc<RwLock<Broker>>`). Matches [v1 TCP concurrency acceptance criteria](#v1-tcp-concurrency-acceptance-criteria): **8** framed clients, **60 s** workload each, **`default`** profile, **9th-client** watchdog. Temp `data_dir`, **`fsync_policy = "never"`** from the baseline script. **Single run; expect noise.**

**Command (Windows):** `powershell -NoProfile -ExecutionPolicy Bypass -File ./scripts/tcp_concurrency_baseline.ps1 -Release`

**Command (Unix):** `bash ./scripts/tcp_concurrency_baseline.sh --release`

**Pre-flight (same day):** `cargo test -p herbatka --test tcp_server_smoke --test broker_persistence` (and optional `recovery_restart_tcp`, `domain_scenarios`) — all passed.

| Criterion | Result |
|-----------|--------|
| `clients` / `duration_per_client_s` | **8** / **60.0** |
| `profile` | **default** |
| `probe_summary` `total_wall_s` | **60.544** |
| `probe_watchdog_ok` `elapsed_ms` | **15.2** (qualitative SLO **&lt; 10 s**: **pass**) |
| Probe exit code | **0** |
| Per-worker `workload_s` / `total_worker_s` | **~60.5** s each (ids 0–7) |
| Per-worker `handshake_ms` | **0.2–0.4** ms (connect **~0.9–1.0** ms) |

**Interpretation:** **`total_wall_s`** tracks one **60 s** overlapping workload window (not serial **N × duration**), consistent with concurrent TCP handlers after the 2026-05-12 transport fix. **Watchdog** at **15.2 ms** is well under the **10 s** bar. At this capture the broker still used a **global** write lock for produce; optional throughput follow-ups were tracked in [status.md](status.md) **Next Up** until per-topic locking landed (2026-05-26).

### 2026-05-26 — full v1 re-check after per-topic locking (8×60, default)

**Scope:** **Release** `herbatka` with Tokio [`run`](../../crates/herbatka/src/tcp/server.rs) + **`std::thread`** per client, **[`SharedBroker`](../../crates/herbatka/src/tcp/server.rs)** (`Arc<Broker>` with per-topic locks). Same bar as [full v1 acceptance (8×60)](#2026-05-18--full-v1-acceptance-860-default-sharedbroker): **8** clients, **60 s** each, **`default`** profile, watchdog. Temp `data_dir`, **`fsync_policy = "never"`**. **Single run; expect noise.**

**Command (Windows):** `powershell -NoProfile -ExecutionPolicy Bypass -File ./scripts/tcp_concurrency_baseline.ps1 -Release`

**Pre-flight:** `cargo test -p herbatka` — all passed (including `tcp_framed_concurrent_produce_different_topics`).

| Criterion | Result |
|-----------|--------|
| `clients` / `duration_per_client_s` | **8** / **60.0** |
| `profile` | **default** |
| `probe_summary` `total_wall_s` | **60.554** |
| `probe_watchdog_ok` `elapsed_ms` | **3.9** (SLO **&lt; 10 s**: **pass**) |
| Probe exit code | **0** |
| Per-worker `workload_s` | **~60.5–60.6** s each (ids 0–7) |

**Interpretation:** No regression vs the 2026-05-18 sign-off; watchdog remained sub‑**20 ms**. Cross-topic produce parallelism is not strongly visible on the **default** profile (per-client topics, produce-heavy); shared-topic or fetch-heavy probes are optional for hotter lock stress — see [status.md — Later](status.md).

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
