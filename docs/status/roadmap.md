# Roadmap

Last updated: 2026-05-14

## Decision Backlog

### Message Timestamp Representation

- **Before refactor:** `Message.timestamp` was `SystemTime` in memory; persistence still stored **`u64` epoch milliseconds** on disk (encode converted, decode rebuilt `SystemTime`).
- **After refactor (2026-05-12):** `Message.timestamp` is **`u64` epoch milliseconds** everywhere in the broker domain; [`now_epoch_millis`](../../crates/herbatka/src/time.rs) is the only `SystemTime::now()` call site for new messages; [`persistence.rs`](../../crates/herbatka/src/log/persistence.rs) writes/reads the eight LE bytes directly. **Segment on-disk format unchanged.**
- Why (primary driver is **architecture and consistency**, not hot-path performance):
  - **`SystemTime`** is the right type at the **OS boundary** (reading the clock); the durable log already stores **wall time as `u64` ms**. Keeping `SystemTime` on `Message` forced **encode to convert to `u64` anyway** and **decode to rebuild `SystemTime`**, so the split representation added churn without a strong benefit for the broker core.
  - **`u64` epoch ms** matches **on-disk segments** and keeps a **single canonical** time shape in produce / fetch / persistence paths (same as today’s **segment record** encoding—no segment format change).
  - **Explicit policy** for edge cases (e.g. pre-epoch) at construction time instead of folding behavior only inside persistence encode (`duration_since` / fallbacks).
  - Performance impact of the type change alone is expected to be **minor**; the refactor is justified by **clearer layering** and **less redundant conversion**, not by benchmarking gains.
- Rollout (completed 2026-05-12):
  1. Introduce a helper for current epoch millis (for consistent call sites).
  2. Change `Message.timestamp` type to `u64`.
  3. Simplify persistence encode/decode to pass `u64` directly.
  4. Update tests and integration paths.
  5. Run full test suite and validate recovery compatibility on existing segment files.
- Status: **completed** (2026-05-12).

### Sparse-Index Startup Skip For Closed Segments

- Status: completed.
- Acceptance evidence:
  - Closed segments are skipped only when checkpoint metadata and sparse index compatibility checks pass.
  - Tail segment replay path is unchanged and still performs corrupted-tail truncation recovery.
  - Incompatible/missing metadata falls back safely to replay.
- Verification:
  - `cargo test --test broker_persistence`
  - `cargo test --lib`
- Telemetry contract:
  - Startup fallback reasons tracked and emitted with stable keys:
    - `tail_segment`
    - `missing_checkpoint`
    - `missing_or_invalid_index`
    - `index_incompatible`

## Final Steps (v1 Closure)

### 1) Split Deliverables: Broker vs UI/Simulator

- Goal: keep `herbatka` as a focused broker project.
- Plan after v1:
  - Separate UI client from broker runtime.
  - Separate simulator from broker runtime.
- Done criteria:
  - Broker builds and runs independently.
  - UI consumes broker only via public client/protocol boundary.
  - Simulator consumes broker only via public boundary.
  - Documentation reflects the new project boundaries.

### 2) Declare Feature-Complete v1

- Goal: prevent endless scope growth in v1.
- v1 feature-complete means:
  - Core broker produce/fetch/persistence/recovery are stable.
  - Basic operability and tests are in place.
  - Known critical risks are either resolved or explicitly accepted.
- After v1 complete:
  - No new v1 features (bug fixes, reliability, docs, and polish only).
  - New capabilities are tracked under `v2 backlog`.
- **Declared v1.0.0 on 2026-05-14** (annotated tag `v1.0.0`).
