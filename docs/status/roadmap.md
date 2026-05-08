# Roadmap

Last updated: 2026-04-30

## Decision Backlog

### Message Timestamp Representation

- Current state: `Message.timestamp` is `SystemTime` in memory, while persistence encodes it as `u64` epoch milliseconds.
- Proposal: migrate `Message.timestamp` to `u64` (epoch millis) as the domain type.
- Why:
  - Align in-memory and on-disk representation.
  - Remove repetitive conversion logic (`SystemTime <-> u64`) in persistence paths.
  - Avoid ambiguous pre-epoch fallback behavior during encoding.
- Impact:
  - Update all message constructors that currently use `SystemTime::now()`.
  - Update tests that compare/use `SystemTime` timestamps.
  - Keep wire format unchanged (`u64` little-endian millis), so no data-format migration is required.
- Rollout plan:
  1. Introduce a helper for current epoch millis (for consistent call sites).
  2. Change `Message.timestamp` type to `u64`.
  3. Simplify persistence encode/decode to pass `u64` directly.
  4. Update tests and integration paths.
  5. Run full test suite and validate recovery compatibility on existing segment files.
- Status: planned, not implemented yet.

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
