# Benchmarks

This document tracks benchmark history for Herbatka.

## Startup Replay Benchmarks

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
