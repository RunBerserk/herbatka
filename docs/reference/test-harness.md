# Simulator Test Harness (Local, Minimal)

This harness gives a deterministic, repeatable local check for simulator -> broker flow.

## Deterministic Smoke Flow

1. Start broker in terminal A:
   - `cargo run -p herbatka --bin herbatka`
2. Run simulator in terminal B:
   - `cargo run -p herbatka-simulator --bin simulator -- --addr 127.0.0.1:7000 --topic events --vehicles 5 --rate 10 --duration-secs 5 --scenario steady --load-profile constant --seed 42 --quiet`

Pass condition:

- Command exits with code `0`.
- Final line contains `simulation done: ok=<n>` with `ok > 0`.

Fail condition:

- Non-zero exit code.
- `ok=0`.

## Minimal Repeatable Verification Commands

- Simulator unit coverage:
  - `cargo test -p herbatka-simulator`
- TCP protocol path:
  - `cargo test -p herbatka --test tcp_server_smoke`
- Additional backend confidence:
  - `cargo test -p herbatka --test consumer_flow --test persistence_flow --test broker_persistence`
- Domain hardening (framed TCP, JSON payloads on dedicated topics):
  - `cargo test -p herbatka --test domain_scenarios`
  - Stock: `demo.market.quotes`; logistics: `demo.logistics.shipments` ([domain_scenarios.rs](../../crates/herbatka/tests/domain_scenarios.rs))
  - Car fleet: in-process protobuf — `cargo test -p herbatka --test fleet_protobuf_roundtrip`
- Recovery over TCP (framed produce/fetch after broker restart on same `data_dir`):
  - `cargo test -p herbatka --test recovery_restart_tcp`
  - Full startup matrix (tail truncate, checkpoint/index fallback): `cargo test -p herbatka --test broker_persistence`
- TCP concurrency v1 sign-off (manual, **8 clients × 60 s**, default profile): `powershell -NoProfile -ExecutionPolicy Bypass -File ./scripts/tcp_concurrency_baseline.ps1 -Release` (or `bash ./scripts/tcp_concurrency_baseline.sh --release`) — see [benchmarks.md — full v1 acceptance](../status/benchmarks.md#2026-05-18--full-v1-acceptance-860-default-sharedbroker)

## Notes

- Keep `--seed` fixed for reproducible payload jitter.
- Start with moderate load (`rate=10`) before trying heavier scenarios.
