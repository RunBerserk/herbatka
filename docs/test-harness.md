# Simulator Test Harness (Local, Minimal)

This harness gives a deterministic, repeatable local check for simulator -> broker flow.

## Deterministic Smoke Flow

1. Start broker in terminal A:
   - `cargo run --bin herbatka`
2. Run simulator in terminal B:
   - `cargo run --bin simulator -- --addr 127.0.0.1:7000 --topic events --vehicles 5 --rate 10 --duration-secs 5 --scenario steady --load-profile constant --seed 42 --quiet`

Pass condition:

- Command exits with code `0`.
- Final line contains `simulation done: ok=<n>` with `ok > 0`.

Fail condition:

- Non-zero exit code.
- `ok=0`.

## Minimal Repeatable Verification Commands

- Simulator unit coverage:
  - `cargo test --bin simulator`
- TCP protocol path:
  - `cargo test --test tcp_server_smoke`
- Additional backend confidence:
  - `cargo test --test consumer_flow --test persistence_flow --test broker_persistence`

## Notes

- Keep `--seed` fixed for reproducible payload jitter.
- Start with moderate load (`rate=10`) before trying heavier scenarios.
