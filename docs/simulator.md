# Simulator Guide

The simulator sends deterministic JSON telemetry events to the broker over TCP using the `PRODUCE` command.

## Quickstart

1. Start broker:
   - `cargo run --bin herbatka`
2. Run simulator:
   - `cargo run --bin simulator -- --addr 127.0.0.1:7000 --topic events --vehicles 5 --rate 10 --duration-secs 5 --scenario burst --load-profile ramp --seed 42`

## Flags

- `--addr <host:port>`: broker TCP address.
- `--topic <name>`: target topic for `PRODUCE`.
- `--vehicles <n>`: number of vehicle identities used in payloads.
- `--rate <events_per_sec>`: base event rate before scenario/profile modulation.
- `--duration-secs <n>`: run duration.
- `--scenario <steady|burst|idle|reconnect>`: behavior mode.
- `--load-profile <constant|ramp|spike>`: traffic-intensity pattern over time.
- `--seed <u64>`: deterministic variation seed for payload jitter.
- `--quiet`: suppress periodic progress lines and print only final summary.

## Summary Counters

Final output format:

- `simulation done: ok=<n>, err=<n>, total=<n> | connect_err=<n>, write_err=<n>, read_err=<n>, non_ok=<n>, reconnect_attempts=<n>, reconnect_ok=<n>, skipped=<n>`

Counter meaning:

- `ok`: accepted `PRODUCE` operations.
- `err`: failed operations (includes any failure type).
- `total`: `ok + err`.
- `connect_err`: connection attempts that failed.
- `write_err`: failures while writing/flushing requests.
- `read_err`: failures while reading broker response.
- `non_ok`: broker returned non-`OK` response.
- `reconnect_attempts`: reconnect triggers attempted by reconnect scenario.
- `reconnect_ok`: reconnect attempts that succeeded.
- `skipped`: events intentionally not sent (for example, `idle` windows).

## Common Failure Signals

- `ok=0` and non-zero exit:
  - broker not reachable or all requests failed.
  - first check: broker is running at `--addr`.
- high `connect_err`:
  - unstable address/network or broker startup timing.
  - first check: run broker first, then simulator.
- high `non_ok`:
  - broker protocol/topic-side rejection.
  - first check: inspect broker logs and validate request path with producer.
- high `read_err`/`write_err`:
  - connection interruptions or server-side close under load.
  - first check: re-run with lower `--rate` and compare.
