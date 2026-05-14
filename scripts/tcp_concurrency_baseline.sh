#!/usr/bin/env bash
# Mirror of tcp_concurrency_baseline.ps1: temp broker config, run tcp_concurrency_probe, teardown.
# Requires: bash 4+, cargo, python3 (for ephemeral port). Optional: nc for readiness (else /dev/tcp).
set -euo pipefail

RELEASE=""
SHORT=""
FETCH_HEAVY=""
MAX_PRESSURE=""
for arg in "$@"; do
  case "$arg" in
    --release | -release) RELEASE=1 ;;
    --short | -short) SHORT=1 ;;
    --fetch-heavy | -fetch-heavy) FETCH_HEAVY=1 ;;
    --max-pressure | -max-pressure) MAX_PRESSURE=1 ;;
    -h | --help)
      echo "Usage: $0 [--release] [--short] [--fetch-heavy|--max-pressure]"
      echo "  --release      build and run broker + probe in release mode"
      echo "  --short        4 clients x 3s workload (default: 8 x 60s)"
      echo "  --fetch-heavy  tcp_concurrency_probe --profile fetch-heavy"
      echo "  --max-pressure tcp_concurrency_probe --profile max-pressure (CPU-heavy)"
      exit 0
      ;;
  esac
done

if [[ -n "$FETCH_HEAVY" && -n "$MAX_PRESSURE" ]]; then
  echo "error: use only one of --fetch-heavy or --max-pressure" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

if ! command -v python3 >/dev/null 2>&1; then
  echo "error: python3 is required to pick a free TCP port" >&2
  exit 1
fi

PORT="$(python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()")"
TEMP_BASE="$(mktemp -d "${TMPDIR:-/tmp}/herbatka_tcp_bench_XXXXXX")"
DATA_DIR="$TEMP_BASE/data"
mkdir -p "$DATA_DIR"
DATA_DIR_ABS="$(cd "$DATA_DIR" && pwd)"
CONFIG_PATH="$TEMP_BASE/herbatka.toml"

cat >"$CONFIG_PATH" <<EOF
data_dir = "${DATA_DIR_ABS//\\//}"
segment_max_bytes = 65536
fsync_policy = "never"
listen_addr = "127.0.0.1:$PORT"
EOF

BROKER_PID=""
cleanup() {
  if [[ -n "${BROKER_PID}" ]] && kill -0 "$BROKER_PID" 2>/dev/null; then
    echo "Stopping broker (pid $BROKER_PID) ..."
    kill -TERM "$BROKER_PID" 2>/dev/null || true
    for _ in $(seq 1 50); do
      kill -0 "$BROKER_PID" 2>/dev/null || break
      sleep 0.1
    done
    kill -KILL "$BROKER_PID" 2>/dev/null || true
    wait "$BROKER_PID" 2>/dev/null || true
  fi
  unset HERBATKA_CONFIG || true
  rm -rf "$TEMP_BASE" 2>/dev/null || true
}
trap cleanup EXIT INT TERM HUP

BUILD_ARGS=(build -p herbatka)
if [[ -n "$RELEASE" ]]; then
  BUILD_ARGS+=(--release)
fi
echo "Building herbatka (${BUILD_ARGS[*]}) ..."
cargo "${BUILD_ARGS[@]}"

PROFILE_DIR="debug"
[[ -n "$RELEASE" ]] && PROFILE_DIR="release"
BROKER_EXE="$REPO_ROOT/target/$PROFILE_DIR/herbatka"
if [[ ! -x "$BROKER_EXE" ]]; then
  echo "error: broker binary not found or not executable: $BROKER_EXE" >&2
  exit 1
fi

export HERBATKA_CONFIG="$CONFIG_PATH"
echo "Starting broker: $BROKER_EXE (HERBATKA_CONFIG=$CONFIG_PATH)"
"$BROKER_EXE" &
BROKER_PID=$!

ready=0
for _ in $(seq 1 150); do
  if command -v nc >/dev/null 2>&1; then
    if nc -z 127.0.0.1 "$PORT" 2>/dev/null; then
      ready=1
      break
    fi
  else
    if (echo >/dev/tcp/127.0.0.1/"$PORT") >/dev/null 2>&1; then
      ready=1
      break
    fi
  fi
  sleep 0.1
done

if [[ "$ready" -ne 1 ]]; then
  echo "error: broker did not accept TCP on 127.0.0.1:$PORT within timeout" >&2
  exit 1
fi

PROBE_ADDR="127.0.0.1:$PORT"
PROBE_ARGS=(run -p herbatka --bin tcp_concurrency_probe --)
if [[ -n "$RELEASE" ]]; then
  PROBE_ARGS=(run --release -p herbatka --bin tcp_concurrency_probe --)
fi
if [[ -n "$SHORT" ]]; then
  PROBE_ARGS+=(--addr "$PROBE_ADDR" --duration-secs 3 --clients 4)
else
  PROBE_ARGS+=(--addr "$PROBE_ADDR" --duration-secs 60 --clients 8)
fi
if [[ -n "$FETCH_HEAVY" ]]; then
  PROBE_ARGS+=(--profile fetch-heavy)
elif [[ -n "$MAX_PRESSURE" ]]; then
  PROBE_ARGS+=(--profile max-pressure)
fi

echo "Running probe: cargo ${PROBE_ARGS[*]}"
cargo "${PROBE_ARGS[@]}"

echo "Done."
echo "Usage: $0 [--release] [--short] [--fetch-heavy|--max-pressure]"
