#!/usr/bin/env bash
# Mirrors scripts/startup_replay_bench.ps1 — timed broker_persistence subsets (lightweight replay paths).
#
# Usage:
#   ./scripts/startup_replay_bench.sh [iterations]
#   bash scripts/startup_replay_bench.sh 5
#
# Sub-second timing expects GNU date (Linux, Git Bash on Windows typically works).

set -euo pipefail

iterations="${1:-3}"
if ! [[ "$iterations" =~ ^[0-9]+$ ]] || [[ "$iterations" -lt 1 ]]; then
  echo "error: iterations must be an integer >= 1 (got: ${1:-})" >&2
  exit 1
fi

now_s() { date +%s.%N; }

measure_test_case() {
  local label=$1 test_name=$2
  local -a durations=()
  local i start end elapsed avg min max out

  for ((i = 1; i <= iterations; i++)); do
    echo "[${label}] run ${i}/${iterations} ..." >&2
    start=$(now_s)
    cargo test --test broker_persistence "$test_name" -- --exact >&2
    end=$(now_s)
    elapsed=$(awk -v s="$start" -v e="$end" 'BEGIN { printf "%.9f\n", e - s }')
    durations+=("$elapsed")
  done

  out=$(printf '%s\n' "${durations[@]}" | awk '
    {
      sum += $1
      if (NR == 1 || $1 < min) min = $1
      if (NR == 1 || $1 > max) max = $1
    }
    END {
      if (NR == 0) exit 1
      printf "%.3f %.3f %.3f", sum / NR, min, max
    }
  ')
  read -r avg min max <<<"$out"
  printf '%s|%s|%s|%s|%s|\n' "$label" "$avg" "$min" "$max" "$iterations"
}

echo "Startup replay benchmark (lightweight)"
echo "Repo root: $PWD"
echo "Iterations: ${iterations}"
echo ""

row1=$(measure_test_case "metadata-skip-startup-path" "restart_replays_multiple_segments_in_order")
row2=$(measure_test_case "fallback-decode-startup-path" "corrupt_or_missing_sparse_index_falls_back_safely")

echo ""
echo "Summary:"
echo "scenario                                   average_s    min_s    max_s runs"
echo "----------------------------------------------------------------------------"
while IFS='|' read -r scenario avg min max runs _; do
  [[ -z "$scenario" ]] && continue
  printf '%-42s %8s %8s %8s %5s\n' "$scenario" "$avg" "$min" "$max" "$runs"
done <<EOF
$row1
$row2
EOF

echo ""
echo "Usage notes:"
echo "  ./scripts/startup_replay_bench.sh"
echo "  ./scripts/startup_replay_bench.sh 5"
