#!/usr/bin/env bash
# Mirrors scripts/startup_ab_speed.ps1 — timed large-dataset startup profile test.
#
# Usage:
#   ./scripts/startup_ab_speed.sh [iterations]
#   bash scripts/startup_ab_speed.sh 5
#
# Sub-second timing expects GNU date (Linux, Git Bash on Windows typically works).

set -euo pipefail

iterations="${1:-3}"
if ! [[ "$iterations" =~ ^[0-9]+$ ]] || [[ "$iterations" -lt 1 ]]; then
  echo "error: iterations must be an integer >= 1 (got: ${1:-})" >&2
  exit 1
fi

TEST_NAME="startup_large_dataset_restart_profile"

echo "Startup A/B benchmark target: ${TEST_NAME}"
echo "Repo root: $PWD"
echo "Iterations: ${iterations}"
echo ""

now_s() { date +%s.%N; }

durations=()
for ((i = 1; i <= iterations; i++)); do
  echo "[run ${i}/${iterations}] ..." >&2
  start=$(now_s)
  cargo test --test broker_persistence "$TEST_NAME" -- --exact --nocapture >&2
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

echo ""
echo "Summary:"
echo "scenario                                   average_s    min_s    max_s runs"
echo "----------------------------------------------------------------------------"
printf '%-42s %8s %8s %8s %5s\n' "$TEST_NAME" "$avg" "$min" "$max" "$iterations"

echo ""
echo "Usage:"
echo "  ./scripts/startup_ab_speed.sh"
echo "  ./scripts/startup_ab_speed.sh 5"
