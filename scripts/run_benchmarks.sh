#!/usr/bin/env bash
# Step 32: run every Step 31 benchmark harness and save raw JSON results.
#
# Usage: ./scripts/run_benchmarks.sh [build-dir]
#   build-dir defaults to build/linux-bench.
#
# Raw output goes to results/raw/<timestamp>/ (gitignored). Each run records
# the source commit, build type, compiler, CPU, and workload seeds in a
# metadata.json alongside the benchmark JSON, so reports are reproducible.

set -euo pipefail

BUILD_DIR="${1:-build/linux-bench}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="$REPO_ROOT/results/raw/$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$OUT_DIR"

BINARIES=(
  choreoos-bench-apply
  choreoos-bench-codec
  choreoos-bench-wal
  choreoos-bench-replay
  choreoos-bench-snapshot
  choreoos-bench-cluster
  choreoos-bench-recovery
)

{
  echo "{"
  echo "  \"commit\": \"$(git -C "$REPO_ROOT" rev-parse HEAD)\","
  echo "  \"commit_short\": \"$(git -C "$REPO_ROOT" rev-parse --short HEAD)\","
  echo "  \"build_type\": \"$(grep CMAKE_BUILD_TYPE:STRING "$BUILD_DIR/CMakeCache.txt" | cut -d= -f2)\","
  echo "  \"compiler\": \"$(grep CMAKE_CXX_COMPILER:FILEPATH "$BUILD_DIR/CMakeCache.txt" | cut -d= -f2)\","
  echo "  \"cxx_flags\": \"$(grep CMAKE_CXX_FLAGS_RELEASE:STRING "$BUILD_DIR/CMakeCache.txt" | cut -d= -f2)\","
  echo "  \"cpu\": \"$(lscpu | grep 'Model name' | cut -d: -f2 | xargs)\","
  echo "  \"memory\": \"$(free -h | awk '/^Mem:/{print $2}')\","
  echo "  \"os\": \"$(uname -srm)\","
  echo "  \"date_utc\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\""
  echo "}"
} > "$OUT_DIR/metadata.json"

for bin in "${BINARIES[@]}"; do
  path="$BUILD_DIR/cpp-runtime/benchmarks/$bin"
  if [[ ! -x "$path" ]]; then
    echo "SKIP $bin (not built)" >&2
    continue
  fi
  echo "RUN $bin" >&2
  "$path" --benchmark_format=json > "$OUT_DIR/$bin.json"
done

echo "Results in $OUT_DIR" >&2
