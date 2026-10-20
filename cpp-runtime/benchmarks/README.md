# C++ benchmarks

Seven Google Benchmark executables already exist: `choreoos-bench-apply`,
`-codec`, `-wal`, `-replay`, `-snapshot`, `-cluster` and `-recovery`.
Build in Release with `CHOREOOS_BUILD_BENCHMARKS=ON`; use
[`scripts/run_benchmarks.sh`](../../scripts/run_benchmarks.sh) for collection
and [`aggregate.py`](../../python-tools/analysis/aggregate.py) for summaries.

Read each suite before interpreting its numbers. Cluster benchmarks use the
**legacy Replica with SimulatedNetwork**, not TCP or the transition backend.
Logical ticks are not network milliseconds. WAL includes sync and buffered
modes; buffered results are not durable-write throughput. No current baseline,
profile or optimization decision has been reproduced by this documentation
change. T1-D tracks those acceptance gates in
[the ledger](../../docs/AGENT_STATE.md).
