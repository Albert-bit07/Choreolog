#!/usr/bin/env python3
"""Step 32: aggregate benchmark JSON into trustworthy summary reports.

Reads results/raw/<timestamp>/*.json (Google Benchmark format) plus
metadata.json, computes p50/p95/p99 across repetitions for each benchmark,
and writes:
  - summary.csv  : one row per benchmark with percentiles
  - summary.md   : human-readable report with environment metadata

Usage: python3 aggregate.py <results-dir> [output-dir]
"""

import csv
import json
import sys
from pathlib import Path


def percentile(sorted_vals, pct):
    """Nearest-rank percentile."""
    if not sorted_vals:
        return float("nan")
    k = max(0, min(len(sorted_vals) - 1, int(round(pct / 100 * len(sorted_vals))) - 1))
    return sorted_vals[k]


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <results-dir> [output-dir]", file=sys.stderr)
        sys.exit(1)
    results_dir = Path(sys.argv[1])
    out_dir = Path(sys.argv[2]) if len(sys.argv) > 2 else results_dir / "report"
    out_dir.mkdir(parents=True, exist_ok=True)

    metadata = {}
    meta_path = results_dir / "metadata.json"
    if meta_path.exists():
        metadata = json.loads(meta_path.read_text())

    # Group measurements by benchmark name across files/repetitions.
    by_name = {}
    for path in sorted(results_dir.glob("choreoos-bench-*.json")):
        data = json.loads(path.read_text())
        for b in data.get("benchmarks", []):
            name = b["name"]
            # Prefer items_per_second when the harness reports it; fall back
            # to real_time converted to a per-item rate via items_per_second.
            entry = {
                "real_time_ns": b["real_time"],
                "cpu_time_ns": b["cpu_time"],
                "iterations": b["iterations"],
                "items_per_second": b.get("items_per_second"),
            }
            by_name.setdefault(name, []).append(entry)

    rows = []
    for name, entries in sorted(by_name.items()):
        times = sorted(e["real_time_ns"] for e in entries)
        ips = [e["items_per_second"] for e in entries if e["items_per_second"]]
        ips_sorted = sorted(ips)
        rows.append(
            {
                "benchmark": name,
                "samples": len(entries),
                "p50_ns": percentile(times, 50),
                "p95_ns": percentile(times, 95),
                "p99_ns": percentile(times, 99),
                "p50_items_per_s": percentile(ips_sorted, 50) if ips_sorted else "",
                "p95_items_per_s": percentile(ips_sorted, 95) if ips_sorted else "",
            }
        )

    # CSV
    csv_path = out_dir / "summary.csv"
    with csv_path.open("w", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "benchmark",
                "samples",
                "p50_ns",
                "p95_ns",
                "p99_ns",
                "p50_items_per_s",
                "p95_items_per_s",
            ],
        )
        w.writeheader()
        w.writerows(rows)

    # Markdown report
    md_path = out_dir / "summary.md"
    with md_path.open("w") as f:
        f.write("# ChoreoOS Benchmark Summary\n\n")
        f.write(f"Generated from `{results_dir.name}`\n\n")
        if metadata:
            f.write("## Environment\n\n")
            for k in ("commit_short", "build_type", "compiler", "cpu", "os", "date_utc"):
                if k in metadata:
                    f.write(f"- {k}: {metadata[k]}\n")
            if "note" in metadata:
                f.write(f"- note: {metadata['note']}\n")
            f.write("\n")
        f.write("## Results (p50/p95/p99 across repetitions)\n\n")
        f.write("| Benchmark | Samples | p50 | p95 | p99 | p50 items/s |\n")
        f.write("|---|---|---|---|---|---|\n")
        for r in rows:
            def fmt_ns(v):
                if v >= 1e9:
                    return f"{v/1e9:.2f}s"
                if v >= 1e6:
                    return f"{v/1e6:.1f}ms"
                if v >= 1e3:
                    return f"{v/1e3:.1f}us"
                return f"{v:.0f}ns"
            ips = r["p50_items_per_s"]
            ips_s = f"{ips:,.0f}" if ips != "" else "n/a"
            f.write(
                f"| {r['benchmark']} | {r['samples']} | {fmt_ns(r['p50_ns'])} | "
                f"{fmt_ns(r['p95_ns'])} | {fmt_ns(r['p99_ns'])} | {ips_s} |\n"
            )
    print(f"Wrote {csv_path} and {md_path} ({len(rows)} benchmarks)")


if __name__ == "__main__":
    main()
