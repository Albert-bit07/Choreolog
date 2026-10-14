#!/usr/bin/env python3
"""Isolated reproducible real-TCP three-node latency A/B; never writes to main."""
import json
import os
import pathlib
import subprocess
import tempfile
import time
from statistics import median

ROOT = pathlib.Path(__file__).resolve().parents[1]
NODE = ROOT / "build/real-tcp-audit/cpp-runtime/choreoos-node"
BENCH = ROOT / "build/real-tcp-audit/cpp-runtime/benchmarks/choreoos-bench-real-tcp"
SAMPLES = 160
# Alternating order helps reduce host drift; each case uses fresh data directories.
CASES = ["current", "extra_fsync", "extra_fsync", "current"]
results = []
for trial, name in enumerate(CASES):
    base_port = 21000 + trial * 10
    with tempfile.TemporaryDirectory(prefix=f"choreo-audit-{trial}-") as tmp:
        root = pathlib.Path(tmp)
        procs = []
        handles = []
        try:
            for i in range(3):
                node_id = f"node-{i+1}"
                peers = ",".join(
                    f"node-{j+1}@127.0.0.1:{base_port+j}"
                    for j in range(3) if j != i
                )
                cfg = root / f"n{i+1}.yaml"
                cfg.write_text(
                    f"id: {node_id}\nleader: node-1\n"
                    f"data: {root / node_id}\nlisten: 127.0.0.1:{base_port+i}\n"
                    f"peers: {peers}\nelections: false\nheartbeat_ms: 30\n"
                )
                env = os.environ.copy()
                if name == "extra_fsync":
                    env["CHOREOOS_BENCH_EXTRA_META_FSYNC"] = "1"
                else:
                    env.pop("CHOREOOS_BENCH_EXTRA_META_FSYNC", None)
                log = open(root / f"n{i+1}.log", "w")
                handles.append(log)
                procs.append(
                    subprocess.Popen(
                        [str(NODE), str(cfg)],
                        stdout=log, stderr=subprocess.STDOUT,
                        env=env,
                    )
                )
            time.sleep(1.2)
            for p in procs:
                if p.poll() is not None:
                    raise RuntimeError(f"node exited early rc={p.returncode}")
            cmd = subprocess.run(
                [str(BENCH), str(base_port), str(SAMPLES)],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True, timeout=140
            )
            if cmd.returncode != 0:
                for k in range(3):
                    print(f"NODE {k+1}: {(root / f'n{k+1}.log').read_text()[-3000:]}", flush=True)
                raise RuntimeError(f"trial {trial} {name} failed:\n{cmd.stderr[-3500:]}")
            record = json.loads(cmd.stdout.strip().splitlines()[-1])
            record.update(mode=name, trial=trial)
            results.append(record)
            print(f"TRIAL {trial}: {json.dumps(record,sort_keys=True)}",flush=True)
        finally:
            for p in procs:
                if p.poll() is None:
                    p.terminate()
            for p in procs:
                try: p.wait(timeout=8)
                except subprocess.TimeoutExpired: p.kill();p.wait()
            for h in handles: h.close()
by_mode = {mode: [r for r in results if r["mode"] == mode] for mode in ["current","extra_fsync"]}
summary = {
    mode: {
        "n_trials": len(values),
        "median_p50_ms": median(v["p50_ms"] for v in values),
        "median_p99_ms": median(v["p99_ms"] for v in values),
        "median_ops_per_s": median(v["measured_ops_per_sec"] for v in values),
    }
    for mode, values in by_mode.items()
}
summary["p50_improvement_pct"] = 100 * (
    1 - summary["current"]["median_p50_ms"] / summary["extra_fsync"]["median_p50_ms"]
)
summary["p99_improvement_pct"] = 100 * (
    1 - summary["current"]["median_p99_ms"] / summary["extra_fsync"]["median_p99_ms"]
)
print("AB_SUMMARY: " + json.dumps(summary, sort_keys=True), flush=True)
with open("real_tcp_audit_results.json","w") as f:
    json.dump({"context":{
        "purpose":"A/B current implementation vs audit-only extra fsync; not a historical before/after",
        "samples_per_trial":SAMPLES,"trials":CASES,"network":"real loopback TCP 3 node processes",
        "commit_measurement":"client TCP connect through committed response",
        "durability":"sync mode, controlled extra file fsync in audit code",
    },"trials":results,"summary":summary},f,indent=2)
