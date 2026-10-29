#!/usr/bin/env python3
"""Selected typed TLC witness-to-compiled-core checkpoints, not full refinement.

The TLC model fsyncs term and vote separately. The compiled core writes them
together. A model torn term-only follower record is deliberately NOT equated
to the compiled failed-write record; only justified endpoints are compared.
"""
from __future__ import annotations

import base64
import copy
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import zlib

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from export_tlc_trace import parse_trace
from test_tlc_dual_fault_witness import verify, phase_edges, node, model
from test_tlc_combined_core_bridge import (
    combined_candidate_and_follower_failure_schedule,
    compare_combined_candidate_and_follower_failures,
)

LOG_SHA = "ea0f202da0d2c083afd74e7cd0d2070f687a9b123adeab93f71bbbb6d0f026c9"
MODEL_BLOB = "a3bf3f48653c76dd14fa742a45ea823ec32e7acc"
CONFIG_BLOB = "b56797d7fa36934731c8bdd4fc393fe66a510b90"
INVARIANT = "NoDualFailureAlternativeQuorumWitness"
PEERS = {"s1": "a", "s2": "b", "s3": "c"}


class ProjectionMismatch(ValueError):
    pass


def require(condition, reason):
    if not condition:
        raise ProjectionMismatch(reason)


def git_blob(data):
    return hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()


def pinned_source(path, expected):
    data = path.read_bytes()
    if git_blob(data) == expected:
        return data
    require(b"\r\n" in data and b"\r" not in data.replace(b"\r\n", b""),
            "unreviewed source bytes: " + path.name)
    normalized = data.replace(b"\r\n", b"\n")
    require(git_blob(normalized) == expected, "source SHA mismatch: " + path.name)
    return normalized


def witness(model_path, config_path):
    compressed = (HERE / "fixtures" / "dual_persistence_20261029.zlib.b64"
                  ).read_text(encoding="ascii").strip()
    raw = zlib.decompress(base64.b64decode(compressed, validate=True))
    require(hashlib.sha256(raw).hexdigest() == LOG_SHA, "TLC log digest mismatch")
    model_bytes = pinned_source(model_path, MODEL_BLOB)
    config_bytes = pinned_source(config_path, CONFIG_BLOB)
    with tempfile.TemporaryDirectory(prefix="choreo-dual-witness-") as folder:
        root = Path(folder)
        model_copy, config_copy = root / model_path.name, root / config_path.name
        log_copy = root / "witness.txt"
        model_copy.write_bytes(model_bytes)
        config_copy.write_bytes(config_bytes)
        log_copy.write_bytes(raw)
        document = {
            "v": 1, "kind": "tlc-text-behavior",
            "model": {"name": model_copy.name,
                      "sha256": hashlib.sha256(model_bytes).hexdigest()},
            "config": {"name": config_copy.name,
                       "sha256": hashlib.sha256(config_bytes).hexdigest()},
            "log_sha256": LOG_SHA,
            "tlc_version": raw.decode("utf-8").splitlines()[0],
            "invariant": INVARIANT,
            "states": parse_trace(raw.decode("utf-8"), INVARIANT),
        }
        verify(document, model_copy, config_copy, log_copy)
    return document


def disk_projection(record):
    return {
        "term": record["term"],
        "votedFor": None if record["votedFor"] == model("Nil")
                    else PEERS[record["votedFor"]["$model"]],
        "log": [{"term": entry["term"], "val": entry["val"]["$model"]}
                for entry in record["log"]["$sequence"]],
    }


def compare(document, rows):
    require(len(rows) >= 11, "missing compiled fault suffix")
    edges = phase_edges(document["states"])
    (staged_b, failed_b, restarted_b, retried_b, synced_b, staged_a,
     failed_a, restarted_a, staged_c, synced_c, elected_b) = rows[-11:]
    model_old_b = disk_projection(node(edges[1][1], "disk", "s2"))
    require(model_old_b == {"term": 1, "votedFor": "a",
                            "log": [{"term": 1, "val": "v1"}]},
            "TLC failed candidate predecessor diverged")
    for row in (failed_b, restarted_b):
        require(all(row["post"]["durable"][key] == value
                    for key, value in model_old_b.items()),
                "failed/restarted compiled candidate diverged from model disk")
    for phase in (1, 2):
        values = edges[phase][1]
        require(node(values, "mem", "s2") == node(values, "disk", "s2"),
                "TLC failed/restarted candidate retained undurable memory")
    require(all(row["effects"]["send"] == []
                for row in (staged_b, failed_b, restarted_b, retried_b)),
            "compiled candidate sent vote requests before successful fsync")

    retry = node(edges[3][1], "pending", "s2")
    require(retry["target"]["term"] == 2 and
            retry["target"]["votedFor"] == model("s2") and
            disk_projection(node(edges[3][1], "disk", "s2")) == model_old_b and
            retried_b["post"]["term"] == 2 and
            retried_b["post"]["votedFor"] == "b",
            "model/core retry staging diverged")
    durable_b = disk_projection(node(edges[4][1], "disk", "s2"))
    require(durable_b["term"] == 2 and durable_b["votedFor"] == "b" and
            durable_b["log"] == model_old_b["log"] and
            all(synced_b["post"]["durable"][key] == value
                for key, value in durable_b.items()),
            "model/core durable self-vote diverged")
    expected = {(PEERS[msg["mdst"]["$model"]], msg["mterm"],
                 PEERS[msg["msrc"]["$model"]], msg["mlastIdx"], msg["mlastTerm"])
                for msg in retry["after"]["$set"]}
    sent = synced_b["effects"]["send"]
    actual = {(msg["to"], msg["term"], msg["candidate"],
               msg["last_index"], msg["last_term"]) for msg in sent}
    ids = [msg["request_id"] for msg in sent]
    require(len(expected) == len(actual) == len(sent) == 2 and
            expected == actual and
            all(type(identity) is int and identity > 0 for identity in ids) and
            len(set(ids)) == 2,
            "deferred TLC vote requests differ from compiled wire observations")

    # The model's selected torn outcome retains term 2 / Nil; C++'s
    # failed combined write retains the older acknowledged term 1 / a.
    # This difference is an explicit gap, NOT a stuttering equivalence.
    model_a = disk_projection(node(edges[5][1], "disk", "s1"))
    core_a = failed_a["post"]["durable"]
    require(model_a["term"] == 2 and model_a["votedFor"] is None and
            core_a["term"] == 1 and core_a["votedFor"] == "a" and
            model_a["log"] == core_a["log"] == model_old_b["log"] and
            failed_a["effects"]["send"] == [] and
            failed_a["effects"]["halt"] is True and
            restarted_a["effects"]["send"] == [],
            "term-only TLC torn record conflated with C++ failed combined write")

    # Third-peer term-only state is a model intermediate checkpoint; C++
    # persists term+vote together. Compare the stable post-fsync endpoints.
    phase_five = [s["variables"] for s in document["states"]
                  if s["variables"]["faultPhase"] == 5]
    third = [disk_projection(node(values, "disk", "s3"))
             for values in phase_five]
    require(any(d["term"] == 2 and d["votedFor"] is None for d in third) and
            any(d["term"] == 2 and d["votedFor"] == "b" for d in third) and
            staged_c["post"]["durable"]["term"] == 0 and
            staged_c["post"]["durable"]["votedFor"] is None and
            staged_c["effects"]["send"] == [] and
            synced_c["post"]["durable"]["term"] == 2 and
            synced_c["post"]["durable"]["votedFor"] == "b",
            "third-peer split/combined persistence endpoints diverged")
    final = document["states"][-1]["variables"]
    final_c = disk_projection(node(final, "disk", "s3"))
    require(final_c["term"] == 2 and final_c["votedFor"] == "b" and
            node(final, "role", "s2") == model("Leader") and
            elected_b["post"]["role"] == "Leader" and
            elected_b["post"]["term"] == 2,
            "alternative durable quorum/leadership endpoint diverged")
    beats = elected_b["effects"]["send"]
    require(len(beats) == 2 and {m["to"] for m in beats} == {"a", "c"} and
            all(m["kind"] == "AEReq" and m["term"] == 2 and
                m["leader"] == "b" and type(m["request_id"]) is int and
                m["request_id"] > max(ids) for m in beats) and
            len({m["request_id"] for m in beats}) == 2,
            "compiled heartbeat destination or request identity diverged")


def check_mutations(document, rows):
    mutants = []
    for phase, field, actor, key, value in (
        (1, "disk", "s2", "term", 2),
        (3, "pending", "s2", "target", {"term": 2, "votedFor": model("s1")}),
        (4, "disk", "s2", "votedFor", model("Nil")),
        (5, "disk", "s1", "term", 1),
    ):
        altered = copy.deepcopy(document)
        node(phase_edges(altered["states"])[phase][1], field, actor)[key] = value
        mutants.append((altered, rows))
    for offset, path, key, value in (
        (-10, ("post", "durable"), "term", 2),
        (-9, ("post", "durable"), "votedFor", "b"),
        (-8, ("effects",), "send", [{"kind": "RVReq"}]),
        (-7, ("post", "durable"), "votedFor", "a"),
        (-7, ("effects", "send", 0), "to", "b"),
        (-5, ("post", "durable"), "term", 2),
        (-2, ("post", "durable"), "votedFor", "a"),
        (-1, ("effects", "send", 0), "request_id",
         rows[-7]["effects"]["send"][0]["request_id"]),
    ):
        altered = copy.deepcopy(rows)
        target = altered[offset]
        for part in path:
            target = target[part]
        target[key] = value
        mutants.append((document, altered))
    for index, (doc, core) in enumerate(mutants, 1):
        try:
            compare(doc, core)
        except (ProjectionMismatch, KeyError, TypeError, ValueError):
            continue
        raise ProjectionMismatch(f"mutation {index} was not rejected")
    return len(mutants)


def main():
    if len(sys.argv) != 4:
        raise ProjectionMismatch("usage: test_tlc_dual_fault_core_bridge.py RUNNER MODEL CONFIG")
    runner, model_path, config_path = map(Path, sys.argv[1:])
    doc = witness(model_path, config_path)
    commands = combined_candidate_and_follower_failure_schedule(False)
    result = subprocess.run([str(runner), "--replay-inputs"],
                            input="\n".join(commands) + "\n",
                            capture_output=True, text=True, timeout=30, check=False)
    require(result.returncode == 0,
            "compiled replay rejected: " + result.stderr.strip())
    rows = [json.loads(line) for line in result.stdout.splitlines()]
    compare_combined_candidate_and_follower_failures(rows, False)
    compare(doc, rows)
    count = check_mutations(doc, rows)
    print(f"Selected 32-state TLC/core checkpoints passed; {count} negative mutations rejected.")
    print("Model term-only torn state differs from compiled failed combined write; no full refinement.")


if __name__ == "__main__":
    try:
        main()
    except (ProjectionMismatch, OSError, ValueError, KeyError, TypeError,
            zlib.error, subprocess.TimeoutExpired) as exc:
        sys.exit(f"TLC dual-fault/core bridge FAIL: {exc}")
