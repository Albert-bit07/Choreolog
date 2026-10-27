#!/usr/bin/env python3
"""Independent bounded safety/trace-contract checker for PR #11.

The transition cases are driven by the real, compiled C++ pure core.
The checker validates their observable projection against explicit invariants
from formal/raft/ChoreoRaft.tla, not an executable TLA+ refinement relation.
A model-to-code TLC trace replay must be added before claiming refinement.
"""
from __future__ import annotations

import argparse
import copy
import json
import pathlib
import subprocess
import sys
from collections import defaultdict

REQUIRED_SCENARIOS = {"election-commit", "failed-persist",
                      "conflict-protection", "log-repair", "stale-ack"}


class Violation(ValueError):
    """An invalid or unsafe core trace."""


def demand(condition: bool, message: str) -> None:
    if not condition:
        raise Violation(message)


def uint(value: object) -> bool:
    return type(value) is int and value >= 0


def check_projection(state: object, actions: set[str]) -> None:
    demand(isinstance(state, dict), "missing state projection")
    for name in ("term", "commit"):
        demand(uint(state.get(name)), f"invalid {name}")
    demand(state.get("role") in ("Follower", "Candidate", "Leader"), "invalid role")
    demand(state.get("votedFor") is None or isinstance(state.get("votedFor"), str),
           "invalid votedFor")
    demand(type(state.get("alive")) is bool, "invalid availability")
    log = state.get("log")
    demand(isinstance(log, list), "invalid log")
    for entry in log:
        demand(isinstance(entry, dict) and uint(entry.get("term")) and
               isinstance(entry.get("val"), str), "invalid log entry")
    disk = state.get("durable")
    demand(isinstance(disk, dict), "missing durable projection")
    # This v1 safety contract has no snapshot/model-offset relation. Never
    # silently interpret a retained suffix as a complete index-1 log.
    for projection in (state, disk):
        for boundary in ("snapshot_index", "snapshot_term"):
            demand(type(projection.get(boundary, 0)) is int and
                   projection.get(boundary, 0) == 0,
                   "snapshot offsets are outside the v1 trace contract")
    for name in ("term", "log_len", "commit", "applied"):
        demand(uint(disk.get(name)), f"invalid durable.{name}")
    demand(disk["term"] <= state["term"], "disk term ahead of staged term")
    demand(disk["applied"] <= disk["commit"] <= disk["log_len"],
           "durable applied/commit/log ordering invalid")
    demand(state["commit"] <= len(log), "commit beyond staged log")
    # A new append may stage truncation of an uncommitted suffix. Until the
    # WAL write completes, the old durable log can legitimately be longer.


def validate(rows: list[dict], schema: dict) -> dict[str, int]:
    actions = set(schema["properties"]["act"]["enum"])
    demand(bool(rows), "trace is empty")
    previous: dict[tuple[str, str], dict] = {}
    scenarios: dict[str, list[dict]] = defaultdict(list)
    for index, row in enumerate(rows):
        demand(isinstance(row, dict), "trace row not object")
        demand(row.get("v") == 1 and type(row.get("v")) is int, "unsupported version")
        demand(uint(row.get("seq")), "sequence must be nonnegative")
        scenario, actor, action = row.get("scenario"), row.get("node"), row.get("act")
        demand(scenario in REQUIRED_SCENARIOS, "unknown scenario")
        demand(row["seq"] == len(scenarios[scenario]),
               f"{scenario}: sequence must be gap-free and ordered within each scenario")
        demand(actor in ("a", "b", "c"), "unknown node")
        demand(action in actions, f"action not declared in TLA+ schema: {action}")
        pre, post = row.get("pre"), row.get("post")
        check_projection(pre, actions)
        check_projection(post, actions)
        before_token, after_token = row.get("pending_before"), row.get("pending_after")
        demand(before_token is None or uint(before_token), "invalid previous token")
        demand(after_token is None or uint(after_token), "invalid following token")
        input_token = row.get("input_token")
        if action in ("PersistCompleted", "PersistFailed"):
            demand(uint(input_token), "persist callback missing input token")
        else:
            demand(input_token is None, "unexpected persistence callback token")
        effects = row.get("effects")
        demand(isinstance(effects, dict), "missing effects")
        persist, sends, applied, halt = (effects.get("persist"),
                                         effects.get("send"),
                                         effects.get("apply"),
                                         effects.get("halt"))
        demand(persist is None or uint(persist), "invalid persisted token")
        demand(isinstance(sends, list) and all(isinstance(m, dict) and
               m.get("kind") in ("RVReq", "RVResp", "AEReq", "AEResp") and
               type(m.get("positive")) is bool for m in sends), "invalid sends")
        demand(uint(applied), "invalid apply count")
        demand(type(halt) is bool, "invalid halt effect")
        prior = previous.get((scenario, actor))
        if prior is not None:
            demand(pre == prior["post"], f"{scenario}/{actor}: pre-state discontinuity")
            demand(before_token == prior["pending_after"],
                   f"{scenario}/{actor}: persistence token discontinuity")
        previous[(scenario, actor)] = row
        scenarios[scenario].append(row)

        if action in ("PersistFailed", "Restart"):
            # An unacknowledged in-memory term/log may disappear on recovery.
            # Durable knowledge and committed history must still be monotone.
            demand(post["term"] == post["durable"]["term"],
                   "recovery retained an unacknowledged staged term")
        else:
            demand(post["term"] >= pre["term"], "term regression")
        demand(post["durable"]["term"] >= pre["durable"]["term"], "durable term regression")
        demand(post["commit"] >= pre["commit"], "commit index regression")
        demand(post["durable"]["commit"] >= pre["durable"]["commit"],
               "durable commit rollback")
        demand(post["durable"]["applied"] >= pre["durable"]["applied"],
               "durable application rollback")
        demand(post["durable"]["log_len"] >= post["durable"]["commit"],
               "committed entry absent from durable log")
        if after_token is None:
            demand(post["durable"]["log_len"] == len(post["log"]),
                   "settled state disagrees with durable log length")
        if pre["commit"] > 0:
            demand(post["log"][:pre["commit"]] == pre["log"][:pre["commit"]],
                   "locally committed prefix changed")
        if pre["alive"] is False and action != "Restart":
            demand(post == pre and not sends and persist is None and not halt,
                   "halted node emitted side effects")
        if persist is not None:
            demand(before_token is None and after_token == persist,
                   "persist request not correctly staged")
            demand(pre["durable"] == post["durable"],
                   "disk state changed before write completion")
            demand(not sends and applied == 0, "dependent effect escaped before fsync")
        elif action == "PersistCompleted":
            if before_token == input_token:
                demand(after_token is None, "matched durable write was not retired")
                demand(post["durable"]["term"] >= pre["durable"]["term"],
                       "completion regressed durable state")
            else:
                demand(after_token == before_token and post == pre and
                       not sends and applied == 0,
                       "stale persistence completion changed state or released effects")
        else:
            demand(pre["durable"] == post["durable"],
                   f"durable state changed without PersistCompleted ({action})")
        if action == "PersistFailed" and before_token == input_token:
            demand(before_token is not None and post["alive"] is False and
                   halt and not sends and applied == 0 and after_token is None,
                   "matched write failure did not fail-stop")
        elif action == "PersistFailed":
            demand(post == pre and before_token == after_token and not sends and
                   persist is None and applied == 0 and not halt,
                   "stale write failure changed state")
        else:
            demand(not halt, "spurious halt effect")
        if applied:
            demand(action == "PersistCompleted", "application escaped before persistence")
            demand(post["durable"]["applied"] > pre["durable"]["applied"],
                   "application acknowledgement without durable progress")
        if action == "Restart":
            demand(post["alive"] is True and after_token is None and not sends,
                   "restart retained pending I/O")
        if any(m["kind"] == "RVResp" and m["positive"] for m in sends):
            demand(post["votedFor"] is not None and
                   post["durable"]["term"] == post["term"],
                   "granted vote without durable term/vote")
        if any(m["kind"] == "AEResp" and m["positive"] for m in sends):
            demand(post["durable"]["term"] == post["term"],
                   "successful append acknowledged before durable term")
    demand(set(scenarios) == REQUIRED_SCENARIOS,
           f"missing scenarios: {sorted(REQUIRED_SCENARIOS - set(scenarios))}")
    success = scenarios["election-commit"]
    demand(any(r["post"]["role"] == "Leader" and r["node"] == "a" for r in success),
           "election never produced a leader")
    demand(any(r["node"] == "b" and r["post"]["durable"]["log_len"] >= 1 for r in success),
           "no follower durable replication")
    demand(any(r["node"] == "a" and r["post"]["durable"]["commit"] >= 1 for r in success),
           "no committed entry")
    demand(any(r["node"] == "a" and r["post"]["durable"]["applied"] >= 1 and
               r["effects"]["apply"] >= 1 for r in success), "no durable application")
    demand(any(r["effects"]["halt"] for r in scenarios["failed-persist"]),
           "write-failure path not exercised")
    demand(any(r["act"] == "Restart" and r["post"]["alive"]
               for r in scenarios["failed-persist"]), "restart absent")
    repair = scenarios["log-repair"]
    demand(any(r["post"]["durable"]["log_len"] == 1 and
               r["post"]["log"][0]["val"] == "replacement" for r in repair),
           "uncommitted suffix repair never durably completed")
    conflict = scenarios["conflict-protection"]
    demand(any(r["effects"]["send"] and
               any(m["kind"] == "AEResp" and not m["positive"]
                   for m in r["effects"]["send"]) for r in conflict),
           "conflicting append was not rejected")
    return {name: len(lines) for name, lines in sorted(scenarios.items())}


def parse_trace(output: str) -> list[dict]:
    try:
        return [json.loads(line) for line in output.splitlines() if line.strip()]
    except json.JSONDecodeError as exc:
        raise Violation(f"invalid NDJSON: {exc}") from exc


def check_mutants(rows: list[dict], schema: dict) -> None:
    """Prove the checker rejects independently seeded bad trace outcomes."""
    mutants: list[list[dict]] = []
    # Crash safety: disk changes before a write has completed.
    m = copy.deepcopy(rows)
    r = next(r for r in m if r["scenario"] == "election-commit" and
             r["effects"]["persist"] is not None)
    r["post"]["durable"]["term"] += 1
    mutants.append(m)

    # Snapshot offsets require a separately defined trace/model relation.
    m = copy.deepcopy(rows)
    for row in m:
        for projection in (row["pre"], row["post"]):
            projection["snapshot_index"] = 1
    mutants.append(m)

    # Committed prefix must survive every subsequent transition.
    m = copy.deepcopy(rows)
    r = next(r for r in m if r["scenario"] == "conflict-protection")
    r["post"]["log"][0]["val"] = "mutated"
    mutants.append(m)

    # A vote reply must not escape before the required durable write.
    m = copy.deepcopy(rows)
    r = next(r for r in m if r["scenario"] == "failed-persist" and
             r["effects"]["persist"] is not None)
    r["effects"]["send"].append({"kind": "RVResp", "positive": True})
    mutants.append(m)

    # Can't acknowledge applied state ahead of the durable committed log.
    m = copy.deepcopy(rows)
    r = next(r for r in m if r["scenario"] == "election-commit" and
             r["post"]["durable"]["applied"] > 0)
    r["post"]["durable"]["applied"] = 500
    mutants.append(m)

    # No unrecorded changes in between two transitions of the same node.
    m = copy.deepcopy(rows)
    r = next(r for r in m if r["scenario"] == "election-commit" and
             r["node"] == "a" and r["act"] == "AdvanceCommit")
    r["pre"]["term"] += 1
    mutants.append(m)

    for idx, mutant in enumerate(mutants, start=1):
        try:
            validate(mutant, schema)
        except Violation:
            continue
        raise Violation(f"negative mutation #{idx} went undetected")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runner", required=True, help="path to compiled C++ scenario executable")
    parser.add_argument("--schema", required=True, type=pathlib.Path)
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--snapshot-contract", action="store_true",
                        help="check v2 snapshot diagnostics, not TLC refinement")
    args = parser.parse_args()
    schema = json.loads(args.schema.read_text(encoding="utf-8"))
    command = [args.runner] + (["--snapshot-contract"] if args.snapshot_contract else [])
    execution = subprocess.run(command, capture_output=True, text=True, timeout=60,
                               check=False)
    demand(execution.returncode == 0,
           f"C++ scenario runner failed ({execution.returncode}): {execution.stderr}")
    rows = parse_trace(execution.stdout)
    if args.snapshot_contract:
        from raft_snapshot_contract import validate as validator, check_mutants as mutations
    else:
        validator, mutations = validate, check_mutants
    counts = validator(rows, schema)
    if args.self_test:
        mutations(rows, schema)
    print("Raft trace contract PASS:", ", ".join(f"{name}={count}" for name, count in counts.items()))
    if args.self_test:
        count = "ten snapshot" if args.snapshot_contract else "six"
        print(f"All {count} deliberately invalid trace mutations were rejected.")
    print("Scope: bounded observable safety contract, NOT TLC-to-C++ refinement.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (Violation, OSError, ValueError, subprocess.TimeoutExpired) as exc:
        print(f"Raft trace contract FAIL: {exc}", file=sys.stderr)
        sys.exit(1)
