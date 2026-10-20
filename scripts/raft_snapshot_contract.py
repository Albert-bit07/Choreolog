"""Snapshot trace checks, not TLC refinement or application-payload verification."""

from __future__ import annotations

import copy
from collections import Counter


class Violation(ValueError):
    """A snapshot trace violates its bounded diagnostic contract."""


def demand(condition: bool, message: str) -> None:
    if not condition:
        raise Violation(message)


def uint(value: object) -> bool:
    return type(value) is int and value >= 0


SCENARIOS = {"compact", "install-matching", "install-conflict", "compact-failed", "install-failed"}
FIELDS = ("term", "votedFor", "log", "commit", "applied", "snapshot_index", "snapshot_term")


def durable(state: dict) -> dict:
    return {key: state[key] for key in FIELDS}


def term_at(state: dict, index: int) -> int | None:
    if index == state["snapshot_index"]:
        return state["snapshot_term"]
    offset = index - state["snapshot_index"] - 1
    return state["log"][offset]["term"] if 0 <= offset < len(state["log"]) else None


def check_state(state: object) -> None:
    demand(isinstance(state, dict), "missing snapshot state")
    for key in FIELDS:
        demand(key in state, f"missing {key}")
    for key in ("term", "commit", "applied", "snapshot_index", "snapshot_term"):
        demand(uint(state[key]), f"invalid {key}")
    demand(state["votedFor"] is None or isinstance(state["votedFor"], str), "invalid vote")
    demand(isinstance(state["log"], list), "invalid retained log")
    prior = state["snapshot_term"]
    for entry in state["log"]:
        demand(
            isinstance(entry, dict)
            and uint(entry.get("term"))
            and prior <= entry["term"] <= state["term"]
            and entry["term"] > 0
            and isinstance(entry.get("val"), str),
            "invalid retained entry",
        )
        prior = entry["term"]
    boundary = state["snapshot_index"]
    demand((boundary == 0) == (state["snapshot_term"] == 0), "invalid snapshot anchor")
    demand(state["snapshot_term"] <= state["term"], "snapshot term beyond current term")
    demand(
        boundary <= state["applied"] <= state["commit"] <= boundary + len(state["log"]),
        "snapshot/applied/commit/log ordering invalid",
    )


def target_for(row: dict) -> dict:
    old = row["pre"]["durable"]
    target = copy.deepcopy(old)
    if row["act"] == "CompactCommitted":
        demand(
            old["applied"] == old["commit"] > old["snapshot_index"], "invalid compaction admission"
        )
        count = old["commit"] - old["snapshot_index"]
        target.update(
            snapshot_index=old["commit"],
            snapshot_term=term_at(old, old["commit"]),
            log=old["log"][count:],
        )
        demand(row["effects"]["persist_kind"] == "LocalCompaction", "wrong compaction intent")
    elif row["act"] == "InstallSnapshot":
        request = row["in"]
        demand(
            set(request) == {"term", "leader", "index", "snapshot_term", "request_id", "ready"},
            "incomplete snapshot request context",
        )
        for key in ("term", "index", "snapshot_term", "request_id"):
            demand(uint(request[key]) and request[key] > 0, f"invalid snapshot {key}")
        demand(
            request["leader"] in ("a", "c") and type(request["ready"]) is bool,
            "invalid snapshot sender/readiness",
        )
        demand(request["term"] >= old["term"], "unexpected stale install in fixture")
        if request["term"] > old["term"]:
            target.update(term=request["term"], votedFor=None)
        kind = "StateUpdate"
        if request["ready"] and request["index"] > old["commit"]:
            index = request["index"]
            committed_term = term_at(old, old["commit"])
            demand(
                committed_term is None or request["snapshot_term"] >= committed_term,
                "snapshot predates known committed term",
            )
            suffix = (
                old["log"][index - old["snapshot_index"] :]
                if term_at(old, index) == request["snapshot_term"]
                else []
            )
            target.update(
                snapshot_index=index,
                snapshot_term=request["snapshot_term"],
                log=suffix,
                commit=index,
                applied=index,
            )
            kind = "SnapshotInstall"
        demand(row["effects"]["persist_kind"] == kind, "wrong install intent")
    else:
        raise Violation("unsupported persistence action in snapshot contract")
    return target


def validate(rows: list[dict], schema: dict) -> dict[str, int]:
    demand(schema["properties"]["v"]["const"] == 2, "snapshot contract requires schema v2")
    actions = set(schema["properties"]["act"]["enum"])
    previous = {}
    pending = {}
    counts = Counter()
    completed = set()
    failed = set()
    restarted = set()
    for row in rows:
        demand(
            isinstance(row, dict) and type(row.get("v")) is int and row["v"] == 2,
            "unsupported snapshot trace version",
        )
        scenario = row.get("scenario")
        demand(scenario in SCENARIOS and row.get("node") == "b", "unknown snapshot fixture")
        demand(uint(row.get("seq")) and row["seq"] == counts[scenario], "snapshot sequence gap")
        counts[scenario] += 1
        action, pre, post = row.get("act"), row.get("pre"), row.get("post")
        demand(action in actions, "undeclared snapshot action")
        for state in (pre, post):
            check_state(state)
            check_state(state.get("durable"))
            demand(
                state.get("role") in ("Follower", "Candidate", "Leader")
                and type(state.get("alive")) is bool,
                "invalid role/availability",
            )
        before, after = row.get("pending_before"), row.get("pending_after")
        for token in (before, after):
            demand(token is None or (uint(token) and token > 0), "invalid pending token")
        prior = previous.get(scenario)
        if prior:
            demand(
                pre == prior["post"] and before == prior["pending_after"],
                "snapshot continuity broken",
            )
        previous[scenario] = row
        effects = row.get("effects")
        demand(
            isinstance(effects, dict)
            and isinstance(effects.get("send"), list)
            and uint(effects.get("apply"))
            and effects["apply"] == 0
            and type(effects.get("halt")) is bool,
            "invalid snapshot effects",
        )
        demand(isinstance(row.get("in"), dict), "missing input context")
        old, disk = pre["durable"], post["durable"]
        for key in ("term", "commit", "applied", "snapshot_index"):
            demand(disk[key] >= old[key], f"durable {key} rollback")
        if not pre["alive"] and action != "Restart":
            demand(
                post == pre
                and before == after
                and not effects["send"]
                and effects.get("persist") is None
                and not effects["halt"],
                "halted node made progress",
            )
        issued = effects.get("persist")
        if issued is not None:
            demand(
                uint(issued) and issued > 0 and before is None and after == issued,
                "invalid persistence admission",
            )
            demand(disk == old, "disk changed before snapshot persistence completion")
            demand(
                not effects["send"] and not effects["halt"],
                "snapshot effect escaped before persistence",
            )
            target = target_for(row)
            demand(durable(post) == target, "staged snapshot target differs from input contract")
            pending[scenario] = (issued, target, row["in"])
        elif action in ("PersistCompleted", "PersistFailed"):
            token = row.get("input_token")
            demand(uint(token) and row["in"] == {"token": token}, "callback context differs")
            if token != before:
                demand(
                    post == pre and after == before and not effects["send"] and not effects["halt"],
                    "stale callback released snapshot effects",
                )
            else:
                demand(
                    scenario in pending and pending[scenario][0] == token and after is None,
                    "callback lacks admitted snapshot write",
                )
                _, target, request = pending.pop(scenario)
                if action == "PersistFailed":
                    demand(
                        disk == old
                        and durable(post) == old
                        and not post["alive"]
                        and effects["halt"]
                        and not effects["send"],
                        "snapshot failure did not fail-stop",
                    )
                    if target["snapshot_index"] > old["snapshot_index"]:
                        failed.add(scenario)
                else:
                    demand(
                        disk == target and durable(post) == target and not effects["halt"],
                        "completed snapshot differs from admitted target",
                    )
                    if target["snapshot_index"] > old["snapshot_index"]:
                        completed.add(scenario)
                    if request.get("ready"):
                        demand(
                            effects["send"]
                            == [
                                {
                                    "kind": "SnapshotReply",
                                    "positive": True,
                                    "index": request["index"],
                                    "request_id": request["request_id"],
                                }
                            ],
                            "snapshot acknowledgement differs from admitted request",
                        )
                    else:
                        demand(not effects["send"], "compaction/incomplete install emitted reply")
        else:
            demand(
                disk == old and issued is None and not effects["halt"],
                "unexplained disk/effect change",
            )
            if action == "Restart":
                demand(
                    after is None
                    and post["alive"]
                    and post["role"] == "Follower"
                    and durable(post) == disk
                    and not effects["send"],
                    "restart retained volatile work",
                )
                pending.pop(scenario, None)
                restarted.add(scenario)
            elif action == "InstallSnapshot":
                request = row["in"]
                demand(
                    before is None
                    and after is None
                    and request.get("ready") is True
                    and request["term"] == disk["term"]
                    and term_at(disk, request["index"]) == request["snapshot_term"],
                    "duplicate install has no durable matching anchor",
                )
                demand(
                    post == pre
                    and effects["send"]
                    == [
                        {
                            "kind": "SnapshotReply",
                            "positive": True,
                            "index": request["index"],
                            "request_id": request["request_id"],
                        }
                    ],
                    "duplicate install changed state or reply context",
                )
            else:
                demand(
                    action == "SendAppend" and post == pre and not effects["send"],
                    "unsupported snapshot fixture action",
                )
        if after is None:
            demand(durable(post) == disk, "settled snapshot differs from disk")
    demand(set(counts) == SCENARIOS and restarted == SCENARIOS, "missing snapshot scenario/restart")
    demand(
        {"compact", "install-matching", "install-conflict"} <= completed
        and failed == {"compact-failed", "install-failed"},
        "missing completion/failure witnesses",
    )
    demand(not pending, "unfinished snapshot persistence")
    return dict(sorted(counts.items()))


def check_mutants(rows: list[dict], schema: dict) -> None:
    # Check the intended error text too: a later continuity failure must not
    # mask a missing local safety check.
    cases = [
        (
            "compact",
            "CompactCommitted",
            lambda r: r["post"]["durable"].update(snapshot_index=6, snapshot_term=3),
            "disk changed before",
        ),
        ("compact", "CompactCommitted", lambda r: r["post"]["log"].pop(), "staged snapshot target"),
        (
            "install-conflict",
            "InstallSnapshot",
            lambda r: r["post"]["log"].append({"term": 4, "val": "wrong"}),
            "staged snapshot target",
        ),
        (
            "install-matching",
            "InstallSnapshot",
            lambda r: r["effects"]["send"].append(
                {"kind": "SnapshotReply", "positive": True, "index": 7, "request_id": 91}
            ),
            "snapshot effect escaped",
        ),
        (
            "install-failed",
            "PersistFailed",
            lambda r: r["post"].update(alive=True),
            "failure did not fail-stop",
        ),
        (
            "install-matching",
            "PersistCompleted",
            lambda r: r["effects"]["send"][0].update(request_id=92),
            "acknowledgement differs",
        ),
        (
            "install-matching",
            "PersistCompleted",
            lambda r: r["effects"]["send"][0].update(index=8),
            "acknowledgement differs",
        ),
    ]
    for scenario, action, mutate, expected in cases:
        bad = copy.deepcopy(rows)
        candidates = [r for r in bad if r["scenario"] == scenario and r["act"] == action]
        if action == "PersistCompleted":
            row = next(r for r in candidates if r["effects"]["send"])
        else:
            row = next(
                (r for r in candidates if r["effects"]["persist_kind"] == "SnapshotInstall"),
                candidates[0],
            )
        mutate(row)
        try:
            validate(bad, schema)
        except Violation as error:
            demand(expected in str(error), f"mutation failed for wrong reason: {error}")
        else:
            raise Violation(f"{scenario}/{action} mutation escaped detection")

    # A term-only incomplete-transfer write is not an installation witness.
    # Remove the real install while preserving a continuous valid trace.
    bad = copy.deepcopy(rows)
    family = [r for r in bad if r["scenario"] == "install-matching"]
    restart = family[-1]
    restart.update(
        seq=2, pre=copy.deepcopy(family[1]["post"]), post=copy.deepcopy(family[1]["post"])
    )
    bad = [r for r in bad if r["scenario"] != "install-matching"] + family[:2] + [restart]
    try:
        validate(bad, schema)
    except Violation as error:
        demand(
            "missing completion/failure witnesses" in str(error),
            f"vacuous installation failed for wrong reason: {error}",
        )
    else:
        raise Violation("incomplete transfer was counted as a successful installation")
