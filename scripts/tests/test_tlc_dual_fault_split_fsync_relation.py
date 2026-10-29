#!/usr/bin/env python3
"""Bounded split-fsync/combined-write observations for the dual-fault witness.

Only stable durable endpoints and justified wire events correspond. The TLC
term-only intermediate record is explicitly unmatched by the C++ combined
write; this test does not assert a stuttering refinement or disk atomicity.
"""
from __future__ import annotations

import copy
import json
from pathlib import Path
import subprocess
import sys

from test_tlc_dual_fault_core_bridge import (
    ProjectionMismatch, compare, disk_projection, require, witness,
)
from test_tlc_dual_fault_witness import model, node, raw_messages
from test_tlc_combined_core_bridge import (
    combined_candidate_and_follower_failure_schedule,
    compare_combined_candidate_and_follower_failures,
)


def third_peer_states(document):
    states = [s["variables"] for s in document["states"]
              if s["variables"]["faultPhase"] == 5]
    require(len(states) >= 5, "third-peer split fsync states missing")
    return states


def third_peer_positive_replies(values):
    """All positive replies from s3, including misrouted or stale-term grants."""
    return [msg for msg, count in raw_messages(values["net"])
            for _ in range(count)
            if msg.get("mtype") == "RVResp"
            and msg.get("msrc") == model("s3")
            and msg.get("granted") is True]


def granted_replies(values):
    return [msg for msg in third_peer_positive_replies(values)
            if msg.get("mdst") == model("s2")
            and msg.get("mterm") == 2]


def check_relation(document, rows):
    """Check a bounded observation relation, including two unmatched states.

    The TLC term-only fsync and vote fsync are separate transitions. The
    compiled callback has no term-only durable state: we compare the old and
    fully voted endpoints, and require no early positive reply on either side.
    """
    require(len(rows) >= 11, "compiled dual-fault suffix missing")
    states = third_peer_states(document)
    disks = [disk_projection(node(s, "disk", "s3")) for s in states]
    endpoints = [(d["term"], d["votedFor"]) for d in disks]
    old, term_only, voted = (0, None), (2, None), (2, "b")
    require(endpoints[0] == old and endpoints[-1] == voted,
            "model third-peer durable endpoints diverged")
    transitions = [(i, endpoints[i - 1], endpoints[i])
                   for i in range(1, len(endpoints))
                   if endpoints[i] != endpoints[i - 1]]
    require([(a, b) for _, a, b in transitions] ==
            [(old, term_only), (term_only, voted)],
            "model term/vote fsync ordering or stutter diverged")
    term_at, vote_at = (transition[0] for transition in transitions)
    require(term_at < vote_at and disks[0]["log"] == disks[-1]["log"] and
            all(d["log"] == disks[0]["log"] for d in disks),
            "model third-peer log changed during vote persistence")

    # The pre-fsync staged targets are not yet durable. TLC must preserve
    # the separate term-only state and not publish a grant until vote fsync.
    term_pending = node(states[term_at - 1], "pending", "s3")
    vote_pending = node(states[vote_at - 1], "pending", "s3")
    require(isinstance(term_pending, dict) and
            term_pending["target"]["term"] == 2 and
            term_pending["target"]["votedFor"] == model("Nil") and
            isinstance(vote_pending, dict) and
            vote_pending["target"]["term"] == 2 and
            vote_pending["target"]["votedFor"] == model("s2") and
            node(states[term_at], "pending", "s3") == model("Nil") and
            node(states[vote_at], "pending", "s3") == model("Nil"),
            "model staged term/vote writes were not separately fsynced")
    # Count every positive third-peer response, not just the expected
    # destination/term: a misrouted or stale-term grant is still a violation.
    require(all(not third_peer_positive_replies(s) for s in states[:vote_at]) and
            len(third_peer_positive_replies(states[vote_at])) == 1 and
            len(granted_replies(states[vote_at])) == 1,
            "model third peer sent an early, duplicate or misidentified grant")

    synced_b, staged_a, failed_a, restarted_a, staged_c, synced_c, elected_b = rows[-7:]
    require(staged_c["node"] == synced_c["node"] == "c" and
            staged_c["post"]["term"] == 2 and
            staged_c["post"]["votedFor"] == "b" and
            staged_c["post"]["durable"]["term"] == old[0] and
            staged_c["post"]["durable"]["votedFor"] is None and
            staged_c["effects"]["send"] == [] and
            type(staged_c["pending_after"]) is int and
            staged_c["pending_after"] > 0 and
            staged_c["effects"]["persist"] == staged_c["pending_after"],
            "compiled third peer released an undurable vote")
    require(synced_c["pending_before"] == staged_c["pending_after"] and
            synced_c["input_token"] == staged_c["pending_after"] and
            synced_c["pending_after"] is None and
            synced_c["post"]["durable"]["term"] == voted[0] and
            synced_c["post"]["durable"]["votedFor"] == voted[1] and
            synced_c["post"]["durable"]["log"] == disks[-1]["log"],
            "compiled combined vote fsync failed to reach model durable endpoint")
    requests = [msg for msg in synced_b["effects"]["send"]
                if msg.get("kind") == "RVReq" and msg.get("to") == "c"]
    replies = synced_c["effects"]["send"]
    require(len(requests) == len(replies) == 1 and
            replies[0].get("kind") == "RVResp" and
            replies[0].get("to") == "b" and
            replies[0].get("term") == 2 and
            replies[0].get("positive") is True and
            type(replies[0].get("request_id")) is int and
            replies[0]["request_id"] == requests[0].get("request_id"),
            "compiled third-peer grant did not correlate to the durable request")

    # This is a deliberate non-corresponding failure boundary, not a stutter.
    # The model has fsynced term 2 / Nil; the C++ failed combined write retains
    # term 1 / a. Their log endpoint is still shared.
    model_failed = disk_projection(node(states[0], "disk", "s1"))
    core_failed = failed_a["post"]["durable"]
    require((model_failed["term"], model_failed["votedFor"]) == (2, None)
            and (core_failed["term"], core_failed["votedFor"]) == (1, "a")
            and model_failed["log"] == core_failed["log"] and
            failed_a["effects"]["send"] == [] and
            restarted_a["effects"]["send"] == [],
            "model term-only failure was incorrectly identified with C++ write")
    return len(transitions)


def check_mutations(document, rows):
    mutants = []
    # Model transitions: premature term, premature vote, lost term fsync,
    # undurable reply, lost vote fsync, and durable log divergence.
    for phase_index, field, value in (
        (1, "term", 2),
        (2, "votedFor", model("s2")),
        (2, "term", 1),
        (3, "votedFor", model("s2")),
        (4, "votedFor", model("Nil")),
    ):
        changed = copy.deepcopy(document)
        values = third_peer_states(changed)[phase_index]
        node(values, "disk", "s3")[field] = value
        mutants.append((changed, rows))
    changed = copy.deepcopy(document)
    node(third_peer_states(changed)[-1], "disk", "s3")["log"] = {"$sequence": [{"term": 99, "val": model("v1")}]}
    mutants.append((changed, rows))
    # Network mutations: a forged grant before vote fsync, a wrong
    # destination/term, or a duplicated positive response must all fail.
    def vote_index(states):
        return next(i for i, state in enumerate(states)
                    if disk_projection(node(state, "disk", "s3"))["votedFor"] == "b")

    changed = copy.deepcopy(document)
    states = third_peer_states(changed)
    index = vote_index(states)
    grant = copy.deepcopy(granted_replies(states[index])[0])
    before = states[index - 1]["net"]
    if before == {"$sequence": []}:
        states[index - 1]["net"] = {"$function": [[grant, 1]]}
    else:
        before["$function"].append([grant, 1])
    mutants.append((changed, rows))

    for field, value in (("mdst", model("s1")), ("mterm", 1)):
        changed = copy.deepcopy(document)
        states = third_peer_states(changed)
        index = vote_index(states)
        matching = granted_replies(states[index])
        require(len(matching) == 1, "missing baseline third-peer reply")
        matching[0][field] = value
        mutants.append((changed, rows))

    changed = copy.deepcopy(document)
    states = third_peer_states(changed)
    index = vote_index(states)
    for pair in raw_messages(states[index]["net"]):
        if pair[0].get("mtype") == "RVResp" and pair[0].get("msrc") == model("s3") and pair[0].get("granted") is True:
            pair[1] += 1
            break
    else:
        raise ProjectionMismatch("missing baseline third-peer reply multiplicity")
    mutants.append((changed, rows))

    # Compiled transitions: no early send, exact persistence token, stable
    # durable endpoint and correlated positive reply are all mandatory.
    for offset, path, key, value in (
        (-3, ("post", "durable"), "term", 2),
        (-3, ("effects",), "send", [{"kind": "RVResp"}]),
        (-3, (), "pending_after", None),
        (-2, (), "input_token", -1),
        (-2, ("post", "durable"), "votedFor", None),
        (-2, ("effects", "send", 0), "request_id", -1),
        (-2, ("effects", "send", 0), "to", "a"),
        (-2, ("effects", "send", 0), "positive", False),
        (-2, ("effects", "send", 0), "term", 1),
        (-2, ("effects", "send", 0), "kind", "AEReq"),
        (-5, ("post", "durable"), "term", 2),
    ):
        changed = copy.deepcopy(rows)
        target = changed[offset]
        for part in path:
            target = target[part]
        target[key] = value
        mutants.append((document, changed))
    for index, (doc, core) in enumerate(mutants, 1):
        try:
            check_relation(doc, core)
        except (ProjectionMismatch, KeyError, TypeError, ValueError, IndexError):
            continue
        raise ProjectionMismatch(f"split-fsync mutation {index} was not rejected")
    return len(mutants)


def main():
    if len(sys.argv) != 4:
        raise ProjectionMismatch("usage: split_fsync_relation.py RUNNER MODEL CONFIG")
    runner, model_path, config_path = map(Path, sys.argv[1:])
    document = witness(model_path, config_path)
    commands = combined_candidate_and_follower_failure_schedule(False)
    result = subprocess.run([str(runner), "--replay-inputs"],
                            input="\n".join(commands) + "\n",
                            capture_output=True, text=True, timeout=30, check=False)
    require(result.returncode == 0,
            "compiled replay rejected: " + result.stderr.strip())
    rows = [json.loads(line) for line in result.stdout.splitlines()]
    compare_combined_candidate_and_follower_failures(rows, False)
    compare(document, rows)
    transitions = check_relation(document, rows)
    mutations = check_mutations(document, rows)
    print(f"Bounded split/combined relation: {transitions} ordered model fsyncs, "
          f"{mutations} negative mutations rejected.")
    print("Two model term-only boundaries are unmatched; no full refinement.")


if __name__ == "__main__":
    try:
        main()
    except (ProjectionMismatch, OSError, ValueError, KeyError, TypeError,
            IndexError, subprocess.TimeoutExpired) as exc:
        sys.exit(f"TLC split-fsync relation FAIL: {exc}")
