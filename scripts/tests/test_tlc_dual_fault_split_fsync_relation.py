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
    # The former leader's failed combined persistence is not a successful
    # term-only model fsync. The exact staged token must fail closed; restart
    # must reconstruct the old acknowledged vote, not the staged vote for b.
    old_durable = staged_a["post"]["durable"]
    token = staged_a["pending_after"]
    require(staged_a["node"] == failed_a["node"] == restarted_a["node"] == "a" and
            staged_a["act"] == "HandleRVReq" and
            failed_a["act"] == "PersistFailed" and
            restarted_a["act"] == "Restart" and
            type(token) is int and token > 0 and
            staged_a["effects"]["persist"] == token and
            staged_a["effects"]["send"] == [] and
            failed_a["pending_before"] == failed_a["input_token"] == token and
            failed_a["pending_after"] is None and
            failed_a["pre"]["durable"] == old_durable and
            failed_a["post"]["durable"] == old_durable and
            failed_a["post"]["term"] == old_durable["term"] == 1 and
            failed_a["post"]["votedFor"] == old_durable["votedFor"] == "a" and
            failed_a["effects"]["halt"] is True and
            failed_a["effects"]["send"] == [] and
            restarted_a["pending_after"] is None and
            restarted_a["post"]["durable"] == old_durable and
            restarted_a["post"]["term"] == 1 and
            restarted_a["post"]["votedFor"] == "a" and
            restarted_a["effects"]["send"] == [],
            "failed combined vote write or restart leaked a durable grant")
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
    # A model term-only failure must not grow a durable vote, and a
    # compiled failed write/restart must not invent a vote for b or bypass
    # the persistence callback identity.
    changed = copy.deepcopy(document)
    node(third_peer_states(changed)[0], "disk", "s1")["votedFor"] = model("s2")
    mutants.append((changed, rows))
    for offset, path, key, value in (
        (-5, ("post", "durable"), "votedFor", "b"),
        (-4, ("post", "durable"), "votedFor", "b"),
        (-5, ("effects",), "send", [{"kind": "RVResp", "positive": True,
                                    "to": "b", "term": 2}]),
        (-5, (), "input_token", -1),
        (-4, ("post",), "term", 2),
        (-6, (), "pending_after", None),
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




def third_peer_vote_requests(values):
    """Outstanding candidate-b requests addressed to the third peer."""
    return [(msg, count) for msg, count in raw_messages(values["net"])
            if msg.get("mtype") == "RVReq"
            and msg.get("msrc") == model("s2")
            and msg.get("mdst") == model("s3")]


def check_request_conservation(document, rows):
    """Bounded RequestVote causality across split fsync and combined callback.

    TLC retains the network-bag request through UpdateTerm/term fsync, then
    HandleRVReq consumes it. C++ observes the originating send and correlated
    reply; it does not expose a model-equivalent receive bag.
    """
    states = third_peer_states(document)
    staged = [i for i, values in enumerate(states)
              if isinstance(node(values, "pending", "s3"), dict)
              and node(values, "pending", "s3")["target"]["votedFor"]
              == model("s2")]
    require(len(staged) == 1 and staged[0] > 0,
            "model third-peer vote staging boundary missing or duplicated")
    consumed_at = staged[0]
    expected = {"mtype": "RVReq", "msrc": model("s2"),
                "mdst": model("s3"), "mterm": 2,
                "mlastIdx": 1, "mlastTerm": 1}
    for index, values in enumerate(states):
        requests = third_peer_vote_requests(values)
        if index < consumed_at:
            require(len(requests) == 1 and requests[0][1] == 1 and
                    requests[0][0] == expected,
                    "model vote request changed or disappeared before handling")
        else:
            require(not requests,
                    "model vote request survived HandleRVReq consumption")

    pending = node(states[consumed_at], "pending", "s3")
    after = pending.get("after")
    require(isinstance(after, dict) and set(after) == {"$set"} and
            len(after["$set"]) == 1 and
            after["$set"][0] == granted_replies(next(
                values for values in states if granted_replies(values)))[0],
            "model deferred vote reply diverged from durable release")

    synced_b, staged_c, synced_c = rows[-7], rows[-3], rows[-2]
    outbound = [msg for msg in synced_b["effects"]["send"]
                if msg.get("kind") == "RVReq" and msg.get("to") == "c"]
    require(len(outbound) == 1 and
            all(outbound[0].get(key) == value
                for key, value in (("candidate", "b"), ("term", 2),
                                   ("last_index", 1), ("last_term", 1))) and
            type(outbound[0].get("request_id")) is int and
            outbound[0]["request_id"] > 0 and
            staged_c["effects"]["send"] == [] and
            len(synced_c["effects"]["send"]) == 1 and
            synced_c["effects"]["send"][0].get("request_id")
            == outbound[0]["request_id"],
            "compiled third-peer vote request or reply causality diverged")
    return consumed_at


def check_request_conservation_mutations(document, rows):
    """Reject changes to candidate/log freshness, bag conservation and ID."""
    mutants = []
    for field, value in (("msrc", model("s1")),
                         ("mterm", 1), ("mlastIdx", 0),
                         ("mlastTerm", 0)):
        changed = copy.deepcopy(document)
        third_peer_vote_requests(third_peer_states(changed)[0])[0][0][field] = value
        mutants.append((changed, rows))
    changed = copy.deepcopy(document)
    pair = next(pair for pair in raw_messages(third_peer_states(changed)[0]["net"])
                if pair[0].get("mtype") == "RVReq"
                and pair[0].get("msrc") == model("s2")
                and pair[0].get("mdst") == model("s3"))
    pair[1] += 1
    mutants.append((changed, rows))

    changed = copy.deepcopy(document)
    states = third_peer_states(changed)
    first = third_peer_vote_requests(states[0])[0][0]
    remaining = [pair for pair in raw_messages(states[0]["net"])
                 if pair[0] is not first]
    states[0]["net"] = ({"$function": remaining} if remaining
                        else {"$sequence": []})
    mutants.append((changed, rows))

    changed = copy.deepcopy(document)
    states = third_peer_states(changed)
    staged = next(values for values in states
                  if isinstance(node(values, "pending", "s3"), dict)
                  and node(values, "pending", "s3")["target"]["votedFor"]
                  == model("s2"))
    node(staged, "pending", "s3")["after"]["$set"][0]["mterm"] = 1
    mutants.append((changed, rows))

    for field, value in (("candidate", "a"), ("term", 1),
                         ("last_index", 0), ("last_term", 0),
                         ("request_id", -1)):
        changed = copy.deepcopy(rows)
        request = next(msg for msg in changed[-7]["effects"]["send"]
                       if msg.get("to") == "c")
        request[field] = value
        mutants.append((document, changed))
    changed = copy.deepcopy(rows)
    changed[-2]["effects"]["send"][0]["request_id"] = -1
    mutants.append((document, changed))

    for index, (doc, core) in enumerate(mutants, 1):
        try:
            check_request_conservation(doc, core)
        except (ProjectionMismatch, KeyError, TypeError, ValueError, IndexError):
            continue
        raise ProjectionMismatch(
            f"third-peer vote causality mutation {index} was not rejected")
    return len(mutants)


def model_term_vote_observation(values):
    """Observed in-memory and fsynced term/vote, without hiding disk writes."""
    memory = disk_projection(node(values, "mem", "s3"))
    durable = disk_projection(node(values, "disk", "s3"))
    return ((memory["term"], memory["votedFor"]),
            (durable["term"], durable["votedFor"]))


def compiled_term_vote_observation(row):
    post = row["post"]
    durable = post["durable"]
    return ((post["term"], post["votedFor"]),
            (durable["term"], durable["votedFor"]))


def check_identity_projection_counterexample(document, rows):
    """Falsify a *literal* full-state stuttering claim on this witness.

    A valid refinement could use a different abstraction or a compound model
    action; these assertions do not establish or disprove such a refinement.
    """
    states = third_peer_states(document)
    observed = [model_term_vote_observation(s) for s in states]
    staged, synced = rows[-3:-1]
    combined_stage = compiled_term_vote_observation(staged)
    combined_fsync = compiled_term_vote_observation(synced)

    require(((2, None), (2, None)) in observed,
            "model term-only durable observation disappeared")
    require(combined_stage == ((2, "b"), (0, None)),
            "compiled combined-write staging observation changed")
    require(combined_stage not in observed,
            "literal full-state relation unexpectedly matched combined staging")
    require(combined_fsync == ((2, "b"), (2, "b")) and
            combined_fsync in observed,
            "model and compiled fully durable vote endpoint diverged")

    model_failed = disk_projection(node(states[0], "disk", "s1"))
    compiled_failed = rows[-5]["post"]["durable"]
    require((model_failed["term"], model_failed["votedFor"]) == (2, None)
            and (compiled_failed["term"], compiled_failed["votedFor"]) == (1, "a"),
            "failed term-only and combined-write durable outcomes conflated")
    return len(observed)


def check_identity_projection_mutations(document, rows):
    mutants = []
    changed = copy.deepcopy(document)
    first = third_peer_states(changed)[0]
    node(first, "mem", "s3")["term"] = 2
    node(first, "mem", "s3")["votedFor"] = model("s2")
    mutants.append((changed, rows))  # Invent a model combined-staging state.

    changed = copy.deepcopy(document)
    for values in third_peer_states(changed):
        durable = node(values, "disk", "s3")
        if durable["term"] == 2 and durable["votedFor"] == model("Nil"):
            durable["term"] = 1
    mutants.append((changed, rows))  # Erase the model term-only disk state.

    changed = copy.deepcopy(document)
    for values in third_peer_states(changed):
        memory = node(values, "mem", "s3")
        durable = node(values, "disk", "s3")
        if (memory["term"] == 2 and memory["votedFor"] == model("s2")
                and durable["term"] == 2
                and durable["votedFor"] == model("s2")):
            memory["votedFor"] = model("Nil")
    mutants.append((changed, rows))  # Erase the matching durable endpoint.

    for offset, location, field, value in (
        (-3, ("post",), "votedFor", None),
        (-3, ("post", "durable"), "term", 2),
        (-2, ("post", "durable"), "votedFor", None),
        (-2, ("post",), "term", 1),
        (-5, ("post", "durable"), "term", 2),
    ):
        changed = copy.deepcopy(rows)
        target = changed[offset]
        for key in location:
            target = target[key]
        target[field] = value
        mutants.append((document, changed))

    for index, (doc, core) in enumerate(mutants, 1):
        try:
            check_identity_projection_counterexample(doc, core)
        except (ProjectionMismatch, KeyError, TypeError, ValueError, IndexError):
            continue
        raise ProjectionMismatch(
            f"literal projection counterexample mutation {index} escaped")
    return len(mutants)



def wire_only_grant_prefix(document, rows):
    """Compare only positive vote-grant events, deliberately ignoring disk.

    TLC net-bag appearance and C++ send effects are compared only at this
    selected vote-fsync prefix. This is not a recovery-safe refinement map.
    """
    states = third_peer_states(document)
    disks = [disk_projection(node(s, "disk", "s3")) for s in states]
    vote_at = next((i for i, disk in enumerate(disks)
                    if disk["term"] == 2 and disk["votedFor"] == "b"), None)
    require(vote_at is not None and vote_at > 0,
            "wire prefix has no durable model vote boundary")
    model_events = [
        tuple((msg.get("mdst"), msg.get("mterm"), msg.get("granted"))
              for msg in third_peer_positive_replies(state))
        for state in states[:vote_at + 1]
    ]
    compiled_events = [
        tuple((msg.get("to"), msg.get("term"), msg.get("positive"))
              for msg in row["effects"]["send"]
              if msg.get("kind") == "RVResp" and msg.get("positive") is True)
        for row in rows[-3:-1]
    ]
    require(model_events[:-1] == [()] * vote_at and
            model_events[-1] == ((model("s2"), 2, True),) and
            compiled_events == [(), (("b", 2, True),)],
            "bounded wire-only grant prefix diverged")
    return model_events, compiled_events


def check_wire_projection_limits(document, rows):
    """Demonstrate that a passing wire-only prefix can hide real defects."""
    baseline = wire_only_grant_prefix(document, rows)
    blind_spots = []

    changed = copy.deepcopy(document)
    term_only = [node(s, "disk", "s3") for s in third_peer_states(changed)
                 if (disk_projection(node(s, "disk", "s3"))["term"] == 2
                     and disk_projection(node(s, "disk", "s3"))["votedFor"] is None)]
    require(bool(term_only), "missing model term-only fsync to challenge")
    for disk in term_only:
        disk["term"] = 1
    blind_spots.append((changed, rows))  # Wire view hides term-only fsync.

    changed = copy.deepcopy(rows)
    changed[-2]["effects"]["send"][0]["request_id"] = -1
    blind_spots.append((document, changed))  # Wire view hides correlation ID.

    changed = copy.deepcopy(rows)
    changed[-5]["post"]["durable"]["term"] = 2
    blind_spots.append((document, changed))  # Wire view hides failed disk.

    for index, (doc, core) in enumerate(blind_spots, 1):
        require(wire_only_grant_prefix(doc, core) == baseline,
                f"wire-only blind spot {index} unexpectedly became observable")
        try:
            check_relation(doc, core)
        except ProjectionMismatch:
            continue
        raise ProjectionMismatch(
            f"strict durable/identity relation accepted wire blind spot {index}")
    return len(blind_spots)


def check_wire_projection_mutations(document, rows):
    """The weak projection still rejects forged or premature visible grants."""
    mutants = []
    changed = copy.deepcopy(document)
    states = third_peer_states(changed)
    vote_at = next(i for i, state in enumerate(states)
                   if disk_projection(node(state, "disk", "s3"))["votedFor"] == "b")
    grant = copy.deepcopy(granted_replies(states[vote_at])[0])
    before = states[vote_at - 1]["net"]
    if before == {"$sequence": []}:
        states[vote_at - 1]["net"] = {"$function": [[grant, 1]]}
    else:
        before["$function"].append([grant, 1])
    mutants.append((changed, rows))  # Early model grant.

    changed = copy.deepcopy(document)
    states = third_peer_states(changed)
    vote_at = next(i for i, state in enumerate(states)
                   if disk_projection(node(state, "disk", "s3"))["votedFor"] == "b")
    granted_replies(states[vote_at])[0]["mterm"] = 1
    mutants.append((changed, rows))  # Wrong model grant term.

    for offset, field, value in (
        (-2, "to", "a"),
        (-2, "term", 1),
    ):
        changed = copy.deepcopy(rows)
        changed[offset]["effects"]["send"][0][field] = value
        mutants.append((document, changed))

    changed = copy.deepcopy(rows)
    changed[-3]["effects"]["send"] = [
        {"kind": "RVResp", "to": "b", "term": 2, "positive": True}
    ]
    mutants.append((document, changed))  # Premature compiled grant.

    for index, (doc, core) in enumerate(mutants, 1):
        try:
            wire_only_grant_prefix(doc, core)
        except ProjectionMismatch:
            continue
        raise ProjectionMismatch(f"wire projection mutation {index} escaped")
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
    handled_at = check_request_conservation(document, rows)
    request_mutations = check_request_conservation_mutations(document, rows)
    mutations = check_mutations(document, rows)
    observations = check_identity_projection_counterexample(document, rows)
    counterexample_mutations = check_identity_projection_mutations(document, rows)
    blind_spots = check_wire_projection_limits(document, rows)
    wire_mutations = check_wire_projection_mutations(document, rows)
    print(f"Bounded split/combined relation: {transitions} ordered model fsyncs, "
          f"{mutations} negative mutations rejected.")
    print(f"Literal full-state stuttering counterexample: {observations} model "
          f"observations, {counterexample_mutations} negative mutations rejected.")
    print(f"Wire-only prefix: {wire_mutations} negative mutations rejected, "
          f"{blind_spots} disk/identity blind spots exposed.")
    print(f"Third-peer RequestVote consumed at split-state {handled_at}; "
          f"{request_mutations} causality mutations rejected.")
    print("Two model term-only boundaries are unmatched; no full refinement.")


if __name__ == "__main__":
    try:
        main()
    except (ProjectionMismatch, OSError, ValueError, KeyError, TypeError,
            IndexError, subprocess.TimeoutExpired) as exc:
        sys.exit(f"TLC split-fsync relation FAIL: {exc}")
