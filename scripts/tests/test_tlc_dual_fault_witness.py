#!/usr/bin/env python3
"""Fail-closed semantic checks for one bounded, TLC-generated dual-write witness.

This checks typed model states and selected transition effects, not compiled
C++ correspondence, physical disk atomicity, or unrestricted Raft safety.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tlc_witness_prefix import REQUIRED, canonical, message_bag, node_function


class DualWitnessMismatch(ValueError):
    """The selected bounded dual-persistence behavior has diverged."""


def demand(condition, reason):
    if not condition:
        raise DualWitnessMismatch(reason)


def node(values, field, actor):
    return node_function(values[field])[actor]


def model(name):
    return {"$model": name}


def bag_has_vote_request(values, src, term):
    return any(
        isinstance(msg, dict) and msg.get("mtype") == "RVReq"
        and msg.get("msrc") == model(src) and msg.get("mterm") == term
        for msg, count in raw_messages(values["net"]) if count > 0
    )


def raw_messages(value):
    if value == {"$sequence": []}:
        return []
    demand(isinstance(value, dict) and set(value) == {"$function"},
           "invalid typed network bag")
    result = value["$function"]
    demand(all(isinstance(pair, list) and len(pair) == 2
               and type(pair[1]) is int and pair[1] > 0 for pair in result),
           "invalid network multiplicity")
    message_bag(value)
    return result


def phase_edges(states):
    edges = {}
    for old, new in zip(states, states[1:]):
        before, after = old["variables"], new["variables"]
        a, b = before["faultPhase"], after["faultPhase"]
        demand(type(a) is int and type(b) is int and
               0 <= a <= b <= 5 and b - a <= 1, "invalid fault-phase progression")
        if b != a:
            demand(b not in edges, "duplicate fault-phase transition")
            edges[b] = (before, after)
    demand(set(edges) == {1, 2, 3, 4, 5},
           "missing actual ordered persistence-failure/retry transitions")
    return edges


def verify(document, model_path, config_path, log_path):
    demand(isinstance(document, dict) and document.get("v") == 1 and
           document.get("kind") == "tlc-text-behavior" and
           document.get("invariant") == "NoDualFailureAlternativeQuorumWitness",
           "wrong typed TLC behavior or invariant")
    for field, path in (("model", model_path), ("config", config_path)):
        source = document.get(field)
        demand(isinstance(source, dict) and source.get("name") == path.name and
               source.get("sha256") == hashlib.sha256(path.read_bytes()).hexdigest(),
               f"{field} provenance mismatch")
    demand(document.get("log_sha256") ==
           hashlib.sha256(log_path.read_bytes()).hexdigest(),
           "TLC log provenance mismatch")
    states = document.get("states")
    demand(isinstance(states, list) and len(states) >= 20,
           "missing nonvacuous dual-fault behavior")
    for number, state in enumerate(states, 1):
        demand(isinstance(state, dict) and state.get("state") == number and
               isinstance(state.get("variables"), dict) and
               set(state["variables"]) == REQUIRED | {"faultPhase"},
               "malformed or incomplete typed model state")
        for field in ("alive", "disk", "pending", "role", "mem", "votesGranted"):
            node_function(state["variables"][field])
    edges = phase_edges(states)
    initial = states[0]["variables"]
    final = states[-1]["variables"]
    demand(initial["faultPhase"] == 0 and final["faultPhase"] == 5,
           "witness did not start and finish at required phases")
    demand(all(node(initial, "alive", actor) is True for actor in ("s1", "s2", "s3")),
           "unexpected initially offline peer")

    # Candidate s2 must lose its unacknowledged self-vote write. Its old
    # durable record survives, and no term-two RequestVote may escape.
    before, after = edges[1]
    pending = node(before, "pending", "s2")
    demand(isinstance(pending, dict) and
           pending["target"]["term"] == 2 and
           pending["target"]["votedFor"] == model("s2") and
           node(before, "alive", "s2") is True and
           node(after, "alive", "s2") is False and
           node(before, "disk", "s2") == node(after, "disk", "s2") and
           node(after, "disk", "s2")["term"] == 1 and
           node(after, "pending", "s2") == model("Nil") and
           not bag_has_vote_request(after, "s2", 2),
           "candidate failed write released or retained an undurable vote")
    old_disk = node(after, "disk", "s2")
    demand(node(after, "mem", "s2") ==
           {field: old_disk[field] for field in ("term", "votedFor", "log")},
           "candidate failure did not restore acknowledged memory")

    before, after = edges[2]
    demand(node(before, "alive", "s2") is False and
           node(after, "alive", "s2") is True and
           node(after, "disk", "s2") == old_disk and
           node(after, "pending", "s2") == model("Nil") and
           not bag_has_vote_request(after, "s2", 2),
           "candidate restart fabricated durable state or outbound vote")

    before, after = edges[3]
    retry = node(after, "pending", "s2")
    demand(node(before, "disk", "s2") == old_disk and
           node(after, "disk", "s2") == old_disk and
           isinstance(retry, dict) and retry["target"]["term"] == 2 and
           retry["target"]["votedFor"] == model("s2") and
           node(after, "role", "s2") == model("Candidate") and
           message_bag(before["net"]) == message_bag(after["net"]) and
           not bag_has_vote_request(after, "s2", 2),
           "candidate retry sent before its durable self-vote")

    before, after = edges[4]
    durable = node(after, "disk", "s2")
    demand(node(before, "disk", "s2") == old_disk and
           node(before, "pending", "s2") == retry and
           durable["term"] == 2 and durable["votedFor"] == model("s2") and
           node(after, "pending", "s2") == model("Nil"),
           "retry self-vote fsync did not install the durable vote")
    previous = message_bag(before["net"])
    released = message_bag(after["net"])
    expected = dict(previous)
    requests = []
    for msg in retry["after"]["$set"]:
        demand(msg.get("mtype") == "RVReq" and msg.get("msrc") == model("s2")
               and msg.get("mterm") == 2 and msg.get("mdst") in
               (model("s1"), model("s3")), "incorrect deferred retry request")
        requests.append(msg)
        key = canonical(msg)
        expected[key] = expected.get(key, 0) + 1
    demand(len(requests) == 2 and
           {msg["mdst"]["$model"] for msg in requests} == {"s1", "s3"} and
           released == expected,
           "retry fsync did not release exactly two peer vote requests")

    # Follower s1's failed grant preserves its *previously fsynced*
    # term-two, nil-vote record. This is a different crash checkpoint
    # from the candidate's failed combined write.
    before, after = edges[5]
    old_primary = node(before, "disk", "s1")
    staged = node(before, "pending", "s1")
    demand(old_primary["term"] == 2 and old_primary["votedFor"] == model("Nil") and
           isinstance(staged, dict) and staged["target"]["term"] == 2 and
           staged["target"]["votedFor"] == model("s2") and
           node(before, "alive", "s1") is True and
           node(after, "alive", "s1") is False and
           node(after, "disk", "s1") == old_primary and
           node(after, "pending", "s1") == model("Nil"),
           "follower failure acknowledged an undurable grant")
    demand(not any(msg.get("mtype") == "RVResp" and
                   msg.get("msrc") == model("s1") and
                   msg.get("mdst") == model("s2") and
                   msg.get("mterm") == 2 and msg.get("granted") is True
                   for msg, _ in raw_messages(after["net"])),
           "failed follower released a positive vote reply")

    demand(node(final, "role", "s2") == model("Leader") and
           node(final, "alive", "s1") is False and
           node(final, "disk", "s1") == old_primary and
           node(final, "disk", "s2")["term"] == 2 and
           node(final, "disk", "s2")["votedFor"] == model("s2") and
           node(final, "disk", "s3")["term"] == 2 and
           node(final, "disk", "s3")["votedFor"] == model("s2") and
           canonical(node(final, "votesGranted", "s2")) ==
           canonical({"$set": [model("s2"), model("s3")]}) and
           final["ghostCommitted"] != {"$set": []},
           "alternative quorum did not durably elect the successor")

    # Require a separate term-only fsync and vote fsync at the third peer.
    # A positive reply must not appear until its grant is durable.
    phase_five = [state["variables"] for state in states
                  if state["variables"]["faultPhase"] == 5]
    third_disks = [node(values, "disk", "s3") for values in phase_five]
    demand(any(disk["term"] == 2 and disk["votedFor"] == model("Nil")
               for disk in third_disks) and
           any(disk["term"] == 2 and disk["votedFor"] == model("s2")
               for disk in third_disks),
           "third peer skipped term-only or granted-vote durable checkpoint")


def set_node(values, field, actor, value):
    pairs = values[field]["$function"]
    selected = [pair for pair in pairs if pair[0] == model(actor)]
    demand(len(selected) == 1, "missing mutation target")
    selected[0][1] = value


def test_mutations(document, model_path, config_path, log_path):
    edges = phase_edges(document["states"])
    positions = {}
    for phase, (_, after) in edges.items():
        positions[phase] = next(index for index, state in enumerate(document["states"])
                                if state["variables"] is after)

    def mutate(index, change):
        copy_doc = copy.deepcopy(document)
        values = copy_doc["states"][positions[index]]["variables"]
        change(values)
        return copy_doc

    cases = [
        mutate(1, lambda v: v.update(faultPhase=0)),
        mutate(2, lambda v: v.update(faultPhase=3)),
        mutate(3, lambda v: v.update(faultPhase=4)),
        mutate(4, lambda v: v.update(faultPhase=3)),
        mutate(5, lambda v: v.update(faultPhase=4)),
        mutate(1, lambda v: node(v, "disk", "s2").update(term=2)),
        mutate(1, lambda v: set_node(v, "alive", "s2", True)),
        mutate(3, lambda v: node(v, "pending", "s2")["target"].update(
            votedFor=model("s1"))),
        mutate(4, lambda v: node(v, "disk", "s2").update(votedFor=model("Nil"))),
        mutate(4, lambda v: v.update(net={"$sequence": []})),
        mutate(5, lambda v: node(v, "disk", "s1").update(votedFor=model("s2"))),
        mutate(5, lambda v: set_node(v, "alive", "s1", True)),
    ]
    for field, actor, value in (
        ("disk", "s3", None), ("role", "s2", model("Follower")),
        ("votesGranted", "s2", {"$set": [model("s2")]}),
    ):
        changed = copy.deepcopy(document)
        final = changed["states"][-1]["variables"]
        if field == "disk":
            node(final, field, actor)["votedFor"] = model("Nil")
        else:
            set_node(final, field, actor, value)
        cases.append(changed)
    for number, altered in enumerate(cases, 1):
        try:
            verify(altered, model_path, config_path, log_path)
        except DualWitnessMismatch:
            continue
        raise DualWitnessMismatch(f"negative mutation {number} was not detected")
    return len(cases)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--witness", type=Path, required=True)
    parser.add_argument("--model", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--log", type=Path, required=True)
    args = parser.parse_args()
    doc = json.loads(args.witness.read_text(encoding="utf-8"))
    verify(doc, args.model, args.config, args.log)
    count = test_mutations(doc, args.model, args.config, args.log)
    print(f"Validated typed dual-fault TLC witness; {count} negative mutations rejected.")
    print("Scope: selected model checkpoints only; no compiled-core refinement.")


if __name__ == "__main__":
    try:
        main()
    except (DualWitnessMismatch, OSError, ValueError, KeyError, TypeError) as exc:
        sys.exit(f"Dual-fault TLC witness FAIL: {exc}")
