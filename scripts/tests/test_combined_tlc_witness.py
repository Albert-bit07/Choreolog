#!/usr/bin/env python3
"""Check exported bounded TLC witnesses, not C++ refinement or unbounded safety."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tlc_witness_prefix import NODES, REQUIRED, canonical, message_bag, node_function


class WitnessMismatch(ValueError):
    """A typed model behavior lacks required provenance or semantic evidence."""


def require(condition, reason):
    if not condition:
        raise WitnessMismatch(reason)


def source_matches(actual, path):
    return (
        isinstance(actual, dict)
        and actual.get("name") == path.name
        and actual.get("sha256") == hashlib.sha256(path.read_bytes()).hexdigest()
    )


def model_set(value):
    require(isinstance(value, dict) and set(value) == {"$set"}, "expected typed set")
    require(isinstance(value["$set"], list), "malformed typed set")
    return value["$set"]


def leader_terms(values):
    mapping = values["ghostLeaders"]
    require(isinstance(mapping, dict), "invalid ghostLeaders")
    if set(mapping) == {"$sequence"}:
        # TLC prints a function over 1..MaxTerm as a sequence.
        entries = mapping["$sequence"]
        require(isinstance(entries, list) and len(entries) == 2,
                "expected two leader terms")
        return {1: model_set(entries[0]), 2: model_set(entries[1])}
    require(set(mapping) == {"$function"}, "invalid ghostLeaders mapping")
    entries = mapping["$function"]
    require(isinstance(entries, list) and len(entries) == 2,
            "expected two leader terms")
    terms = {}
    for key, value in entries:
        require(type(key) is int and key in (1, 2) and key not in terms,
                "unexpected leader term")
        terms[key] = model_set(value)
    require(set(terms) == {1, 2}, "incomplete leader terms")
    return terms


def leader_for(values, term):
    selected = leader_terms(values)[term]
    require(len(selected) == 1 and isinstance(selected[0], dict)
            and set(selected[0]) == {"$model"}, "term must have one elected leader")
    actor = selected[0]["$model"]
    require(actor in NODES, "unknown elected leader")
    return actor



def check_higher_term_envelope(states, primary, successor):
    """Check actual model term-fsync then vote-fsync, not a C++ lockstep replay."""
    phases = {}
    for offset, (old, new) in enumerate(zip(states, states[1:]), 1):
        before, after = old["variables"], new["variables"]
        bm, am = node_function(before["mem"])[primary], node_function(after["mem"])[primary]
        bd, ad = node_function(before["disk"])[primary], node_function(after["disk"])[primary]
        bp, ap = node_function(before["pending"])[primary], node_function(after["pending"])[primary]
        if bm["term"] == 1 and am["term"] == 2:
            require("term_stage" not in phases and am["votedFor"] == {"$model": "Nil"}
                    and bd == ad and bd["term"] == 1
                    and isinstance(ap, dict) and "target" in ap
                    and ap["target"]["term"] == 2
                    and ap["target"]["votedFor"] == {"$model": "Nil"}
                    and ap["after"] == {"$set": []}, "invalid higher-term stage")
            phases["term_stage"] = offset
        if bd["term"] == 1 and ad["term"] == 2:
            require("term_sync" not in phases
                    and ad["votedFor"] == {"$model": "Nil"}
                    and isinstance(bp, dict) and "target" in bp
                    and bp["target"]["term"] == 2
                    and bp["target"]["votedFor"] == {"$model": "Nil"}
                    and ap == {"$model": "Nil"}
                    and am["votedFor"] == {"$model": "Nil"}
                    and message_bag(before["net"]) == message_bag(after["net"]),
                    "invalid term fsync")
            phases["term_sync"] = offset
        if (bm["votedFor"] == {"$model": "Nil"}
                and am["votedFor"] == {"$model": successor} and am["term"] == 2):
            require("vote_stage" not in phases and bd == ad and ad["term"] == 2
                    and ad["votedFor"] == {"$model": "Nil"}
                    and isinstance(ap, dict) and "target" in ap
                    and ap["target"]["votedFor"] == {"$model": successor}
                    and ap["target"]["term"] == 2
                    and len(ap["after"]["$set"]) == 1,
                    "invalid higher-term vote stage")
            reply = ap["after"]["$set"][0]
            require(reply["mtype"] == "RVResp" and reply["granted"] is True
                    and reply["mterm"] == 2
                    and reply["msrc"] == {"$model": primary}
                    and reply["mdst"] == {"$model": successor},
                    "invalid deferred higher-term reply")
            phases["vote_stage"] = offset
        if (bd["votedFor"] == {"$model": "Nil"}
                and ad["votedFor"] == {"$model": successor} and ad["term"] == 2):
            require("vote_sync" not in phases
                    and isinstance(bp, dict) and "target" in bp
                    and bp["target"]["votedFor"] == {"$model": successor}
                    and ap == {"$model": "Nil"} and len(bp["after"]["$set"]) == 1,
                    "invalid vote fsync")
            reply = bp["after"]["$set"][0]
            expected = dict(message_bag(before["net"]))
            key = canonical(reply)
            expected[key] = expected.get(key, 0) + 1
            require(message_bag(after["net"]) == expected,
                    "vote reply was not released exactly at fsync")
            phases["vote_sync"] = offset
    require(set(phases) == {"term_stage", "term_sync", "vote_stage", "vote_sync"},
            "missing higher-term split-write phase")
    require(phases["term_stage"] < phases["term_sync"] <
            phases["vote_stage"] < phases["vote_sync"],
            "higher-term term/vote durability order diverged")
    # Validate the actual action envelope, not only the four disk endpoints.
    # UpdateTerm can stage term/vote and change the local role/vote set; its
    # fsync cannot change memory. HandleRVReq consumes one request and stages
    # the reply; only the subsequent fsync may release that reply.
    allowed = {
        "term_stage": {"mem", "pending", "role", "votesGranted"},
        "term_sync": {"disk", "pending"},
        "vote_stage": {"mem", "pending", "net"},
        "vote_sync": {"disk", "pending", "net"},
    }
    for phase, changes in allowed.items():
        before = states[phases[phase] - 1]["variables"]
        after = states[phases[phase]]["variables"]
        unexpected = [field for field in sorted(REQUIRED - changes)
                      if canonical(before[field]) != canonical(after[field])]
        require(not unexpected, f"{phase} modified unrelated model variables: {unexpected}")
        for field in ("mem", "disk", "pending", "role", "votesGranted"):
            if field not in changes:
                continue
            old_nodes, new_nodes = node_function(before[field]), node_function(after[field])
            require(all(canonical(old_nodes[peer]) == canonical(new_nodes[peer])
                        for peer in NODES if peer != primary),
                    f"{phase} modified another peer's {field}")

    # The staged higher-term vote must consume exactly its own RVReq, not
    # fabricate an early RVResp or discard another in-flight message.
    before_vote = states[phases["vote_stage"] - 1]["variables"]
    after_vote = states[phases["vote_stage"]]["variables"]
    bag = before_vote["net"]
    require(isinstance(bag, dict) and set(bag) == {"$function"},
            "higher-term vote lacks a typed request bag")
    requests = [
        message for message, count in bag["$function"]
        if isinstance(message, dict) and message.get("mtype") == "RVReq"
        and message.get("msrc") == {"$model": successor}
        and message.get("mdst") == {"$model": primary}
        and message.get("mterm") == 2 and type(count) is int and count > 0
    ]
    require(len(requests) == 1, "ambiguous higher-term vote request identity")
    expected = dict(message_bag(before_vote["net"]))
    request_key = canonical(requests[0])
    require(request_key in expected, "missing higher-term request")
    if expected[request_key] == 1:
        del expected[request_key]
    else:
        expected[request_key] -= 1
    require(message_bag(after_vote["net"]) == expected,
            "higher-term vote stage changed network beyond consuming RVReq")
    return phases


def verify(document, invariant, model, config, crash=False):
    require(isinstance(document, dict) and document.get("v") == 1
            and document.get("kind") == "tlc-text-behavior", "invalid typed export")
    require(document.get("invariant") == invariant, "wrong witness invariant")
    require(source_matches(document.get("model"), model), "model hash mismatch")
    require(source_matches(document.get("config"), config), "config hash mismatch")
    states = document.get("states")
    require(isinstance(states, list) and len(states) >= 10, "vacuous witness")
    for number, state in enumerate(states, 1):
        require(isinstance(state, dict) and state.get("state") == number,
                "noncontiguous witness state")
        require(isinstance(state.get("variables"), dict)
                and set(state["variables"]) == REQUIRED, "incomplete model state")
        for field in ("alive", "disk", "role"):
            node_function(state["variables"][field])
    first, last = states[0]["variables"], states[-1]["variables"]
    require(all(node_function(first["alive"])[actor] is True for actor in NODES),
            "initial nodes are not all alive")
    primary, successor = leader_for(last, 1), leader_for(last, 2)
    require(primary != successor, "higher term did not change leaders")
    require(node_function(last["role"])[successor] == {"$model": "Leader"},
            "successor is not leader")
    durable = node_function(last["disk"])[successor]
    log = durable["log"]
    require(isinstance(log, dict) and set(log) == {"$sequence"}
            and len(log["$sequence"]) == 1, "successor lacks durable entry")
    committed = model_set(last["ghostCommitted"])
    require(any(isinstance(item, dict) and item.get("idx") == 1
                and canonical(item.get("entry")) == canonical(log["$sequence"][0])
                for item in committed), "durable entry differs from committed entry")
    require(any(model_set(state["variables"]["ghostCommitted"])
                and not leader_terms(state["variables"])[2]
                for state in states), "no committed entry before term-two leadership")
    if crash:
        require(node_function(last["alive"])[primary] is False,
                "former leader is not offline")
        old_disk = node_function(last["disk"])[primary]
        require(old_disk["commit"] == 1 and old_disk["applied"] == 1,
                "former leader lost durable commit or apply")
        require(bool(model_set(last["ghostApplied"])), "durable apply effect absent")
        require(any(
            node_function(before["variables"]["alive"])[primary] is True
            and node_function(after["variables"]["alive"])[primary] is False
            and node_function(before["variables"]["role"])[successor]
                == {"$model": "Leader"}
            and node_function(before["variables"]["disk"])[primary]["applied"] == 1
            for before, after in zip(states, states[1:])
        ), "no post-handoff offline transition after durable apply")
    check_higher_term_envelope(states, primary, successor)
    return primary, successor


def check_mutations(document, invariant, model, config, crash):
    primary, successor = verify(document, invariant, model, config, crash)
    def mutate(mutator):
        changed = copy.deepcopy(document)
        mutator(changed)
        try:
            verify(changed, invariant, model, config, crash)
        except WitnessMismatch:
            return
        raise WitnessMismatch("negative witness mutation was accepted")
    def final(changed):
        return changed["states"][-1]["variables"]
    def replace_node(d, field, actor, value):
        pairs = final(d)[field]["$function"]
        for pair in pairs:
            if pair[0] == {"$model": actor}:
                pair[1] = value
                return
        raise WitnessMismatch("missing node in mutation fixture")

    def remove_term_two_leader(d):
        mapping = final(d)["ghostLeaders"]
        if "$sequence" in mapping:
            mapping["$sequence"][1] = {"$set": []}
        else:
            for pair in mapping["$function"]:
                if pair[0] == 2:
                    pair[1] = {"$set": []}
                    return
            raise WitnessMismatch("term two missing from mutation fixture")

    mutations = [
        lambda d: final(d).update(ghostCommitted={"$set": []}),
        remove_term_two_leader,
        lambda d: replace_node(d, "role", successor, {"$model": "Follower"}),
        lambda d: node_function(final(d)["disk"])[successor]["log"].update(
            {"$sequence": []}),
        lambda d: d["model"].update(sha256="0" * 64),
        lambda d: final(d).update(unexpected=1),
    ]
    if crash:
        mutations.extend([
            lambda d: replace_node(d, "alive", primary, True),
            lambda d: node_function(final(d)["disk"])[primary].update(commit=0),
            lambda d: node_function(final(d)["disk"])[primary].update(applied=0),
            lambda d: final(d).update(ghostApplied={"$set": []}),
        ])

    # Mutate the four *actual* higher-term phases in both model witnesses.
    # Each must be rejected independently; none is silently collapsed into
    # the C++ combined term/vote persistence callback.
    phases = check_higher_term_envelope(document["states"], primary, successor)

    def phase_node(d, phase, field):
        pairs = d["states"][phases[phase]]["variables"][field]["$function"]
        for pair in pairs:
            if pair[0] == {"$model": primary}:
                return pair[1]
        raise WitnessMismatch("missing primary node in phase mutation")

    mutations.extend([
        lambda d: phase_node(d, "term_stage", "pending")["target"].update(term=3),
        lambda d: phase_node(d, "term_stage", "disk").update(term=2),
        lambda d: phase_node(d, "term_sync", "disk").update(
            votedFor={"$model": successor}),
        lambda d: phase_node(d, "vote_stage", "pending").update(after={"$set": []}),
        lambda d: phase_node(d, "vote_stage", "disk").update(
            votedFor={"$model": successor}),
        lambda d: phase_node(d, "vote_sync", "disk").update(
            votedFor={"$model": "Nil"}),
        lambda d: d["states"][phases["vote_sync"]]["variables"].update(
            net=copy.deepcopy(d["states"][phases["vote_stage"]]["variables"]["net"])),
        lambda d: phase_node(d, "vote_stage", "pending")["target"].update(
            votedFor={"$model": "Nil"}),
        # Exercise phase-level side-effect guards, including other-peer state
        # and premature or incorrect network changes before vote fsync.
        lambda d: d["states"][phases["term_stage"]]["variables"].update(epochCtr=99),
        lambda d: node_function(d["states"][phases["term_stage"]]["variables"][
            "mem"])[next(n for n in NODES if n != primary)].update(term=99),
        lambda d: phase_node(d, "term_sync", "mem").update(term=99),
        lambda d: node_function(d["states"][phases["term_sync"]]["variables"][
            "disk"])[next(n for n in NODES if n != primary)].update(term=99),
        lambda d: phase_node(d, "vote_stage", "role").update({"$model": "Leader"}),
        lambda d: d["states"][phases["vote_stage"]]["variables"].update(
            net=copy.deepcopy(d["states"][phases["vote_stage"] - 1]["variables"]["net"])),
        lambda d: phase_node(d, "vote_sync", "mem").update(term=99),
        lambda d: node_function(d["states"][phases["vote_sync"]]["variables"][
            "disk"])[next(n for n in NODES if n != primary)].update(term=99),
    ])

    for index, change in enumerate(mutations, 1):
        try:
            mutate(change)
        except WitnessMismatch as error:
            raise WitnessMismatch(f"negative mutation {index}: {error}") from error
    return len(mutations)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", required=True, type=Path)
    parser.add_argument("--handoff-config", required=True, type=Path)
    parser.add_argument("--crash-config", required=True, type=Path)
    parser.add_argument("--handoff", required=True, type=Path)
    parser.add_argument("--crash", required=True, type=Path)
    args = parser.parse_args()
    for path, invariant, config, crash in (
        (args.handoff, "NoCombinedHandoffWitness", args.handoff_config, False),
        (args.crash, "NoCombinedCrashAfterApplyWitness", args.crash_config, True),
    ):
        document = json.loads(path.read_text(encoding="utf-8"))
        count = check_mutations(document, invariant, args.model, config, crash)
        print(f"PASS {path.name}: {len(document['states'])} states, {count} rejected mutations")
    print("Bounded model-witness semantics only; NO compiled C++ refinement established.")


if __name__ == "__main__":
    try:
        main()
    except (WitnessMismatch, ValueError, KeyError, TypeError, OSError) as error:
        print(f"combined witness FAIL: {error}", file=sys.stderr)
        sys.exit(1)
