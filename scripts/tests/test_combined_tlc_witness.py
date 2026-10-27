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
from tlc_witness_prefix import NODES, REQUIRED, canonical, node_function


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
    return value["$set"]


def leader_for(values, term):
    mapping = values["ghostLeaders"]
    require(isinstance(mapping, dict) and set(mapping) == {"$function"},
            "invalid ghostLeaders mapping")
    entries = mapping["$function"]
    require(isinstance(entries, list) and len(entries) == 2, "expected two leader terms")
    terms = {}
    for key, value in entries:
        require(type(key) is int and key in (1, 2) and key not in terms,
                "unexpected leader term")
        terms[key] = model_set(value)
    require(set(terms) == {1, 2}, "incomplete leader terms")
    selected = terms[term]
    require(len(selected) == 1 and isinstance(selected[0], dict)
            and set(selected[0]) == {"$model"}, "term must have one elected leader")
    actor = selected[0]["$model"]
    require(actor in NODES, "unknown elected leader")
    return actor


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
                and not model_set(dict(state["variables"]["ghostLeaders"]["$function"])[2])
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
    mutations = [
        lambda d: final(d).update(ghostCommitted={"$set": []}),
        lambda d: dict(final(d)["ghostLeaders"]["$function"])[2].update(**{"$set": []}),
        lambda d: node_function(final(d)["role"])[successor].update(**{"$model": "Follower"}),
        lambda d: node_function(final(d)["disk"])[successor]["log"].update(**{"$sequence": []}),
        lambda d: d["model"].update(sha256="0" * 64),
        lambda d: final(d).update(unexpected=1),
    ]
    if crash:
        mutations.extend([
            lambda d: node_function(final(d)["alive"]).__setitem__(primary, True),
            lambda d: node_function(final(d)["disk"])[primary].update(commit=0),
            lambda d: node_function(final(d)["disk"])[primary].update(applied=0),
            lambda d: final(d).update(ghostApplied={"$set": []}),
        ])
    # node_function returns a new dictionary: mutate the actual typed pairs.
    def replace_node(d, field, actor, value):
        pairs = final(d)[field]["$function"]
        for pair in pairs:
            if pair[0] == {"$model": actor}:
                pair[1] = value
                return
        raise WitnessMismatch("missing node in mutation fixture")
    mutations[2] = lambda d: replace_node(d, "role", successor, {"$model": "Follower"})
    if crash:
        mutations[6] = lambda d: replace_node(d, "alive", primary, True)
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
