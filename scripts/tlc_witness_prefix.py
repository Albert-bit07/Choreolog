#!/usr/bin/env python3
"""Conservatively translate supported TLC election-prefix steps to replay inputs.

This is an *input normalizer*, not a model-to-C++ refinement checker. Unknown
steps, malformed typed values, and unexpected side effects fail closed.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import sys

NODES = ("s1", "s2", "s3")
REQUIRED = frozenset({
    "alive", "mem", "disk", "pending", "role", "commitIndex", "nextIndex",
    "matchIndex", "votesGranted", "leaderEpoch", "net", "epochCtr",
    "ghostLeaders", "ghostCommitted", "ghostApplied",
})


class MappingError(ValueError):
    """A trace is ambiguous, unsupported, or inconsistent with the model."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise MappingError(message)


def canonical(value: object) -> str:
    """Semantic equality for typed TLA+ sets/functions; sequence order matters."""
    if isinstance(value, dict):
        if set(value) == {"$set"}:
            values = value["$set"]
            require(isinstance(values, list), "malformed set")
            normalized = [canonical(item) for item in values]
            require(len(normalized) == len(set(normalized)), "duplicate set element")
            return json.dumps(["set", sorted(normalized)])
        if set(value) == {"$function"}:
            pairs = value["$function"]
            require(isinstance(pairs, list), "malformed function")
            entries = []
            for pair in pairs:
                require(isinstance(pair, list) and len(pair) == 2, "malformed function pair")
                entries.append((canonical(pair[0]), canonical(pair[1])))
            require(len({key for key, _ in entries}) == len(entries), "duplicate function key")
            return json.dumps(["function", sorted(entries)])
        return json.dumps(["record", sorted((key, canonical(item)) for key, item in value.items())])
    if isinstance(value, list):
        return json.dumps(["list", [canonical(item) for item in value]])
    return json.dumps(["scalar", value], sort_keys=True)


def symbol(value: object, name: str) -> bool:
    return value == {"$model": name}


def node_function(value: object) -> dict[str, object]:
    require(isinstance(value, dict) and set(value) == {"$function"}, "expected node function")
    nodes = {}
    for pair in value["$function"]:
        require(isinstance(pair, list) and len(pair) == 2, "malformed node function pair")
        key, item = pair
        require(isinstance(key, dict) and set(key) == {"$model"}, "invalid node key")
        name = key["$model"]
        require(name in NODES and name not in nodes, "unexpected or duplicate node")
        nodes[name] = item
    require(set(nodes) == set(NODES), "incomplete node function")
    return nodes


def empty_set(value: object) -> bool:
    return value == {"$set": []}


def message_bag(value: object) -> dict[str, int]:
    # TLC renders an empty function as <<>>; do not interpret nonempty sequences as bags.
    if value == {"$sequence": []}:
        return {}
    require(isinstance(value, dict) and set(value) == {"$function"}, "invalid message bag")
    result = {}
    for pair in value["$function"]:
        require(isinstance(pair, list) and len(pair) == 2, "malformed message bag pair")
        key, count = pair
        require(type(count) is int and count > 0, "invalid message multiplicity")
        encoded = canonical(key)
        require(encoded not in result, "duplicate message bag key")
        result[encoded] = count
    return result


def election_timeout(before: dict, after: dict) -> str | None:
    old_mem, new_mem = node_function(before["mem"]), node_function(after["mem"])
    actors = [node for node in NODES if canonical(old_mem[node]) != canonical(new_mem[node])]
    if len(actors) != 1:
        return None
    actor = actors[0]
    old, new = old_mem[actor], new_mem[actor]
    if not (isinstance(old, dict) and isinstance(new, dict) and
            type(old.get("term")) is int and type(new.get("term")) is int and
            new["term"] == old["term"] + 1 and symbol(new.get("votedFor"), actor)):
        return None
    require(set(old) == {"term", "votedFor", "log"} and set(new) == set(old),
            "invalid staged term/vote fields")
    require(canonical(old["log"]) == canonical(new["log"]), "timeout modified log")
    allowed = {"mem", "pending", "role", "votesGranted"}
    require(all(canonical(before[k]) == canonical(after[k]) for k in REQUIRED - allowed),
            "timeout changed unrelated model state")
    pending_old, pending_new = node_function(before["pending"]), node_function(after["pending"])
    require(symbol(pending_old[actor], "Nil"), "timeout overlapped pending write")
    require(all(canonical(pending_old[n]) == canonical(pending_new[n]) for n in NODES if n != actor),
            "timeout modified another node's pending write")
    write = pending_new[actor]
    require(isinstance(write, dict) and set(write) == {"target", "after", "acks"},
            "timeout pending write malformed")
    require(empty_set(write["acks"]), "timeout released application acknowledgements")
    disk = node_function(before["disk"])[actor]
    require(isinstance(disk, dict), "invalid durable record")
    target = dict(disk)
    target.update(term=new["term"], votedFor=new["votedFor"], log=new["log"])
    require(canonical(write["target"]) == canonical(target), "timeout durable target mismatch")
    roles_old, roles_new = node_function(before["role"]), node_function(after["role"])
    require(symbol(roles_old[actor], "Follower") or symbol(roles_old[actor], "Candidate"),
            "timeout actor not electable")
    require(symbol(roles_new[actor], "Candidate"), "timeout did not become candidate")
    require(all(canonical(roles_old[n]) == canonical(roles_new[n]) for n in NODES if n != actor),
            "timeout changed another node's role")
    votes_old, votes_new = node_function(before["votesGranted"]), node_function(after["votesGranted"])
    require(votes_new[actor] == {"$set": [{"$model": actor}]}, "timeout self-vote missing")
    require(all(canonical(votes_old[n]) == canonical(votes_new[n]) for n in NODES if n != actor),
            "timeout changed another node's votes")
    require(node_function(before["alive"])[actor] is True, "timeout on offline node")
    sends = write["after"]
    require(isinstance(sends, dict) and set(sends) == {"$set"} and len(sends["$set"]) == 2,
            "timeout must stage two vote requests")
    expected = set(NODES) - {actor}
    destinations = set()
    for message in sends["$set"]:
        require(isinstance(message, dict) and set(message) ==
                {"mtype", "msrc", "mdst", "mterm", "mlastTerm", "mlastIdx"},
                "unexpected vote request fields")
        require(message["mtype"] == "RVReq" and symbol(message["msrc"], actor) and
                message["mterm"] == new["term"], "invalid staged vote request")
        destination = message["mdst"]
        require(isinstance(destination, dict) and set(destination) == {"$model"},
                "invalid vote destination")
        destinations.add(destination["$model"])
        log = new["log"]
        require(isinstance(log, dict) and set(log) == {"$sequence"}, "invalid log sequence")
        entries = log["$sequence"]
        last_term = entries[-1]["term"] if entries else 0
        require(message["mlastIdx"] == len(entries) and message["mlastTerm"] == last_term,
                "incorrect vote request log freshness")
    require(destinations == expected, "missing or duplicate vote destination")
    return f"timeout {chr(ord('a') + NODES.index(actor))}"


def persistence_completion(before: dict, after: dict) -> str | None:
    old_pending, new_pending = node_function(before["pending"]), node_function(after["pending"])
    actors = [node for node in NODES if canonical(old_pending[node]) != canonical(new_pending[node])]
    if len(actors) != 1:
        return None
    actor = actors[0]
    write = old_pending[actor]
    if not (isinstance(write, dict) and symbol(new_pending[actor], "Nil")):
        return None
    require(set(write) == {"target", "after", "acks"}, "malformed completed write")
    require(empty_set(write["acks"]), "application acknowledgement mapping not supported")
    require(all(canonical(old_pending[n]) == canonical(new_pending[n]) for n in NODES if n != actor),
            "completion changed another node's pending write")
    require(all(canonical(before[k]) == canonical(after[k])
                for k in REQUIRED - {"pending", "disk", "net"}),
            "completion changed unrelated model state")
    disk_old, disk_new = node_function(before["disk"]), node_function(after["disk"])
    require(canonical(disk_new[actor]) == canonical(write["target"]),
            "completion durable state differs from pending target")
    require(all(canonical(disk_old[n]) == canonical(disk_new[n]) for n in NODES if n != actor),
            "completion changed another node's disk")
    bag = message_bag(before["net"])
    require(isinstance(write["after"], dict) and set(write["after"]) == {"$set"},
            "invalid deferred message set")
    released = set()
    for message in write["after"]["$set"]:
        key = canonical(message)
        require(key not in released, "duplicate deferred message in set")
        released.add(key)
        bag[key] = bag.get(key, 0) + 1
    require(message_bag(after["net"]) == bag, "completion released incorrect messages")
    return f"complete {chr(ord('a') + NODES.index(actor))}"


def normalize(document: object, max_transitions: int | None = None) -> list[str]:
    require(isinstance(document, dict) and document.get("v") == 1 and
            document.get("kind") == "tlc-text-behavior", "unsupported witness format")
    states = document.get("states")
    require(isinstance(states, list) and len(states) >= 2, "witness needs at least two states")
    limit = len(states) - 1 if max_transitions is None else max_transitions
    require(type(limit) is int and 1 <= limit < len(states), "invalid transition limit")
    commands = []
    for i in range(limit + 1):
        state = states[i]
        require(isinstance(state, dict) and state.get("state") == i + 1,
                "noncontiguous TLC state number")
        values = state.get("variables")
        require(isinstance(values, dict) and set(values) == REQUIRED,
                "missing or unexpected model variables")
    for i in range(limit):
        before, after = states[i]["variables"], states[i + 1]["variables"]
        command = election_timeout(before, after)
        if command is None:
            command = persistence_completion(before, after)
        require(command is not None, f"unsupported or ambiguous transition {i + 1}->{i + 2}")
        commands.append(command)
    return commands


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--witness", required=True, type=pathlib.Path)
    parser.add_argument("--output", required=True, type=pathlib.Path)
    parser.add_argument("--max-transitions", type=int,
                        help="explicit partial prefix length; omitted means full witness")
    args = parser.parse_args()
    document = json.loads(args.witness.read_text(encoding="utf-8"))
    commands = normalize(document, args.max_transitions)
    args.output.write_text("\n".join(commands) + "\n", encoding="utf-8")
    print(f"Normalized {len(commands)} model steps; NO C++ replay/refinement verified.")
    if args.max_transitions is not None:
        print("Explicit partial prefix only; remaining witness steps NOT checked.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (MappingError, OSError, ValueError, TypeError, KeyError) as exc:
        print(f"TLC prefix mapping FAIL: {exc}", file=sys.stderr)
        sys.exit(1)
