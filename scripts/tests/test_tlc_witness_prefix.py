"""Fail-closed semantic mapping tests; synthetic states are not TLC evidence."""

import copy
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tlc_witness_prefix import MappingError, normalize


def model(value):
    return {"$model": value}


def fn(values):
    return {"$function": [[model(node), values[node]] for node in ("s1", "s2", "s3")]}


def initial():
    disk = {"term": 0, "votedFor": model("Nil"), "log": {"$sequence": []},
            "commit": 0, "applied": 0}
    return {
        "alive": fn({n: True for n in ("s1", "s2", "s3")}),
        "mem": fn({n: {k: disk[k] for k in ("term", "votedFor", "log")}
                   for n in ("s1", "s2", "s3")}),
        "disk": fn({n: copy.deepcopy(disk) for n in ("s1", "s2", "s3")}),
        "pending": fn({n: model("Nil") for n in ("s1", "s2", "s3")}),
        "role": fn({n: model("Follower") for n in ("s1", "s2", "s3")}),
        "commitIndex": fn({n: 0 for n in ("s1", "s2", "s3")}),
        "nextIndex": fn({n: fn({m: 1 for m in ("s1", "s2", "s3")})
                         for n in ("s1", "s2", "s3")}),
        "matchIndex": fn({n: fn({m: 0 for m in ("s1", "s2", "s3")})
                          for n in ("s1", "s2", "s3")}),
        "votesGranted": fn({n: {"$set": []} for n in ("s1", "s2", "s3")}),
        "leaderEpoch": fn({n: 0 for n in ("s1", "s2", "s3")}),
        "net": {"$sequence": []},
        "epochCtr": 1,
        "ghostLeaders": {"$function": [[i, {"$set": []}] for i in (1,)]},
        "ghostCommitted": {"$set": []},
        "ghostApplied": {"$set": []},
    }


def node(data, name):
    return next(pair[1] for pair in data["$function"] if pair[0] == model(name))


def fixture():
    s0 = initial()
    s1 = copy.deepcopy(s0)
    node(s1["mem"], "s1").update(term=1, votedFor=model("s1"))
    node(s1["role"], "s1")  # role must be replaced in the function pair
    for pair in s1["role"]["$function"]:
        if pair[0] == model("s1"):
            pair[1] = model("Candidate")
    for pair in s1["votesGranted"]["$function"]:
        if pair[0] == model("s1"):
            pair[1] = {"$set": [model("s1")]}
    requests = [
        {"mtype": "RVReq", "msrc": model("s1"), "mdst": model(dst),
         "mterm": 1, "mlastTerm": 0, "mlastIdx": 0}
        for dst in ("s2", "s3")
    ]
    for pair in s1["pending"]["$function"]:
        if pair[0] == model("s1"):
            target = copy.deepcopy(node(s0["disk"], "s1"))
            target.update(term=1, votedFor=model("s1"))
            pair[1] = {"target": target, "after": {"$set": requests}, "acks": {"$set": []}}
    s2 = copy.deepcopy(s1)
    for pair in s2["disk"]["$function"]:
        if pair[0] == model("s1"):
            pair[1] = copy.deepcopy(node(s1["pending"], "s1")["target"])
    for pair in s2["pending"]["$function"]:
        if pair[0] == model("s1"):
            pair[1] = model("Nil")
    s2["net"] = {"$function": [[request, 1] for request in requests]}
    return {"v": 1, "kind": "tlc-text-behavior", "states": [
        {"state": i + 1, "variables": value} for i, value in enumerate((s0, s1, s2))]}


class WitnessPrefixTests(unittest.TestCase):
    def test_exact_election_prefix(self):
        self.assertEqual(normalize(fixture()), ["timeout a", "complete a"])

    def test_permuted_sets_and_functions_are_equivalent(self):
        witness = fixture()
        witness["states"][2]["variables"]["net"]["$function"].reverse()
        self.assertEqual(normalize(witness), ["timeout a", "complete a"])

    def test_negative_mutations_fail_closed(self):
        mutations = [
            lambda w: node(w["states"][1]["variables"]["pending"], "s1")["target"].update(term=2),
            lambda w: node(w["states"][1]["variables"]["mem"], "s2").update(term=1),
            lambda w: w["states"][1]["variables"].update(epochCtr=2),
            lambda w: w["states"][1]["variables"].update(net={"$function": [[
                node(w["states"][1]["variables"]["pending"], "s1")["after"]["$set"][0], 1]]}),
            lambda w: node(w["states"][2]["variables"]["disk"], "s1").update(votedFor=model("Nil")),
            lambda w: w["states"][2]["variables"].update(net={"$sequence": []}),
            lambda w: w["states"][2].update(state=4),
            lambda w: w["states"][0]["variables"].pop("ghostApplied"),
        ]
        for mutate in mutations:
            witness = fixture()
            mutate(witness)
            with self.subTest(mutation=mutate), self.assertRaises(MappingError):
                normalize(witness)

    def test_unsupported_next_action_rejected_not_silently_dropped(self):
        witness = fixture()
        extra = copy.deepcopy(witness["states"][-1])
        extra["state"] = 4
        witness["states"].append(extra)
        with self.assertRaises(MappingError):
            normalize(witness)
        self.assertEqual(normalize(witness, max_transitions=2), ["timeout a", "complete a"])


if __name__ == "__main__":
    unittest.main()
