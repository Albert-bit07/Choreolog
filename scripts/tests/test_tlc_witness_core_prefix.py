"""Compare a bounded synthetic TLC-style election prefix with the compiled C++ core.

This is a *scoped projection test*, not TLC witness provenance or refinement.
The C++ replay driver emits real transition-core rows. The synthetic model
states come from the independently exercised normalizer fixture.
"""

import copy
import json
from pathlib import Path
import subprocess
import sys
import unittest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))
from test_tlc_witness_prefix import fixture, node
from tlc_witness_prefix import normalize

RUNNER = None
NODES = {"s1": "a", "s2": "b", "s3": "c"}


class ProjectionMismatch(ValueError):
    """A compiled-core observation differs from the selected model fields."""


def require(condition, message):
    if not condition:
        raise ProjectionMismatch(message)


def vote(value):
    require(isinstance(value, dict) and set(value) == {"$model"},
            "malformed model vote")
    name = value["$model"]
    require(name == "Nil" or name in NODES, "unsupported model vote")
    return None if name == "Nil" else NODES[name]


def run_core(commands):
    completed = subprocess.run(
        [str(RUNNER), "--replay-inputs"],
        input="\n".join(commands) + "\n",
        text=True,
        capture_output=True,
        timeout=20,
        check=False,
    )
    require(completed.returncode == 0,
            "compiled runner rejected normalized inputs: " + completed.stderr)
    try:
        return [json.loads(line) for line in completed.stdout.splitlines()]
    except json.JSONDecodeError as exc:
        raise ProjectionMismatch("compiled runner emitted invalid JSON") from exc


def compare_prefix(witness, rows):
    commands = normalize(witness)
    require(commands == ["timeout a", "complete a"],
            "fixture no longer describes the supported two-step election")
    require(len(rows) == len(commands), "wrong number of compiled transitions")
    for index, row in enumerate(rows):
        values = witness["states"][index + 1]["variables"]
        mem = node(values["mem"], "s1")
        disk = node(values["disk"], "s1")
        role = node(values["role"], "s1")
        post = row["post"]
        durable = post["durable"]
        require(row["v"] == 2 and row["seq"] == index and row["node"] == "a",
                "unexpected compiled trace identity")
        require(row["act"] == ("Timeout" if index == 0 else "PersistCompleted"),
                "unexpected compiled transition action")
        require(post["term"] == mem["term"], "staged term divergence")
        require(post["votedFor"] == vote(mem["votedFor"]), "staged vote divergence")
        require(post["role"] == role["$model"], "candidate role divergence")
        require(post["alive"] is node(values["alive"], "s1"),
                "availability divergence")
        require(durable["term"] == disk["term"], "durable term divergence")
        require(durable["votedFor"] == vote(disk["votedFor"]),
                "durable vote divergence")
        require(post["commit"] == node(values["commitIndex"], "s1"),
                "commit index divergence")
        require(durable["commit"] == disk["commit"], "durable commit divergence")
        require(durable["applied"] == disk["applied"], "durable apply divergence")
        require(post["log"] == [] and durable["log"] == [],
                "nonempty logs outside this prefix comparison")
        require(row["effects"]["halt"] is False, "unexpected core halt")
    first, second = rows
    require(first["pending_before"] is None, "unexpected initial pending token")
    require(type(first["pending_after"]) is int,
            "timeout did not stage a persistence token")
    require(first["effects"]["persist"] == first["pending_after"],
            "timeout token/effect mismatch")
    require(first["effects"]["send"] == [], "vote requests released before fsync")
    require(second["pending_before"] == first["pending_after"],
            "completion used a different persistence token")
    require(second["pending_after"] is None, "completion left pending write")
    require(second["effects"]["persist"] is None,
            "completion staged an extra write")
    require(len(second["effects"]["send"]) == 2 and
            all(item["kind"] == "RVReq" for item in second["effects"]["send"]),
            "completion did not release exactly two vote requests")


class CompiledElectionPrefixTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if RUNNER is None:
            raise unittest.SkipTest("compiled C++ replay runner is required")

    def test_compiled_core_matches_bounded_synthetic_model_prefix(self):
        witness = fixture()
        rows = run_core(normalize(witness))
        compare_prefix(witness, rows)

    def test_negative_core_projection_mutations_fail(self):
        witness = fixture()
        original = run_core(normalize(witness))
        mutations = [
            lambda r: r[0]["post"].update(term=2),
            lambda r: r[0]["post"].update(votedFor=None),
            lambda r: r[0]["post"]["durable"].update(term=1),
            lambda r: r[0]["post"]["durable"].update(votedFor="a"),
            lambda r: r[0]["effects"].update(send=[{"kind": "RVReq"}]),
            lambda r: r[0].update(pending_after=None),
            lambda r: r[1]["post"]["durable"].update(term=0),
            lambda r: r[1]["post"]["durable"].update(votedFor=None),
            lambda r: r[1]["post"].update(role="Leader"),
            lambda r: r[1]["effects"].update(send=[]),
            lambda r: r[1].update(pending_before=999),
            lambda r: r[1].update(pending_after=999),
        ]
        for mutate in mutations:
            with self.subTest(mutation=mutate):
                rows = copy.deepcopy(original)
                mutate(rows)
                with self.assertRaises(ProjectionMismatch):
                    compare_prefix(witness, rows)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit("usage: test_tlc_witness_core_prefix.py COMPILED_RUNNER")
    RUNNER = Path(sys.argv.pop(1)).resolve()
    unittest.main()
