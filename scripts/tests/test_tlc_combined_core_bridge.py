"""Pinned combined TLC witnesses versus selected compiled C++ durability checkpoints.

These are bounded *checkpoint correspondences*, not lockstep execution or a
refinement proof. TLC separates term and vote fsync, whereas C++ persists a
combined term/vote target; commit/application timing may also differ.
"""

from __future__ import annotations

import base64
import copy
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile
import sys
import unittest
import zlib

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))
from test_combined_tlc_witness import verify, WitnessMismatch, check_higher_term_envelope
from tlc_witness_prefix import node_function

PINNED = {
    False: ("ReachCombinedHandoff.cfg", "e766580a7ca53762b3b3184263176a3f07356572c9ea787429f37b30d233c72f",
            "NoCombinedHandoffWitness"),
    True: ("ReachCombinedCrashAfterApply.cfg", "df1814fbd6fc440735a284c2e5fff0b19150cf5c5df82a623c5208072cd98885",
           "NoCombinedCrashAfterApplyWitness"),
}


class BridgeMismatch(ValueError):
    """A model or compiled-core checkpoint contradicts the bounded relation."""


def demand(ok, why):
    if not ok:
        raise BridgeMismatch(why)


def normalized_source(path, digest):
    """Verify exact source bytes, allowing only Git checkout CRLF translation.

    TLC generated the artifact from LF source. On Windows, Git may check out
    text files with CRLF; reject any other byte change rather than skipping
    source-provenance verification on that platform.
    """
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() == digest:
        return raw
    demand(b"\r\n" in raw and b"\r" not in raw.replace(b"\r\n", b""),
           "source differs from pinned TLC source beyond CRLF translation")
    restored = raw.replace(b"\r\n", b"\n")
    demand(hashlib.sha256(restored).hexdigest() == digest,
           "source hash mismatch after CRLF restoration")
    return restored


def witness(crash, model, config):
    name, digest, invariant = PINNED[crash]
    fixture = HERE / "fixtures" / ("combined_crash_20261027.zlib.b64" if crash
                                   else "combined_handoff_20261027.zlib.b64")
    raw = zlib.decompress(base64.b64decode(fixture.read_text(encoding="ascii").strip(),
                                          validate=True))
    demand(hashlib.sha256(raw).hexdigest() == digest, "pinned witness bytes changed")
    doc = json.loads(raw)
    demand(doc.get("config", {}).get("name") == name, "wrong witness configuration")
    try:
        with tempfile.TemporaryDirectory(prefix="choreo-tlc-source-") as temp:
            checked_model = Path(temp) / model.name
            checked_config = Path(temp) / config.name
            checked_model.write_bytes(normalized_source(model, doc["model"]["sha256"]))
            checked_config.write_bytes(normalized_source(config, doc["config"]["sha256"]))
            actors = verify(doc, invariant, checked_model, checked_config, crash=crash)
    except (WitnessMismatch, KeyError, TypeError, ValueError) as exc:
        raise BridgeMismatch(f"model witness failed semantic verification: {exc}") from exc
    demand(actors == ("s1", "s2"), "unreviewed model leader assignment")
    return doc


def schedule(crash):
    commands = [
        "timeout a", "complete a", "deliver a b RVReq", "complete b",
        "deliver b a RVResp", 'propose a "v1"', "complete a",
        "deliver a b AEReq", "deliver b a AEResp", "heartbeat a",
        "deliver a b AEReq", "complete b", "deliver b a AEResp",
        "advance a", "complete a",
    ]
    if crash:
        commands += ["apply a", "complete a"]
    commands += ["timeout b", "complete b", "deliver b a RVReq",
                 "complete a", "deliver a b RVResp"]
    commands.append("restart a")
    return commands


def run_compiled(runner, crash):
    commands = schedule(crash)
    result = subprocess.run([str(runner), "--replay-inputs"],
                            input="\n".join(commands) + "\n", capture_output=True,
                            text=True, timeout=30, check=False)
    demand(result.returncode == 0,
           "compiled core rejected bounded schedule: " + result.stderr.strip())
    try:
        rows = [json.loads(line) for line in result.stdout.splitlines()]
    except json.JSONDecodeError as exc:
        raise BridgeMismatch("compiled core emitted invalid JSON") from exc
    demand(len(rows) == len(commands), "missing compiled transition rows")
    return rows


def compare_split_fsync_stutter(doc, staged, synced):
    """Check stable election-disk endpoints around the model's split fsyncs.

    TLC exposes a durable term-only intermediate state; the compiled core
    combines term and vote into one checkpoint. That intermediate state is
    intentionally *not* equated to either compiled durable checkpoint.
    This is a bounded stuttering projection, not an action-level proof.
    """
    try:
        phases = check_higher_term_envelope(doc["states"], "s1", "s2")
    except WitnessMismatch as exc:
        raise BridgeMismatch(f"invalid split-fsync model envelope: {exc}") from exc

    def disk_at(state_index):
        return node_function(doc["states"][state_index]["variables"]["disk"])["s1"]

    old = disk_at(phases["term_stage"] - 1)
    term_only = disk_at(phases["term_sync"])
    vote_staged = disk_at(phases["vote_stage"])
    voted = disk_at(phases["vote_sync"])
    demand(old["term"] == 1 and old["votedFor"] == {"$model": "s1"} and
           term_only["term"] == 2 and
           term_only["votedFor"] == {"$model": "Nil"} and
           term_only["term"] == vote_staged["term"] and
           term_only["votedFor"] == vote_staged["votedFor"] and
           voted["term"] == 2 and voted["votedFor"] == {"$model": "s2"} and
           old["log"] == term_only["log"] == voted["log"],
           "model term-only stutter or durable vote phase diverged")
    before, after = staged["post"]["durable"], synced["post"]["durable"]
    demand(before["term"] == old["term"] and before["votedFor"] == "a" and
           after["term"] == voted["term"] and after["votedFor"] == "b" and
           staged["effects"]["send"] == [] and
           staged["pending_after"] == synced["pending_before"] and
           synced["pending_after"] is None,
           "compiled combined write does not span stable model election endpoints")
    demand(term_only["term"] != before["term"] and
           term_only["votedFor"] != voted["votedFor"],
           "model term-only disk state was incorrectly collapsed into C++ state")


def compare(doc, rows, crash):
    commands = schedule(crash)
    demand(len(rows) == len(commands), "wrong compiled schedule length")
    expected_acts = ["Timeout", "PersistCompleted", "HandleRVReq", "PersistCompleted",
                     "HandleRVResp", "ClientPropose", "PersistCompleted",
                     "HandleAEReq", "HandleAEResp", "SendAppend",
                     "HandleAEReq", "PersistCompleted", "HandleAEResp",
                     "AdvanceCommit", "PersistCompleted"]
    if crash:
        expected_acts += ["Apply", "PersistCompleted"]
    expected_acts += ["Timeout", "PersistCompleted", "HandleRVReq",
                      "PersistCompleted", "HandleRVResp", "Restart"]
    for i, (row, act) in enumerate(zip(rows, expected_acts)):
        demand(row.get("v") == 2 and row.get("seq") == i and row.get("act") == act,
               f"compiled row {i} identity or action diverged")
        demand(row.get("effects", {}).get("halt") is False,
               f"compiled core halted at row {i}")

    values = doc["states"][-1]["variables"]
    disk = node_function(values["disk"])
    old_log = disk["s1"]["log"]["$sequence"]
    new_log = disk["s2"]["log"]["$sequence"]
    demand(old_log == new_log and len(old_log) == 1,
           "model handoff did not retain the same durable entry")
    entry = old_log[0]
    demand(entry["term"] == 1 and entry["val"] == {"$model": "v1"},
           "unreviewed model command or term")
    expected_log = [{"term": 1, "val": "v1"}]
    demand(disk["s1"]["term"] == disk["s2"]["term"] == 2 and
           disk["s1"]["votedFor"] == {"$model": "s2"} and
           disk["s2"]["votedFor"] == {"$model": "s2"},
           "model final durable higher-term votes diverged")
    # The model's separate term-fsync and vote-fsync are checked by verify().
    # The core has one combined persistence token and may not be compared to
    # intermediate model disk states as though they were identical.
    stage, fsync = rows[2:4]
    demand(stage["node"] == fsync["node"] == "b" and
           stage["post"]["term"] == 1 and stage["post"]["votedFor"] == "a" and
           stage["post"]["durable"]["term"] == 0 and
           stage["post"]["durable"]["votedFor"] is None and
           stage["effects"]["send"] == [] and
           stage["pending_after"] == stage["effects"]["persist"] and
           type(stage["pending_after"]) is int and stage["pending_after"] > 0 and
           fsync["pending_before"] == stage["pending_after"] and
           fsync["pending_after"] is None and
           fsync["post"]["durable"]["term"] == 1 and
           fsync["post"]["durable"]["votedFor"] == "a",
           "initial durable vote boundary diverged")
    demand(rows[4]["node"] == "a" and rows[4]["post"]["role"] == "Leader",
           "first-term leader promotion diverged")
    demand(rows[6]["post"]["durable"]["log"] == expected_log and
           rows[11]["post"]["durable"]["log"] == expected_log,
           "durable replication prefix differs from model entry")
    demand(rows[14]["post"]["durable"]["commit"] == 1,
           "compiled majority commit was not durable")

    offset = 2 if crash else 0
    b_timeout, b_fsync, a_grant, a_fsync, b_leader = rows[15 + offset:20 + offset]
    compare_split_fsync_stutter(doc, a_grant, a_fsync)
    demand(b_timeout["node"] == b_fsync["node"] == b_leader["node"] == "b" and
           b_timeout["post"]["term"] == 2 and
           b_timeout["post"]["durable"]["term"] == 1 and
           b_timeout["effects"]["send"] == [] and
           b_fsync["post"]["durable"]["term"] == 2 and
           b_fsync["post"]["durable"]["votedFor"] == "b",
           "second-term candidate durability diverged")
    requests = [msg for msg in b_fsync["effects"]["send"]
                if msg.get("kind") == "RVReq" and msg.get("to") == "a"]
    demand(len(requests) == 1 and requests[0]["term"] == 2 and
           requests[0]["candidate"] == "b" and
           requests[0]["last_index"] == 1 and requests[0]["last_term"] == 1 and
           type(requests[0]["request_id"]) is int and requests[0]["request_id"] > 0,
           "higher-term vote request identity/freshness diverged")
    demand(a_grant["node"] == a_fsync["node"] == "a" and
           a_grant["post"]["term"] == 2 and a_grant["post"]["votedFor"] == "b" and
           a_grant["post"]["durable"]["term"] == 1 and
           a_grant["post"]["durable"]["votedFor"] == "a" and
           a_grant["effects"]["send"] == [] and
           type(a_grant["pending_after"]) is int and
           a_grant["pending_after"] == a_grant["effects"]["persist"] and
           a_fsync["pending_before"] == a_grant["pending_after"] and
           a_fsync["pending_after"] is None and
           a_fsync["post"]["durable"]["term"] == disk["s1"]["term"] == 2 and
           a_fsync["post"]["durable"]["votedFor"] == "b" and
           a_fsync["post"]["durable"]["log"] == expected_log,
           "combined higher-term durable vote diverged")
    replies = a_fsync["effects"]["send"]
    demand(len(replies) == 1 and replies[0]["kind"] == "RVResp" and
           replies[0]["to"] == "b" and replies[0]["term"] == 2 and
           replies[0]["positive"] is True and
           replies[0]["request_id"] == requests[0]["request_id"],
           "durable vote reply routing or request correlation diverged")
    demand(b_leader["post"]["role"] == "Leader" and
           b_leader["post"]["term"] == 2 and
           b_leader["post"]["durable"]["term"] == disk["s2"]["term"] == 2 and
           b_leader["post"]["durable"]["votedFor"] == "b" and
           b_leader["post"]["durable"]["log"] == expected_log,
           "higher-term leader checkpoint diverged")
    heartbeats = b_leader["effects"]["send"]
    demand(len(heartbeats) == 2 and {m.get("to") for m in heartbeats} == {"a", "c"}
           and all(m.get("kind") == "AEReq" and m.get("term") == 2
                   and type(m.get("request_id")) is int and m["request_id"] > 0
                   for m in heartbeats) and
           len({m["request_id"] for m in heartbeats}) == 2,
           "second-term leader heartbeats missing or misrouted")
    if crash:
        demand(disk["s1"]["commit"] == disk["s1"]["applied"] == 1 and
               rows[16]["post"]["durable"]["commit"] == 1 and
               rows[16]["post"]["durable"]["applied"] == 1 and
               a_fsync["post"]["durable"]["commit"] == 1 and
               a_fsync["post"]["durable"]["applied"] == 1,
               "crash-witness durable application checkpoint diverged")
    # Restart is a compiled-core projection, not a TLC crash/restart action.
    # The replay runner reconstructs a from its acknowledged durable state.
    recovered = rows[20 + offset]
    demand(recovered["node"] == "a" and recovered["act"] == "Restart" and
           recovered["post"]["role"] == "Follower" and
           recovered["post"]["term"] == 2 and
           recovered["post"]["votedFor"] == "b" and
           recovered["post"]["durable"] == a_fsync["post"]["durable"] and
           recovered["post"]["durable"]["log"] == expected_log and
           recovered["post"]["durable"]["commit"] == 1 and
           recovered["post"]["durable"]["applied"] == (1 if crash else 0) and
           recovered["pending_after"] is None and
           recovered["effects"]["send"] == [] and
           recovered["effects"]["persist"] is None,
           "compiled restart lost acknowledged durable election/log/application state")
    # Handoff witness does NOT require durable application; core commits
    # before the handoff, while the model can leave commitIndex only in memory.
    # Crash witness's final offline action is NOT a compiled restart test.



def interrupted_vote_schedule(crash):
    """Restart the former leader after staging, but before fsyncing, term-two vote."""
    return schedule(crash)[:-3] + ["restart a"]


def compare_interrupted_vote(rows, crash):
    """A staged vote must not become durable or emit a reply across restart.

    This is a compiled-core crash-boundary test, not a TLC Restart action or
    production disk-reopen test. The model's split term/vote fsyncs are not
    collapsed into a claim of intermediate-state equivalence.
    """
    commands = interrupted_vote_schedule(crash)
    demand(len(rows) == len(commands), "interrupted vote schedule length diverged")
    staged, restarted = rows[-2:]
    offset = 2 if crash else 0
    demand(staged.get("v") == 2 and staged.get("seq") == 17 + offset and
           staged.get("node") == "a" and staged.get("act") == "HandleRVReq" and
           restarted.get("v") == 2 and restarted.get("seq") == 18 + offset and
           restarted.get("node") == "a" and restarted.get("act") == "Restart",
           "interrupted vote transition identity diverged")
    durable = staged["post"]["durable"]
    expected_log = [{"term": 1, "val": "v1"}]
    demand(staged["post"]["term"] == 2 and staged["post"]["votedFor"] == "b" and
           durable["term"] == 1 and durable["votedFor"] == "a" and
           durable["log"] == expected_log and durable["commit"] == 1 and
           durable["applied"] == (1 if crash else 0) and
           staged["effects"]["send"] == [] and
           staged["effects"]["persist"] == staged["pending_after"] and
           type(staged["pending_after"]) is int and staged["pending_after"] > 0,
           "interrupted vote was already durable or replied before fsync")
    demand(restarted["pending_before"] == staged["pending_after"] and
           restarted["pending_after"] is None and
           restarted["post"]["durable"] == durable and
           restarted["post"]["term"] == 1 and
           restarted["post"]["votedFor"] == "a" and
           restarted["post"]["role"] == "Follower" and
           restarted["effects"]["send"] == [] and
           restarted["effects"]["persist"] is None and
           restarted["effects"]["halt"] is False,
           "restart retained an unacknowledged vote or leaked deferred effects")


class CombinedCoreBridgeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        demand(len(sys.argv) == 1, "unexpected unittest arguments")

    def test_real_combined_witnesses_and_compiled_checkpoints(self):
        for crash in (False, True):
            with self.subTest(crash=crash):
                doc = witness(crash, MODEL, CRASH_CONFIG if crash else HANDOFF_CONFIG)
                compare(doc, run_compiled(RUNNER, crash), crash)

    def test_interrupted_higher_term_vote_does_not_survive_restart(self):
        for crash in (False, True):
            with self.subTest(crash=crash):
                # Verify the actual model artifact before exercising the
                # separate compiled-core interruption boundary.
                witness(crash, MODEL, CRASH_CONFIG if crash else HANDOFF_CONFIG)
                commands = interrupted_vote_schedule(crash)
                result = subprocess.run(
                    [str(RUNNER), "--replay-inputs"],
                    input="\n".join(commands) + "\n",
                    capture_output=True, text=True, timeout=30, check=False)
                demand(result.returncode == 0,
                       "interrupted compiled schedule failed: " + result.stderr.strip())
                rows = [json.loads(line) for line in result.stdout.splitlines()]
                compare_interrupted_vote(rows, crash)
                mutations = [
                    lambda r: r[-2]["effects"].update(send=[{"kind": "RVResp"}]),
                    lambda r: r[-1]["post"]["durable"].update(term=2),
                    lambda r: r[-1]["post"]["durable"].update(votedFor="b"),
                    lambda r: r[-1]["post"].update(votedFor="b"),
                    lambda r: r[-1].update(pending_after=r[-2]["pending_after"]),
                    lambda r: r[-1]["effects"].update(send=[{"kind": "RVResp"}]),
                    lambda r: r[-1]["effects"].update(persist=99),
                    lambda r: r[-1]["post"]["durable"].update(log=[]),
                    lambda r: r[-1]["post"]["durable"].update(applied=2),
                ]
                for index, mutate in enumerate(mutations, 1):
                    with self.subTest(crash=crash, mutation=index):
                        changed = copy.deepcopy(rows)
                        mutate(changed)
                        with self.assertRaises(BridgeMismatch):
                            compare_interrupted_vote(changed, crash)
                # The old persistence callback is no longer available after
                # restart; the driver must not invent a completion.
                invalid = subprocess.run(
                    [str(RUNNER), "--replay-inputs"],
                    input="\n".join(commands + ["complete a"]) + "\n",
                    capture_output=True, text=True, timeout=30, check=False)
                demand(invalid.returncode != 0 and
                       "pending" in invalid.stderr.lower(),
                       "stale persistence completion unexpectedly accepted")

    def test_negative_model_and_core_divergences(self):
        for crash in (False, True):
            doc = witness(crash, MODEL, CRASH_CONFIG if crash else HANDOFF_CONFIG)
            original = run_compiled(RUNNER, crash)
            offset = 2 if crash else 0
            core_mutations = [
                lambda r: r[11]["post"]["durable"].update(log=[]),
                lambda r: r[14]["post"]["durable"].update(commit=0),
                lambda r: r[15 + offset]["post"]["durable"].update(term=2),
                lambda r: r[18 + offset]["post"]["durable"].update(term=1),
                lambda r: r[18 + offset]["post"]["durable"].update(votedFor="a"),
                lambda r: r[17 + offset]["effects"].update(send=[{"kind": "RVResp"}]),
                lambda r: r[18 + offset]["effects"].update(send=[]),
                lambda r: r[18 + offset]["effects"]["send"][0].update(request_id=0),
                lambda r: r[16 + offset]["effects"]["send"][0].update(last_index=0),
                lambda r: r[19 + offset]["post"].update(role="Candidate"),
                lambda r: r[19 + offset]["effects"]["send"][0].update(to="b"),
                lambda r: r[20 + offset]["post"]["durable"].update(term=1),
                lambda r: r[20 + offset]["post"]["durable"].update(log=[]),
                lambda r: r[20 + offset]["post"].update(votedFor="a"),
                lambda r: r[20 + offset]["effects"].update(send=[{"kind": "RVResp"}]),
            ]
            if crash:
                core_mutations.extend([
                    lambda r: r[16]["post"]["durable"].update(applied=0),
                    lambda r: r[20 + offset]["post"]["durable"].update(applied=0),
                ])
            for index, mutate in enumerate(core_mutations, 1):
                with self.subTest(crash=crash, core_mutation=index):
                    rows = copy.deepcopy(original)
                    mutate(rows)
                    with self.assertRaises(BridgeMismatch):
                        compare(doc, rows, crash)
            phases = check_higher_term_envelope(doc["states"], "s1", "s2")
            model_mutations = [
                lambda d: node_function(d["states"][-1]["variables"]["disk"])["s2"]["log"].update(
                    {"$sequence": []}),
                lambda d: node_function(d["states"][-1]["variables"]["disk"])["s1"].update(
                    votedFor={"$model": "Nil"}),
                # Mutate the actual model's intermediate split-fsync states,
                # not just the final witness checkpoint.
                lambda d: node_function(d["states"][phases["term_sync"]]["variables"]["disk"])[
                    "s1"].update(term=1),
                lambda d: node_function(d["states"][phases["term_sync"]]["variables"]["disk"])[
                    "s1"].update(votedFor={"$model": "s2"}),
                lambda d: node_function(d["states"][phases["vote_stage"]]["variables"]["disk"])[
                    "s1"].update(votedFor={"$model": "s2"}),
            ]
            for index, mutate in enumerate(model_mutations, 1):
                with self.subTest(crash=crash, model_mutation=index):
                    changed = copy.deepcopy(doc)
                    mutate(changed)
                    with self.assertRaises(BridgeMismatch):
                        compare(changed, original, crash)


if __name__ == "__main__":
    if len(sys.argv) != 5:
        sys.exit("usage: test_tlc_combined_core_bridge.py RUNNER MODEL HANDOFF_CONFIG CRASH_CONFIG")
    RUNNER, MODEL, HANDOFF_CONFIG, CRASH_CONFIG = map(Path, sys.argv[1:])
    sys.argv = [sys.argv[0]]
    unittest.main()
