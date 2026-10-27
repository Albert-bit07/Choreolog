"""Exercise schedule delivery through the compiled transition core."""

import json
from pathlib import Path
import subprocess
import sys
import unittest

RUNNER = None
ELECTION = "timeout a\ncomplete a\ndeliver a b RVReq\ncomplete b\ndeliver b a RVResp\n"


def run(schedule):
    result = subprocess.run(
        [str(RUNNER), "--replay-inputs"],
        input=schedule,
        text=True,
        capture_output=True,
        timeout=20,
    )
    return result, [json.loads(line) for line in result.stdout.splitlines()]



TWO_TERM_ELECTION = (
    ELECTION
    + "timeout b\ncomplete b\ndeliver b a RVReq\ncomplete a\n"
    + "deliver a b RVResp\nrestart a\n"
)


def check_two_term_election(rows):
    """Verify a second leader election preserves higher-term votes across restart.

    This is a compiled-core schedule check, not a TLC refinement proof.
    """
    if len(rows) != 11:
        raise ValueError("unexpected two-term replay length")

    def require(condition, reason):
        if not condition:
            raise ValueError(reason)

    require([row["seq"] for row in rows] == list(range(11)),
            "non-contiguous replay sequence")
    require(rows[4]["post"]["role"] == "Leader" and
            rows[4]["post"]["term"] == 1, "first leader was not elected")
    timed_out, term_synced, vote_staged, vote_synced, elected, restarted = rows[5:]
    require(timed_out["node"] == "b" and timed_out["act"] == "Timeout" and
            timed_out["post"]["term"] == 2 and
            timed_out["post"]["votedFor"] == "b" and
            timed_out["effects"]["send"] == [], "second election leaked votes before fsync")
    require(term_synced["node"] == "b" and
            term_synced["post"]["durable"]["term"] == 2 and
            term_synced["post"]["durable"]["votedFor"] == "b",
            "second candidate did not durably self-vote")
    requests = [msg for msg in term_synced["effects"]["send"]
                if msg["kind"] == "RVReq" and msg["to"] == "a"]
    require(len(requests) == 1 and requests[0]["term"] == 2,
            "second candidate did not send a term-two request")
    require(vote_staged["node"] == "a" and
            vote_staged["act"] == "HandleRVReq" and
            vote_staged["post"]["term"] == 2 and
            vote_staged["post"]["votedFor"] == "b" and
            vote_staged["post"]["durable"]["term"] == 1 and
            vote_staged["post"]["durable"]["votedFor"] == "a" and
            vote_staged["effects"]["send"] == [],
            "old leader did not stage a higher-term vote safely")
    token = vote_staged["pending_after"]
    require(type(token) is int and token > 0 and
            vote_staged["effects"]["persist"] == token and
            vote_synced["pending_before"] == token and
            vote_synced["pending_after"] is None,
            "higher-term vote did not preserve fsync token continuity")
    require(vote_synced["post"]["durable"]["term"] == 2 and
            vote_synced["post"]["durable"]["votedFor"] == "b",
            "old leader acknowledged an incorrect durable vote")
    replies = vote_synced["effects"]["send"]
    require(len(replies) == 1 and replies[0]["kind"] == "RVResp" and
            replies[0]["to"] == "b" and replies[0]["term"] == 2 and
            replies[0]["positive"] is True and
            replies[0]["request_id"] == requests[0]["request_id"],
            "higher-term vote reply not correlated with request")
    require(elected["node"] == "b" and elected["post"]["role"] == "Leader" and
            elected["post"]["term"] == 2, "second candidate was not elected")
    require(restarted["node"] == "a" and
            restarted["post"]["term"] == 2 and
            restarted["post"]["votedFor"] == "b" and
            restarted["post"]["durable"]["term"] == 2 and
            restarted["post"]["durable"]["votedFor"] == "b",
            "restart forgot acknowledged higher-term vote")


# A bounded combined *compiled-core* scenario, not a TLC model-checking result.
REPLICATED_HANDOFF = (
    ELECTION.replace("deliver a b RVReq\n",
                     "drop a c RVReq\ndeliver a b RVReq\n", 1)
    + 'propose a "value"\ncomplete a\n'
    + "deliver a b AEReq\ndeliver b a AEResp\nheartbeat a\n"
    + "deliver a b AEReq\ncomplete b\ndeliver b a AEResp\n"
    + "advance a\ncomplete a\napply a\ncomplete a\n"
    + "timeout b\ncomplete b\ndeliver b a RVReq\ncomplete a\n"
    + "deliver a b RVResp\nrestart a\n"
)


def check_replicated_handoff(rows):
    """Check replication, durable application and higher-term handoff together.

    This deliberately checks a fixed bounded schedule, not all interleavings.
    """
    def require(condition, reason):
        if not condition:
            raise ValueError(reason)

    require(len(rows) == 23 and
            [row["seq"] for row in rows] == list(range(23)),
            "unexpected combined replay length or sequence")
    require(rows[4]["post"]["role"] == "Leader", "first election failed")
    follower = rows[11]["post"]["durable"]
    require(len(follower["log"]) == 1 and follower["log"][0]["val"] == "value",
            "follower did not durably replicate the proposal")
    applied = rows[16]["post"]["durable"]
    require(applied["commit"] == 1 and applied["applied"] == 1 and
            len(applied["log"]) == 1 and applied["log"][0]["val"] == "value",
            "original leader did not durably commit and apply the proposal")
    request = [msg for msg in rows[18]["effects"]["send"]
               if msg["kind"] == "RVReq" and msg["to"] == "a"]
    require(len(request) == 1 and request[0]["term"] == 2 and
            request[0]["last_index"] == 1 and request[0]["last_term"] == 1,
            "new candidate did not advertise its replicated log")
    pending = rows[19]
    require(pending["act"] == "HandleRVReq" and pending["node"] == "a" and
            pending["post"]["term"] == 2 and
            pending["post"]["votedFor"] == "b" and
            pending["post"]["durable"]["term"] == 1 and
            pending["effects"]["send"] == [],
            "old leader leaked a vote before durable higher-term write")
    token = pending["pending_after"]
    require(type(token) is int and token > 0 and
            pending["effects"]["persist"] == token and
            rows[20]["pending_before"] == token and
            rows[20]["pending_after"] is None,
            "higher-term persistence callback token mismatch")
    require(rows[20]["post"]["durable"]["term"] == 2 and
            rows[20]["post"]["durable"]["votedFor"] == "b",
            "higher-term vote was not durable")
    replies = rows[20]["effects"]["send"]
    require(len(replies) == 1 and replies[0]["kind"] == "RVResp" and
            replies[0]["to"] == "b" and replies[0]["positive"] is True and
            replies[0]["term"] == 2 and
            replies[0]["request_id"] == request[0]["request_id"],
            "higher-term reply did not match the actual request")
    require(rows[21]["post"]["role"] == "Leader" and
            rows[21]["post"]["term"] == 2,
            "new leader did not win the election")
    recovered = rows[22]["post"]["durable"]
    require(rows[22]["node"] == "a" and
            rows[22]["post"]["term"] == 2 and
            rows[22]["post"]["votedFor"] == "b" and
            recovered["term"] == 2 and recovered["votedFor"] == "b" and
            recovered["commit"] == 1 and recovered["applied"] == 1 and
            len(recovered["log"]) == 1 and recovered["log"][0]["val"] == "value",
            "restart lost the acknowledged vote or applied proposal")



class ReplayInputsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if RUNNER is None:
            raise unittest.SkipTest("invoke this script with the compiled runner path")

    def test_actual_messages_and_write_completions_elect_a_leader(self):
        result, rows = run(ELECTION)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(rows), 5)
        self.assertTrue(all(row["v"] == 2 for row in rows))
        self.assertEqual([row["seq"] for row in rows], list(range(5)))
        self.assertEqual(rows[0]["effects"]["send"], [])
        self.assertEqual(rows[2]["effects"]["send"], [])
        self.assertEqual(rows[2]["post"]["durable"]["term"], 0)
        self.assertEqual(rows[3]["post"]["durable"]["votedFor"], "a")
        self.assertEqual(rows[-1]["post"]["role"], "Leader")
        self.assertEqual(rows[-1]["in"]["request_id"], rows[2]["in"]["request_id"])

    def test_delivery_cannot_invent_an_unreleased_message(self):
        for schedule in ["deliver a b RVReq\n", "timeout a\ndeliver a b RVReq\n"]:
            result, _ = run(schedule)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("message is not available", result.stderr)

    def test_drop_removes_the_actual_message(self):
        result, rows = run("timeout a\ncomplete a\ndrop a b RVReq\ndeliver a b RVReq\n")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("message is not available", result.stderr)
        self.assertEqual(len(rows), 2)

    def test_duplicate_preserves_message_identity(self):
        result, rows = run(
            "timeout a\ncomplete a\nduplicate a b RVReq\ndeliver a b RVReq\n"
            "deliver a b RVReq\ncomplete b\ndeliver b a RVResp\n"
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(rows[2]["in"], rows[3]["in"])
        self.assertEqual(rows[3]["effects"]["send"], [])
        self.assertEqual(rows[-1]["post"]["role"], "Leader")

    def test_failed_write_halts_until_restart_from_acknowledged_disk(self):
        result, rows = run("timeout a\nfail a\nheartbeat a\nrestart a\n")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(rows[1]["post"]["alive"])
        self.assertFalse(rows[2]["post"]["alive"])
        self.assertTrue(rows[-1]["post"]["alive"])
        self.assertEqual(rows[-1]["post"]["term"], 0)
        self.assertEqual(rows[-1]["post"]["durable"]["votedFor"], None)

    def test_quoted_proposal_keeps_payload(self):
        value = 'a value with "quotes" and \\slashes'
        quoted = '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'
        result, rows = run(ELECTION + "propose a " + quoted + "\ncomplete a\n")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(rows[-1]["post"]["log"][-1]["val"], value)

    def test_replicated_value_is_committed_applied_and_compacted(self):
        result, rows = run(
            ELECTION + 'propose a "value"\ncomplete a\n'
            "deliver a b AEReq\ndeliver b a AEResp\nheartbeat a\n"
            "deliver a b AEReq\ncomplete b\ndeliver b a AEResp\n"
            "advance a\ncomplete a\napply a\ncomplete a\ncompact a\ncomplete a\n"
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(rows[-1]["post"]["durable"]["snapshot_index"], 1)
        self.assertEqual(rows[-1]["post"]["durable"]["commit"], 1)
        self.assertEqual(rows[-1]["post"]["durable"]["applied"], 1)
        self.assertEqual(rows[-1]["post"]["durable"]["log"], [])


    def test_second_term_election_and_restart_preserve_durable_vote(self):
        result, rows = run(TWO_TERM_ELECTION)
        self.assertEqual(result.returncode, 0, result.stderr)
        check_two_term_election(rows)

    def test_two_term_trace_mutations_are_detected(self):
        import copy

        result, original = run(TWO_TERM_ELECTION)
        self.assertEqual(result.returncode, 0, result.stderr)
        check_two_term_election(original)
        mutations = [
            lambda r: r[5]["post"].update(term=1),
            lambda r: r[5]["effects"].update(send=[{"kind": "RVReq"}]),
            lambda r: r[6]["post"]["durable"].update(votedFor=None),
            lambda r: r[7]["post"].update(term=1),
            lambda r: r[7]["post"].update(votedFor="a"),
            lambda r: r[7]["post"]["durable"].update(term=2),
            lambda r: r[7]["effects"].update(send=[{"kind": "RVResp"}]),
            lambda r: r[8]["post"]["durable"].update(votedFor=None),
            lambda r: r[8].update(pending_before=-1),
            lambda r: r[8]["effects"].update(send=[]),
            lambda r: r[9]["post"].update(role="Candidate"),
            lambda r: r[10]["post"]["durable"].update(term=1),
            lambda r: r[10]["post"]["durable"].update(votedFor=None),
        ]
        for mutate in mutations:
            with self.subTest(mutation=mutate):
                rows = copy.deepcopy(original)
                mutate(rows)
                with self.assertRaises(ValueError):
                    check_two_term_election(rows)

    def test_failed_higher_term_vote_does_not_release_reply(self):
        schedule = (ELECTION + "timeout b\ncomplete b\ndeliver b a RVReq\n"
                    "fail a\ndeliver a b RVResp\n")
        result, rows = run(schedule)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("message is not available", result.stderr)
        self.assertEqual(len(rows), 9)
        self.assertEqual(rows[7]["effects"]["send"], [])
        self.assertEqual(rows[8]["effects"]["send"], [])
        self.assertFalse(rows[8]["post"]["alive"])


    def test_replicated_entry_survives_two_term_leadership_handoff(self):
        result, rows = run(REPLICATED_HANDOFF)
        self.assertEqual(result.returncode, 0, result.stderr)
        check_replicated_handoff(rows)

    def test_replicated_handoff_mutations_are_rejected(self):
        import copy

        result, original = run(REPLICATED_HANDOFF)
        self.assertEqual(result.returncode, 0, result.stderr)
        check_replicated_handoff(original)
        mutations = [
            lambda r: r[11]["post"]["durable"].update(log=[]),
            lambda r: r[16]["post"]["durable"].update(commit=0),
            lambda r: r[16]["post"]["durable"].update(applied=0),
            lambda r: r[18]["effects"]["send"][0].update(last_index=0),
            lambda r: r[19]["post"]["durable"].update(term=2),
            lambda r: r[19]["effects"].update(send=[{"kind": "RVResp"}]),
            lambda r: r[20]["post"]["durable"].update(votedFor="a"),
            lambda r: r[20].update(pending_before=-1),
            lambda r: r[21]["post"].update(role="Candidate"),
            lambda r: r[22]["post"]["durable"].update(log=[]),
            lambda r: r[22]["post"]["durable"].update(commit=0),
            lambda r: r[22]["post"]["durable"].update(term=1),
        ]
        for mutate in mutations:
            with self.subTest(mutation=mutate):
                rows = copy.deepcopy(original)
                mutate(rows)
                with self.assertRaises(ValueError):
                    check_replicated_handoff(rows)

    def test_invalid_commands_fail_before_transition(self):
        for command, error in [
            ("", "empty replay schedule"),
            ("timeout z", "unknown replay actor"),
            ("timeout a extra", "unexpected replay fields"),
            ("deliver a z RVReq", "unknown replay destination"),
            ("deliver a b", "missing delivery context"),
            ("complete a", "no pending replay write"),
            ("propose a", "missing proposal value"),
            ("unknown a", "unknown replay operation"),
        ]:
            with self.subTest(command=command):
                result, rows = run(command + "\n" if command else "")
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(error, result.stderr)
                self.assertEqual(rows, [])


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit("usage: test_raft_replay_inputs.py RUNNER [unittest options]")
    RUNNER = Path(sys.argv.pop(1)).resolve()
    unittest.main()
