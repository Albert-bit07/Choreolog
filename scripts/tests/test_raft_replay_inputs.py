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
