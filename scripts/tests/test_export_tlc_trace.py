import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from export_tlc_trace import TraceError, parse_trace, parse_value


def transcript(second="(s1 :> [term |-> 1, vote |-> s1])"):
    return (
        """TLC2 Version 2.19
Error: Invariant NoLeaderWitness is violated.
Error: The behavior up to this point is:
State 1: <Initial predicate>
/\\ mem = (s1 :> [term |-> 0, vote |-> Nil])
/\\ net = <<>>
State 2: <Action line 1, col 1 to line 2, col 2 of module Example>
/\\ mem = """
        + second
        + """
/\\ net = <<>>
2 states generated, 2 distinct states found, 0 states left on queue.
Finished in 00s
"""
    )


class ExportTraceTest(unittest.TestCase):
    def test_nested_values_preserve_model_values_strings_and_function_keys(self):
        value = parse_value("([src |-> s1] :> 2 @@ [src |-> s2] :> 1)")
        self.assertEqual(value["$function"][0], [{"src": {"$model": "s1"}}, 2])
        self.assertEqual(
            parse_value('<<Nil, "Nil", TRUE, FALSE, -2, {s1}>>'),
            {
                "$sequence": [
                    {"$model": "Nil"},
                    "Nil",
                    True,
                    False,
                    -2,
                    {"$set": [{"$model": "s1"}]},
                ]
            },
        )
        self.assertEqual(parse_value(r'"a\"b\nc"'), 'a"b\nc')

    def test_real_shape_trace_keeps_sequence_action_and_every_variable(self):
        states = parse_trace(transcript(), "NoLeaderWitness")
        self.assertEqual(len(states), 2)
        self.assertEqual(states[0]["state"], 1)
        self.assertEqual(states[0]["variables"]["net"], {"$sequence": []})
        self.assertIn("module Example", states[1]["location"])
        self.assertEqual(states[1]["variables"]["mem"]["$function"][0][1]["term"], 1)

    def test_unsupported_or_incomplete_values_are_rejected(self):
        for text in [
            "0..1",
            "[term |-> 1",
            "(s1 :> 0 @@)",
            "<<1>> junk",
            "[term |-> 0, term |-> 1]",
            "(s1 :> 1 @@ s1 :> 2)",
        ]:
            with self.subTest(text=text), self.assertRaises(TraceError):
                parse_value(text)

    def test_wrong_invariant_and_truncated_output_are_rejected(self):
        with self.assertRaises(TraceError):
            parse_trace(transcript(), "ElectionSafety")
        with self.assertRaises(TraceError):
            parse_trace(transcript().split("2 states generated")[0], "NoLeaderWitness")

    def test_difftrace_missing_fields_and_sequence_gaps_are_rejected(self):
        for text in [
            transcript().replace("/\\ net = <<>>\n2 states", "2 states"),
            transcript().replace("State 2:", "State 3:"),
            transcript().replace(
                "/\\ net = <<>>\nState 2:", "/\\ net = <<>>\n/\\ net = <<>>\nState 2:"
            ),
        ]:
            with self.subTest(text=text), self.assertRaises(TraceError):
                parse_trace(text, "NoLeaderWitness")

    def test_depth_and_state_limits_are_enforced(self):
        with self.assertRaises(TraceError):
            parse_value("<<" * 70 + "0" + ">>" * 70)
        with self.assertRaises(TraceError):
            parse_trace(transcript(), "NoLeaderWitness", max_states=1)


if __name__ == "__main__":
    unittest.main()
