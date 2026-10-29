#!/usr/bin/env python3
"""Exercise the shipped node/CLI/replay processes and OS writer-lease recovery."""
import argparse
import json
from pathlib import Path
import re
import subprocess
import tempfile
import time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--node", required=True)
    parser.add_argument("--cli", required=True)
    parser.add_argument("--replay", required=True)
    args = parser.parse_args()
    node_binary, cli, replay = (str(Path(p).resolve()) for p in (args.node, args.cli, args.replay))
    with tempfile.TemporaryDirectory(prefix="choreoos-process-recovery-") as root:
        root = Path(root)
        data = root / "node data"
        config = root / "node.conf"
        legacy = root / "legacy.conf"
        common = (f"id: node-a\nleader: node-a\ndata: {data}\nlisten: 127.0.0.1:0\n"
                  "elections: true\nheartbeat_ms: 20\nelection_timeout_min: 2\n"
                  "election_timeout_max: 4\n")
        config.write_text(common + "consensus_engine: transition\n")
        legacy.write_text(common + "consensus_engine: legacy\n")
        processes = []
        logs = []

        def run(argv):
            return subprocess.run(argv, capture_output=True, text=True, timeout=15)

        def launch(path):
            ready = data / "ready"
            ready.unlink(missing_ok=True)  # A crash can leave this marker; never delete the lock.
            log = root / f"process-{len(logs)}.log"
            logs.append(log)
            with log.open("w") as output:
                process = subprocess.Popen([node_binary, str(path)], stdout=output,
                                           stderr=subprocess.STDOUT)
            processes.append(process)
            until = time.monotonic() + 20
            while time.monotonic() < until:
                if process.poll() is not None:
                    raise AssertionError("node startup failed: " + log.read_text(errors="replace"))
                if ready.exists():
                    match = re.search(r"\bport=(\d+)", ready.read_text())
                    if match:
                        expected = "legacy" if path == legacy else "transition"
                        assert "consensus_engine=" + expected in ready.read_text(), ready.read_text()
                        return process, "127.0.0.1:" + match.group(1)
                time.sleep(0.02)
            raise AssertionError("node did not publish its bound port")

        def remote(endpoint, command, *flags):
            result = run([cli, "--output", "json", command, "--leader", endpoint, *flags])
            if result.returncode:
                raise AssertionError("CLI failed: " + result.stdout + result.stderr)
            if command == "status":
                return json.loads(result.stdout)
            # Existing remote write commands print a receipt line even with
            # --output json; preserve that public behavior in this check.
            receipt = re.fullmatch(r"(committed|duplicate) index=(\d+) hash=([0-9a-f]+)\s*", result.stdout)
            if not receipt:
                raise AssertionError("unexpected CLI receipt: " + result.stdout)
            return {"ok": True, "duplicate": receipt.group(1) == "duplicate",
                    "index": int(receipt.group(2)), "hash": receipt.group(3)}

        def reject_conflicting_add(endpoint):
            # The same command ID with a different payload must not replay as a duplicate.
            conflicting = ("--command-id", "recovery-add", "--id", "alice",
                           "--x-mm", "1000", "--y-mm", "1000", "--count", "2")
            result = run([cli, "--output", "json", "add-dancer", "--leader", endpoint,
                          *conflicting])
            assert result.returncode != 0, "conflicting command ID was accepted"
            assert "CommandConflict" in result.stdout + result.stderr, result.stdout + result.stderr

        def wait_leader(endpoint):
            until = time.monotonic() + 20
            while time.monotonic() < until:
                status = remote(endpoint, "status")
                if status["role"] == "leader":
                    return
                time.sleep(0.02)
            raise AssertionError("single-node runtime did not elect a leader")

        def kill(process):
            if process.poll() is None:
                process.kill()
                process.wait(timeout=10)

        create_flags = ("--command-id", "recovery-create", "--id", "opening",
                        "--width-mm", "20000", "--depth-mm", "12000")
        try:
            process, endpoint = launch(config)
            wait_leader(endpoint)
            created = remote(endpoint, "create", *create_flags)
            assert created["ok"], created
            original_index = created["index"]
            for argv in ([node_binary, str(config)], [node_binary, str(legacy)],
                         [cli, "--output", "json", "snapshot", "--store", str(data)],
                         [replay, str(data)]):
                refused = run(argv)
                assert refused.returncode != 0, "concurrent store writer was accepted"
                assert "locked" in refused.stdout + refused.stderr, refused.stdout + refused.stderr
            kill(process)
            assert (data / ".choreoos-writer.lock").exists()
            process, endpoint = launch(config)
            wait_leader(endpoint)
            retried = remote(endpoint, "create", *create_flags)
            assert retried["ok"] and retried["duplicate"], retried
            assert retried["index"] == original_index, retried
            add_flags = ("--command-id", "recovery-add", "--id", "alice",
                         "--x-mm", "1000", "--y-mm", "1000", "--count", "1")
            added = remote(endpoint, "add-dancer", *add_flags)
            assert added["ok"] and not added["duplicate"], added
            kill(process)
            recovered = run([replay, str(data)])
            assert recovered.returncode == 0, recovered.stdout + recovered.stderr
            process, endpoint = launch(legacy)
            wait_leader(endpoint)
            compatible = remote(endpoint, "create", *create_flags)
            assert compatible["ok"] and compatible["duplicate"], compatible
            assert compatible["index"] == original_index, compatible
            legacy_add = remote(endpoint, "add-dancer", *add_flags)
            assert legacy_add["ok"] and legacy_add["duplicate"], legacy_add
            assert legacy_add["index"] == added["index"], legacy_add
            # CLI 'hash' is the current committed state hash, not a command-receipt hash.
            # A new leader may commit an election no-op, legitimately changing that hash.
            legacy_state = remote(endpoint, "status")
            assert legacy_add["hash"] == legacy_state["hash"], (legacy_add, legacy_state)
            reject_conflicting_add(endpoint)
            assert remote(endpoint, "status")["hash"] == legacy_state["hash"]
            kill(process)
            process, endpoint = launch(config)
            wait_leader(endpoint)
            transition_add = remote(endpoint, "add-dancer", *add_flags)
            assert transition_add["ok"] and transition_add["duplicate"], transition_add
            assert transition_add["index"] == added["index"], transition_add
            transition_state = remote(endpoint, "status")
            assert transition_add["hash"] == transition_state["hash"], (transition_add, transition_state)
            reject_conflicting_add(endpoint)
            assert remote(endpoint, "status")["hash"] == transition_state["hash"]
            kill(process)
            print("PASS: transition process writes/recovery, both-backend writer exclusion, "
                  "offline-tool exclusion, OS crash-release, durable retry and "
                  "transition/legacy/transition retry receipts after process kills")
        except Exception:
            for log in logs:
                print(f"--- {log.name} ---\n{log.read_text(errors='replace')}")
            raise
        finally:
            for process in processes:
                kill(process)


if __name__ == "__main__":
    main()
