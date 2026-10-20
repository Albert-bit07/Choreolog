# 60–120 second Choreo Log demonstration

This recording plan uses the **existing real-process recovery checker**, not a synthetic transcript. It demonstrates a transition-backend node surviving a forced process termination, keeping its durable operation receipt, excluding competing store writers and reopening the same storage. A three-node majority/follower catch-up demo requires a separate verified orchestration and is covered by [cluster tests](../cpp-runtime/tests/integration/cluster_test.cpp) and [transition cluster tests](../cpp-runtime/tests/integration/transition_cluster_test.cpp); do not narrate this script as a three-node demonstration.

## Prepare

Prerequisites: Python 3, CMake >=3.28, C++20 compiler, configured vcpkg dependencies, and the binaries `choreoos-node`, `choreoos-cli`, `choreoos-replay`. Linux with Ninja and `VCPKG_ROOT` set:

```bash
cmake --preset linux-gcc
cmake --build --preset linux-gcc
ctest --preset linux-gcc --output-on-failure
python3 scripts/check_runtime_recovery.py \
  --node build/linux-gcc/cpp-runtime/choreoos-node \
  --cli build/linux-gcc/cpp-runtime/choreoos-cli \
  --replay build/linux-gcc/cpp-runtime/choreoos-replay
```

Windows PowerShell with the documented Visual Studio preset:

```powershell
.\scripts\dev.ps1 check
python scripts/check_runtime_recovery.py --node build/windows-msvc/cpp-runtime/Debug/choreoos-node.exe --cli build/windows-msvc/cpp-runtime/Debug/choreoos-cli.exe --replay build/windows-msvc/cpp-runtime/Debug/choreoos-replay.exe
```

The script creates a `TemporaryDirectory`, kills **only its own child processes**, tests a durable receipt before/after restart and removes **only its own temporary contents**. It does not touch the repository's `node-data` folders or kill a running user cluster. Do not run this against a concurrently modified build tree.

**Expected completion (only if the script actually succeeds):** a single `PASS: transition process writes/recovery, ...` line and zero exit code. On failure, the script prints captured process logs and raises an exception. No example output has been fabricated here. This review did **not execute** these commands because the current workspace cannot clone/build from GitHub.

## Suggested recording (about 90 seconds)

- **0–15 sec:** Show the README diagram. “This is my independently built C++20 replicated event runtime. Persisting a log record is not the same as committing it.”
- **15–30 sec:** Show `docs/transition-runtime.md` and `scripts/check_runtime_recovery.py`. “The selectable transition backend separates the Raft transition logic from disk and network effects.”
- **30–65 sec:** Run the exact recovery-check command above on an already built machine. Point out whether it finishes successfully. “The test starts a process, writes a command, forces termination, restarts, and checks that the original operation is recognized as a duplicate rather than applied again.”
- **65–80 sec:** Show the script's lock-exclusion checks and the retry assertion. “Recovery derives truth from stored metadata and the durable application receipt.”
- **80–95 sec:** Show the cluster test paths. “Three-node replication and catch-up are separately tested. This recording focuses on a reproducible process-crash story, not a formal proof of all fault schedules.”

For a manual three-node capture, first validate a disposable cluster and its ports/configurations on the machine used for recording. Do not reuse the existing `scripts/cluster.ps1` against persistent `node-data` for a disposable demo: it uses fixed data paths and does not provide temporary-directory cleanup.
