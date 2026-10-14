# Development environment

## Historical Windows development toolchain

The initial scaffold was configured and tested on a Windows workstation with the versions below. These are historical local measurements, **not** claims about the current GitHub Actions runner or required tool versions:

- Windows build `10.0.26200`
- CMake `4.2.1`
- Visual Studio Build Tools 2019 `16.11.52`
- MSVC `19.29.30159` with C++20 mode
- Windows SDK `10.0.19041.0`
- Ninja `1.10.2` bundled with Visual Studio
- clang-format and clang-tidy `12.0.0` bundled with Visual Studio
- vcpkg tool `2026-07-27`
- GoogleTest `1.18.0`
- Python `3.10.11`

The current MSVC toolchain is sufficient for the first C++20 milestone. Moving
to a newer compiler remains desirable before advanced concurrency and
cross-platform compatibility work.

## Pending local tools and platform notes

The original development workstation had a .NET runtime but no .NET SDK when this note was written. Its .NET 10 SDK installation attempt failed due to insufficient disk space and a later retry was canceled. This is **historical workstation state**, not a verified statement about the current machine or CI. The generated C# management client is already in the repository; the Avalonia dashboard and Temporal Worker remain separate future work. Consult the current CI workflow for runner-provided SDKs and compilers.

Docker Desktop or another supported container runtime is deferred until the
Temporal milestone. That stack lives under `temporal/` and never stores
authoritative ChoreoOS node state.

## Configure, build, and test

vcpkg is bootstrapped at `%USERPROFILE%\vcpkg`. The PowerShell developer script
uses that location when `VCPKG_ROOT` is not already set.

```powershell
.\scripts\dev.ps1 configure
.\scripts\dev.ps1 build
.\scripts\dev.ps1 test
```

Run all checks:

```powershell
.\scripts\dev.ps1 check
```

Format C++ source:

```powershell
.\scripts\dev.ps1 format
```

Run the current CLI foundation:

```powershell
.\scripts\dev.ps1 run-cli
```

## Presets

- `windows-msvc`: local Visual Studio 2019 debug build
- `linux-gcc`: Linux GCC debug build using Ninja
- `linux-sanitizers`: Linux GCC build with AddressSanitizer and
  UndefinedBehaviorSanitizer

CI independently builds with Ninja on current Windows and Ubuntu runners.

## Current Raft development (merged PR #10)

The pure transition core is in `cpp-runtime/include/choreoos/raft/transition.hpp` and `cpp-runtime/src/raft/transition.cpp`, with focused GoogleTests in `cpp-runtime/tests/unit/raft_transition_test.cpp`. It is an **experimental** library component: existing CLI, TCP nodes and gRPC gateway still use `replication::Replica`. The new core is built by the ordinary CMake targets and passed formatting, Windows/Linux tests and sanitizer CI in PR #10 before merge. The bounded TLA+ models and their scope limitations are in [`formal/raft/README.md`](../formal/raft/README.md).
