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

## Current Raft development

The pure core in `cpp-runtime/src/raft/transition.cpp` is hosted by RuntimeHost
and CoreReplica on real nodes when `consensus_engine: transition` is selected.
Legacy remains the default. See [runtime ownership and migration](transition-runtime.md)
and the [field/persistence inventory](consensus-field-inventory.md).
The read-barrier primitive exists; public reads still observe local state.

For Linux, bootstrap the vcpkg revision in `vcpkg.json`, export `VCPKG_ROOT`,
then use the checked-in presets (CMake >= 3.28 and Ninja):

```sh
cmake --preset linux-gcc
cmake --build --preset linux-gcc
ctest --preset linux-gcc
cmake --preset linux-sanitizers
cmake --build --preset linux-sanitizers
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 UBSAN_OPTIONS=print_stacktrace=1:halt_on_error=1 \
  ctest --preset linux-sanitizers -E '^CatchUpTest'
```

The two CatchUp stress tests are included in the normal suite; their sanitizer
exclusion matches CI. Use clang-format 18 for touched C++ files and the
clang-tidy-18 command in `.github/workflows/ci.yml` for the affected sources.
Windows/Linux CI uses Ninja; the local Windows preset uses VS 2019.

The management gateway requires a separate configure with
`CHOREOOS_BUILD_MANAGEMENT_API=ON` and protobuf/gRPC development packages.
CI installs .NET 8, builds the generated C# client and runs real three-process
failover/retry checks against **both** backends. Dashboard and Temporal worker
implementation remain separate milestones. Consult the workflow for exact
commands; SDK availability on one workstation is not project completion status.
