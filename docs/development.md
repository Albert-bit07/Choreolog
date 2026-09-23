# Development environment

## Verified Windows toolchain

The initial scaffold was configured and tested on Windows with:

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

## Pending tools

A .NET runtime is installed, but no .NET SDK is currently available. The .NET
10 SDK installer first failed because the system drive had insufficient free
space and a later retry was cancelled. The SDK is not needed for the initial
C++ milestones, but must be installed before creating the Avalonia and
Temporal Worker projects.

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
