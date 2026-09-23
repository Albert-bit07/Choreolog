[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [ValidateSet("configure", "build", "test", "check", "format", "format-check", "run-cli")]
    [string]$Command = "check",

    [Parameter(Position = 1, ValueFromRemainingArguments = $true)]
    [string[]]$CommandArgs
)

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$Preset = "windows-msvc"

function Invoke-Checked {
    param(
        [Parameter(Mandatory = $true)]
        [scriptblock]$Action,
        [Parameter(Mandatory = $true)]
        [string]$FailureMessage
    )

    & $Action
    if ($LASTEXITCODE -ne 0) {
        throw $FailureMessage
    }
}

function Find-ClangFormat {
    $command = Get-Command clang-format -ErrorAction SilentlyContinue
    if ($command) {
        return $command.Source
    }

    $vswhere = "${env:ProgramFiles(x86)}\Microsoft Visual Studio\Installer\vswhere.exe"
    if (Test-Path $vswhere) {
        $visualStudio = & $vswhere -latest -products * `
            -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 `
            -property installationPath
        if ($visualStudio) {
            $candidate = Join-Path $visualStudio "VC\Tools\Llvm\x64\bin\clang-format.exe"
            if (Test-Path $candidate) {
                return $candidate
            }
        }
    }

    throw "clang-format was not found."
}

if (-not $env:VCPKG_ROOT) {
    $env:VCPKG_ROOT = Join-Path $env:USERPROFILE "vcpkg"
}

$sourceFiles = @(
    Get-ChildItem -Path (Join-Path $ProjectRoot "cpp-runtime") -Recurse -File |
        Where-Object { $_.Extension -in ".cpp", ".hpp", ".h" }
)

Push-Location $ProjectRoot
try {
    switch ($Command) {
        "configure" {
            Invoke-Checked { cmake --preset $Preset } "CMake configuration failed."
        }
        "build" {
            Invoke-Checked { cmake --build --preset $Preset } "Build failed."
        }
        "test" {
            Invoke-Checked { ctest --preset $Preset } "Tests failed."
        }
        "format" {
            $clangFormat = Find-ClangFormat
            Invoke-Checked { & $clangFormat -i $sourceFiles.FullName } "Formatting failed."
        }
        "format-check" {
            $clangFormat = Find-ClangFormat
            Invoke-Checked {
                & $clangFormat --dry-run --Werror $sourceFiles.FullName
            } "Formatting check failed."
        }
        "run-cli" {
            $executable = Join-Path $ProjectRoot "build\windows-msvc\cpp-runtime\Debug\choreoos-cli.exe"
            if (-not (Test-Path $executable)) {
                throw "CLI is not built. Run '.\scripts\dev.ps1 build' first."
            }
            & $executable @CommandArgs
        }
        "check" {
            & $PSCommandPath format-check
            & $PSCommandPath configure
            & $PSCommandPath build
            & $PSCommandPath test
        }
    }
}
finally {
    Pop-Location
}
