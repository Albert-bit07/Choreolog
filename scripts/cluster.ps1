param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("start", "stop", "status")]
    [string]$Command
)

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$Node = Join-Path $ProjectRoot "build\windows-msvc\cpp-runtime\Debug\choreoos-node.exe"
$Cli = Join-Path $ProjectRoot "build\windows-msvc\cpp-runtime\Debug\choreoos-cli.exe"
$PidFile = Join-Path $ProjectRoot "tmp\cluster-pids.txt"
$Configs = @(
    "configs\node1.yaml",
    "configs\node2.yaml",
    "configs\node3.yaml"
)

function Start-Cluster {
    if (-not (Test-Path $Node)) {
        throw "Build choreoos-node before starting the cluster."
    }
    New-Item -ItemType Directory -Force -Path (Join-Path $ProjectRoot "tmp") | Out-Null
    $pids = @()
    foreach ($config in $Configs) {
        $process = Start-Process -FilePath $Node -ArgumentList (Join-Path $ProjectRoot $config) -PassThru -WorkingDirectory $ProjectRoot
        $pids += $process.Id
    }
    Set-Content -Path $PidFile -Value $pids
    Write-Host "Started node pids $($pids -join ', ')"
}

function Stop-Cluster {
    if (-not (Test-Path $PidFile)) {
        Write-Host "No cluster pid file."
        return
    }
    foreach ($processId in Get-Content $PidFile) {
        Stop-Process -Id $processId -ErrorAction SilentlyContinue
    }
    Remove-Item $PidFile -ErrorAction SilentlyContinue
    Write-Host "Stopped cluster."
}

switch ($Command) {
    "start" { Start-Cluster }
    "stop" { Stop-Cluster }
    "status" {
        & $Cli status --leader 127.0.0.1:7101
        & $Cli status --leader 127.0.0.1:7102
        & $Cli status --leader 127.0.0.1:7103
    }
}
