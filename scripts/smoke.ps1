[CmdletBinding()]
param(
    [string]$ControlAddr = "127.0.0.1:7401",
    [string]$FrontendAddr = "127.0.0.1:7402",
    [string]$NodeAddr = "127.0.0.1:7403",
    [string]$AdminAddr = "127.0.0.1:7404",
    [string]$FrontendEnabled = "true",
    [string]$AdminEnabled = "true",
    [string]$Namespace = "orders",
    [string]$Space = "session",
    [string]$Entity = "SessionView",
    [string]$Key = "smoke:1",
    [string]$Value = "nespa-smoke",
    [int]$HeartbeatMs = 500
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
$workDir = Join-Path $repoRoot ".codex\\smoke"
New-Item -ItemType Directory -Force -Path $workDir | Out-Null

$serverExe = Join-Path $workDir "nespa-smoke.exe"
$clientExe = Join-Path $workDir "smoke-client.exe"
$serverLog = Join-Path $workDir "server.log"

$controlBase = "http://$ControlAddr"
$frontendBase = "http://$FrontendAddr"
$adminBase = "http://$AdminAddr"

function Ensure-UriAlive {
    param([string]$Uri)

    $deadline = (Get-Date).AddSeconds(10)
    while ((Get-Date) -lt $deadline) {
        try {
            $response = Invoke-WebRequest -Uri $Uri -UseBasicParsing -Method Get -TimeoutSec 1
            if ($response.StatusCode -ge 200 -and $response.StatusCode -lt 300) {
                return
            }
        } catch {
            Start-Sleep -Milliseconds 200
        }
    }
    throw "timeout waiting for $Uri"
}

function Parse-Bool {
    param(
        [string]$Value,
        [string]$Name
    )

    if ([string]::IsNullOrWhiteSpace($Value)) {
        throw "invalid value for ${Name}: '$Value'"
    }

    switch ($Value.Trim().ToLowerInvariant()) {
        "1" { return $true }
        "true" { return $true }
        "t" { return $true }
        "on" { return $true }
        "yes" { return $true }
        "y" { return $true }
        "0" { return $false }
        "false" { return $false }
        "f" { return $false }
        "off" { return $false }
        "no" { return $false }
        "n" { return $false }
        default { throw "invalid value for ${Name}: '$Value'" }
    }
}

function Post-Control {
    param(
        [string]$Path,
        [hashtable]$Body
    )

    $uri = "$controlBase$Path"
    $payload = $Body | ConvertTo-Json -Compress
    Invoke-WebRequest -Uri $uri -UseBasicParsing -Method Post -ContentType "application/json" -Body $payload -TimeoutSec 3 | Out-Null
}

function New-ControlCatalog {
    Post-Control -Path "/v1/control/namespaces" -Body @{ namespace = $Namespace }
    Post-Control -Path "/v1/control/spaces" -Body @{ namespace = $Namespace; space = $Space }
    Post-Control -Path "/v1/control/entities" -Body @{ namespace = $Namespace; space = $Space; entity = $Entity }
}

function Get-Json {
    param([string]$Uri)

    $response = Invoke-WebRequest -Uri $Uri -UseBasicParsing -Method Get -TimeoutSec 3
    return $response.Content | ConvertFrom-Json
}

function Wait-SnapshotReady {
    $deadline = (Get-Date).AddSeconds(10)
    while ((Get-Date) -lt $deadline) {
        $snapshot = Get-Json -Uri "$controlBase/v1/control/snapshot"
        if (($snapshot.revision -gt 0) -and ($snapshot.routes | Where-Object { $_.namespace -eq $Namespace -and $_.space -eq $Space })) {
            return
        }
        Start-Sleep -Milliseconds 200
    }
    throw "timeout waiting for snapshot route"
}

function Invoke-SmokeClient {
    & $clientExe -control-addr $ControlAddr -namespace $Namespace -space $Space -entity $Entity -key $Key -value $Value
}

try {
Write-Host "build server and client"
    Set-Location $repoRoot
    go build -o $serverExe ./cmd
    go build -o $clientExe ./scripts/smoke
    $frontendEnabled = Parse-Bool -Value $FrontendEnabled -Name "FrontendEnabled"
    $adminEnabled = Parse-Bool -Value $AdminEnabled -Name "AdminEnabled"

    Write-Host "start server"
    $serverArgs = @(
        "--control-addr", $ControlAddr,
        "--control-cluster-id", "smoke",
        "--frontend-enabled", (& {
            if ($frontendEnabled) { "true" } else { "false" }
        }),
        "--frontend-addr", $FrontendAddr,
        "--node-addr", $NodeAddr,
        "--node-id", "smoke-node",
        "--node-heartbeat-interval", "${HeartbeatMs}ms",
        "--admin-enabled", (& {
            if ($adminEnabled) { "true" } else { "false" }
        }),
        "--admin-addr", $AdminAddr
    )
    $server = Start-Process -FilePath $serverExe -ArgumentList $serverArgs -RedirectStandardOutput $serverLog -RedirectStandardError "$workDir\\server.err" -PassThru

    try {
        Ensure-UriAlive -Uri "$controlBase/healthz"
        if ($frontendEnabled) {
            Ensure-UriAlive -Uri "$frontendBase/healthz"
        }
        if ($adminEnabled) {
            Ensure-UriAlive -Uri "$adminBase/healthz"
        }

        New-ControlCatalog
        Wait-SnapshotReady
        Invoke-SmokeClient

        if ($adminEnabled) {
            $adminSummary = Get-Json -Uri "$adminBase/v1/admin/summary"
            if ($adminSummary.control_addr -ne $ControlAddr) {
                throw "admin summary control_addr mismatch: $($adminSummary.control_addr) != $ControlAddr"
            }
        }

        Write-Host "smoke ok"
    }
    finally {
        if ($server -and -not $server.HasExited) {
            Stop-Process -Id $server.Id -Force
        }
    }
}
finally {
    if (Test-Path -Path $workDir) {
        Remove-Item -Path $workDir -Recurse -Force
    }
}
