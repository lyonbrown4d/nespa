[CmdletBinding()]
param(
    [string]$ControlAddr = "127.0.0.1:17501",
    [string]$Node1Addr = "127.0.0.1:17503",
    [string]$Node2Addr = "127.0.0.1:17504",
    [string]$Namespace = "orders",
    [string]$Space = "session",
    [string]$Entity = "SessionView",
    [string]$Key = "smoke:multi",
    [string]$Value = "nespa-smoke",
    [int]$HeartbeatMs = 300
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
$workDir = Join-Path ([System.IO.Path]::GetTempPath()) ("nespa-smoke-multinode-" + [guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Force -Path $workDir | Out-Null

$serverExe = Join-Path $workDir "nespa-smoke.exe"
$clientExe = Join-Path $workDir "smoke-client.exe"
$controlBase = "http://$ControlAddr"

function Invoke-Native {
    param(
        [string]$FilePath,
        [string[]]$Arguments
    )

    & $FilePath @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$FilePath failed with exit code $LASTEXITCODE"
    }
}

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

function Get-Json {
    param([string]$Uri)

    $response = Invoke-WebRequest -Uri $Uri -UseBasicParsing -Method Get -TimeoutSec 3
    return $response.Content | ConvertFrom-Json
}

function Post-Control {
    param(
        [string]$Path,
        [hashtable]$Body
    )

    $payload = $Body | ConvertTo-Json -Compress
    Invoke-WebRequest -Uri "$controlBase$Path" -UseBasicParsing -Method Post -ContentType "application/json" -Body $payload -TimeoutSec 3 | Out-Null
}

function New-ControlCatalog {
    Post-Control -Path "/v1/control/namespaces" -Body @{ namespace = $Namespace }
    Post-Control -Path "/v1/control/spaces" -Body @{ namespace = $Namespace; space = $Space }
    Post-Control -Path "/v1/control/entities" -Body @{ namespace = $Namespace; space = $Space; entity = $Entity }
}

function Wait-ScopedRouteCount {
    param([int]$Count)

    $deadline = (Get-Date).AddSeconds(15)
    while ((Get-Date) -lt $deadline) {
        $snapshot = Get-Json -Uri "$controlBase/v1/control/snapshot"
        $routes = @($snapshot.routes | Where-Object { $_.namespace -eq $Namespace -and $_.space -eq $Space })
        if (($snapshot.revision -gt 0) -and ($routes.Count -eq $Count)) {
            return $snapshot
        }
        Start-Sleep -Milliseconds 200
    }
    throw "timeout waiting for $Count scoped routes"
}

function Start-Nespa {
    param(
        [string]$Name,
        [string[]]$Arguments
    )

    $stdout = Join-Path $workDir "$Name.log"
    $stderr = Join-Path $workDir "$Name.err"
    return Start-Process -FilePath $serverExe -ArgumentList $Arguments -RedirectStandardOutput $stdout -RedirectStandardError $stderr -PassThru
}

function Invoke-SmokeClient {
    param([string]$Mode)

    Invoke-Native -FilePath $clientExe -Arguments @(
        "-mode", $Mode,
        "-control-addr", $ControlAddr,
        "-namespace", $Namespace,
        "-space", $Space,
        "-entity", $Entity,
        "-key", $Key,
        "-value", $Value
    )
}

$processes = @()
try {
    Write-Host "build server and client"
    Set-Location $repoRoot
    Invoke-Native -FilePath "go" -Arguments @("build", "-o", $serverExe, "./cmd")
    Invoke-Native -FilePath "go" -Arguments @("build", "-o", $clientExe, "./scripts/smoke")

    Write-Host "start control"
    $control = Start-Nespa -Name "control" -Arguments @(
        "--control-enabled=true",
        "--control-addr", $ControlAddr,
        "--control-cluster-id", "smoke-multinode",
        "--control-liveness-sweep-interval", "500ms",
        "--control-liveness-suspect-after", "1200ms",
        "--control-liveness-dead-after", "2400ms",
        "--node-enabled=false",
        "--frontend-enabled=false",
        "--admin-enabled=false"
    )
    $processes += $control
    Ensure-UriAlive -Uri "$controlBase/healthz"

    Write-Host "start nodes"
    $node1 = Start-Nespa -Name "node1" -Arguments @(
        "--control-enabled=false",
        "--control-addr", $ControlAddr,
        "--node-enabled=true",
        "--node-addr", $Node1Addr,
        "--node-id", "smoke-node-1",
        "--node-heartbeat-interval", "${HeartbeatMs}ms",
        "--frontend-enabled=false",
        "--admin-enabled=false"
    )
    $processes += $node1
    $node2 = Start-Nespa -Name "node2" -Arguments @(
        "--control-enabled=false",
        "--control-addr", $ControlAddr,
        "--node-enabled=true",
        "--node-addr", $Node2Addr,
        "--node-id", "smoke-node-2",
        "--node-heartbeat-interval", "${HeartbeatMs}ms",
        "--frontend-enabled=false",
        "--admin-enabled=false"
    )
    $processes += $node2

    $nodesReady = $false
    $deadline = (Get-Date).AddSeconds(10)
    while ((Get-Date) -lt $deadline) {
        $nodeBody = Get-Json -Uri "$controlBase/v1/control/nodes"
        $nodes = @($nodeBody.nodes)
        if (($nodes | Where-Object { $_.node_id -eq "smoke-node-1" -and $_.state -eq "healthy" }) -and
            ($nodes | Where-Object { $_.node_id -eq "smoke-node-2" -and $_.state -eq "healthy" })) {
            $nodesReady = $true
            break
        }
        Start-Sleep -Milliseconds 200
    }
    if (-not $nodesReady) {
        throw "timeout waiting for healthy nodes"
    }

    New-ControlCatalog
    Wait-ScopedRouteCount -Count 2 | Out-Null
    Invoke-SmokeClient -Mode "multinode"

    Write-Host "stop second node and wait for route shrink"
    if ($node2 -and -not $node2.HasExited) {
        Stop-Process -Id $node2.Id -Force
    }
    Wait-ScopedRouteCount -Count 1 | Out-Null
    Invoke-SmokeClient -Mode "shrink"

    $events = Get-Json -Uri "$controlBase/v1/control/rebalance/events"
    if (@($events.events).Count -lt 3) {
        throw "expected rebalance events, got $(@($events.events).Count)"
    }

    Write-Host "multinode smoke ok"
}
finally {
    foreach ($process in $processes) {
        if ($process -and -not $process.HasExited) {
            Stop-Process -Id $process.Id -Force
        }
    }
    if (Test-Path -Path $workDir) {
        Remove-Item -Path $workDir -Recurse -Force
    }
}
