param(
    [switch]$Release,
    [switch]$Short,
    [switch]$FetchHeavy,
    [switch]$MaxPressure
)

$ErrorActionPreference = "Stop"

if ($FetchHeavy -and $MaxPressure) {
    throw "Use only one of -FetchHeavy or -MaxPressure"
}
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $repoRoot

$tempBase = Join-Path ([System.IO.Path]::GetTempPath()) ("herbatka_tcp_bench_" + [Guid]::NewGuid().ToString("n"))
$dataDir = Join-Path $tempBase "data"
$configPath = Join-Path $tempBase "herbatka.toml"

$brokerProc = $null

try {
    New-Item -ItemType Directory -Path $dataDir -Force | Out-Null

    $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, 0)
    $listener.Start()
    $port = $listener.LocalEndpoint.Port
    $listener.Stop()

    $dataDirForward = $dataDir.Replace("\", "/")
    @"
data_dir = "$dataDirForward"
segment_max_bytes = 65536
fsync_policy = "never"
listen_addr = "127.0.0.1:$port"
"@ | Set-Content -Path $configPath -Encoding utf8

    $buildArgs = @("build", "-p", "herbatka")
    if ($Release) {
        $buildArgs += "--release"
    }
    Write-Host "Building herbatka ($($buildArgs -join ' ')) ..."
    & cargo @buildArgs
    if ($LASTEXITCODE -ne 0) {
        throw "cargo build failed with exit code $LASTEXITCODE"
    }

    $exeName = if ($env:OS -match "Windows") { "herbatka.exe" } else { "herbatka" }
    $profileDir = if ($Release) { "release" } else { "debug" }
    $brokerExe = Join-Path $repoRoot ("target/" + $profileDir + "/" + $exeName)
    if (-not (Test-Path $brokerExe)) {
        throw "Broker binary not found at $brokerExe"
    }

    $env:HERBATKA_CONFIG = $configPath
    Write-Host "Starting broker: $brokerExe (HERBATKA_CONFIG=$configPath)"
    $brokerProc = Start-Process -FilePath $brokerExe `
        -WorkingDirectory $repoRoot `
        -WindowStyle Hidden `
        -PassThru

    $ready = $false
    for ($i = 0; $i -lt 150; $i++) {
        try {
            $client = New-Object System.Net.Sockets.TcpClient
            $client.Connect("127.0.0.1", $port)
            $client.Close()
            $ready = $true
            break
        }
        catch {
            Start-Sleep -Milliseconds 100
        }
    }
    if (-not $ready) {
        throw "Broker did not accept TCP on 127.0.0.1:$port within timeout"
    }

    $probeAddr = "127.0.0.1:$port"
    $probeArgs = @("run", "-p", "herbatka", "--bin", "tcp_concurrency_probe", "--")
    if ($Release) {
        $probeArgs = @("run", "--release", "-p", "herbatka", "--bin", "tcp_concurrency_probe", "--")
    }
    if ($Short) {
        $probeArgs += @("--addr", $probeAddr, "--duration-secs", "3", "--clients", "4")
    }
    else {
        $probeArgs += @("--addr", $probeAddr, "--duration-secs", "60", "--clients", "8")
    }
    if ($FetchHeavy) {
        $probeArgs += @("--profile", "fetch-heavy")
    }
    elseif ($MaxPressure) {
        $probeArgs += @("--profile", "max-pressure")
    }

    Write-Host "Running probe: cargo $($probeArgs -join ' ')"
    & cargo @probeArgs
    if ($LASTEXITCODE -ne 0) {
        throw "tcp_concurrency_probe failed with exit code $LASTEXITCODE"
    }
}
finally {
    if ($null -ne $brokerProc -and -not $brokerProc.HasExited) {
        Write-Host "Stopping broker (pid $($brokerProc.Id)) ..."
        Stop-Process -Id $brokerProc.Id -Force -ErrorAction SilentlyContinue
        try {
            Wait-Process -Id $brokerProc.Id -Timeout 5 -ErrorAction SilentlyContinue
        }
        catch { }
    }
    Remove-Item env:HERBATKA_CONFIG -ErrorAction SilentlyContinue
    if (Test-Path $tempBase) {
        Remove-Item -LiteralPath $tempBase -Recurse -Force -ErrorAction SilentlyContinue
    }
}

Write-Host "Done."
Write-Host "Usage: powershell -NoProfile -ExecutionPolicy Bypass -File ./scripts/tcp_concurrency_baseline.ps1 [-Release] [-Short] [-FetchHeavy] [-MaxPressure]"
Write-Host "  -FetchHeavy    tcp_concurrency_probe --profile fetch-heavy (read-skew + 1 ms batch pacing)"
Write-Host "  -MaxPressure   tcp_concurrency_probe --profile max-pressure (read-skew, no sleeps; CPU-heavy)"
Write-Host "Unix: bash ./scripts/tcp_concurrency_baseline.sh [--release] [--short] [--fetch-heavy|--max-pressure]"
