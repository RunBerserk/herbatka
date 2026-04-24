param(
    [int]$Iterations = 3
)

$ErrorActionPreference = "Stop"

if ($Iterations -lt 1) {
    throw "Iterations must be >= 1"
}

function Measure-TestCase {
    param(
        [string]$Label,
        [string]$TestName
    )

    $durations = @()
    for ($i = 1; $i -le $Iterations; $i++) {
        Write-Host "[$Label] run $i/$Iterations ..."
        $sw = [System.Diagnostics.Stopwatch]::StartNew()
        cargo test --test broker_persistence $TestName -- --exact | Out-Host
        $sw.Stop()
        $durations += $sw.Elapsed.TotalSeconds
    }

    $avg = ($durations | Measure-Object -Average).Average
    $min = ($durations | Measure-Object -Minimum).Minimum
    $max = ($durations | Measure-Object -Maximum).Maximum

    [PSCustomObject]@{
        scenario = $Label
        average_seconds = [Math]::Round($avg, 3)
        min_seconds = [Math]::Round($min, 3)
        max_seconds = [Math]::Round($max, 3)
        runs = $Iterations
    }
}

Write-Host "Startup replay benchmark (lightweight)"
Write-Host "Repo root: $PWD"
Write-Host "Iterations: $Iterations"

$indexed = Measure-TestCase -Label "metadata-skip-startup-path" -TestName "restart_replays_multiple_segments_in_order"
$fallback = Measure-TestCase -Label "fallback-decode-startup-path" -TestName "corrupt_or_missing_sparse_index_falls_back_safely"

Write-Host ""
Write-Host "Summary:"
@($indexed, $fallback) | Format-Table -AutoSize

Write-Host ""
Write-Host "Usage notes:"
Write-Host "  pwsh ./scripts/startup_replay_bench.ps1"
Write-Host "  pwsh ./scripts/startup_replay_bench.ps1 -Iterations 5"
