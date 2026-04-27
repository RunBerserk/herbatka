param(
    [int]$Iterations = 3
)

$ErrorActionPreference = "Stop"

if ($Iterations -lt 1) {
    throw "Iterations must be >= 1"
}

$durations = @()

Write-Host "Startup A/B benchmark target: startup_large_dataset_restart_profile"
Write-Host "Repo root: $PWD"
Write-Host "Iterations: $Iterations"

for ($i = 1; $i -le $Iterations; $i++) {
    Write-Host "[run $i/$Iterations] ..."
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    cargo test --test broker_persistence startup_large_dataset_restart_profile -- --exact --nocapture | Out-Host
    $sw.Stop()
    $durations += $sw.Elapsed.TotalSeconds
}

$avg = ($durations | Measure-Object -Average).Average
$min = ($durations | Measure-Object -Minimum).Minimum
$max = ($durations | Measure-Object -Maximum).Maximum

Write-Host ""
Write-Host "Summary:"
[PSCustomObject]@{
    scenario = "startup_large_dataset_restart_profile"
    average_seconds = [Math]::Round($avg, 3)
    min_seconds = [Math]::Round($min, 3)
    max_seconds = [Math]::Round($max, 3)
    runs = $Iterations
} | Format-Table -AutoSize

Write-Host ""
Write-Host "Usage:"
Write-Host "  pwsh ./scripts/startup_ab_speed.ps1"
Write-Host "  pwsh ./scripts/startup_ab_speed.ps1 -Iterations 5"
