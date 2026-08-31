<#
.SYNOPSIS
    Repeated fault injection: a distribution of the duplication rate, not one
    observation.

.DESCRIPTION
    docs/irp-framing.md reported 0.20% duplication from a single taskmanager
    kill and said plainly that one observation is not an estimate. A first
    six-round series confirmed it the hard way: the values ran 1.96, 1.38, 1.19,
    0.60, 0.20, 0.20 per cent, so the figure in the document was the MINIMUM of
    the range, not a typical value.

    Two things that series got wrong, both fixed here.

    JITTER. Duplicates are the records processed between the last checkpoint and
    the kill, so the count is decided by where the kill lands inside the 2 s
    checkpoint cycle. A FIXED offset does not sample that phase - it tracks it,
    and the six values came out strictly non-increasing, which six independent
    draws would do once in 720 times. The offset is now drawn per round, so the
    phase is sampled instead of followed.

    A CLEAN WAREHOUSE. fault_injection.py reports score divergence and decision
    changes across the WHOLE table, not the round's delta. Run after the
    security-overhead arms, which deliberately replay the same rows four times,
    those figures are dominated by re-sends and say nothing about the injected
    fault. The series now refuses to start against a non-empty table rather than
    producing numbers that cannot be attributed.

    EACH ROUND SENDS A DISJOINT SLICE (--skip). Replaying ids the warehouse
    already holds makes every row look like a duplicate, and the tool correctly
    declares such a measurement void.

    ASCII only, same reason as run.ps1: Windows PowerShell 5.1 reads .ps1 as
    ANSI without a BOM, and one stray em-dash breaks the parser at an unrelated
    line.

.EXAMPLE
    .\run.ps1 latency-setup        # empty warehouse, empty topic, job running
    .\run-kill-series.ps1          # 6 rounds, 500 messages, kill at 45 +/- 12 s
#>
param(
    [int]$Rounds = 6,
    [int]$Messages = 500,
    [int]$SkipBase = 0,
    [int]$KillAfterSeconds = 45,
    # Drawn uniformly per round. Wider than the 2 s checkpoint interval by a
    # large margin, so the phase is sampled rather than tracked.
    [int]$KillJitterSeconds = 12,
    [int]$DrainSeconds = 90,
    # Escape hatch. Using it makes the divergence and decision-change figures
    # uninterpretable; the duplication rate itself still stands.
    [switch]$AllowDirtyWarehouse
)

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot
$genPath = (Resolve-Path "data-generator").Path

# Credentials from .env, same as run.ps1 reads them.
$DotEnv = @{}
if (Test-Path ".env") {
    foreach ($line in Get-Content ".env") {
        if ($line -match '^\s*([A-Z_][A-Z0-9_]*)\s*=\s*(.*)$') { $DotEnv[$Matches[1]] = $Matches[2].Trim() }
    }
}
$ChUser = if ($DotEnv.CLICKHOUSE_USER) { $DotEnv.CLICKHOUSE_USER } else { "fraud" }
$ChPassword = if ($DotEnv.CLICKHOUSE_PASSWORD) { $DotEnv.CLICKHOUSE_PASSWORD } else { "fraud_ch" }

# --- precondition: the warehouse must be empty ------------------------------
$existing = (docker compose exec -T clickhouse clickhouse-client -u $ChUser --password $ChPassword `
                -q "SELECT count() FROM fraud.transactions_scored") -join ""
$existing = [int]($existing.Trim())
if ($existing -gt 0 -and -not $AllowDirtyWarehouse) {
    Write-Host ""
    Write-Host "REFUSING TO START: the warehouse already holds $existing rows." -ForegroundColor Red
    Write-Host "  fault_injection.py reports score divergence and decision changes over the"
    Write-Host "  whole table. Rows left by earlier runs - the security-overhead arms replay"
    Write-Host "  the same transactions four times - would dominate those figures, and the"
    Write-Host "  result could not be attributed to the injected fault."
    Write-Host ""
    Write-Host "  Start from a clean slate:" -ForegroundColor Yellow
    Write-Host "    .\run.ps1 latency-setup" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "  Or pass -AllowDirtyWarehouse to measure the duplication RATE only." -ForegroundColor Yellow
    Write-Host ""
    exit 1
}

Write-Host ""
Write-Host "kill series: $Rounds rounds, $Messages messages each" -ForegroundColor Cyan
Write-Host "kill offset: $KillAfterSeconds +/- $KillJitterSeconds s, drawn per round" -ForegroundColor Cyan
Write-Host "slices: rows $SkipBase .. $($SkipBase + $Rounds * $Messages), disjoint by round" -ForegroundColor Cyan
Write-Host "warehouse at start: $existing rows" -ForegroundColor Cyan
Write-Host "expect roughly $([Math]::Ceiling($Rounds * ($KillAfterSeconds + $DrainSeconds + 120) / 60)) minutes" -ForegroundColor Cyan

$summary = @()

for ($i = 1; $i -le $Rounds; $i++) {
    $skip = $SkipBase + ($i - 1) * $Messages
    $offset = $KillAfterSeconds + (Get-Random -Minimum (-$KillJitterSeconds) -Maximum ($KillJitterSeconds + 1))
    Write-Host ""
    Write-Host "################ ROUND $i / $Rounds  (rows $skip..$($skip + $Messages), kill at +$offset s) ################" -ForegroundColor Yellow

    Push-Location "stream-processor"
    try { python fault_injection.py --phase before } finally { Pop-Location }

    $job = Start-Job -ScriptBlock {
        param($gen, $n, $s)
        docker run --rm -i --network fraud-detection_fraudnet `
            -v "${gen}:/gen" -w /gen fraud-sink-writer:latest `
            python kafka_producer.py --file out/transactions.csv --realtime --speed 200 `
                --bootstrap kafka:9092 --topic transactions.raw --skip $s --limit $n
    } -ArgumentList $genPath, $Messages, $skip

    Write-Host "==> producer started, killing the worker in $offset s" -ForegroundColor Cyan
    Start-Sleep -Seconds $offset

    # `kill`, not `stop`: a graceful stop checkpoints on the way out, which
    # would test nothing.
    Write-Host "==> kill taskmanager" -ForegroundColor Red
    docker compose kill taskmanager | Out-Null
    Start-Sleep -Seconds 2
    docker compose start taskmanager | Out-Null

    Write-Host "==> waiting for the producer to finish" -ForegroundColor Cyan
    Wait-Job $job | Out-Null

    # A background job turns native stderr into error records, and under
    # ErrorActionPreference Stop that aborts the whole series - the producer's
    # DeprecationWarning killed the first attempt on round 1. Merging the
    # streams is what keeps a warning from ending the experiment.
    $prevEA = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    $out = (Receive-Job $job 2>&1 | Out-String)
    $ErrorActionPreference = $prevEA
    Remove-Job $job

    if ($out -match "produced ([\d,]+) messages") {
        Write-Host "    producer: $($Matches[0])" -ForegroundColor DarkGray
    } else {
        Write-Host "    WARNING: no send count from the producer; this round's LOSS line" -ForegroundColor Red
        Write-Host "    cannot be trusted against --expect $Messages." -ForegroundColor Red
    }

    Write-Host "==> letting the job recover and the topic drain ($DrainSeconds s)" -ForegroundColor Cyan
    Start-Sleep -Seconds $DrainSeconds

    $prevEA = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    Push-Location "stream-processor"
    try { $report = (python fault_injection.py --phase after --expect $Messages 2>&1 | Out-String) } finally { Pop-Location }
    $ErrorActionPreference = $prevEA
    Write-Host $report

    $dupes = if ($report -match "duplicate rows\s+:\s+([\d,]+)") { [int](($Matches[1]) -replace ',', '') } else { -1 }
    $lost = if ($report -match "NOTHING LOST") { "none" } else { "SEE ROUND" }
    $summary += [pscustomobject]@{ Round = $i; KillOffsetS = $offset; DuplicateRows = $dupes; Lost = $lost }
}

Write-Host ""
Write-Host "=== SERIES SUMMARY ===" -ForegroundColor Green
$summary | Format-Table -AutoSize
Write-Host "Duplicate count is the traffic between the last checkpoint and the kill," -ForegroundColor Green
Write-Host "so it should track the kill offset modulo the 2 s checkpoint interval." -ForegroundColor Green
