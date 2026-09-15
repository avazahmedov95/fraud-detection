<#
.SYNOPSIS
    Operations and measurement entry point. On Windows it replaces the
    Makefile; on any host it is where the measurements live.

.DESCRIPTION
    A SUPERSET of the Makefile, not a translation of it. The Makefile carries
    the everyday targets - up, down, generate, produce, load-graph, submit-job.
    This file additionally carries every sequenced experiment: measure-plain /
    measure-tls / measure-crypto, latency-setup, pipeline, kill-worker,
    make-certs, status, and the TLS and encrypted producer arms. Each is an
    ordered protocol rather than a command, and every seam between its steps
    has lost a run at least once, so they are not worth expressing twice in two
    languages. Reproducing any figure in docs/ goes through this file.

    An earlier header claimed the two files were kept in step as equivalents.
    They were not - the Makefile had none of the measurement targets and its
    only paced producer ran on the host, which is the configuration that
    corrupts latency figures. The claim is withdrawn rather than repaired.

    ASCII only, deliberately. Windows PowerShell 5.1 reads .ps1 files as ANSI
    unless they carry a BOM, so any non-ASCII character (an em-dash, a curly
    quote) is mangled into bytes that break the parser - with errors pointing at
    unrelated lines.

.EXAMPLE
    .\run.ps1 help
    .\run.ps1 up
    .\run.ps1 pipeline      # clean -> up -> load-graph -> produce -> submit-job
#>

param(
    [Parameter(Position = 0)]
    [string]$Target = "help",

    # Message count for the produce-stream targets; 0 means "until Ctrl+C". Set it for
    # A/B comparisons, so both arms get the same length and cache warming.
    #   .\run.ps1 produce-stream-docker 400
    #   .\run.ps1 produce-stream-secure 400
    [Parameter(Position = 1)]
    [int]$Count = 0,

    # Close and reopen the producer every N messages, so the TLS handshake recurs
    # instead of being amortised over one connection.
    #   .\run.ps1 measure-tls 1200 -Reconnect 20
    [int]$Reconnect = 0,

    # The analyst queue (`cases`); named, since position 1 is already the [int] count.
    #   .\run.ps1 cases
    #   .\run.ps1 cases -Case t_0041237
    #   .\run.ps1 cases -Case t_0041237 -Verdict CONFIRMED_FRAUD -By analyst.k
    #   .\run.ps1 cases -Stats
    [string]$Case = "",
    [ValidateSet("", "CONFIRMED_FRAUD", "FALSE_POSITIVE")]
    [string]$Verdict = "",
    [string]$By = "",
    [switch]$Stats,

    # Throughput sweep. -Rate is one arm's offered events/s; -Rates overrides
    # the swept list.
    #   .\run.ps1 measure-throughput 3000
    #   .\run.ps1 measure-throughput 3000 -Rates 100,1000,10000
    [double]$Rate = 0,
    [int[]]$Rates,

    # One dependency for kill-dependency; omit to run all four.
    [ValidateSet("", "redis", "neo4j", "clickhouse", "kafka")]
    [string]$Service = ""
)

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

$GenDir = "data-generator"

# Credentials come from .env, same as docker compose reads them.
function Get-DotEnv {
    $vars = @{}
    if (Test-Path ".env") {
        foreach ($line in Get-Content ".env") {
            if ($line -match '^\s*([A-Z_][A-Z0-9_]*)\s*=\s*(.*)$') {
                $vars[$Matches[1]] = $Matches[2].Trim()
            }
        }
    }
    return $vars
}

$DotEnv = Get-DotEnv
$Neo4jPassword = if ($DotEnv.NEO4J_PASSWORD) { $DotEnv.NEO4J_PASSWORD } else { "fraud_neo4j" }
$ChUser = if ($DotEnv.CLICKHOUSE_USER) { $DotEnv.CLICKHOUSE_USER } else { "fraud" }
$ChPassword = if ($DotEnv.CLICKHOUSE_PASSWORD) { $DotEnv.CLICKHOUSE_PASSWORD } else { "fraud_ch" }

# Built once so every producer target speaks the same churn setting; an arm
# where only one side reconnects would compare two different experiments.
$rc = if ($Reconnect -gt 0) { @("--reconnect-every", "$Reconnect") } else { @() }

function Invoke-Step {
    param([string]$Description, [scriptblock]$Action)
    Write-Host ""
    Write-Host "==> $Description" -ForegroundColor Cyan
    & $Action
    if ($LASTEXITCODE -ne 0) {
        throw "step failed (exit $LASTEXITCODE): $Description"
    }
}

function Wait-Ready {
    <#
    Poll a service until it answers, instead of sleeping a fixed interval.

    A blind sleep is wrong in both directions: too short and the next step hits
    a service that is listening but not serving (cypher-shell then blocks with
    no output, which looks like a hang), too long and every run pays for the
    worst case. Polling also makes a genuine failure visible as a timeout rather
    than as an indefinite wait.
    #>
    param(
        [string]$Name,
        [scriptblock]$Probe,
        [int]$TimeoutSeconds = 120
    )
    Write-Host ""
    Write-Host "==> waiting for $Name" -ForegroundColor Cyan -NoNewline
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $ok = $false
        try { $ok = (& $Probe) } catch { $ok = $false }
        if ($ok) {
            Write-Host "  ready" -ForegroundColor Green
            return
        }
        Write-Host "." -NoNewline
        Start-Sleep -Seconds 3
    }
    Write-Host ""
    throw "$Name did not become ready within $TimeoutSeconds s. Check: docker compose logs $Name"
}

# Every module the PyFlink job imports, shipped with it; kept in one place.
$JobModules = @(
    "config.py", "capabilities.py", "features.py", "geo.py", "rules.py",
    "enrichment.py", "receiver_store.py", "fusion.py", "payload_crypto.py",
    "bins.py"
) | ForEach-Object { "/opt/flink/usrjobs/$_" }

function Assert-JobRunning {
    <#
    Refuse to produce traffic that nothing will score.

    Recreating the jobmanager - which `docker compose up -d` does on any change
    to its image, mounts or environment - discards the running job, because a
    session cluster keeps its job list in memory. Nothing about the stack looks
    wrong afterwards: every container is Up, Kafka accepts the traffic, and the
    producer reports success. The events simply accumulate unscored.

    That mistake cost four measurement runs before this guard existed, so the
    check belongs here, before the traffic, rather than in the operator's
    memory.
    #>
    param([int]$TimeoutSeconds = 60)

    # Poll: a freshly submitted job spends seconds in CREATED/INITIALIZING.
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    $last = @()
    while ((Get-Date) -lt $deadline) {
        try {
            $jobs = (Invoke-RestMethod -Uri "http://localhost:8081/jobs/overview" -TimeoutSec 5).jobs
        } catch {
            Write-Host ""
            Write-Host "Flink REST API unreachable on :8081 - is the stack up?" -ForegroundColor Red
            return $false
        }
        if (@($jobs | Where-Object { $_.state -eq "RUNNING" }).Count -gt 0) { return $true }

        $last = @($jobs | Where-Object { $_.state -notin @("FAILED", "CANCELED", "FINISHED") })
        if ($last.Count -eq 0) { break }   # nothing pending; no point waiting

        Write-Host "  waiting for the job to reach RUNNING ($($last[0].state))..." -ForegroundColor DarkGray
        Start-Sleep -Seconds 3
    }

    Write-Host ""
    Write-Host "NO RUNNING FLINK JOB - this traffic would not be scored." -ForegroundColor Red
    if ($last.Count -gt 0) {
        Write-Host "  A job exists but never reached RUNNING (state: $($last[0].state))."
        Write-Host "  Look at why before producing anything:" -ForegroundColor Yellow
        Write-Host '    $j = (Invoke-RestMethod "http://localhost:8081/jobs/overview").jobs[0]'
        Write-Host '    (Invoke-RestMethod "http://localhost:8081/jobs/$($j.jid)/exceptions").rootException'
    } else {
        Write-Host "  Recreating the jobmanager discards the job: a session cluster"
        Write-Host "  holds its job list in memory only, and nothing else looks wrong."
        Write-Host "  Resubmit first:  .\run.ps1 resume-job" -ForegroundColor Yellow
    }
    Write-Host ""
    return $false
}

function Assert-NoActiveJob {
    <#
    Refuse to start a second job beside a running one.

    `flink run` does not replace a job, it starts another beside it. And a Flink
    KafkaSource does not split partitions through the consumer group - each job's
    own enumerator assigns itself every partition, and the group id is used only
    to commit offsets. Two jobs therefore score every event twice: duplicate rows
    in the warehouse, twice the work on the one taskmanager, both committing
    offsets to the same group - and any latency figure taken meanwhile measured
    under that load. Nothing fails and nothing warns.

    Found on 2026-09-12, replacing a job resumed from a stale checkpoint before a
    measurement: the replacement is a cancel and then a submit, and submit-job
    had no way to say so.
    #>
    param([int]$TimeoutSeconds = 60)

    # Poll: the REST endpoint may not be listening yet right after `up`.
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ($true) {
        try {
            $active = @((Invoke-RestMethod -Uri "http://localhost:8081/jobs/overview" -TimeoutSec 5).jobs |
                Where-Object { $_.state -notin @("FAILED", "CANCELED", "FINISHED") })
            break
        } catch {
            if ((Get-Date) -gt $deadline) {
                Write-Host "Flink REST API unreachable on :8081 - is the stack up?" -ForegroundColor Red
                return $false
            }
            Start-Sleep -Seconds 3
        }
    }
    if ($active.Count -eq 0) { return $true }

    Write-Host ""
    Write-Host "A JOB IS ALREADY ACTIVE - not submitting a second one." -ForegroundColor Red
    foreach ($j in $active) { Write-Host "  $($j.jid)  $($j.name)  $($j.state)" }
    Write-Host "  Two jobs would each read every partition and score every event twice."
    Write-Host "  Cancel it first, then resubmit:" -ForegroundColor Yellow
    Write-Host '    Invoke-RestMethod -Method Patch "http://localhost:8081/jobs/<jid>?mode=cancel"'
    Write-Host ""
    return $false
}

function Invoke-Native {
    <#
    Run a native command and return its output lines, stderr included.

    $ErrorActionPreference = "Stop" at the top of this script turns ANY stderr
    output from a native command into a terminating error as soon as the output
    is captured into a variable - and kafka-consumer-groups.sh writes routine
    warnings to stderr. The same command run without capture prints happily,
    which is why `status` works and a helper that reads the numbers does not.
    #>
    param([scriptblock]$Command)
    $prev = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        return (& $Command 2>&1 | ForEach-Object { "$_" })
    } finally {
        $ErrorActionPreference = $prev
    }
}

function Get-ConsumerLag {
    <#
    Total unread messages for the scoring job, across partitions.

    Parsed from kafka-consumer-groups rather than inferred from row counts,
    because ClickHouse lags behind the decision by tens of seconds and would
    report "still draining" long after the job had caught up.
    #>
    $out = Invoke-Native { docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh `
        --bootstrap-server kafka:9092 --describe --group fraud-cep }
    $total = 0
    $seen = $false
    foreach ($line in $out) {
        if ($line -match '^\s*fraud-cep\s+\S+\s+\d+\s+(?:\d+|-)\s+(?:\d+|-)\s+(\d+)') {
            $total += [int]$Matches[1]
            $seen = $true
        }
    }
    if (-not $seen) { return 0 }
    return $total
}

function Wait-Drained {
    param([string]$What = "topic", [int]$TimeoutSeconds = 300)
    Write-Host "==> waiting for $What to drain" -ForegroundColor Cyan -NoNewline
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $lag = Get-ConsumerLag
        if ($lag -eq 0) { Write-Host "  drained" -ForegroundColor Green; return $true }
        Write-Host "." -NoNewline
        Start-Sleep -Seconds 3
    }
    Write-Host ""
    Write-Host "still lagging after $TimeoutSeconds s - is the job healthy?" -ForegroundColor Red
    return $false
}

function Reset-FeatureState {
    <#
        Clear the Redis state that accumulates ACROSS passes, so every pass of
        the dependency matrix scores the same slice from the same start.

        THREE namespaces, and leaving any one of them makes the arms
        incomparable. `age:*` is the enrichment cache. `rcv:*` are the payee
        inbound windows MULE_FAN_IN reads - scored on WALL CLOCK, so replaying
        the same rows a second time finds the first run still inside the
        window. `mule:fanin:hist` is the population histogram the RELATIVE
        threshold is derived from, so an unflushed pass does not merely see
        warmer state, it scores against a different threshold.

        This is what produced 43 -> 45 -> 47 MULE alerts across three arms that
        should have been identical. Note which counts did NOT drift: the CEP
        windows key on event_time from the CSV, so replaying the same rows
        lands them in the same simulated windows - STRUCTURING and ATO came out
        4 and 1 on every arm. Only the wall-clock state drifted, and only the
        wall-clock state lives in Redis.

        WITHDRAWN 2026-09-13: "flushing it is therefore sufficient, and no job
        restart is needed between passes." The event-time windows do take a
        replayed row into the same simulated window - and the sender's history
        deque still holds the previous pass's copy of it, so every replay adds
        another copy to the same window. Velocity doubles on the second pass and
        triples on the third. Measured on six passes of one 1,000-row slice: ATO
        alerts 0, 0, 7, 24, 48 by pass number, whichever service was down, and
        APP 6 on the first pass then 0, every payee already seen. The 4-and-1
        evidence above predates 2026-09-08, when VELOCITY and
        DISTINCT_PAYEE_BURST could not fire on this generator at all, so it
        could not have shown the drift. The job is therefore also restarted with
        EMPTY keyed state before every pass - and cancelled BEFORE the flush,
        not after it. The population histogram's writes are batched, and a
        cancelled job flushes its last batch on close: done the other way round,
        the first test of this reset left mule:fanin:hist holding 5,236
        observations from the pass it was meant to erase.
    #>

    # Stop the job first, so nothing it still holds can land after the flush.
    $jobs = @((Invoke-RestMethod -Uri "http://localhost:8081/jobs/overview" -TimeoutSec 10).jobs |
        Where-Object { $_.state -notin @("FAILED", "CANCELED", "FINISHED") })
    foreach ($j in $jobs) {
        Invoke-RestMethod -Method Patch -Uri "http://localhost:8081/jobs/$($j.jid)?mode=cancel" -TimeoutSec 10 | Out-Null
    }
    $deadline = (Get-Date).AddSeconds(90)
    while ((Get-Date) -lt $deadline) {
        $left = @((Invoke-RestMethod -Uri "http://localhost:8081/jobs/overview" -TimeoutSec 10).jobs |
            Where-Object { $_.state -notin @("FAILED", "CANCELED", "FINISHED") })
        if ($left.Count -eq 0) { break }
        Start-Sleep -Seconds 2
    }
    $del = {
        param($pattern)
        docker compose exec -T redis sh -c "redis-cli --scan --pattern '$pattern' | xargs -r redis-cli DEL" | Out-Null
    }
    & $del "age:*"
    & $del "rcv:*"
    docker compose exec -T redis sh -c "redis-cli DEL mule:fanin:hist" | Out-Null

    # Only now the fresh job, with empty keyed state.
    Invoke-SubmitJob | Out-Host
    if (-not (Assert-JobRunning)) { throw "no fresh job reached RUNNING - the pass would measure nothing" }
}

function Invoke-DependencyOutage {
    <#
        Produce $Count transactions with $Service down. Returns how many the
        producer reported DELIVERING, or -1 if it printed no count.

        Two shapes, because the transport is not like the others. Redis, Neo4j
        and ClickHouse sit downstream of the topic: stop them first and every
        message is offered to a pipeline that is already degraded. Kafka IS the
        topic - stopping it first means the producer cannot bootstrap, nothing
        is offered, and an arm that offers nothing cannot lose anything. The
        first version of this ran all four the same way and the kafka row said
        only that nothing arrived. A transport outage has to land in the MIDDLE
        of a stream, which is also the only way it happens in production.
    #>
    param([string]$Service, [int]$Count)

    $genPath = (Resolve-Path "data-generator").Path
    $produce = {
        param($gen, $count)
        docker run --rm -i --network fraud-detection_fraudnet `
            -v "${gen}:/gen" -w /gen fraud-sink-writer:latest `
            python kafka_producer.py --file out/transactions.csv --realtime --speed 200 `
                --bootstrap kafka:9092 --topic transactions.raw --limit $count
    }

    # A background job turns native stderr into error records, and under
    # ErrorActionPreference Stop one DeprecationWarning would end the matrix.
    $prevEA = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    $out = ""
    try {
        if ($Service -eq "kafka") {
            $job = Start-Job -ScriptBlock $produce -ArgumentList $genPath, $Count
            Write-Host "==> producing; kafka goes down in 20 s, mid-stream" -ForegroundColor Cyan
            Start-Sleep -Seconds 20
            Write-Host "==> stop kafka" -ForegroundColor Red
            docker compose stop kafka | Out-Null
            Start-Sleep -Seconds 20
            # Back up while the producer is still running - that is the experiment.
            Write-Host "==> start kafka (20 s outage)" -ForegroundColor Cyan
            docker compose start kafka | Out-Null
            Wait-Job $job | Out-Null
            $out = (Receive-Job $job 2>&1 | Out-String)
            Remove-Job $job
            Write-Host $out
            # The broker is back, so this is an ordinary drain.
            [void](Wait-Drained "arm kafka")
        } elseif ($Service -eq "control") {
            # Nothing is stopped. This pass exists to produce the reference
            # alert mix on the same transactions every other arm will score.
            & $produce $genPath $Count 2>&1 | Tee-Object -Variable lines | Out-Host
            $out = ($lines | Out-String)
            $sw = [Diagnostics.Stopwatch]::StartNew()
            [void](Wait-Drained "control pass")
            $sw.Stop()
            # The number every degraded arm is compared against on the clock.
            Write-Host "    drained in $([int]$sw.Elapsed.TotalSeconds) s, everything healthy" -ForegroundColor DarkGray
        } else {
            docker compose stop $Service | Out-Null
            # Tee, not capture: four minutes of silence looks like a hang.
            & $produce $genPath $Count 2>&1 | Tee-Object -Variable lines | Out-Host
            $out = ($lines | Out-String)
            # Drain WHILE the dependency is still down (the finally restarts it),
            # and time it: failing open is not failing fast, and only the clock
            # shows the difference.
            Write-Host "==> draining with $Service still down" -ForegroundColor Cyan
            $sw = [Diagnostics.Stopwatch]::StartNew()
            $drained = Wait-Drained "arm $Service (degraded)"
            $sw.Stop()
            $secs = [int]$sw.Elapsed.TotalSeconds
            if ($drained) {
                Write-Host "    drained in $secs s with $Service down" -ForegroundColor DarkGray
            } else {
                Write-Host "    DID NOT DRAIN in $secs s with $Service down." -ForegroundColor Yellow
                Write-Host "    Compare against the control pass. A pipeline that keeps" -ForegroundColor Yellow
                Write-Host "    deciding but no longer keeps up is a degradation the row" -ForegroundColor Yellow
                Write-Host "    count cannot see, and the tail was scored after restart." -ForegroundColor Yellow
            }
        }
    } finally {
        # The restart belongs in finally, so a failed run never leaves the service down.
        # "control" is an arm, not a container - nothing was stopped for it.
        $running = if ($Service -eq "control") { @($Service) }
                   else { @(docker compose ps --status running --services 2>$null) }
        if ($running -notcontains $Service) {
            Write-Host "==> restarting $Service" -ForegroundColor Cyan
            docker compose start $Service | Out-Null
        }
        $ErrorActionPreference = $prevEA
    }

    if ($out -match "produced ([\d,]+) messages") {
        $delivered = [int](($Matches[1]) -replace ',', '')
        Write-Host "    producer delivered $delivered" -ForegroundColor DarkGray
        return $delivered
    }
    Write-Host "    producer printed no send count - it could not reach the broker" -ForegroundColor Yellow
    return -1
}

function Invoke-Measurement {
    <#
    One arm of the security-overhead comparison, end to end.

    Every step here was a manual command at some point, and every seam between
    them lost at least one run: producing with no job submitted, comparing arms
    of different lengths, reading a window that still contained the previous
    arm's backlog. Sequencing them in one place is what makes the arms
    comparable, which is the whole point of the exercise.

      plain   - plaintext transport, plaintext payload   (the baseline)
      tls     - mutual TLS transport, plaintext payload  (reviewer point 3a)
      crypto  - plaintext transport, AES-256-GCM payload (reviewer point 3b)
    #>
    param(
        [ValidateSet("plain", "tls", "crypto")] [string]$Arm,
        [int]$Messages,
        # Declared, not inherited by dynamic scope; the banner shows the value bound.
        [int]$Reconnect = 0
    )
    if ($Messages -le 0) { $Messages = 400 }

    if (-not (Assert-JobRunning)) { return }

    # The transport is fixed when the job graph is built: check the container's own
    # environment before a `tls` arm.
    $jobProto = (Invoke-Native { docker compose exec -T taskmanager sh -c 'echo $KAFKA_SECURITY_PROTOCOL' }) -join ""
    $jobProto = $jobProto.Trim()
    if (-not $jobProto) { $jobProto = "PLAINTEXT" }
    $wanted = if ($Arm -eq "tls") { "SSL" } else { "PLAINTEXT" }
    if ($jobProto -ne $wanted) {
        Write-Host ""
        Write-Host "ARM MISMATCH: the job's Kafka transport is $jobProto, arm '$Arm' needs $wanted." -ForegroundColor Red
        Write-Host "  The job would read over a different transport than the producer writes,"
        Write-Host "  so the result would belong to neither arm. Fix it first:"
        Write-Host ""
        if ($wanted -eq "SSL") {
            Write-Host '    $env:KAFKA_SECURITY_PROTOCOL="SSL"; $env:KAFKA_BOOTSTRAP="kafka:9094"' -ForegroundColor Yellow
        } else {
            Write-Host '    Remove-Item Env:KAFKA_SECURITY_PROTOCOL, Env:KAFKA_BOOTSTRAP -ErrorAction SilentlyContinue' -ForegroundColor Yellow
        }
        Write-Host '    docker compose up -d jobmanager taskmanager' -ForegroundColor Yellow
        Write-Host '    .\run.ps1 resume-job' -ForegroundColor Yellow
        Write-Host ""
        return
    }

    Write-Host ""
    $churn = if ($Reconnect -gt 0) { ", reconnecting every $Reconnect" } else { ", one connection" }
    Write-Host "=== ARM '$Arm' : $Messages messages, job transport $jobProto$churn ===" -ForegroundColor Cyan

    # Warm-up, discarded: switching transports recreates the Flink containers, and a
    # cold JVM once made TLS look 41 ms faster. Produced before the drain and settle,
    # so outside the reporting window.
    $warm = [Math]::Max(100, [int]($Messages / 4))
    Write-Host "==> warm-up: $warm messages, discarded" -ForegroundColor Cyan
    switch ($Arm) {
        "plain"  { & $PSCommandPath produce-stream-docker $warm -Reconnect $Reconnect }
        "tls"    { & $PSCommandPath produce-stream-tls    $warm -Reconnect $Reconnect }
        "crypto" { & $PSCommandPath produce-stream-secure $warm -Reconnect $Reconnect }
    }

    if (-not (Wait-Drained "the backlog")) { return }

    # A quiet gap so rows written by whatever ran before this fall outside the
    # reporting window. experiments/latency.py filters on write time, not send.
    Write-Host "==> settling for 90 s so earlier rows leave the window" -ForegroundColor Cyan
    Start-Sleep -Seconds 90

    Write-Host "==> flushing the enrichment cache (cold start, both arms alike)" -ForegroundColor Cyan
    docker compose exec -T redis sh -c "redis-cli --scan --pattern 'age:*' | xargs -r redis-cli DEL" | Out-Null

    $startedAt = Get-Date
    switch ($Arm) {
        "plain"  { & $PSCommandPath produce-stream-docker $Messages -Reconnect $Reconnect }
        "tls"    { & $PSCommandPath produce-stream-tls    $Messages -Reconnect $Reconnect }
        "crypto" { & $PSCommandPath produce-stream-secure $Messages -Reconnect $Reconnect }
    }

    if (-not (Wait-Drained "this arm")) { return }
    Write-Host "==> letting the sink flush" -ForegroundColor Cyan
    Start-Sleep -Seconds 20

    $window = [int][Math]::Ceiling(((Get-Date) - $startedAt).TotalMinutes) + 1
    Write-Host ""
    Write-Host "=== RESULT, arm '$Arm' (window: last $window min) ===" -ForegroundColor Green
    Push-Location "stream-processor"
    try { python experiments/latency.py --since-minutes $window } finally { Pop-Location }
}

function Get-LatestCheckpoint {
    <#
    Newest retained checkpoint inside the cluster, or $null.

    `ls -dt` orders by modification time, newest first. Checkpoints live at
    /opt/flink/checkpoints/<job-id>/chk-<n>, and a resubmitted job gets a NEW
    job id - so the directory worth restoring from belongs to the PREVIOUS run,
    which is why this searches across job ids rather than under one.
    #>
    $out = Invoke-Native { docker compose exec -T jobmanager sh -c "ls -dt /opt/flink/checkpoints/*/chk-* 2>/dev/null | head -1" }
    if ($LASTEXITCODE -ne 0) { return $null }
    $path = ($out -join "").Trim()
    # A stderr line merged in by Invoke-Native would not look like a path.
    if ($path -notmatch '^/opt/flink/checkpoints/') { return $null }
    if (-not $path) { return $null }
    return $path
}

function Restart-TaskManager {
    <#
    A fresh TaskManager JVM for every job this file submits.

    Every job submitted to this session cluster loads its own user-code
    classloader, and cancelling the job does not give the Metaspace back: the
    eighth job on one TaskManager process died of OutOfMemoryError: Metaspace
    (256 MB) two seconds after it was submitted, and nothing brought it back
    (irp-framing.md 8, twenty-first). The leak is in the job's class loading and
    is not fixed here. Restarting the TaskManager before every submission means
    it never accumulates; the restart policy in docker-compose.yml is the second
    line, for a TaskManager that dies anyway.

    Waits for a NEW registration: the old one can linger in the JobManager's
    list until its heartbeat times out, and a check that accepts it would submit
    onto a process that no longer exists.
    #>
    $deadline = (Get-Date).AddSeconds(60)
    $oldIds = $null
    while ($null -eq $oldIds -and (Get-Date) -lt $deadline) {
        try {
            $oldIds = @((Invoke-RestMethod -Uri "http://localhost:8081/taskmanagers" -TimeoutSec 10).taskmanagers |
                ForEach-Object { $_.id })
        } catch { Start-Sleep -Seconds 3 }
    }
    if ($null -eq $oldIds) { throw "Flink REST API unreachable on :8081 - is the stack up?" }
    docker compose restart taskmanager | Out-Null
    $deadline = (Get-Date).AddSeconds(120)
    while ((Get-Date) -lt $deadline) {
        try {
            $fresh = @((Invoke-RestMethod -Uri "http://localhost:8081/taskmanagers" -TimeoutSec 10).taskmanagers |
                Where-Object { ($oldIds -notcontains $_.id) -and $_.freeSlots -ge 1 })
            if ($fresh.Count -ge 1) { return }
        } catch {}
        Start-Sleep -Seconds 3
    }
    throw "no fresh TaskManager registered within 120 s of a restart"
}

function Invoke-SubmitJob {
    <#
    Submit the PyFlink job, optionally restoring from a retained checkpoint.

    Without a checkpoint the job starts with EMPTY keyed state. Nothing looks
    wrong from the outside - Kafka offsets are committed, so no event is
    re-read and no error appears anywhere - but every sender's velocity and
    structuring window starts blank, and the rules that depend on accumulated
    history cannot fire until it rebuilds. A restart of the jobmanager (a
    session cluster keeps its job list in memory) therefore silently degrades
    detection for as long as the windows take to refill.

    Retained checkpoints exist to avoid exactly that, but `flink run` ignores
    them unless given -s. That is why resuming is a separate, explicit target:
    restoring the wrong state silently would be worse than starting clean.
    #>
    param([string]$FromCheckpoint)

    if (-not (Assert-NoActiveJob)) { exit 1 }
    Restart-TaskManager

    & $PSCommandPath serve-prep
    $modules = $JobModules -join ","

    Push-Location "stream-processor"
    try {
        $bundleTime = python -c "import config; print(config.PY_BUNDLE_TIME_MS)"
        $bundleSize = python -c "import config; print(config.PY_BUNDLE_SIZE)"
    } finally { Pop-Location }

    # Built as an array and splatted: written inline, PowerShell 5.1 splits
    # "-Dpython.fn-execution.bundle.time=50" at the first dot.
    $dockerArgs = @(
        "compose", "exec", "jobmanager",
        "flink", "run", "-d",
        "-Dpython.fn-execution.bundle.time=$bundleTime",
        "-Dpython.fn-execution.bundle.size=$bundleSize"
    )
    if ($FromCheckpoint) {
        Write-Host "restoring keyed state from $FromCheckpoint" -ForegroundColor Green
        $dockerArgs += @("-s", $FromCheckpoint)
    } else {
        Write-Host "starting with EMPTY keyed state - velocity and structuring" -ForegroundColor Yellow
        Write-Host "windows begin blank. To keep state: .\run.ps1 resume-job" -ForegroundColor Yellow
    }
    $dockerArgs += @("-py", "/opt/flink/usrjobs/fraud_job.py", "--pyFiles", $modules)
    & docker @dockerArgs
}

switch ($Target.ToLower()) {

    "help" {
        Write-Host ""
        Write-Host "  Usage: .\run.ps1 <target>"
        Write-Host ""
        $targets = [ordered]@{
            "up"             = "build images and start the whole stack"
            "down"           = "stop the stack (keep data volumes)"
            "clean"          = "stop the stack and delete all data volumes"
            "ps"             = "list running services"
            "logs"           = "tail logs from all services"
            "topics"         = "list Kafka topics"
            "generate"       = "generate the synthetic dataset"
            "produce"        = "replay the dataset into Kafka (batch)"
            "produce-stream" = "replay paced to original timing (200x)"
            "produce-stream-docker" = "same, but inside Docker - one clock, for latency runs"
            "produce-stream-secure" = "same again, with AES-256-GCM payloads (security overhead)"
            "produce-stream-tls"    = "same again, over mutual TLS on :9094 (transport overhead)"
            "make-certs"            = "generate the CA and certificates for mutual TLS"
            "measure-plain"         = "full measurement arm: baseline      (.\run.ps1 measure-plain 400)"
            "measure-tls"           = "full measurement arm: mutual TLS    (needs the job resubmitted with SSL)"
            "measure-crypto"        = "full measurement arm: encrypted payload"
            "measure-throughput"    = "latency vs offered load (.\run.ps1 measure-throughput 3000)"
            "load-graph"     = "load the account population into Neo4j"
            "serve-prep"     = "copy the ONNX model next to the Flink job"
            "submit-job"     = "fresh TaskManager, then the PyFlink job (empty state)"
            "resume-job"     = "same, restoring keyed state from the newest checkpoint"
            "sink-logs"      = "tail the sink-writer logs"
            "boundaries"     = "audit every place one component hands something to another"
            "cases"          = "the analyst work queue (-Case <id> to show, +-Verdict/-By to resolve, -Stats for totals)"
            "latency"        = "end-to-end latency vs the 300ms target"
            "verify-audit"   = "recompute the audit hash chain, report tampering"
            "status"         = "diagnose the whole path: containers, job, rows, offsets"
            "kill-worker"    = "kill and restart the taskmanager (fault injection)"
            "kill-dependency" = "stop Redis/Neo4j/ClickHouse/Kafka in turn and report what degrades"
            "query-scored"   = "decision counts in ClickHouse"
            "pipeline"       = "clean -> up -> load-graph -> produce -> submit-job"
            "latency-setup"  = "same as pipeline but no batch dump (for latency runs)"
        }
        foreach ($k in $targets.Keys) {
            Write-Host ("  {0,-16}" -f $k) -ForegroundColor Cyan -NoNewline
            Write-Host $targets[$k]
        }
        Write-Host ""
    }

    "up"     { docker compose up -d --build }
    "down"   { docker compose down }
    "clean"  { docker compose down -v }
    "ps"     { docker compose ps }
    "logs"   { docker compose logs -f }

    "topics" {
        docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
    }

    "generate" {
        Push-Location $GenDir
        try { python generator.py --profile realistic --out ./out } finally { Pop-Location }
    }

    "produce" {
        Push-Location $GenDir
        try {
            python kafka_producer.py --file out/transactions.csv --bootstrap localhost:29092 --topic transactions.raw
        } finally { Pop-Location }
    }

    "produce-stream" {
        if (-not (Assert-JobRunning)) { break }
        Push-Location $GenDir
        try {
            python kafka_producer.py --file out/transactions.csv --realtime --speed 200 --bootstrap localhost:29092 --topic transactions.raw
        } finally { Pop-Location }
    }

    "produce-stream-docker" {
        if (-not (Assert-JobRunning)) { break }
        # Runs the producer INSIDE the Docker network: from the host, ingested_at and
        # scored_at_job come from two clocks (the containers' VM drifts), which once
        # produced a steady 640 ms tail that was not latency at all.
        $genPath = (Resolve-Path "data-generator").Path
        $limit = if ($Count -gt 0) { @("--limit", "$Count") } else { @() }
        docker run --rm -i `
            --network fraud-detection_fraudnet `
            -v "${genPath}:/gen" -w /gen `
            fraud-sink-writer:latest `
            python kafka_producer.py --file out/transactions.csv --realtime --speed 200 `
                --bootstrap kafka:9092 --topic transactions.raw @limit @rc
    }

    # Same container and clock as produce-stream-docker, paced at a fixed rate
    # instead of to the original inter-event gaps. -Rate is events/s.
    "produce-at-rate" {
        if (-not (Assert-JobRunning)) { break }
        $genPath = (Resolve-Path "data-generator").Path
        $limit = if ($Count -gt 0) { @("--limit", "$Count") } else { @() }
        docker run --rm -i `
            --network fraud-detection_fraudnet `
            -v "${genPath}:/gen" -w /gen `
            fraud-sink-writer:latest `
            python kafka_producer.py --file out/transactions.csv --rate $Rate `
                --bootstrap kafka:9092 --topic transactions.raw @limit
    }

    # Latency against offered load: one arm per rate, each with its own ingest
    # window, so the arms can run back to back.
    "measure-throughput" {
        if (-not (Assert-JobRunning)) { break }
        # The knee is below 100 ev/s (88 ms at 3 ev/s, 1286 ms at 100).
        $rates = if ($Rates) { $Rates } else { @(5, 10, 25, 50, 100, 250) }
        $n = if ($Count -gt 0) { $Count } else { 3000 }

        Write-Host ""
        Write-Host "=== THROUGHPUT SWEEP: $($rates -join ', ') ev/s, $n messages each ===" -ForegroundColor Cyan

        # One warm-up, discarded, AFTER the cache flush: every arm must meet a warm
        # cache and a warm JVM, or the first rate is charged for the deployment's age.
        docker compose exec -T redis sh -c "redis-cli --scan --pattern 'age:*' | xargs -r redis-cli DEL" | Out-Null
        Write-Host "==> warm-up: 500 messages, discarded (also warms the cache)" -ForegroundColor Cyan
        & $PSCommandPath produce-at-rate 500 -Rate 200
        if (-not (Wait-Drained "the warm-up")) { break }

        $windows = @()
        foreach ($r in $rates) {
            Write-Host ""
            Write-Host "--- offering $r ev/s ---" -ForegroundColor Cyan
            $from = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() / 1000.0
            & $PSCommandPath produce-at-rate $n -Rate $r
            $to = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() / 1000.0 + 1
            if (-not (Wait-Drained "arm $r")) {
                Write-Host "  arm $r did not drain - recorded anyway, it is the finding" -ForegroundColor Yellow
            }
            $windows += @{ rate = $r; from = $from; to = $to }
        }

        Write-Host "==> letting the sink flush" -ForegroundColor Cyan
        Start-Sleep -Seconds 20
        # WriteAllText with an explicit no-BOM encoding: `Set-Content -Encoding
        # utf8` writes a BOM on Windows PowerShell 5.1, and json.load rejects it.
        $json = $windows | ConvertTo-Json
        $out = Join-Path (Get-Location) "stream-processor/throughput_windows.json"
        [System.IO.File]::WriteAllText($out, $json, (New-Object System.Text.UTF8Encoding($false)))
        python stream-processor/experiments/latency.py throughput
    }

    # One dependency at a time: take it out, produce the SAME slice through the
    # outage, bring it back. -Service picks one; with none, all four run. A healthy
    # CONTROL pass runs first: an arm's alert mix means something only against it.
    "kill-dependency" {
        if (-not (Assert-JobRunning)) { break }
        $svcs = if ($Service) { @($Service) } else { @("redis", "neo4j", "clickhouse", "kafka") }
        # Prepended, never skipped: a stale reference from an earlier run is
        # the same error this control exists to remove.
        $svcs = @("control") + $svcs
        $n = if ($Count -gt 0) { $Count } else { 1000 }
        # Every arm reads its baseline from ClickHouse, so it must be up first.
        docker compose start clickhouse | Out-Null
        Wait-Ready "clickhouse" {
            $r = docker compose exec -T clickhouse clickhouse-client -u $ChUser --password $ChPassword -q "SELECT 1" 2>&1
            $LASTEXITCODE -eq 0
        }
        foreach ($svc in $svcs) {
            Write-Host ""
            $what = if ($svc -eq "control") { "healthy reference pass" } else { "out of service" }
            Write-Host "=== $svc : $what, $n messages ===" -ForegroundColor Cyan
            Reset-FeatureState
            python stream-processor/experiments/outage.py --service $svc --phase before
            if ($LASTEXITCODE -ne 0) {
                Write-Host "no baseline for $svc - skipping the arm rather than measuring against nothing" -ForegroundColor Red
                continue
            }
            # -Last 1: a function returns all it emitted, not just `return`.
            $sent = [int]((Invoke-DependencyOutage -Service $svc -Count $n) | Select-Object -Last 1)
            # Left: the sink batch (500 rows or 5 s) and the service coming back.
            Write-Host "==> letting the sink flush" -ForegroundColor Cyan
            Start-Sleep -Seconds 30
            python stream-processor/experiments/outage.py --service $svc --phase after --expect $n --sent $sent
        }
        Write-Host ""
        Write-Host "Logs worth reading beside these numbers:" -ForegroundColor Cyan
        Write-Host "  docker compose logs sink-writer | Select-String DISCARDED"
        Write-Host "  docker compose logs taskmanager | Select-String -Pattern 'failing open|unavailable'"
    }

    "measure-plain"  { Invoke-Measurement -Arm plain  -Messages $Count -Reconnect $Reconnect }
    "measure-tls"    { Invoke-Measurement -Arm tls    -Messages $Count -Reconnect $Reconnect }
    "measure-crypto" { Invoke-Measurement -Arm crypto -Messages $Count -Reconnect $Reconnect }

    "make-certs" {
        # CA and certificates for the mutual-TLS arm. Run BEFORE the first `up`: a
        # missing certs directory is mounted empty and the broker fails to start.
        $certs = Join-Path (Get-Location) "infra\kafka\certs"
        New-Item -ItemType Directory -Force -Path $certs | Out-Null
        $script = Join-Path (Get-Location) "infra\kafka\make-certs.sh"
        # --entrypoint sh: the image's ENTRYPOINT is `openssl`.
        docker run --rm `
            --entrypoint sh `
            -v "${certs}:/certs" `
            -v "${script}:/make-certs.sh:ro" `
            -e CERTS=/certs `
            alpine/openssl:latest /make-certs.sh

        # keytool from the Kafka image's JDK: an openssl-made PKCS12 truststore
        # reads back as zero entries in Java.
        $ts = Join-Path (Get-Location) "infra\kafka\make-truststore.sh"
        docker run --rm `
            --entrypoint sh `
            -v "${certs}:/certs" `
            -v "${ts}:/make-truststore.sh:ro" `
            -e CERTS=/certs `
            "apache/kafka:$($DotEnv.KAFKA_IMAGE -replace '.*:', '')" /make-truststore.sh
    }

    "produce-stream-tls" {
        if (-not (Assert-JobRunning)) { break }
        # Transport arm: only the listener differs (9094, mutual TLS). Use -Count so
        # both arms are the same length.
        $genPath = (Resolve-Path "data-generator").Path
        $certPath = (Resolve-Path "infra\kafka\certs").Path
        $limit = if ($Count -gt 0) { @("--limit", "$Count") } else { @() }
        docker run --rm -i `
            --network fraud-detection_fraudnet `
            -v "${genPath}:/gen" -v "${certPath}:/certs:ro" -w /gen `
            fraud-sink-writer:latest `
            python kafka_producer.py --file out/transactions.csv --realtime --speed 200 `
                --bootstrap kafka:9094 --topic transactions.raw --tls @limit @rc
    }

    "produce-stream-secure" {
        if (-not (Assert-JobRunning)) { break }
        # Identical to produce-stream-docker except that payloads are encrypted; the
        # cluster decrypts per record by envelope prefix, so no restart between arms.
        $genPath = (Resolve-Path "data-generator").Path
        if (-not $DotEnv.PAYLOAD_KEY_HEX) {
            Write-Host "PAYLOAD_KEY_HEX is not set in .env" -ForegroundColor Red
            break
        }
        $limit = if ($Count -gt 0) { @("--limit", "$Count") } else { @() }
        docker run --rm -i `
            --network fraud-detection_fraudnet `
            -e "PAYLOAD_KEY_HEX=$($DotEnv.PAYLOAD_KEY_HEX)" `
            -v "${genPath}:/gen" -w /gen `
            fraud-sink-writer:latest `
            python kafka_producer.py --file out/transactions.csv --realtime --speed 200 `
                --bootstrap kafka:9092 --topic transactions.raw --encrypt @limit
    }

    "load-graph" {
        # Copied into the container and read with -f, never piped: PowerShell 5.1 can
        # prepend a BOM to a native command's stdin.
        docker compose cp "infra/neo4j/import.cypher" neo4j:/tmp/import.cypher
        if ($LASTEXITCODE -eq 0) {
            docker compose exec -T neo4j cypher-shell -u neo4j -p $Neo4jPassword -f /tmp/import.cypher
        }
    }

    # What one component produces against what the next expects. Run before a
    # walkthrough and after touching any record, schema or wire format.
    "boundaries" { python tools/boundary_audit.py -v }

    # The analyst surface. One target, four shapes, chosen by which parameters
    # are present - see the param block above for why they are named.
    "cases" {
        if ($Stats) {
            $argv = @("stats")
        } elseif ($Case -and $Verdict) {
            if (-not $By) {
                throw "-By is required: a disposition is a label a model may be retrained on, and an unattributed label cannot be audited or withdrawn."
            }
            $argv = @("resolve", $Case, $Verdict, "--by", $By)
        } elseif ($Case) {
            $argv = @("show", $Case)
        } else {
            $argv = @("list")
        }
        docker compose exec -T case-manager python queue_cli.py @argv
    }

    "serve-prep" {
        Copy-Item "ml/models/model.onnx" "stream-processor/" -Force
        Copy-Item "ml/models/feature_names.json" "stream-processor/" -Force
        # The model's REVIEW / BLOCK cutoffs: an unweighted committee's scale
        # belongs to it, and config._model_thresholds reads them from here.
        Copy-Item "ml/models/thresholds.json" "stream-processor/" -Force
        # The BIN table: bins.py reads it, and the job dies at import without it.
        Copy-Item "data-generator/banks.csv" "stream-processor/" -Force
        Write-Host "model + feature spec copied to stream-processor/"
    }

    # The latency knobs go to the client as -D too (read during job-graph
    # translation); the values come from config.py.
    "submit-job" { Invoke-SubmitJob }

    "resume-job" {
        $chk = Get-LatestCheckpoint
        if (-not $chk) {
            Write-Host ""
            Write-Host "no retained checkpoint under /opt/flink/checkpoints" -ForegroundColor Yellow
            Write-Host "  Nothing to restore from. Either the cluster has never"
            Write-Host "  checkpointed, or the volume was wiped by 'clean'."
            Write-Host "  Start fresh with: .\run.ps1 submit-job"
            break
        }
        Invoke-SubmitJob -FromCheckpoint $chk
    }

    "sink-logs" { docker compose logs -f sink-writer }

    "latency" {
        Push-Location "stream-processor"
        try { python experiments/latency.py } finally { Pop-Location }
    }

    "verify-audit" {
        Push-Location "sink-writer"
        try { python verify_audit.py } finally { Pop-Location }
    }

    "status" {
        # One place to answer "why is nothing arriving". Checks the whole path
        # from container to stored row, in the order it can break.
        Write-Host ""
        Write-Host "== containers ==" -ForegroundColor Cyan
        docker compose ps --format "table {{.Name}}\t{{.Status}}"

        Write-Host ""
        Write-Host "== Flink jobs ==" -ForegroundColor Cyan
        try {
            $jobs = Invoke-RestMethod -Uri "http://localhost:8081/jobs/overview" -TimeoutSec 5
            if ($jobs.jobs.Count -eq 0) {
                Write-Host "  no jobs submitted" -ForegroundColor Yellow
            }
            foreach ($j in $jobs.jobs) {
                $colour = if ($j.state -eq "RUNNING") { "Green" } else { "Red" }
                Write-Host ("  {0,-40} {1}" -f $j.name, $j.state) -ForegroundColor $colour
                if ($j.state -ne "RUNNING") {
                    Write-Host "    -> resubmit with: .\run.ps1 submit-job" -ForegroundColor Yellow
                }
            }
        } catch {
            Write-Host "  Flink REST API unreachable on :8081" -ForegroundColor Red
        }

        Write-Host ""
        Write-Host "== rows in ClickHouse ==" -ForegroundColor Cyan
        docker compose exec clickhouse clickhouse-client -u $ChUser --password $ChPassword -q `
            "SELECT count() AS scored, uniqExact(transaction_id) AS distinct_txn FROM fraud.transactions_scored FORMAT Vertical"

        Write-Host ""
        Write-Host "== Kafka topic offsets ==" -ForegroundColor Cyan
        # kafka-get-offsets.sh: stable across Kafka versions, unlike GetOffsetShell.
        docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh `
            --bootstrap-server kafka:9092 --topic transactions.raw

        Write-Host ""
        Write-Host "== consumer group lag ==" -ForegroundColor Cyan
        docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh `
            --bootstrap-server kafka:9092 --describe --group fraud-cep
        Write-Host ""
    }

    "kill-worker" {
        # `kill`, not `stop`: a graceful stop checkpoints on the way out.
        docker compose kill taskmanager
        Start-Sleep -Seconds 2
        docker compose start taskmanager
        Write-Host "taskmanager killed and restarted" -ForegroundColor Yellow
    }

    "query-scored" {
        docker compose exec clickhouse clickhouse-client -u $ChUser --password $ChPassword -q "SELECT decision, count() FROM fraud.transactions_scored GROUP BY decision ORDER BY decision"
    }

    "pipeline" {
        # The whole sequence, stopping at the first failure. The wait is for
        # services that accept connections before they are ready to serve.
        Invoke-Step "removing old containers and volumes" { & $PSCommandPath clean }
        Invoke-Step "starting the stack (first run builds Flink, takes minutes)" { & $PSCommandPath up }
        Wait-Ready "neo4j" {
            $r = docker compose exec -T neo4j cypher-shell -u neo4j -p $Neo4jPassword "RETURN 1" 2>&1
            $LASTEXITCODE -eq 0
        }
        Wait-Ready "clickhouse" {
            $r = docker compose exec -T clickhouse clickhouse-client -u $ChUser --password $ChPassword -q "SELECT 1" 2>&1
            $LASTEXITCODE -eq 0
        }
        Invoke-Step "loading the account population into Neo4j" { & $PSCommandPath load-graph }
        Invoke-Step "replaying transactions into Kafka" { & $PSCommandPath produce }
        Invoke-Step "submitting the Flink job" { & $PSCommandPath submit-job }
        Write-Host ""
        Write-Host "Stack is running. Give the job a minute to drain the topic, then:" -ForegroundColor Green
        Write-Host "  .\run.ps1 query-scored     # confirm rows are landing"
        Write-Host "  .\run.ps1 latency          # the measurement"
        Write-Host "  Flink UI: http://localhost:8081"
    }

    "latency-setup" {
        # Same as `pipeline` without `produce`: a latency run needs an empty topic fed
        # at a rate the job keeps up with, not a 50k backlog.
        Invoke-Step "removing old containers and volumes" { & $PSCommandPath clean }
        Invoke-Step "starting the stack (first run builds Flink, takes minutes)" { & $PSCommandPath up }
        Wait-Ready "neo4j" {
            $r = docker compose exec -T neo4j cypher-shell -u neo4j -p $Neo4jPassword "RETURN 1" 2>&1
            $LASTEXITCODE -eq 0
        }
        Wait-Ready "clickhouse" {
            $r = docker compose exec -T clickhouse clickhouse-client -u $ChUser --password $ChPassword -q "SELECT 1" 2>&1
            $LASTEXITCODE -eq 0
        }
        Invoke-Step "loading the account population into Neo4j" { & $PSCommandPath load-graph }
        Invoke-Step "submitting the Flink job" { & $PSCommandPath submit-job }
        Write-Host ""
        Write-Host "Empty topic, job running. Now feed it a paced stream:" -ForegroundColor Green
        Write-Host "  .\run.ps1 produce-stream-docker 7000           # about 30 min at this pacing"
        Write-Host "  cd stream-processor"
        Write-Host "  python experiments/latency.py --since-minutes 35"
        Write-Host ""
        Write-Host "  produce-stream-docker, NOT produce-stream: the plain target runs the" -ForegroundColor Yellow
        Write-Host "  producer on the host, so ingested_at and scored_at_job come from two" -ForegroundColor Yellow
        Write-Host "  clocks. Measured offsets reached +205 ms and -279 ms minutes apart -" -ForegroundColor Yellow
        Write-Host "  the same order as the quantity being measured - and that is what" -ForegroundColor Yellow
        Write-Host "  produced a stable 640 ms tail no amount of tuning could move." -ForegroundColor Yellow
    }

    default {
        Write-Host "unknown target '$Target'" -ForegroundColor Red
        & $PSCommandPath help
        exit 1
    }
}
