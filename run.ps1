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

    # Message count for the produce-stream targets. Zero means "until Ctrl+C".
    # For an A/B comparison always set it: stopping two arms by hand gives them
    # different lengths and different amounts of enrichment-cache warming, and
    # that difference is larger than the effect such comparisons measure.
    #   .\run.ps1 produce-stream-docker 400
    #   .\run.ps1 produce-stream-secure 400
    [Parameter(Position = 1)]
    [int]$Count = 0,

    # Close and reopen the producer every N messages. The transport arms hold
    # ONE connection each, so the handshake is amortised over the whole arm and
    # what they measure is mostly TLS record framing. A switch reconnects; this
    # is the only condition under which the mutual-TLS answer could change.
    #   .\run.ps1 measure-tls 1200 -Reconnect 20
    [int]$Reconnect = 0,

    # The analyst queue (`cases`). Named rather than positional: position 1 is
    # already an [int] for the produce/measure targets, so a bare
    # `.\run.ps1 cases list` would fail binding "list" to a count before this
    # script ever ran.
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

# The PyFlink job needs every module it imports shipped with it. Missing one
# fails at submit time with an ImportError inside the cluster, a long way from
# where the mistake was made - so the list lives in one place.
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

    # Poll rather than sample once. A freshly submitted job spends several
    # seconds in CREATED/INITIALIZING while it restores state and starts the
    # Python worker, so an instantaneous check right after `resume-job` reports
    # "no running job" for a job that is about to run perfectly well - which is
    # a guard that blocks correct work, the worst kind.
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
        wall-clock state lives in Redis. Flushing it is therefore sufficient,
        and no job restart is needed between passes.
    #>
    $del = {
        param($pattern)
        docker compose exec -T redis sh -c "redis-cli --scan --pattern '$pattern' | xargs -r redis-cli DEL" | Out-Null
    }
    & $del "age:*"
    & $del "rcv:*"
    docker compose exec -T redis sh -c "redis-cli DEL mule:fanin:hist" | Out-Null
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
            # Back up WHILE the producer is still running - that is the whole
            # experiment. A restart deferred to the finally below would land
            # after the producer had already given up.
            Write-Host "==> start kafka (20 s outage)" -ForegroundColor Cyan
            docker compose start kafka | Out-Null
            Wait-Job $job | Out-Null
            $out = (Receive-Job $job 2>&1 | Out-String)
            Remove-Job $job
            Write-Host $out
            # The broker is already back - it had to be, for the producer to
            # finish - so this drain is the ordinary one, not a degraded one.
            # The transport arm cannot be drained WHILE it is down, which is
            # exactly why it reads as the control for the transport rather
            # than as a treatment.
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
            # DRAIN WHILE IT IS STILL DOWN. Restarting first and draining after
            # lets the tail of the queue be scored with the dependency healthy,
            # so the arm mixes degraded and healthy records in a ratio nobody
            # measured. The finally below is what brings the service back.
            #
            # TIME IT. The first run of this shape treated a drain that did not
            # finish as a warning and moved on - and that warning was the whole
            # result: the healthy pass drained in seconds, redis and neo4j did
            # not drain in five minutes. Failing OPEN is not the same as failing
            # FAST, and the difference only shows on the clock.
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
        # THE RESTART BELONGS HERE. It used to sit in the happy path, so a
        # producer that died - or a Ctrl+C - left the service stopped, and the
        # NEXT run could not read its baseline from a warehouse that was still
        # down. A harness that leaves the cluster broken costs more than the
        # measurement it was taking.
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
        # Declared rather than inherited. Without it PowerShell would still find
        # the script-level $Reconnect by dynamic scoping and the arm would run
        # correctly - until someone moved this function, at which point churn
        # would silently become 0 and the arm would report a no-churn result
        # under a churn label. The banner below prints what was actually bound.
        [int]$Reconnect = 0
    )
    if ($Messages -le 0) { $Messages = 400 }

    if (-not (Assert-JobRunning)) { return }

    # The transport is fixed when the job graph is built, so a `tls` arm run
    # against a job that connected in plaintext measures nothing. Check the
    # container's own environment rather than trusting that the right variables
    # were exported before the last resubmit.
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

    # Warm-up, discarded. Switching transports REQUIRES recreating the Flink
    # containers, so whichever arm runs first after that recreate meets a cold
    # JVM, an unwarmed JIT and fresh connections. Measured without this, the
    # mutual-TLS arm came out 41 ms FASTER at the median than plaintext with
    # non-overlapping confidence intervals - a clean, impossible result, and a
    # sign that the deployment's age was dominating the transport being tested.
    #
    # These records are produced before the drain and the settle below, so they
    # are outside the reporting window by construction.
    $warm = [Math]::Max(100, [int]($Messages / 4))
    Write-Host "==> warm-up: $warm messages, discarded" -ForegroundColor Cyan
    switch ($Arm) {
        "plain"  { & $PSCommandPath produce-stream-docker $warm -Reconnect $Reconnect }
        "tls"    { & $PSCommandPath produce-stream-tls    $warm -Reconnect $Reconnect }
        "crypto" { & $PSCommandPath produce-stream-secure $warm -Reconnect $Reconnect }
    }

    if (-not (Wait-Drained "the backlog")) { return }

    # A quiet gap so rows written by whatever ran before this fall outside the
    # reporting window. latency_report filters on write time, not send time.
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
    try { python latency_report.py --since-minutes $window } finally { Pop-Location }
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

    & $PSCommandPath serve-prep
    $modules = $JobModules -join ","

    Push-Location "stream-processor"
    try {
        $bundleTime = python -c "import config; print(config.PY_BUNDLE_TIME_MS)"
        $bundleSize = python -c "import config; print(config.PY_BUNDLE_SIZE)"
    } finally { Pop-Location }

    # Built as an array and splatted. Written inline, PowerShell 5.1 splits
    # "-Dpython.fn-execution.bundle.time=50" at the first dot and passes the
    # remainder as a positional argument, which Flink then reads as a JAR
    # path. Array elements are passed through untouched.
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
            "submit-job"     = "submit the PyFlink CEP+ML job (empty state)"
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
        try { python generator.py --out ./out } finally { Pop-Location }
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
        # Runs the producer INSIDE the Docker network, which is the only way to
        # get a trustworthy latency figure on Windows or macOS.
        #
        # `ingested_at` is stamped by the producer and `scored_at_job` by Flink.
        # Run from the host, those are two different clocks: containers live in
        # a VM whose clock drifts from the host and is resynced periodically.
        # A measured +205 ms offset - and the jumps when it resyncs - land
        # straight in the decision-path figure, and produced a stable-looking
        # 640 ms tail that responded to no amount of tuning because it was not
        # latency at all.
        #
        # Inside the network, producer, Flink and ClickHouse share one clock.
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

    # Latency against offered load. One arm per rate, each with its own ingest
    # window, so the arms can run back to back without the 90 s settle the
    # transport comparison needs - those arms are separated by WRITE time, these
    # by INGEST time, which is exact.
    "measure-throughput" {
        if (-not (Assert-JobRunning)) { break }
        # The knee is BELOW 100. Measured at 3 ev/s the decision path sits at
        # 88 ms; at 100 ev/s it is already 1286 ms. Sweeping 100..5000 samples
        # nothing but the saturated regime - every arm reads the same because
        # they are all past the limit.
        $rates = if ($Rates) { $Rates } else { @(5, 10, 25, 50, 100, 250) }
        $n = if ($Count -gt 0) { $Count } else { 3000 }

        Write-Host ""
        Write-Host "=== THROUGHPUT SWEEP: $($rates -join ', ') ev/s, $n messages each ===" -ForegroundColor Cyan

        # One warm-up, discarded. A cold JVM and unwarmed JIT would be charged
        # to whichever rate happens to run first, and the sweep would report a
        # knee that is really the deployment's age.
        # Flush BEFORE the warm-up, not after. Flushing after threw the warm-up
        # away and charged every cold Neo4j lookup to whichever arm ran first -
        # which made the 100 ev/s arm look worse than the 1000 ev/s one. All
        # arms have to meet the cache in the same state, and the state worth
        # reporting is the warm one, because that is steady operation.
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
        python stream-processor/throughput_report.py
    }

    # One dependency at a time: take it out, produce the SAME slice through the
    # outage, bring it back, and report what the pipeline silently stopped
    # doing. -Service picks one; with none, all four run in sequence.
    #
    # A healthy CONTROL pass always runs first and is not optional. Every arm
    # replays the same transactions, so the alert mix an arm produces means
    # something only against the mix those same transactions produce with
    # nothing stopped. Measured against the whole table instead, all four arms
    # report the same "degradation" - including the arm that predicts none -
    # because what is really being reported is the contents of the slice.
    "kill-dependency" {
        if (-not (Assert-JobRunning)) { break }
        $svcs = if ($Service) { @($Service) } else { @("redis", "neo4j", "clickhouse", "kafka") }
        # Prepended, never skipped: a stale reference from an earlier run is
        # the same error this control exists to remove.
        $svcs = @("control") + $svcs
        $n = if ($Count -gt 0) { $Count } else { 1000 }
        # Every arm reads its baseline FROM ClickHouse, the clickhouse arm
        # included, so the warehouse has to be up before the matrix starts.
        # Without this the before-phase exits 1, PowerShell ignores a native
        # exit code, and the arm spends five minutes producing traffic it has
        # nothing to compare against.
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
            python stream-processor/dependency_failure.py --service $svc --phase before
            if ($LASTEXITCODE -ne 0) {
                Write-Host "no baseline for $svc - skipping the arm rather than measuring against nothing" -ForegroundColor Red
                continue
            }
            # -Last 1: a PowerShell function returns everything it emitted, not
            # just the value after `return`. An array here would become several
            # --sent arguments and argparse would reject the call.
            $sent = [int]((Invoke-DependencyOutage -Service $svc -Count $n) | Select-Object -Last 1)
            # The topic drained inside the call, while the service was still
            # down. What is left is the sink batch - 500 rows or 5 s - plus
            # room for the restarted service to accept connections again.
            Write-Host "==> letting the sink flush" -ForegroundColor Cyan
            Start-Sleep -Seconds 30
            python stream-processor/dependency_failure.py --service $svc --phase after --expect $n --sent $sent
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
        # Private CA, broker certificate and client certificate for the mutual
        # TLS arm. Must run BEFORE the first `up`: docker-compose mounts
        # infra/kafka/certs into the broker, and a missing directory is created
        # empty, which the broker then fails to start against.
        $certs = Join-Path (Get-Location) "infra\kafka\certs"
        New-Item -ItemType Directory -Force -Path $certs | Out-Null
        $script = Join-Path (Get-Location) "infra\kafka\make-certs.sh"
        # --entrypoint sh is required: the alpine/openssl image sets ENTRYPOINT
        # to `openssl`, so a shell command appended to it is read as an openssl
        # subcommand ("Invalid command 'sh'").
        docker run --rm `
            --entrypoint sh `
            -v "${certs}:/certs" `
            -v "${script}:/make-certs.sh:ro" `
            -e CERTS=/certs `
            alpine/openssl:latest /make-certs.sh

        # Second stage in the Kafka image, which ships a JDK. keytool is
        # required for the truststore: a PKCS12 written by `openssl pkcs12
        # -export -nokeys` reads back as ZERO entries in Java, so the broker
        # would come up trusting nothing and reject every client certificate.
        $ts = Join-Path (Get-Location) "infra\kafka\make-truststore.sh"
        docker run --rm `
            --entrypoint sh `
            -v "${certs}:/certs" `
            -v "${ts}:/make-truststore.sh:ro" `
            -e CERTS=/certs `
            "apache/kafka:$($DotEnv.KAFKA_IMAGE -replace '.*:', '')" /make-truststore.sh
    }

    "handshake-bench" {
        # What ONE connection costs, plaintext against mutual TLS. The transport
        # arms hold a single connection each, so this is the figure they cannot
        # resolve - the same relationship the AES-GCM microbenchmark has to the
        # payload arms in 7.4. Runs inside the network for the certificates and
        # for one clock. `-Count` sets the number of pairs, default 30.
        $genPath = (Resolve-Path "data-generator").Path
        $certPath = (Resolve-Path "infra\kafka\certs").Path
        $pairs = if ($Count -gt 0) { $Count } else { 30 }
        docker run --rm -i `
            --network fraud-detection_fraudnet `
            -v "${genPath}:/gen" -v "${certPath}:/certs:ro" -w /gen `
            fraud-sink-writer:latest `
            python handshake_bench.py --n $pairs
    }

    "produce-stream-tls" {
        if (-not (Assert-JobRunning)) { break }
        # Transport arm: same producer, same pacing, same payload - only the
        # listener differs (9094, mutual TLS, instead of 9092 plaintext).
        # Combine with `-Count` so both arms are the same length.
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
        # Identical to produce-stream-docker except that payloads are encrypted:
        # the two are the arms of the security-overhead measurement (reviewer
        # point 3), so everything else about them must stay the same - same
        # image, same network, same pacing, same clock.
        #
        # The cluster already holds the key, and it decrypts per record based on
        # the envelope prefix, so no restart is needed to switch arms.
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
        # `docker compose exec -T` has no TTY, so the script is piped in.
        Get-Content "infra/neo4j/import.cypher" | docker compose exec -T neo4j cypher-shell -u neo4j -p $Neo4jPassword
    }

    # Reading a file says a component is right; it does not say that what it
    # PRODUCES is what the next one EXPECTS. Three defects in one day lived in
    # that gap. Run this before a walkthrough, and after touching any record,
    # schema or wire format.
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
        # The BIN table. bins.py resolves the card issuer from it, and the job
        # dir is what gets mounted into the cluster - without this the job dies
        # at import with FileNotFoundError instead of scoring.
        Copy-Item "data-generator/banks.csv" "stream-processor/" -Force
        Write-Host "model + feature spec copied to stream-processor/"
    }

    # The latency knobs go to the client as -D. The job sets them itself via
    # env.configure(), but options read during job-graph translation are safest
    # given to the client directly. Values come from config.py, so there is
    # still one source of truth. See Invoke-SubmitJob.
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
        try { python latency_report.py } finally { Pop-Location }
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
        # kafka-get-offsets.sh, not kafka-run-class kafka.tools.GetOffsetShell:
        # the class moved to org.apache.kafka.tools in Kafka 3.x and the old
        # path fails with ClassNotFoundException. The wrapper script is stable
        # across versions.
        docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh `
            --bootstrap-server kafka:9092 --topic transactions.raw

        Write-Host ""
        Write-Host "== consumer group lag ==" -ForegroundColor Cyan
        docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh `
            --bootstrap-server kafka:9092 --describe --group fraud-cep
        Write-Host ""
    }

    "kill-worker" {
        # Deliberate fault for the exactly-once investigation. `kill` rather
        # than `stop`: a graceful stop checkpoints on the way out, which would
        # test nothing.
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
        # Same as `pipeline` but WITHOUT `produce`. The batch dump puts 50k
        # messages into the topic at once, and the resulting backlog takes
        # minutes to drain - every latency figure measured against it is queue
        # depth. For a latency run the topic has to start empty and be fed at a
        # rate the job can keep up with, which is what produce-stream does.
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
        Write-Host "  python latency_report.py --since-minutes 35"
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
