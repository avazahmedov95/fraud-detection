<#
.SYNOPSIS
    Operations entry point on Windows; the Makefile carries the same targets.

.DESCRIPTION
    ASCII only, deliberately. Windows PowerShell 5.1 reads .ps1 files as ANSI
    unless they carry a BOM, so any non-ASCII character is mangled into bytes
    that break the parser.

.EXAMPLE
    .\run.ps1 help
    .\run.ps1 pipeline      # clean -> up -> seed-confirmed -> produce -> submit-job
#>

param(
    [Parameter(Position = 0)]
    [string]$Target = "help",

    # The analyst queue (`cases`):
    #   .\run.ps1 cases
    #   .\run.ps1 cases -Case t_0041237
    #   .\run.ps1 cases -Case t_0041237 -Verdict CONFIRMED_FRAUD -By analyst.k
    #   .\run.ps1 cases -Case t_0041237 -Report -By analyst.k   (a client's report)
    #   .\run.ps1 cases -Stats
    [string]$Case = "",
    [ValidateSet("", "CONFIRMED_FRAUD", "FALSE_POSITIVE")]
    [string]$Verdict = "",
    [string]$By = "",
    [switch]$Report,
    [switch]$Stats
)

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

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

if (-not (Test-Path ".env")) {
    Write-Host ".env is missing: copy .env.example to .env and set the passwords in it" -ForegroundColor Red
    exit 1
}
$DotEnv = Get-DotEnv
# verify_audit.py reads the same credentials.
foreach ($k in "CLICKHOUSE_USER", "CLICKHOUSE_PASSWORD", "CLICKHOUSE_DB") {
    if ($DotEnv[$k] -and -not (Test-Path "env:$k")) { Set-Item "env:$k" $DotEnv[$k] }
}
$ChUser = $DotEnv.CLICKHOUSE_USER
$ChPassword = $DotEnv.CLICKHOUSE_PASSWORD

# Every module the PyFlink job imports, shipped with it (tools/boundary_audit.py
# checks this list and the Makefile's against the job's imports).
$JobModules = @(
    "config.py", "capabilities.py", "features.py", "geo.py", "rules.py",
    "receiver_store.py", "fusion.py", "payload_crypto.py"
) | ForEach-Object { "/opt/flink/usrjobs/$_" }

function Invoke-Step {
    param([string]$Description, [scriptblock]$Action)
    Write-Host ""
    Write-Host "==> $Description" -ForegroundColor Cyan
    & $Action
    if ($LASTEXITCODE -ne 0) { throw "step failed (exit $LASTEXITCODE): $Description" }
}

function Wait-Ready {
    # Poll a service until it answers, rather than sleeping a fixed interval.
    param([string]$Name, [scriptblock]$Probe, [int]$TimeoutSeconds = 120)
    Write-Host ""
    Write-Host "==> waiting for $Name" -ForegroundColor Cyan -NoNewline
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $ok = $false
        try { $ok = (& $Probe) } catch { $ok = $false }
        if ($ok) { Write-Host "  ready" -ForegroundColor Green; return }
        Write-Host "." -NoNewline
        Start-Sleep -Seconds 3
    }
    throw "$Name did not become ready within $TimeoutSeconds s. Check: docker compose logs $Name"
}

function Get-ActiveJobs {
    # Call it as @(Get-ActiveJobs): a function's output is unrolled, so one job comes
    # back as the job itself, whose .Count PowerShell 5.1 leaves empty - and the
    # guard against a second job then let one through.
    $jobs = (Invoke-RestMethod -Uri "http://localhost:8081/jobs/overview" -TimeoutSec 5).jobs
    return @($jobs | Where-Object { $_.state -notin @("FAILED", "CANCELED", "FINISHED") })
}

function Assert-JobRunning {
    # Traffic nothing scores looks like success: Kafka accepts it and every
    # container is Up. Recreating the jobmanager discards the job.
    $deadline = (Get-Date).AddSeconds(60)
    while ((Get-Date) -lt $deadline) {
        try { $active = @(Get-ActiveJobs) } catch {
            Write-Host "Flink REST API unreachable on :8081 - is the stack up?" -ForegroundColor Red
            return $false
        }
        if (@($active | Where-Object { $_.state -eq "RUNNING" }).Count -gt 0) { return $true }
        if ($active.Count -eq 0) { break }
        Start-Sleep -Seconds 3
    }
    Write-Host "NO RUNNING FLINK JOB - this traffic would not be scored. Resubmit: .\run.ps1 resume-job" -ForegroundColor Red
    return $false
}

function Get-LatestCheckpoint {
    # A resubmitted job gets a new id, so the newest checkpoint is searched for
    # across job ids. $null when there is none.
    $prev = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $out = docker compose exec -T jobmanager sh -c "ls -dt /opt/flink/checkpoints/*/chk-* 2>/dev/null | head -1" 2>$null
    } finally { $ErrorActionPreference = $prev }
    $path = ("$out").Trim()
    if ($path -match '^/opt/flink/checkpoints/') { return $path }
    return $null
}

function Restart-TaskManager {
    # A fresh TaskManager JVM per job: cancelling a job does not give back its
    # Metaspace, and the eighth job on one process died of it. Waits for a NEW
    # registration, not the old one lingering until its heartbeat times out.
    $oldIds = $null
    $deadline = (Get-Date).AddSeconds(60)
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
    # Without a checkpoint the job starts with EMPTY keyed state: every sender's
    # windows begin blank. resume-job restores the newest checkpoint instead.
    param([string]$FromCheckpoint)

    $active = @(Get-ActiveJobs)
    if ($active.Count -gt 0) {
        # A second job would read every partition and score every event twice.
        Write-Host "A JOB IS ALREADY ACTIVE ($($active[0].jid)) - cancel it first:" -ForegroundColor Red
        Write-Host '    Invoke-RestMethod -Method Patch "http://localhost:8081/jobs/<jid>?mode=cancel"'
        exit 1
    }
    Restart-TaskManager
    & $PSCommandPath serve-prep

    Push-Location "stream-processor"
    try {
        $bundleTime = python -c "import config; print(config.PY_BUNDLE_TIME_MS)"
        $bundleSize = python -c "import config; print(config.PY_BUNDLE_SIZE)"
    } finally { Pop-Location }

    # An array, splatted: written inline, PowerShell 5.1 splits
    # "-Dpython.fn-execution.bundle.time=50" at the first dot.
    $dockerArgs = @("compose", "exec", "jobmanager", "flink", "run", "-d",
        "-Dpython.fn-execution.bundle.time=$bundleTime",
        "-Dpython.fn-execution.bundle.size=$bundleSize")
    if ($FromCheckpoint) {
        Write-Host "restoring keyed state from $FromCheckpoint" -ForegroundColor Green
        $dockerArgs += @("-s", $FromCheckpoint)
    } else {
        Write-Host "starting with EMPTY keyed state. To keep state: .\run.ps1 resume-job" -ForegroundColor Yellow
    }
    $dockerArgs += @("-py", "/opt/flink/usrjobs/fraud_job.py", "--pyFiles", ($JobModules -join ","))
    & docker @dockerArgs
}

switch ($Target.ToLower()) {

    "help" {
        $targets = [ordered]@{
            "up"             = "build images and start the whole stack"
            "down"           = "stop the stack (keep data volumes)"
            "clean"          = "stop the stack and DELETE all data volumes"
            "ps"             = "list running services"
            "logs"           = "tail logs from all services"
            "topics"         = "list Kafka topics"
            "make-certs"     = "generate the TLS certificates Kafka needs (before the first up)"
            "generate"       = "generate the synthetic dataset"
            "produce"        = "replay the dataset into Kafka (batch)"
            "produce-stream" = "replay paced to the original timing (200x)"
            "export-model"   = "the trained model to ONNX for the job, in a container"
            "seed-confirmed" = "load the history's confirmed fraud accounts into Redis"
            "retrain"        = "a new model from the logged decisions and people's verdicts, against the served one"
            "promote-model"  = "serve the retrained model (after reading retrain's comparison)"
            "serve-prep"     = "copy the model, its cut-off and the second look's band next to the job"
            "submit-job"     = "fresh TaskManager, then the PyFlink job (empty state)"
            "resume-job"     = "same, restoring keyed state from the newest checkpoint"
            "cases"          = "the analyst queue (-Case <id> to show, +-Verdict/-By to resolve, +-Report/-By for a client's report, -Stats)"
            "status"         = "containers, job, rows, offsets"
            "query-scored"   = "decision counts in ClickHouse"
            "sink-logs"      = "tail the sink-writer logs"
            "verify-audit"   = "recompute the audit hash chain, report tampering"
            "boundaries"     = "check every place one component hands something to another"
            "pipeline"       = "clean -> up -> seed-confirmed -> produce -> submit-job"
        }
        Write-Host ""
        Write-Host "  Usage: .\run.ps1 <target>"
        Write-Host ""
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

    "make-certs" {
        # A missing certs directory is mounted empty and the broker fails to start.
        $certs = Join-Path (Get-Location) "infra\kafka\certs"
        New-Item -ItemType Directory -Force -Path $certs | Out-Null
        $script = Join-Path (Get-Location) "infra\kafka\make-certs.sh"
        docker run --rm --entrypoint sh -v "${certs}:/certs" -v "${script}:/make-certs.sh:ro" `
            -e CERTS=/certs alpine/openssl:latest /make-certs.sh
        # keytool from the Kafka image's JDK: an openssl-made PKCS12 truststore
        # reads back as zero entries in Java.
        $ts = Join-Path (Get-Location) "infra\kafka\make-truststore.sh"
        docker run --rm --entrypoint sh -v "${certs}:/certs" -v "${ts}:/make-truststore.sh:ro" `
            -e CERTS=/certs "apache/kafka:$($DotEnv.KAFKA_IMAGE -replace '.*:', '')" /make-truststore.sh
    }

    "generate" {
        Push-Location data-generator
        try { python generator.py --out ./out } finally { Pop-Location }
    }

    "produce" {
        Push-Location data-generator
        try {
            python kafka_producer.py --file out/transactions.csv --bootstrap 127.0.0.1:29092 --topic transactions.raw
        } finally { Pop-Location }
    }

    "produce-stream" {
        if (-not (Assert-JobRunning)) { break }
        Push-Location data-generator
        try {
            python kafka_producer.py --file out/transactions.csv --realtime --speed 200 --bootstrap 127.0.0.1:29092 --topic transactions.raw
        } finally { Pop-Location }
    }

    "export-model" {
        # In a container: Smart App Control blocks onnx's native library on this
        # host (infra/ml/Dockerfile). The repository is mounted, so the files land
        # in ml/models.
        docker build -q -t fraud-ml-export -f infra/ml/Dockerfile .
        if ($LASTEXITCODE -eq 0) {
            docker run --rm -v "$((Get-Location).Path):/repo" fraud-ml-export
        }
    }

    "retrain" {
        # Reads the warehouse, so the stack must be up; writes ml/models/candidate/.
        Push-Location ml
        try { python retrain.py --cache models_matrix.npz } finally { Pop-Location }
    }

    "promote-model" {
        # The steps after train.py, for retrain.py's candidate: export, the second
        # look's band under the new cut-off, then everything that reads the model.
        if (-not (Test-Path "ml/models/candidate/model.joblib")) {
            throw "no candidate in ml/models/candidate: run .\run.ps1 retrain first"
        }
        Copy-Item "ml/models/candidate/*" "ml/models/" -Force
        & $PSCommandPath export-model
        $tabpfn = Join-Path (Split-Path (Get-Location).Path) ".venv-models\Scripts\python.exe"
        if ((Test-Path $tabpfn) -and $DotEnv.TABPFN_CHECKPOINT) {
            $ckpt = (Resolve-Path $DotEnv.TABPFN_CHECKPOINT).Path
            Push-Location ml
            try { & $tabpfn second_look.py --cache models_matrix.npz --tabpfn-model $ckpt } finally { Pop-Location }
        } else {
            Write-Host "The second look's band was not re-chosen (it needs ..\.venv-models and TABPFN_CHECKPOINT): the second look stays off under the new cut-off." -ForegroundColor Yellow
        }
        & $PSCommandPath resume-job
        docker compose up -d --build case-manager
        docker compose restart second-look demo
    }

    "seed-confirmed" {
        Push-Location ml
        try { python seed_confirmed.py --port $DotEnv.REDIS_HOST_PORT } finally { Pop-Location }
    }

    "serve-prep" {
        Copy-Item "ml/models/model.onnx" "stream-processor/" -Force
        Copy-Item "ml/models/thresholds.json" "stream-processor/" -Force
        if (Test-Path "ml/models/second_look.json") {
            Copy-Item "ml/models/second_look.json" "stream-processor/" -Force
        }
        Write-Host "model, cut-off and the second look's band copied to stream-processor/"
    }

    "submit-job" { Invoke-SubmitJob }

    "resume-job" {
        $chk = Get-LatestCheckpoint
        if (-not $chk) {
            Write-Host "no retained checkpoint - start fresh with: .\run.ps1 submit-job" -ForegroundColor Yellow
            break
        }
        Invoke-SubmitJob -FromCheckpoint $chk
    }

    "cases" {
        if ($Stats) {
            $argv = @("stats")
        } elseif ($Case -and ($Verdict -or $Report)) {
            if (-not $By) { throw "-By is required: an unattributed label cannot be audited or withdrawn." }
            $argv = if ($Report) { @("report", $Case, "--by", $By) } else { @("resolve", $Case, $Verdict, "--by", $By) }
        } elseif ($Case) {
            $argv = @("show", $Case)
        } else {
            $argv = @("list")
        }
        docker compose exec -T case-manager python queue_cli.py @argv
    }

    "status" {
        Write-Host ""
        Write-Host "== containers ==" -ForegroundColor Cyan
        docker compose ps --format "table {{.Name}}\t{{.Status}}"
        Write-Host ""
        Write-Host "== Flink jobs ==" -ForegroundColor Cyan
        try {
            $jobs = (Invoke-RestMethod -Uri "http://localhost:8081/jobs/overview" -TimeoutSec 5).jobs
            if ($jobs.Count -eq 0) { Write-Host "  no jobs submitted" -ForegroundColor Yellow }
            foreach ($j in $jobs) {
                $colour = if ($j.state -eq "RUNNING") { "Green" } else { "Red" }
                Write-Host ("  {0,-40} {1}" -f $j.name, $j.state) -ForegroundColor $colour
            }
        } catch {
            Write-Host "  Flink REST API unreachable on :8081" -ForegroundColor Red
        }
        Write-Host ""
        Write-Host "== rows in ClickHouse ==" -ForegroundColor Cyan
        docker compose exec clickhouse clickhouse-client -u $ChUser --password $ChPassword -q `
            "SELECT count() AS scored, uniqExact(transaction_id) AS distinct_txn FROM fraud.transactions_scored FORMAT Vertical"
        Write-Host ""
        Write-Host "== Kafka offsets and the job's lag ==" -ForegroundColor Cyan
        docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh `
            --bootstrap-server kafka:9092 --describe --group fraud-cep
        Write-Host ""
    }

    "query-scored" {
        docker compose exec clickhouse clickhouse-client -u $ChUser --password $ChPassword -q "SELECT decision, count() FROM fraud.transactions_scored GROUP BY decision ORDER BY decision"
    }

    "sink-logs" { docker compose logs -f sink-writer }

    "verify-audit" {
        Push-Location "sink-writer"
        try { python verify_audit.py } finally { Pop-Location }
    }

    "boundaries" { python tools/boundary_audit.py -v }

    "pipeline" {
        Invoke-Step "removing old containers and volumes" { & $PSCommandPath clean }
        Invoke-Step "starting the stack (first run builds Flink, takes minutes)" { & $PSCommandPath up }
        Wait-Ready "clickhouse" {
            $r = docker compose exec -T clickhouse clickhouse-client -u $ChUser --password $ChPassword -q "SELECT 1" 2>&1
            $LASTEXITCODE -eq 0
        }
        Invoke-Step "loading the confirmed fraud accounts into Redis" { & $PSCommandPath seed-confirmed }
        Invoke-Step "replaying transactions into Kafka" { & $PSCommandPath produce }
        Invoke-Step "submitting the Flink job" { & $PSCommandPath submit-job }
        Write-Host ""
        Write-Host "Stack is running. Flink UI: http://localhost:8081, demo: http://localhost:8090" -ForegroundColor Green
    }

    default {
        Write-Host "unknown target '$Target' - see .\run.ps1 help" -ForegroundColor Red
        exit 1
    }
}
