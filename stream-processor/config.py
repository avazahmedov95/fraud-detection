"""
Configuration for the Flink stream processor (enrichment + CEP).

Connection defaults assume the job runs INSIDE the Flink container on the Docker
network (service names). Override via environment variables for other setups.

Thresholds and weights are deliberately explicit so they can be tuned and
documented. STRUCTURING_THRESHOLD and LIMIT_DAILY are mirrored from the
generator and MUST match it - the generator places structuring amounts just
under the threshold this side watches, which is why that pattern's recall is
partly true by construction. Neither is a figure from Regulation No. 3759: that
document sets information-security requirements and no sum thresholds. See the
correction in data-generator/config.py.
"""

import os

# --- Connections ------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")

# --- Transport security (reviewer point 3, mutual TLS arm) ------------------
# The broker exposes plaintext on 9092 and mutual TLS on 9094, both serving the
# same partitions, so the two arms of the measurement differ only in which
# listener the clients dial. Unlike payload encryption - decided per record - the
# transport is fixed when the job graph is built, so switching arms needs a
# resubmit. That asymmetry is worth stating when reporting: the payload arms ran
# against one deployment, the transport arms did not.
KAFKA_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT").upper()
KAFKA_SSL_CA = os.getenv("KAFKA_SSL_CA", "/certs/ca.crt")
KAFKA_SSL_KEYSTORE = os.getenv("KAFKA_SSL_KEYSTORE", "/certs/client.keystore.pem")


def kafka_security_properties():
    """Connector properties for the configured transport; empty on PLAINTEXT.

    PEM keystores rather than JKS - the Java client has accepted them since
    Kafka 2.7, and it means the broker, this job and the Python producer all
    read the same files.

    `ssl.endpoint.identification.algorithm=https` is left ON deliberately. The
    usual shortcut in a prototype is to disable hostname verification; it would
    also remove part of the handshake cost this measurement exists to quantify,
    turning a security-overhead figure into an argument for not verifying.
    """
    if KAFKA_SECURITY_PROTOCOL != "SSL":
        return {}
    return {
        "security.protocol": "SSL",
        "ssl.truststore.type": "PEM",
        "ssl.truststore.location": KAFKA_SSL_CA,
        "ssl.keystore.type": "PEM",
        "ssl.keystore.location": KAFKA_SSL_KEYSTORE,
        "ssl.endpoint.identification.algorithm": "https",
    }
TOPIC_RAW = os.getenv("TOPIC_RAW", "transactions.raw")
TOPIC_SCORED = os.getenv("TOPIC_SCORED", "transactions.scored")
TOPIC_ALERTS = os.getenv("TOPIC_ALERTS", "fraud.alerts")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "fraud-cep")

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "fraud_neo4j")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
ENRICH_CACHE_TTL_S = 3600           # cache receiver-age lookups for 1h

SECS_LOGIN_MIN_HISTORY = 5           # холодный старт: до 5 наблюдений z=0
W_COACHED_SESSION = 0.35             # в один ряд с W_NEW_PAYEE_HIGH
COACHED_SESSION_Z = 2.0

# --- Deployment capabilities -------------------------------------------------
# Which features and rules are active depends on what the deploying bank can
# observe (MyID integration, session telemetry, geo, receiver account age...).
# That lives in capabilities.py and is configured through CAP_* environment
# variables — see that module. Changing any of them changes the feature
# contract, so retrain and re-export afterwards; ml/ablation.py does both.

# --- Mirrored from the generator (must match it) -----------------------------
# Chosen figures, not regulatory ones - see data-generator/config.py for what
# 3759 actually says and where the real BRV-denominated threshold sits.
STRUCTURING_THRESHOLD = 10_000_000  # UZS — the band structuring stays under
LIMIT_DAILY = 100_000_000           # UZS — a bank operating limit, not a CBU one

# --- Rule windows (seconds) -------------------------------------------------
VELOCITY_WINDOW_S = 600             # 10 min
STRUCTURING_WINDOW_S = 3600         # 1 h
DISTINCT_PAYEE_WINDOW_S = 600       # 10 min
DAILY_WINDOW_S = 86400              # 24 h
RECENT_RETENTION_S = 86400          # prune per-sender history older than 24h

# --- Rule thresholds --------------------------------------------------------
VELOCITY_MAX_COUNT = 5              # > 5 transfers in the velocity window
STRUCTURING_MIN_COUNT = 3          # >= 3 sub-threshold transfers in the window
STRUCTURING_BAND_LOW = 0.80        # "just under" band: [0.80*T, 1.0*T)
DISTINCT_PAYEE_MAX = 5             # > 5 distinct payees in the window (fan-out)
AMOUNT_DEVIATION_SIGMA = 4.0       # amount > mean + sigma*std
# IMPOSSIBLE_TRAVEL: geo change that no real journey could achieve. 900 km/h is
# commercial jet cruise speed, so anything above it is physically impossible
# rather than merely unusual — this is what separates the rule from
# GEO_ANOMALY, which fires on any away-from-home region and false-positives on
# ordinary travellers. The distance floor absorbs the error introduced by
# representing a region by its administrative centre (see geo.py): adjacent
# regions such as Tashkent City / Tashkent Region are ~35 km apart at their
# centres but border each other, so short hops are never judged impossible.
MAX_PLAUSIBLE_KMH = 900.0
MIN_TRAVEL_DISTANCE_KM = 100.0
# MULE_FAN_IN: distinct senders converging on one receiver. Set above the
# DISTINCT_PAYEE_MAX fan-OUT threshold: receiving from several people in an hour
# is ordinary (a shared bill, a family pooling money), whereas paying out to
# several unrelated new payees is not.
RECEIVER_WINDOW_S = 3600
MULE_FAN_IN_MIN_SENDERS = 6

# How that threshold is applied. "absolute" is the deployed behaviour and the
# default: fire at >= MULE_FAN_IN_MIN_SENDERS, full stop.
#
# "relative" replaces the constant with a quantile of the population's OWN
# live distribution of rcv_distinct_senders_1h. The reason is measured, not
# aesthetic: running the deployed rules on IBM AMLSim (validation/README.md 3)
# showed that 6 encodes the in-degree density of THIS generator's population.
# On a scale-free transaction graph 2.69% of receiver-days exceed it as
# ordinary hub behaviour, and the rule fired on 3.12% of legitimate traffic
# while catching 0.0% of the fan-in typology. The constant was an unstated
# assumption about the deployment population.
#
# Normalising against the RECEIVER's own history - the idiom used by amount_z
# and secs_login_z - was considered and rejected: mule drop accounts are
# frequently fresh (that is what FRESH_RECEIVER is for), so a per-receiver
# baseline is absent exactly where the rule is needed. The population baseline
# has no such hole.
#
# This does NOT recover the AMLSim result and is not claimed to. There the sign
# was inverted - SAR receivers had FEWER senders - and a high-tail quantile
# cannot fix a sign flip, nor should it: AML layering and P2P mule collection
# are different phenomena. What it fixes is silent recalibration when the same
# rule meets a bank with different traffic density.
MULE_FAN_IN_MODE = os.getenv("MULE_FAN_IN_MODE", "absolute")   # absolute | relative
MULE_FAN_IN_QUANTILE = float(os.getenv("MULE_FAN_IN_QUANTILE", "0.999"))
# Below this many observations the population estimate is not worth trusting and
# the rule falls back to the absolute constant. Failing back to a known
# behaviour beats firing on an estimate built from nothing.
MULE_FAN_IN_MIN_OBS = int(os.getenv("MULE_FAN_IN_MIN_OBS", "5000"))
# Recomputing an exact quantile from the histogram is O(bins); doing it on every
# event is waste. The threshold moves slowly, so it is cached and refreshed.
MULE_FAN_IN_REFRESH_EVERY = int(os.getenv("MULE_FAN_IN_REFRESH_EVERY", "512"))
AMOUNT_DEVIATION_MIN_HISTORY = 5   # need this much history before deviation fires
NEW_PAYEE_AMOUNT_FACTOR = 3.0      # amount > factor * sender mean
NEW_PAYEE_ABS_FLOOR = 2_000_000    # ...and above this absolute floor (UZS)
FRESH_RECEIVER_DAYS = 30           # receiver account younger than this is "fresh"

# --- Rule weights (contribution to the combined CEP score, capped at 1.0) ---
W_NEW_PAYEE_HIGH = 0.35
W_FRESH_RECEIVER = 0.15
W_VELOCITY = 0.30
W_STRUCTURING = 0.40
W_DISTINCT_BURST = 0.25
W_DEVICE_CHANGE = 0.20
W_GEO_ANOMALY = 0.20
# Weighted above every other single rule: an impossible journey is a physical
# contradiction, not a statistical oddity, so on its own it must reach REVIEW.
W_IMPOSSIBLE_TRAVEL = 0.45
W_MULE_FAN_IN = 0.35
W_AMOUNT_DEVIATION = 0.25
W_DAILY_LIMIT = 0.30

# --- Decision thresholds ----------------------------------------------------
# Calibrated against the FULL capability set. Reduced deployments do not use
# these directly — see SCALE_THRESHOLDS_BY_CAPABILITY below.
REVIEW_THRESHOLD = 0.40
BLOCK_THRESHOLD = 0.70

# The CEP score is additive, so a fixed threshold is implicitly a statement about
# how many rules must agree. Hold it fixed while the available rules shrink and
# the layer goes silent rather than degrading: measured on PaySim with two rules
# available, the highest score any fraud reached was 0.35 against a 0.40 cutoff,
# so nothing was ever flagged — while the rules themselves separated the classes
# 4:1. Scaling carries the threshold across as a proportion of what the weakest
# fraud pattern can still reach.
#
# Off restores the previous fixed-threshold behaviour, for comparison.
SCALE_THRESHOLDS_BY_CAPABILITY = (
    os.getenv("SCALE_THRESHOLDS_BY_CAPABILITY", "1").lower()
    not in ("0", "false", "no"))

# --- Latency tuning ----------------------------------------------------------
# PyFlink batches records before crossing into the Python process; the defaults
# (100000 records / 1000 ms) are chosen for throughput and dominate latency at
# any realistic P2P rate, because the bundle never fills. Measured with the
# defaults: 7.6 ms of scoring behind 1923 ms of waiting.
#
# Turned down here because the requirement is to decide before settlement. The
# cost is more, smaller round trips to Python — lower peak throughput in
# exchange for a bounded wait.
PY_BUNDLE_TIME_MS = int(os.getenv("PY_BUNDLE_TIME_MS", "50"))
PY_BUNDLE_SIZE = int(os.getenv("PY_BUNDLE_SIZE", "100"))
# Flink's own batching between operators. 5 ms rather than 0: disabling it
# entirely sends each record individually and costs far more than it saves.
BUFFER_TIMEOUT_MS = int(os.getenv("BUFFER_TIMEOUT_MS", "5"))
# How long a consumer fetch waits on an empty topic before returning empty. The
# 500 ms default is a throughput setting too: at P2P arrival rates it adds up to
# half a second to any transaction unlucky enough to land just after a fetch.
KAFKA_FETCH_MAX_WAIT_MS = int(os.getenv("KAFKA_FETCH_MAX_WAIT_MS", "20"))
# Checkpoint interval. Not only a recovery setting: with AT_LEAST_ONCE the Kafka
# sink flushes its producer at checkpoint barriers, so nothing leaves the job
# between them, and the snapshot itself briefly stalls processing. At the 30 s
# default the warehouse path measured a 30 s median - the interval exactly.
# Recovery replays at most this much work, so shortening it costs throughput but
# also shortens the worst case after a failure.
CHECKPOINT_INTERVAL_MS = int(os.getenv("CHECKPOINT_INTERVAL_MS", "2000"))

# --- Restart behaviour -------------------------------------------------------
# Found by fault injection: killing the taskmanager once left the job gone —
# `no jobs submitted` — rather than restarting. Whatever the default resolves to
# in this Flink build, a fraud pipeline must not depend on it: a worker dying is
# an ordinary event (host reboot, OOM, deployment), and a detection system that
# quietly stops after one is worse than one that never started, because nothing
# alerts on silence.
#
# Restarts are effectively unlimited, with a delay long enough for a container
# to come back and short enough that recovery is measured in seconds. The
# failure-rate window bounds a crash loop: more than RESTART_ATTEMPTS failures
# inside it and the job stops for good, which is the state a human should see.
RESTART_ATTEMPTS = int(os.getenv("RESTART_ATTEMPTS", "10"))
RESTART_DELAY_MS = int(os.getenv("RESTART_DELAY_MS", "5000"))
RESTART_WINDOW_MS = int(os.getenv("RESTART_WINDOW_MS", "300000"))   # 5 min

# --- Checkpoint retention ----------------------------------------------------
# Also found by fault injection, and the more serious of the two. Checkpoints
# default to living only as long as the job: cancel it or lose the JobManager
# and they are discarded, so the next submission starts from nothing and the
# Kafka source falls back to the beginning of the topic.
#
# Observed: 6,890 stored rows covering 1,714 distinct transactions — every
# transaction scored roughly four times, once per submission. In production that
# is not a metrics artefact but four identical alerts per fraud, on transfers
# that settled days earlier.
#
# Retained checkpoints go to a directory on a mounted volume so they outlive
# both the job and the container.
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "file:///opt/flink/checkpoints")

# Model version stamped onto outputs (ML score is fused in at phase 6).
MODEL_VERSION = os.getenv("MODEL_VERSION", "cep+ml-fusion-v1")
# Stamped instead when the ONNX session could not be created and the job fell
# back to rules only. Without a distinct value, a degraded run is stored as a
# fused one and no later query can separate them.
MODEL_VERSION_CEP_ONLY = os.getenv("MODEL_VERSION_CEP_ONLY", "cep-only-fallback")


# --- Phase 6: ML serving + score fusion -------------------------------------
# The ONNX model + feature spec are looked up next to the job by default; mount
# or copy ml/models/model.onnx and feature_names.json here at deploy time
# (`serve-prep` does exactly that).
#
# Resolution is deliberately NOT just relative to __file__. `flink run
# --pyFiles` ships the Python modules to the TaskManager and unpacks them into a
# per-job temporary directory, so at runtime __file__ is
#
#   /tmp/python-dist-<uuid>/python-files/blob_p-<hash>/config.py
#
# and model.onnx is not beside it — it is a binary artefact, not a pyFile. The
# job found nothing and degraded to CEP-only with only an INFO line to say so:
#
#   [fraud_job] ONNX model not found at /tmp/python-dist-.../model.onnx; CEP-only
#
# Every record scored in such a run carries rule scores alone, while still being
# stamped with the fusion model version — indistinguishable afterwards from a
# real fused run. The mounted job directory is tried first because that is where
# both artefacts actually live (docker-compose: ./stream-processor mounted at
# /opt/flink/usrjobs on jobmanager and taskmanager alike).
JOB_DIR = os.path.dirname(os.path.abspath(__file__))
MOUNTED_JOB_DIR = os.getenv("FLINK_JOB_DIR", "/opt/flink/usrjobs")


def _resolve_artefact(env_var, filename, extra_dirs=()):
    """Locate a deploy-time artefact, mounted directory first.

    `extra_dirs` are searched last, for artefacts that also have a home in the
    repository (banks.csv lives in data-generator/ and is copied to the job dir
    by serve-prep). They are never searched first: in a deployment the mounted
    copy is the one that ships, and preferring a repository path would let a
    local file silently override what was deployed.
    """
    override = os.getenv(env_var)
    if override:
        return override
    for directory in (MOUNTED_JOB_DIR, JOB_DIR) + tuple(extra_dirs):
        candidate = os.path.join(directory, filename)
        if os.path.exists(candidate):
            return candidate
    # Found nowhere. Return the local path so the failure message names the
    # directory an operator would look in first.
    return os.path.join(JOB_DIR, filename)


MODEL_ONNX_PATH = _resolve_artefact("MODEL_ONNX_PATH", "model.onnx")
FEATURE_NAMES_PATH = _resolve_artefact("FEATURE_NAMES_PATH", "feature_names.json")

# The BIN table, from which bins.py resolves the card issuer. Resolved HERE, the
# same way as the two artefacts above, and for the same reason spelled out in
# the comment on JOB_DIR: `--pyFiles` copies the Python modules into a Beam temp
# directory, so a path derived from a module's __file__ points at that temp
# directory and finds no data files at all. bins.py originally did exactly that
# and failed the job at import with FileNotFoundError - the trap this resolver
# exists to close, walked into a second time.
#
# data-generator/ is included as a last resort so tests and the offline replay
# harnesses work in a checkout where serve-prep has not been run.
BANKS_CSV_PATH = _resolve_artefact(
    "BANKS_CSV", "banks.csv",
    extra_dirs=(os.path.join(JOB_DIR, "..", "data-generator"),))

# Fusion happens at the DECISION layer, not by averaging scores. We evaluated
# naive blends (noisy-OR, weighted, ml-augmented) and all DEGRADE ranking vs the
# model alone (PR-AUC 0.953 -> ~0.91-0.94), because the rule score is lower-
# resolution. So: final_score is the model probability (graded risk), and the CEP
# layer contributes deterministic must-flags + reason codes + a model-down fallback.
FINAL_REVIEW_THRESHOLD = 0.40
FINAL_BLOCK_THRESHOLD = 0.80

# Compliance must-flags: these deterministic CEP patterns force at least REVIEW
# regardless of the model score (AML / Regulation No. 3759 obligations). On the
# synthetic test slice they are high-precision (38 fraud vs 2 legit).
MANDATORY_REVIEW_RULES = ("STRUCTURING", "DAILY_LIMIT_BREACH")
