"""Every tunable of the stream processor. Thresholds mirrored from the generator
are marked as such and MUST match it."""

import os

# --- Connections ------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")

# Plaintext 9092 and mutual TLS 9094 serve the same partitions, so the two
# measurement arms differ only in the listener. The transport is fixed when the
# job graph is built, so switching arms needs a resubmit.
KAFKA_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT").upper()
KAFKA_SSL_CA = os.getenv("KAFKA_SSL_CA", "/certs/ca.crt")
KAFKA_SSL_KEYSTORE = os.getenv("KAFKA_SSL_KEYSTORE", "/certs/client.keystore.pem")


def kafka_security_properties():
    """Connector properties for the configured transport; empty on PLAINTEXT."""
    if KAFKA_SECURITY_PROTOCOL != "SSL":
        return {}
    return {
        "security.protocol": "SSL",
        # PEM, so broker, job and producer read the same files.
        "ssl.truststore.type": "PEM",
        "ssl.truststore.location": KAFKA_SSL_CA,
        "ssl.keystore.type": "PEM",
        "ssl.keystore.location": KAFKA_SSL_KEYSTORE,
        # ON deliberately: disabling hostname verification would also remove
        # part of the handshake cost this measurement quantifies.
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
ENRICH_CACHE_TTL_S = 3600           # receiver-age lookups cached for 1h

SECS_LOGIN_MIN_HISTORY = 5          # cold start: z = 0 below this many observations
W_COACHED_SESSION = 0.35            # deliberately level with W_NEW_PAYEE_HIGH
COACHED_SESSION_Z = 2.0

# Which features and rules are active lives in capabilities.py, set by CAP_*.
# Changing one changes the feature contract: retrain and re-export after.

# --- Mirrored from the generator (must match it) -----------------------------
# Chosen figures, not regulatory ones - data-generator/config.py says why.
STRUCTURING_THRESHOLD = 10_000_000  # UZS - the band structuring stays under
LIMIT_DAILY = 100_000_000           # UZS - a bank operating limit, not a CBU one

# --- Rule windows (seconds) -------------------------------------------------
VELOCITY_WINDOW_S = 600             # 10 min
STRUCTURING_WINDOW_S = 3600         # 1 h
DISTINCT_PAYEE_WINDOW_S = 600       # 10 min
DAILY_WINDOW_S = 86400              # 24 h
RECENT_RETENTION_S = 86400          # prune per-sender history older than this

# --- Rule thresholds --------------------------------------------------------
VELOCITY_MAX_COUNT = 5              # > 5 transfers in the velocity window
STRUCTURING_MIN_COUNT = 3           # >= 3 sub-threshold transfers in the window
STRUCTURING_BAND_LOW = 0.80         # "just under" band: [0.80*T, 1.0*T)
DISTINCT_PAYEE_MAX = 5              # > 5 distinct payees in the window (fan-out)
AMOUNT_DEVIATION_SIGMA = 4.0        # amount > mean + sigma*std

# 900 km/h is jet cruise speed: above it is impossible, not merely unusual, and
# that is what separates this from GEO_ANOMALY. The distance floor absorbs the
# error from putting a region at its administrative centre (geo.py) - adjacent
# regions can be ~35 km apart there.
MAX_PLAUSIBLE_KMH = 900.0
MIN_TRAVEL_DISTANCE_KM = 100.0

# Above the fan-OUT threshold: receiving from several people in an hour is
# ordinary, paying out to several unrelated new payees is not.
RECEIVER_WINDOW_S = 3600
MULE_FAN_IN_MIN_SENDERS = 6

# "relative" replaces the constant with a quantile of the population's own live
# distribution, because 6 encodes THIS generator's in-degree density: on AMLSim
# the rule fired on 3.12% of legitimate traffic and caught 0.0% of the fan-in
# typology (validation/README.md 3). A per-RECEIVER baseline was rejected -
# drop accounts are fresh, so it is absent exactly where the rule is needed.
MULE_FAN_IN_MODE = os.getenv("MULE_FAN_IN_MODE", "absolute")   # absolute | relative
MULE_FAN_IN_QUANTILE = float(os.getenv("MULE_FAN_IN_QUANTILE", "0.999"))
MULE_FAN_IN_MIN_OBS = int(os.getenv("MULE_FAN_IN_MIN_OBS", "5000"))      # else fall back
MULE_FAN_IN_REFRESH_EVERY = int(os.getenv("MULE_FAN_IN_REFRESH_EVERY", "512"))

AMOUNT_DEVIATION_MIN_HISTORY = 5   # history needed before deviation can fire
NEW_PAYEE_AMOUNT_FACTOR = 3.0      # amount > factor * sender mean
NEW_PAYEE_ABS_FLOOR = 2_000_000    # ...and above this absolute floor (UZS)
FRESH_RECEIVER_DAYS = 30           # younger receiver account is "fresh"

# --- Rule weights (contribution to the CEP score, capped at 1.0) ------------
W_NEW_PAYEE_HIGH = 0.35
W_FRESH_RECEIVER = 0.15
W_VELOCITY = 0.30
W_STRUCTURING = 0.40
W_DISTINCT_BURST = 0.25
W_DEVICE_CHANGE = 0.20
W_GEO_ANOMALY = 0.20
W_IMPOSSIBLE_TRAVEL = 0.45   # alone must reach REVIEW: a physical contradiction
W_MULE_FAN_IN = 0.35
W_AMOUNT_DEVIATION = 0.25
W_DAILY_LIMIT = 0.30

# --- Decision thresholds ----------------------------------------------------
# Calibrated against the FULL capability set; reduced deployments scale them.
REVIEW_THRESHOLD = 0.40
BLOCK_THRESHOLD = 0.70

# The CEP score is additive, so a fixed threshold implicitly says how many rules
# must agree. Held fixed as rules disappear, the layer goes silent rather than
# degrading: on PaySim with two rules the best fraud scored 0.35 against a 0.40
# cutoff, so nothing was flagged - while those rules separated the classes 4:1.
SCALE_THRESHOLDS_BY_CAPABILITY = (
    os.getenv("SCALE_THRESHOLDS_BY_CAPABILITY", "1").lower()
    not in ("0", "false", "no"))

# --- Latency tuning ----------------------------------------------------------
# Every default below was chosen for throughput and turned down here, because
# the requirement is to decide before settlement.
#
# PyFlink batches before crossing into Python. With the defaults the bundle
# never fills: measured 7.6 ms of scoring behind 1923 ms of waiting.
PY_BUNDLE_TIME_MS = int(os.getenv("PY_BUNDLE_TIME_MS", "50"))
PY_BUNDLE_SIZE = int(os.getenv("PY_BUNDLE_SIZE", "100"))
# 5 rather than 0: sending each record individually costs more than it saves.
BUFFER_TIMEOUT_MS = int(os.getenv("BUFFER_TIMEOUT_MS", "5"))
# The 500 ms default adds half a second to a transaction landing just after a
# fetch on an empty topic.
KAFKA_FETCH_MAX_WAIT_MS = int(os.getenv("KAFKA_FETCH_MAX_WAIT_MS", "20"))
# Not only recovery: with AT_LEAST_ONCE the sink flushes at checkpoint barriers,
# so nothing leaves the job between them. At the 30 s default the warehouse path
# measured a 30 s median - the interval exactly.
CHECKPOINT_INTERVAL_MS = int(os.getenv("CHECKPOINT_INTERVAL_MS", "2000"))

# --- Restart behaviour -------------------------------------------------------
# Found by fault injection: one taskmanager kill left the job gone rather than
# restarting. A detection system that quietly stops is worse than one that never
# started, because nothing alerts on silence. The window still bounds a crash
# loop - exceed it and the job stops for good, which a human should see.
RESTART_ATTEMPTS = int(os.getenv("RESTART_ATTEMPTS", "10"))
RESTART_DELAY_MS = int(os.getenv("RESTART_DELAY_MS", "5000"))
RESTART_WINDOW_MS = int(os.getenv("RESTART_WINDOW_MS", "300000"))   # 5 min

# Also fault injection, and worse: checkpoints default to living only as long as
# the job, so the next submission restarts from the beginning of the topic.
# Observed 6,890 rows over 1,714 transactions - each scored about four times.
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "file:///opt/flink/checkpoints")

MODEL_VERSION = os.getenv("MODEL_VERSION", "cep+ml-fusion-v1")
# Distinct value, or a degraded run is stored as a fused one and no later query
# can separate them.
MODEL_VERSION_CEP_ONLY = os.getenv("MODEL_VERSION_CEP_ONLY", "cep-only-fallback")

# --- Deploy-time artefacts ---------------------------------------------------
# NEVER resolve these relative to __file__. `flink run --pyFiles` unpacks the
# modules into a per-job temp directory where the binary artefacts are not, and
# the job then degrades to CEP-only while still stamping the fusion version. The
# mounted job directory is tried first because that is where they live.
JOB_DIR = os.path.dirname(os.path.abspath(__file__))
MOUNTED_JOB_DIR = "/opt/flink/usrjobs"


def _resolve_artefact(env_var, filename, extra_dirs=()):
    """Locate a deploy-time artefact, mounted directory first.

    `extra_dirs` are searched LAST: preferring a repository path would let a
    local file silently override what was deployed.
    """
    override = os.getenv(env_var)
    if override:
        return override
    for directory in (MOUNTED_JOB_DIR, JOB_DIR) + tuple(extra_dirs):
        candidate = os.path.join(directory, filename)
        if os.path.exists(candidate):
            return candidate
    # Name the directory an operator would look in first.
    return os.path.join(JOB_DIR, filename)


MODEL_ONNX_PATH = _resolve_artefact("MODEL_ONNX_PATH", "model.onnx")
FEATURE_NAMES_PATH = _resolve_artefact("FEATURE_NAMES_PATH", "feature_names.json")
# bins.py derived this from its own __file__ once and failed the job at import -
# the trap above, walked into twice. data-generator/ is a last resort so tests
# and offline replay work without serve-prep.
BANKS_CSV_PATH = _resolve_artefact(
    "BANKS_CSV", "banks.csv",
    extra_dirs=(os.path.join(JOB_DIR, "..", "data-generator"),))

# Fusion happens at the DECISION layer - fusion.py says why every blend degraded.
FINAL_REVIEW_THRESHOLD = 0.40
FINAL_BLOCK_THRESHOLD = 0.80

# Force at least REVIEW regardless of the model score (AML / Regulation 3759).
# High-precision on the synthetic slice: 38 fraud vs 2 legit.
MANDATORY_REVIEW_RULES = ("STRUCTURING", "DAILY_LIMIT_BREACH")
