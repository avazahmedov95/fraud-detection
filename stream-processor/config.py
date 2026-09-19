"""Every tunable of the stream processor. Thresholds mirrored from the generator
are marked as such and MUST match it."""

import json
import os

# --- Connections ------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")

# Plaintext 9092 or mutual TLS 9094 - the same partitions; switching needs a resubmit.
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

# Which features and rules are active is not here: capabilities.py, set by CAP_*.

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

# Faster than a jet (900 km/h) is impossible, not unusual; the distance floor
# absorbs placing each region at its administrative centre (geo.py).
MAX_PLAUSIBLE_KMH = 900.0
MIN_TRAVEL_DISTANCE_KM = 100.0

# Above the fan-OUT threshold: receiving from several people in an hour is
# ordinary, paying out to several unrelated new payees is not.
RECEIVER_WINDOW_S = 3600
MULE_FAN_IN_MIN_SENDERS = 6

# "relative": a quantile of the live population instead of the constant 6, which
# fits only this generator's density (validation/README.md 3).
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
W_GEO_ANOMALY = 0.20
# Reaches REVIEW alone at the CEP layer (0.45 > 0.40); the fused decision follows
# the model unless the rule is in MANDATORY_REVIEW_RULES.
W_IMPOSSIBLE_TRAVEL = 0.45
W_MULE_FAN_IN = 0.35
W_AMOUNT_DEVIATION = 0.25
W_DAILY_LIMIT = 0.30

# --- Decision thresholds ----------------------------------------------------
# Calibrated against the FULL capability set; reduced deployments scale them.
REVIEW_THRESHOLD = 0.40
# No BLOCK anywhere: the system never blocks on its own - every alert goes to a
# person (the owner's decision, 2026-09-19).

# Off restores the pre-2026 fixed cutoffs, kept so the two can be compared.
# Why they are scaled at all: capabilities.scaled_threshold.
SCALE_THRESHOLDS_BY_CAPABILITY = (
    os.getenv("SCALE_THRESHOLDS_BY_CAPABILITY", "1").lower()
    not in ("0", "false", "no"))

# --- Latency tuning: every default below favoured throughput ------------------
# PyFlink batches records before crossing into Python; the default bundle never fills.
PY_BUNDLE_TIME_MS = int(os.getenv("PY_BUNDLE_TIME_MS", "50"))
PY_BUNDLE_SIZE = int(os.getenv("PY_BUNDLE_SIZE", "100"))
# 5 rather than 0: sending each record individually costs more than it saves.
BUFFER_TIMEOUT_MS = int(os.getenv("BUFFER_TIMEOUT_MS", "5"))
# The 500 ms default adds half a second to a transaction landing just after a
# fetch on an empty topic.
KAFKA_FETCH_MAX_WAIT_MS = int(os.getenv("KAFKA_FETCH_MAX_WAIT_MS", "20"))
# With AT_LEAST_ONCE the sink flushes at checkpoint barriers, so this interval
# bounds the warehouse delay as well as recovery.
CHECKPOINT_INTERVAL_MS = int(os.getenv("CHECKPOINT_INTERVAL_MS", "2000"))

# --- Restart behaviour -------------------------------------------------------
# Restart after a crash rather than stop silently; the window bounds a crash loop.
RESTART_ATTEMPTS = int(os.getenv("RESTART_ATTEMPTS", "10"))
RESTART_DELAY_MS = int(os.getenv("RESTART_DELAY_MS", "5000"))
RESTART_WINDOW_MS = int(os.getenv("RESTART_WINDOW_MS", "300000"))   # 5 min

# Keep checkpoints past the job, or the next submission rescores the topic.
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "file:///opt/flink/checkpoints")

MODEL_VERSION = os.getenv("MODEL_VERSION", "cep+ml-fusion-v2")
# Distinct value, or a degraded run is stored as a fused one and no later query
# can separate them.
MODEL_VERSION_CEP_ONLY = os.getenv("MODEL_VERSION_CEP_ONLY", "cep-only-fallback")

# --- Deploy-time artefacts ---------------------------------------------------
# Never relative to __file__: --pyFiles unpacks modules into a temp directory
# without the artefacts, and the job would fall back to CEP-only.
JOB_DIR = os.path.dirname(os.path.abspath(__file__))
MOUNTED_JOB_DIR = "/opt/flink/usrjobs"


def _resolve_artefact(env_var, filename, extra_dirs=()):
    """Locate a deploy-time artefact: the mounted directory first, `extra_dirs` last,
    so a local file cannot override what was deployed."""
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
THRESHOLDS_PATH = _resolve_artefact("THRESHOLDS_PATH", "thresholds.json")
# data-generator/ last, for tests and offline replay without serve-prep.
BANKS_CSV_PATH = _resolve_artefact(
    "BANKS_CSV", "banks.csv",
    extra_dirs=(os.path.join(JOB_DIR, "..", "data-generator"),))

# Fusion happens at the DECISION layer - fusion.py says why every blend degraded.
FINAL_REVIEW_THRESHOLD = 0.40


def _model_review_threshold(path):
    """The model's REVIEW cutoff, chosen on validation rows by ml/train.py and
    shipped beside model.onnx. Without the file the fixed cutoff stands, as it does
    for the CEP-only fallback."""
    try:
        with open(path, encoding="utf-8") as fh:
            return float(json.load(fh)["review"])
    except (OSError, KeyError, TypeError, ValueError):
        return FINAL_REVIEW_THRESHOLD


MODEL_REVIEW_THRESHOLD = _model_review_threshold(THRESHOLDS_PATH)

# Force at least REVIEW regardless of the model score (AML / Regulation 3759).
# High-precision on the synthetic slice: 38 fraud vs 2 legit.
MANDATORY_REVIEW_RULES = ("STRUCTURING", "DAILY_LIMIT_BREACH")
