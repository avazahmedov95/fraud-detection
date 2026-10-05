"""Every setting of the stream processor. Thresholds mirrored from the generator
must match it."""

import json
import os

# --- Connections ------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
# PLAINTEXT on 9092 or mutual TLS (SSL) on 9094; switching needs a resubmit.
KAFKA_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT").upper()
KAFKA_SSL_CA = os.getenv("KAFKA_SSL_CA", "/certs/ca.crt")
KAFKA_SSL_KEYSTORE = os.getenv("KAFKA_SSL_KEYSTORE", "/certs/client.keystore.pem")


def kafka_security_properties():
    """Connector properties for the configured transport; empty on PLAINTEXT."""
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
TOPIC_SECOND_LOOK = os.getenv("TOPIC_SECOND_LOOK", "fraud.second_look")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "fraud-cep")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
#: A call takes a millisecond; an unreachable Redis must not cost the OS's connect timeout.
REDIS_TIMEOUT_S = 0.5
#: After a failure the job decides without Redis this long before asking again.
REDIS_RETRY_AFTER_S = 5.0

# --- Mirrored from the generator --------------------------------------------
STRUCTURING_THRESHOLD = 10_000_000  # UZS, the sum structuring stays under
LIMIT_DAILY = 100_000_000           # UZS, a bank limit

# --- Rule windows (seconds) -------------------------------------------------
VELOCITY_WINDOW_S = 600
STRUCTURING_WINDOW_S = 3600
DISTINCT_PAYEE_WINDOW_S = 600
DAILY_WINDOW_S = 86400
RECENT_RETENTION_S = 86400          # per-sender history kept this long

# --- Rule thresholds --------------------------------------------------------
VELOCITY_MAX_COUNT = 5              # > 5 transfers in the velocity window
STRUCTURING_MIN_COUNT = 3           # >= 3 transfers just under the threshold
STRUCTURING_BAND_LOW = 0.80         # "just under": [0.80*T, T)
DISTINCT_PAYEE_MAX = 5              # > 5 distinct payees in the window
AMOUNT_DEVIATION_SIGMA = 4.0        # amount > mean + sigma*std
AMOUNT_DEVIATION_MIN_HISTORY = 5    # history needed before deviation can fire
NEW_PAYEE_AMOUNT_FACTOR = 3.0       # amount > factor * the sender's mean
NEW_PAYEE_ABS_FLOOR = 2_000_000     # ...and above this (UZS)
COACHED_SESSION_Z = 2.0             # login-to-confirm this far above the sender's own
SECS_LOGIN_MIN_HISTORY = 5          # z = 0 below this many observations
MAX_PLAUSIBLE_KMH = 900.0           # faster than a jet is impossible
MIN_TRAVEL_DISTANCE_KM = 100.0      # each region is one point (geo.py)
RECEIVER_WINDOW_S = 3600
MULE_FAN_IN_MIN_SENDERS = 6

# counterparty_history: a day and a week, the longest window 30-day data supports.
LINK_DAY_S = 86400
LINK_WEEK_S = 604800
LINK_PRUNE_AT = 256                 # memory only: the features filter by time anyway

# confirmed_cases: the confirmed fraud accounts (Redis), added to by verdicts and
# seeded from the history; offline, a fraud joins them a day later.
CONFIRMED_KEY = "confirmed:accounts"
HISTORY_KEY = "confirmed:history"   # the history's copy; the job does not read it
CONFIRMATION_DELAY_S = 86400

# --- Rule weights (summed into the CEP score, capped at 1.0) ----------------
W_NEW_PAYEE_HIGH = 0.35
W_COACHED_SESSION = 0.35
W_VELOCITY = 0.30
W_STRUCTURING = 0.40
W_DISTINCT_BURST = 0.25
W_GEO_ANOMALY = 0.20
W_IMPOSSIBLE_TRAVEL = 0.45          # reaches REVIEW alone at the CEP layer
W_MULE_FAN_IN = 0.35
W_AMOUNT_DEVIATION = 0.25
W_DAILY_LIMIT = 0.30

# --- Decision ---------------------------------------------------------------
# The rule score's cut-off with no model loaded, set on every capability; reduced
# profiles scale it (capabilities.py). Also the model's without thresholds.json.
REVIEW_THRESHOLD = 0.40
SCALE_THRESHOLDS_BY_CAPABILITY = (
    os.getenv("SCALE_THRESHOLDS_BY_CAPABILITY", "1").lower() not in ("0", "false", "no"))
# Always held, whatever the model says (AML, Regulation 3759).
MANDATORY_REVIEW_RULES = ("STRUCTURING", "DAILY_LIMIT_BREACH")

# --- Latency (stream-processor/README.md, Speed) ----------------------------
# Copies side by side: one Python worker and one task slot each. One copy queued
# at 100 transfers a second; three hold p99 under 300 ms to 200.
JOB_PARALLELISM = int(os.getenv("JOB_PARALLELISM", "3"))
# How long PyFlink gathers records before handing them to Python.
PY_BUNDLE_TIME_MS = int(os.getenv("PY_BUNDLE_TIME_MS", "20"))
PY_BUNDLE_SIZE = int(os.getenv("PY_BUNDLE_SIZE", "100"))
BUFFER_TIMEOUT_MS = int(os.getenv("BUFFER_TIMEOUT_MS", "5"))
KAFKA_FETCH_MAX_WAIT_MS = int(os.getenv("KAFKA_FETCH_MAX_WAIT_MS", "20"))
# The Kafka sinks flush at checkpoints, so this also bounds the warehouse delay.
CHECKPOINT_INTERVAL_MS = int(os.getenv("CHECKPOINT_INTERVAL_MS", "2000"))

# Restart after a crash; the window bounds a crash loop.
RESTART_ATTEMPTS = int(os.getenv("RESTART_ATTEMPTS", "10"))
RESTART_DELAY_MS = int(os.getenv("RESTART_DELAY_MS", "5000"))
RESTART_WINDOW_MS = int(os.getenv("RESTART_WINDOW_MS", "300000"))
# Kept past the job, or the next submission would rescore the topic.
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "file:///opt/flink/checkpoints")

MODEL_VERSION = os.getenv("MODEL_VERSION", "cep+ml-fusion-v2")
MODEL_VERSION_CEP_ONLY = os.getenv("MODEL_VERSION_CEP_ONLY", "cep-only-fallback")

# --- Deployed artefacts -----------------------------------------------------
JOB_DIR = os.path.dirname(os.path.abspath(__file__))
MOUNTED_JOB_DIR = "/opt/flink/usrjobs"   # --pyFiles unpacks the modules without them


def _resolve_artefact(env_var, filename, extra_dirs=()):
    """The mounted directory first, so a local file cannot override the deployed one."""
    override = os.getenv(env_var)
    if override:
        return override
    for directory in (MOUNTED_JOB_DIR, JOB_DIR) + tuple(extra_dirs):
        candidate = os.path.join(directory, filename)
        if os.path.exists(candidate):
            return candidate
    return os.path.join(JOB_DIR, filename)


MODEL_ONNX_PATH = _resolve_artefact("MODEL_ONNX_PATH", "model.onnx")
THRESHOLDS_PATH = _resolve_artefact("THRESHOLDS_PATH", "thresholds.json")


def _model_review_threshold(path):
    """The model's REVIEW cut-off chosen by ml/train.py; the fixed one without it."""
    try:
        with open(path, encoding="utf-8") as fh:
            return float(json.load(fh)["review"])
    except (OSError, KeyError, TypeError, ValueError):
        return REVIEW_THRESHOLD


MODEL_REVIEW_THRESHOLD = _model_review_threshold(THRESHOLDS_PATH)

SECOND_LOOK_PATH = _resolve_artefact("SECOND_LOOK_PATH", "second_look.json")


def _second_look_from(path, review):
    """Where the second look's band begins (ml/second_look.py), or None: no file, or a
    band chosen under another cut-off, which means nothing under this one."""
    try:
        with open(path, encoding="utf-8") as fh:
            spec = json.load(fh)
        return float(spec["from"]) if float(spec["review"]) == review else None
    except (OSError, KeyError, TypeError, ValueError):
        return None


SECOND_LOOK_FROM = _second_look_from(SECOND_LOOK_PATH, MODEL_REVIEW_THRESHOLD)
# Renewed by the second look while it answers; without it the job holds the band itself.
SECOND_LOOK_ALIVE_KEY = "second-look:alive"
SECOND_LOOK_UNSCORED = "second-look:unscored"
