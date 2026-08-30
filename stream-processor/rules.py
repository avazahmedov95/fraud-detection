"""
Pure CEP / rule engine — consumes the shared feature contract (features.py).

No Flink/Redis/Neo4j imports, so it can be unit-tested and replayed offline. The
Flink job keeps `SenderState` in keyed state and calls `evaluate` per event.

`evaluate` returns both the rule-based decision AND the model feature vector
(`features`), so the same call drives CEP now and feeds ML score fusion (phase 6).
`SenderState` carries Welford accumulators so the amount baseline is full-history
with O(1) memory; `features.py` reads and advances this state.

Scores are design targets for tuning, not validated production metrics.
"""

from dataclasses import dataclass, field
from collections import deque, Counter

import config as C
import features as F
import capabilities as CAP


@dataclass
class ReceiverState:
    """Inbound history for ONE receiver, keyed by payee rather than by sender.

    The stream is partitioned by sender, so this state does not live in Flink
    keyed state like SenderState does — in production it is a shared store
    (Redis) written by every partition. Kept as a plain object here so the rule
    engine stays testable and replayable offline.
    """
    inbound: deque = field(default_factory=deque)     # (ts, sender_pinfl, amount)


@dataclass
class SenderState:
    seen_payees: set = field(default_factory=set)
    events: deque = field(default_factory=deque)        # (ts, amount, payee, device)
    known_devices: set = field(default_factory=set)
    region_counts: Counter = field(default_factory=Counter)
    # Where and when the sender was last seen, for the travel-speed check.
    last_region: str = ""
    last_region_ts: float = 0.0
    # Welford running stats for the amount baseline (full history, O(1) memory)
    n_amt: int = 0
    mean_amt: float = 0.0
    m2_amt: float = 0.0
    # Welford running stats for the login→confirm latency baseline (log space)
    n_secs: int = 0
    mean_secs: float = 0.0
    m2_secs: float = 0.0


_THRESHOLD_CACHE = {}


def _thresholds():
    """Decision cutoffs for the active capability profile.

    Cached on the profile rather than recomputed per event: this runs on every
    transaction, and the answer only changes when the deployment changes.
    """
    if not C.SCALE_THRESHOLDS_BY_CAPABILITY:
        return C.REVIEW_THRESHOLD, C.BLOCK_THRESHOLD
    key = tuple(sorted(CAP.MODES.items()))
    if key not in _THRESHOLD_CACHE:
        _THRESHOLD_CACHE[key] = (
            CAP.scaled_threshold(C.REVIEW_THRESHOLD),
            CAP.scaled_threshold(C.BLOCK_THRESHOLD))
    return _THRESHOLD_CACHE[key]


def evaluate(event: dict, receiver_age_days, state: SenderState, now: float,
             receiver_state: "ReceiverState | None" = None) -> dict:
    """Score one event from the shared features. Mutates state (after extraction).

    `receiver_state` carries the payee's inbound history. It is optional: if the
    shared store is unreachable the pipeline fails open on this signal rather
    than stalling, exactly as the Neo4j and Redis lookups do.
    """
    f = F.extract(event, receiver_age_days, state, now, receiver_state)

    hits = []
    score = 0.0
    on = CAP.rule_enabled          # data behind the rule is available?

    # APP-style: new, high-value payee above an absolute floor.
    if on("NEW_PAYEE_HIGH_AMOUNT") and (
            f["is_new_payee"] and f["amount"] >= C.NEW_PAYEE_ABS_FLOOR
            and f["amount_gt_factor_mean"]):
        hits.append("NEW_PAYEE_HIGH_AMOUNT"); score += C.W_NEW_PAYEE_HIGH
    # Guarded on age_known: when the age is unavailable `receiver_is_fresh` is
    # NaN, and NaN is truthy in Python — an unguarded test would fire this rule
    # on every inter-bank transfer.
    if on("FRESH_RECEIVER") and f["receiver_age_known"] and f["receiver_is_fresh"] == 1:
        hits.append("FRESH_RECEIVER"); score += C.W_FRESH_RECEIVER
    if on("VELOCITY") and f["vel_10m"] > C.VELOCITY_MAX_COUNT:
        hits.append("VELOCITY"); score += C.W_VELOCITY
    if on("STRUCTURING") and f["sub_threshold_1h"] >= C.STRUCTURING_MIN_COUNT:
        hits.append("STRUCTURING"); score += C.W_STRUCTURING
    if on("DISTINCT_PAYEE_BURST") and f["distinct_payees_10m"] > C.DISTINCT_PAYEE_MAX:
        hits.append("DISTINCT_PAYEE_BURST"); score += C.W_DISTINCT_BURST
    if on("DEVICE_CHANGE") and f["device_is_new"]:
        hits.append("DEVICE_CHANGE"); score += C.W_DEVICE_CHANGE
    if on("GEO_ANOMALY") and f["geo_is_anomaly"]:
        hits.append("GEO_ANOMALY"); score += C.W_GEO_ANOMALY
    # Physically impossible journey since the sender's previous transaction.
    # Distinct from GEO_ANOMALY: that one fires on any away-from-home region and
    # so flags ordinary travellers, whereas this fires only when the move could
    # not have happened at all — a signature of a hijacked session or a shared
    # credential being used in two places at once.
    if on("IMPOSSIBLE_TRAVEL") and (
            f["travel_distance_km"] >= C.MIN_TRAVEL_DISTANCE_KM
            and f["travel_kmh"] > C.MAX_PLAUSIBLE_KMH):
        hits.append("IMPOSSIBLE_TRAVEL"); score += C.W_IMPOSSIBLE_TRAVEL
    if on("AMOUNT_DEVIATION") and f["has_history"] and f["amount_z"] > C.AMOUNT_DEVIATION_SIGMA:
        hits.append("AMOUNT_DEVIATION"); score += C.W_AMOUNT_DEVIATION
    if on("COACHED_SESSION") and f["active_call"] and f["secs_login_z"] > C.COACHED_SESSION_Z:
        hits.append("COACHED_SESSION"); score += C.W_COACHED_SESSION
    if on("DAILY_LIMIT_BREACH") and f["daily_sum_ratio"] > 1.0:
        hits.append("DAILY_LIMIT_BREACH"); score += C.W_DAILY_LIMIT
    # Fan-IN: many distinct senders converging on this payee. The mirror image
    # of DISTINCT_PAYEE_BURST, and the only rule here that looks at the payee's
    # history rather than the sender's.
    if on("MULE_FAN_IN") and f["rcv_distinct_senders_1h"] >= C.MULE_FAN_IN_MIN_SENDERS:
        hits.append("MULE_FAN_IN"); score += C.W_MULE_FAN_IN

    score = min(1.0, score)

    review_at, block_at = _thresholds()
    if score >= block_at:
        decision = "BLOCK"
    elif score >= review_at:
        decision = "REVIEW"
    else:
        decision = "ALLOW"

    vector = F.to_vector(f)
    F.update_state(state, event, now)
    F.update_receiver_state(receiver_state, event, now)

    return {
        "is_new_payee": bool(f["is_new_payee"]),
        # The age the bank could actually see, not the ground truth — so the
        # audit trail matches what the decision was based on.
        "receiver_account_age_days": (
            receiver_age_days if f["receiver_age_known"] else None),
        "cep_score": round(score, 4),
        "decision": decision,
        "rule_hits": hits,
        "features": vector,
        "active_call": int(f["active_call"]),
        "secs_login_z": round(f["secs_login_z"], 3),
    }
