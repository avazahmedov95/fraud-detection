"""The 10 rules over features.py's features: the rule score, the rules that fired and
the model's feature vector. No Flink or Redis, so training replays it unchanged."""

from collections import deque, Counter
from dataclasses import dataclass, field

import config as C
import features as F
import capabilities as CAP


@dataclass
class ReceiverState:
    """One payee's side, kept in Redis (receiver_store.py)."""
    inbound: deque = field(default_factory=deque)     # (ts, sender_pinfl, amount)
    payers: dict = field(default_factory=dict)        # payer -> last paid, a week
    last_inbound_ts: float = 0.0
    contacts: dict = field(default_factory=dict)      # card -> last dealt with, a week


@dataclass
class SenderState:
    seen_payees: set = field(default_factory=set)
    payee_times: dict = field(default_factory=dict)     # payee -> last paid
    events: deque = field(default_factory=deque)        # (ts, amount, payee)
    region_counts: Counter = field(default_factory=Counter)
    last_region: str = ""
    last_region_ts: float = 0.0
    n_amt: int = 0                                      # running amount stats
    mean_amt: float = 0.0
    m2_amt: float = 0.0
    n_secs: int = 0                                     # login-to-confirm, log space
    mean_secs: float = 0.0
    m2_secs: float = 0.0


def evaluate(event: dict, state: SenderState, now: float,
             receiver_state: "ReceiverState | None" = None,
             sender_inbound_ts=None,
             confirmed=frozenset()) -> dict:
    """Score one event, then update the states. `receiver_state` is None while Redis
    is down; `confirmed`: the confirmed fraud accounts this event touches."""
    f = F.extract(event, state, now, receiver_state, sender_inbound_ts, confirmed)

    hits = []
    score = 0.0
    on = CAP.rule_enabled

    if on("NEW_PAYEE_HIGH_AMOUNT") and (
            f["is_new_payee"] and f["amount"] >= C.NEW_PAYEE_ABS_FLOOR
            and f["amount_gt_factor_mean"]):
        hits.append("NEW_PAYEE_HIGH_AMOUNT"); score += C.W_NEW_PAYEE_HIGH
    if on("VELOCITY") and f["vel_10m"] > C.VELOCITY_MAX_COUNT:
        hits.append("VELOCITY"); score += C.W_VELOCITY
    if on("STRUCTURING") and f["sub_threshold_1h"] >= C.STRUCTURING_MIN_COUNT:
        hits.append("STRUCTURING"); score += C.W_STRUCTURING
    if on("DISTINCT_PAYEE_BURST") and f["distinct_payees_10m"] > C.DISTINCT_PAYEE_MAX:
        hits.append("DISTINCT_PAYEE_BURST"); score += C.W_DISTINCT_BURST
    if on("GEO_ANOMALY") and f["geo_is_anomaly"]:
        hits.append("GEO_ANOMALY"); score += C.W_GEO_ANOMALY
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
    if on("MULE_FAN_IN") and f["rcv_distinct_senders_1h"] >= C.MULE_FAN_IN_MIN_SENDERS:
        hits.append("MULE_FAN_IN"); score += C.W_MULE_FAN_IN

    score = min(1.0, score)

    vector = F.to_vector(f)
    F.update_state(state, event, now)
    F.update_receiver_state(receiver_state, event, now)

    return {
        "is_new_payee": bool(f["is_new_payee"]),
        "cep_score": round(score, 4),
        "rule_hits": hits,
        "features": vector,
        "active_call": int(f["active_call"]),
        "secs_login_z": round(f["secs_login_z"], 3),
    }
