"""The CEP rule engine over the shared feature contract in features.py; returns both
the rule decision and the model's feature vector. No Flink, Redis or Neo4j imports,
so it replays offline unchanged."""

from dataclasses import dataclass, field
from collections import deque, Counter

import logging

import config as C
import features as F
import capabilities as CAP


@dataclass
class ReceiverState:
    """Inbound history for ONE receiver, keyed by payee: the stream is partitioned
    by sender, so this lives in a shared Redis store, not Flink keyed state."""
    inbound: deque = field(default_factory=deque)     # (ts, sender_pinfl, amount)
    #: payer -> when they last paid, over the counterparty window; and when this
    #: account was last paid at all. Both are the AML counters, not the CEP window.
    payers: dict = field(default_factory=dict)
    last_inbound_ts: float = 0.0


#: Below this a "receiver with many senders" is not a claim anyone would make.
FAN_IN_FLOOR = 2


def quantile_threshold(counts, n, q):
    """Smallest count k with P(X < k) >= q, floored at FAN_IN_FLOOR so the rule
    cannot fire on ordinary transfers. Shared by both baselines."""
    target = q * n
    cum = 0
    thr = len(counts) - 1
    for k, c in enumerate(counts):
        cum += c
        if cum >= target:
            thr = k
            break
    return max(thr, FAN_IN_FLOOR)


_warned_no_baseline = False


def _warn_relative_without_baseline():
    """Once per process: a per-event line on the 300 ms path would be its own defect."""
    global _warned_no_baseline
    if not _warned_no_baseline:
        _warned_no_baseline = True
        logging.getLogger("rules").warning(
            "MULE_FAN_IN_MODE=relative but no baseline was passed to "
            "evaluate(); the rule is running on the absolute threshold "
            "(%d senders). Every caller that should reach this mode passes one: "
            "the Flink job a receiver_store.PopulationStore, the offline "
            "harnesses (experiments/replay.py and its subcommands) a "
            "PopulationBaseline. So this line means a CALLER is missing it, not "
            "that the deployment cannot support it.", C.MULE_FAN_IN_MIN_SENDERS)


@dataclass
class PopulationBaseline:
    """Live distribution of `rcv_distinct_senders_1h` across all receivers, so
    MULE_FAN_IN can fire on a quantile; without it the rule uses the constant."""
    BINS: int = 257
    counts: list = field(default_factory=lambda: [0] * 257)
    n: int = 0
    _cached_thr: int = -1
    _cached_at: int = -1

    def observe(self, senders: int) -> None:
        self.counts[min(int(senders), self.BINS - 1)] += 1
        self.n += 1

    def threshold(self, q: float, fallback: int) -> int:
        if self.n < C.MULE_FAN_IN_MIN_OBS:
            return fallback
        if (self._cached_at >= 0
                and self.n - self._cached_at < C.MULE_FAN_IN_REFRESH_EVERY):
            return self._cached_thr
        self._cached_thr = quantile_threshold(self.counts, self.n, q)
        self._cached_at = self.n
        return self._cached_thr


@dataclass
class SenderState:
    seen_payees: set = field(default_factory=set)
    #: payee -> when this sender last paid them, for the counterparty counts.
    payee_times: dict = field(default_factory=dict)
    events: deque = field(default_factory=deque)        # (ts, amount, payee)
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


def _review_threshold():
    """The REVIEW cutoff for the active capability profile, cached per profile."""
    if not C.SCALE_THRESHOLDS_BY_CAPABILITY:
        return C.REVIEW_THRESHOLD
    key = tuple(sorted(CAP.MODES.items()))
    if key not in _THRESHOLD_CACHE:
        _THRESHOLD_CACHE[key] = CAP.scaled_threshold(C.REVIEW_THRESHOLD)
    return _THRESHOLD_CACHE[key]


def evaluate(event: dict, state: SenderState, now: float,
             receiver_state: "ReceiverState | None" = None,
             sender_inbound_ts=None,
             population: "PopulationBaseline | None" = None) -> dict:
    """Score one event from the shared features. Mutates state (after extraction).
    `receiver_state` is optional: an unreachable shared store fails open here."""
    f = F.extract(event, state, now, receiver_state, sender_inbound_ts)

    hits = []
    score = 0.0
    on = CAP.rule_enabled          # data behind the rule is available?

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
    # Distinct from GEO_ANOMALY: that flags ordinary travellers on any
    # away-from-home region; this fires only when the move was impossible.
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
    # Fan-IN: the only rules here that look at the payee's history, not the sender's.
    # Constant, or a quantile of the live population - see MULE_FAN_IN_MODE.
    fan_in_thr = C.MULE_FAN_IN_MIN_SENDERS
    if C.MULE_FAN_IN_MODE == "relative":
        if population is not None:
            fan_in_thr = population.threshold(C.MULE_FAN_IN_QUANTILE,
                                              C.MULE_FAN_IN_MIN_SENDERS)
        else:
            # Relative asked for, no baseline given: a silent fallback leaves the
            # knob set while the rule runs on the constant it exists to replace.
            _warn_relative_without_baseline()
    burst = f["rcv_distinct_senders_1h"] >= fan_in_thr
    if on("MULE_FAN_IN") and burst:
        hits.append("MULE_FAN_IN"); score += C.W_MULE_FAN_IN
    # The same collection spread over a day, which the one-hour window sees a slice
    # of - and only where the burst rule is silent, so one collection cannot score
    # twice. A store that is down leaves the count at zero, and the rule with it.
    if (on("COLLECTOR") and not burst
            and f["payee_payers_24h"] >= C.COLLECTOR_MIN_PAYERS):
        hits.append("COLLECTOR"); score += C.W_COLLECTOR

    score = min(1.0, score)

    # No BLOCK: the system never blocks on its own (config.py).
    decision = "REVIEW" if score >= _review_threshold() else "ALLOW"

    vector = F.to_vector(f)
    F.update_state(state, event, now)
    F.update_receiver_state(receiver_state, event, now)
    if population is not None:
        # After the decision: an event must not join the baseline it is judged against.
        population.observe(f["rcv_distinct_senders_1h"])

    return {
        "is_new_payee": bool(f["is_new_payee"]),
        "cep_score": round(score, 4),
        "decision": decision,
        "rule_hits": hits,
        "features": vector,
        "active_call": int(f["active_call"]),
        "secs_login_z": round(f["secs_login_z"], 3),
    }
