"""The single train/serve feature contract: the SAME function builds the vector for
offline training and for online scoring, so the model cannot be served features it
was not trained on. Pure."""

import logging
import math
from collections import Counter
import datetime

import config as C
import geo as G
import capabilities as CAP
import bins as B

# Derived from the capability registry, so the train/serve contract cannot
# drift from what the deployment can observe.
FEATURE_NAMES = CAP.feature_names()


_warned_no_pinfl = False


def _warn_pinfl_unavailable():
    """Once per process: on the 300 ms path, and the condition never changes."""
    global _warned_no_pinfl
    if not _warned_no_pinfl:
        _warned_no_pinfl = True
        logging.getLogger("features").warning(
            "CAP_PAYEE_IDENTITY=pinfl but the events carry no receiver_pinfl; "
            "keying the payee by card instead. The live wire format does not "
            "carry the payee's identity - only the offline harnesses, which "
            "read the generated CSV, can run this mode.")


_warned_no_key = False


def _warn_no_payee_key():
    """Once per process: neither identity is present, so the payee has no key."""
    global _warned_no_key
    if not _warned_no_key:
        _warned_no_key = True
        logging.getLogger("features").warning(
            "no receiver_card on the event, so the payee key is empty and EVERY "
            "payee shares one receiver-side state. Fan-in is then computed over "
            "the whole stream and will fire on ordinary traffic. A source with "
            "account identifiers but no PANs (PaySim) needs "
            "CAP_PAYEE_IDENTITY=pinfl.")


#: Values that mean False when a flag arrives as text.
_FALSEY_TEXT = {"", "0", "false", "f", "no", "n", "none", "null", "nan"}


def truthy(v) -> int:
    """Coerce a wire-shaped flag to 0/1. Kafka fields arrive as text, and "False"
    is a non-empty string."""
    if isinstance(v, str):
        return 0 if v.strip().lower() in _FALSEY_TEXT else 1
    return 1 if v else 0


def age_or_none(v):
    """Receiver account age as an int, or None when unknown - not 0, which means
    "opened today"."""
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def event_from(row: dict) -> dict:
    """One generated-CSV row as the event `rules.evaluate` expects - offline only,
    and the one copy of this mapping every replay uses."""
    return {
        "amount_uzs": row["amount_uzs"],
        "sender_pinfl": row["sender_pinfl"],
        "receiver_pinfl": row["receiver_pinfl"],
        "device_id": row["device_id"],
        "sender_region": row["sender_region"],
        "sender_network": row.get("sender_network", ""),
        "receiver_network": row.get("receiver_network", ""),
        # Behavioural session signals - COACHED_SESSION and the secs_login_z
        # baseline train as constant zeros without them.
        "active_call": truthy(row.get("active_call")),
        "secs_login_to_confirm": row.get("secs_login_to_confirm", 0.0),
        # The cards: the replay resolves the issuer from the BIN table, as the job does.
        "sender_card": row.get("sender_card", ""),
        "receiver_card": row.get("receiver_card", ""),
        "is_family_transfer": truthy(row.get("is_family_transfer")),
    }


def _issuer(event: dict, side: str) -> str:
    """Card issuer for one side of the transfer, resolved from the PAN's BIN."""
    return B.issuer_of(event.get(f"{side}_card"))


def is_on_us(event: dict) -> bool:
    """True when both parties bank with the same issuer. Unknown issuers count as
    inter-bank: an unresolvable BIN is not evidence of a shared institution."""
    s, r = _issuer(event, "sender"), _issuer(event, "receiver")
    return bool(s) and bool(r) and s == r


def payee_key(event: dict) -> str:
    """The identity the payee is pinned to - card (default) or pinfl - uniform per
    deployment: every receiver-side store keys on it, and a per-transfer mix loses
    fan-in hits."""
    if CAP.mode("payee_identity") == "pinfl":
        pinfl = str(event.get("receiver_pinfl", "") or "")
        if pinfl:
            return pinfl
        # Normal live: receiver_pinfl is not on the wire, and "" would disable fan-in.
        _warn_pinfl_unavailable()
    card = str(event.get("receiver_card", "") or "")
    if not card:
        # An empty key would merge every payee into one state and manufacture fan-in.
        _warn_no_payee_key()
    return card


def payer_key(event: dict) -> str:
    """The SENDER in payee-key space: the key their own inbound transfers were
    recorded under, and their outbound ones are, so link_history can read both.
    Card or PINFL by the same payee_identity mode as payee_key."""
    if CAP.mode("payee_identity") == "pinfl":
        pinfl = str(event.get("sender_pinfl", "") or "")
        if pinfl:
            return pinfl
    return str(event.get("sender_card", "") or "")


def visible_receiver_age(event: dict, receiver_age_days):
    """Apply the receiver_age capability mode (CAP_RECEIVER_AGE); None means
    "not obtainable", never a value."""
    mode = CAP.mode("receiver_age")
    if mode == "off":
        return None
    if mode == "on_us" and not is_on_us(event):
        return None
    return receiver_age_days


def extract(event: dict, receiver_age_days, state, now: float,
            receiver_state=None, sender_inbound=None, sender_outbound=None,
            payee_outbound=None) -> dict:
    """Read-only feature extraction; does NOT mutate any state. `receiver_state`
    may be None when the shared store is down - inbound features then read as zero,
    the fail-open behaviour used elsewhere. The last three are link_history's."""
    amount = float(event["amount_uzs"])
    payee = payee_key(event)
    device = event.get("device_id", "")
    region = event.get("sender_region", "")
    s_net = event.get("sender_network", "")
    r_net = event.get("receiver_network", "")

    ev = state.events
    n_hist = state.n_amt
    has_history = n_hist >= C.AMOUNT_DEVIATION_MIN_HISTORY
    mean = state.mean_amt
    std = math.sqrt(state.m2_amt / n_hist) if n_hist > 0 else 0.0

    def win_count(window):                      # count in window, current included
        return sum(1 for e in ev if now - e[0] <= window) + 1

    band_low = C.STRUCTURING_BAND_LOW * C.STRUCTURING_THRESHOLD
    sub = sum(1 for e in ev
              if now - e[0] <= C.STRUCTURING_WINDOW_S and band_low <= e[1] < C.STRUCTURING_THRESHOLD)
    if band_low <= amount < C.STRUCTURING_THRESHOLD:
        sub += 1

    distinct = {e[2] for e in ev if now - e[0] <= C.DISTINCT_PAYEE_WINDOW_S} | {payee}

    device_is_new = 1 if (state.known_devices and device not in state.known_devices) else 0

    geo_is_anomaly = 0
    if state.region_counts:
        home = state.region_counts.most_common(1)[0][0]
        if sum(state.region_counts.values()) >= 3 and region != home:
            geo_is_anomaly = 1

    # Rule helper, not a model feature: near-zero for almost every event so it
    # carries little gradient, but as a rule it is a physical contradiction.
    travel_kmh = 0.0
    travel_distance_km = 0.0
    if state.last_region and region and region != state.last_region:
        d = G.region_distance_km(state.last_region, region)
        if d is not None:
            travel_distance_km = d
            travel_kmh = G.implied_speed_kmh(
                state.last_region, region, now - state.last_region_ts)

    secs_since_last = (now - ev[-1][0]) if ev else float(C.RECENT_RETENTION_S)
    daily_sum = sum(e[1] for e in ev if now - e[0] <= C.DAILY_WINDOW_S) + amount

    amount_to_mean = (amount / mean) if mean > 0 else float(C.NEW_PAYEE_AMOUNT_FACTOR + 1)
    amount_z = ((amount - mean) / std) if std > 0 else 0.0

    age = visible_receiver_age(event, receiver_age_days)
    age_known = 1 if age is not None else 0
    if age is not None:
        receiver_age = float(age)
        receiver_is_fresh = 1.0 if age < C.FRESH_RECEIVER_DAYS else 0.0
    else:
        # NaN, not -1: LightGBM branches on missing natively, while -1 reads as the
        # youngest account. train.py withholds it on some rows so the model learns it.
        receiver_age = float("nan")
        receiver_is_fresh = float("nan")

    # Inbound concentration on the PAYEE - the fan-in shape sender-keyed state
    # cannot see. Distinct senders, not transfers: ten from one person is a habit.
    rcv_senders, rcv_inflow = 0, 0.0
    if receiver_state is not None:
        recent = []
        for e in reversed(receiver_state.inbound):      # newest first, to the hour
            if now - e[0] > C.RECEIVER_WINDOW_S:
                break
            recent.append(e)
        rcv_senders = len({e[1] for e in recent} | {event.get("sender_pinfl", "")})
        rcv_inflow = sum(e[2] for e in recent) + amount

    # link_history: four days of who paid whom, read for both parties, strictly before
    # this transfer. Zero when a store is unreachable, as above; in the vector only
    # when the capability is on.
    payee_payers = sender_payers = sender_payees = payee_payees = money_back = 0
    if CAP.enabled("link_history"):
        if receiver_state is not None:
            payee_payers = _recent(receiver_state, now)[0]
        if sender_inbound is not None:
            sender_payers = _recent(sender_inbound, now)[0]
        if sender_outbound is not None:
            sender_payees = _recent(sender_outbound, now)[0]
        if payee_outbound is not None:
            payee_payees, back = _recent(payee_outbound, now, payer_key(event))
            money_back = min(C.LINK_BACK_CAP, back)

    active_call = truthy(event.get("active_call"))
    secs_login = float(event.get("secs_login_to_confirm") or 0.0)

    # z-score in LOG space: session latency is lognormal, so untransformed the
    # right tail dominates and z is meaningless.
    log_secs = math.log1p(secs_login)
    secs_login_z = 0.0
    if state.n_secs >= C.SECS_LOGIN_MIN_HISTORY:
        std_secs = math.sqrt(state.m2_secs / state.n_secs)
        if std_secs > 1e-6:
            secs_login_z = (log_secs - state.mean_secs) / std_secs

    feat = {
        "log_amount": math.log1p(amount),
        "amount_to_mean": amount_to_mean,
        "amount_z": amount_z,
        "is_new_payee": 0 if payee in state.seen_payees else 1,
        "rcv_distinct_senders_1h": rcv_senders,
        "rcv_inflow_1h": math.log1p(rcv_inflow),
        "payee_payers_96h": payee_payers,
        "sender_payers_96h": sender_payers,
        "sender_payees_96h": sender_payees,
        "payee_payees_96h": payee_payees,
        "money_back_96h": money_back,
        "receiver_age": receiver_age,
        "receiver_is_fresh": receiver_is_fresh,
        "receiver_age_known": age_known,
        # MyID kinship; in the vector only when the myid_kinship capability is on.
        "is_family": truthy(event.get("is_family_transfer")),
        "vel_10m": win_count(C.VELOCITY_WINDOW_S),
        "vel_1h": win_count(C.STRUCTURING_WINDOW_S),
        "distinct_payees_10m": len(distinct),
        "sub_threshold_1h": sub,
        "device_is_new": device_is_new,
        "active_call": active_call,
        "secs_login_z": secs_login_z,
        "geo_is_anomaly": geo_is_anomaly,
        "secs_since_last": secs_since_last,
        "daily_sum_ratio": daily_sum / C.LIMIT_DAILY,
        "hour": float(datetime.datetime.fromtimestamp(now, datetime.timezone.utc).hour),
        "cross_network": 1 if (s_net and r_net and s_net != r_net) else 0,
        # --- rule helpers (NOT model features) ---
        "travel_kmh": travel_kmh,
        "travel_distance_km": travel_distance_km,
        "amount": amount,
        "has_history": 1 if has_history else 0,
        "amount_gt_factor_mean": 1 if ((not has_history) or (amount > C.NEW_PAYEE_AMOUNT_FACTOR * mean)) else 0,
    }
    return feat


def to_vector(feat: dict) -> list:
    """Feature dict -> ordered float vector matching FEATURE_NAMES."""
    return [float(feat[name]) for name in FEATURE_NAMES]


def _recent(state, now, of=""):
    """(distinct counterparties, entries naming `of`) in `state` over the
    LINK_WINDOW_S before `now`, strictly before it: the running counts less the stale
    head and whatever is stamped `now`, so O(stale) rather than O(four days). A state
    whose counts are not current - built by a store read - is scanned instead."""
    lo = now - C.LINK_WINDOW_S
    if state.n != len(state.inbound):
        seen = [e[1] for e in state.inbound if lo <= e[0] < now]
        return len(set(seen)), (seen.count(of) if of else 0)
    gone = Counter()
    for e in state.inbound:                              # oldest first, to the window
        if e[0] >= lo:
            break
        gone[e[1]] += 1
    for e in reversed(state.inbound):                    # newest first, to before now
        if e[0] < now:
            break
        gone[e[1]] += 1
    distinct = len(state.counts) - sum(1 for c, k in gone.items() if state.counts[c] == k)
    return distinct, ((state.counts.get(of, 0) - gone.get(of, 0)) if of else 0)


def kept_window_s() -> float:
    """How long the receiver-side stores keep a transfer: four days when link_history
    reads that far back, the hour MULE_FAN_IN needs otherwise."""
    return C.LINK_WINDOW_S if CAP.enabled("link_history") else C.RECEIVER_WINDOW_S


def update_receiver_state(receiver_state, event: dict, now: float) -> None:
    """Advance the payee's inbound history (call AFTER extract)."""
    _record(receiver_state, event.get("sender_pinfl", ""), event, now)


def update_outbound_state(outbound_state, event: dict, now: float) -> None:
    """Advance the sender's outbound history for link_history (call AFTER extract)."""
    if CAP.enabled("link_history"):
        _record(outbound_state, payee_key(event), event, now)


def _record(state, counterparty, event, now):
    if state is None:
        return
    current = state.n == len(state.inbound)
    state.inbound.append((now, counterparty, float(event["amount_uzs"])))
    keep = kept_window_s()
    dropped = []
    while state.inbound and now - state.inbound[0][0] > keep:
        dropped.append(state.inbound.popleft())
    if not current:                   # counts never kept for this state: leave them off
        return
    state.counts[counterparty] += 1
    state.n += 1
    for _, c, _amount in dropped:
        state.counts[c] -= 1
        if not state.counts[c]:
            del state.counts[c]
        state.n -= 1


def update_state(state, event: dict, now: float) -> None:
    """Advance per-sender state with the current event (call AFTER extract)."""
    amount = float(event["amount_uzs"])
    # Same resolver as extract(): a differently-keyed write would make
    # is_new_payee read 1 on every event forever.
    payee = payee_key(event)
    state.seen_payees.add(payee)
    state.events.append((now, amount, payee, event.get("device_id", "")))
    # Bound the window deque (memory). Stale entries are time-filtered in extract anyway.
    while state.events and now - state.events[0][0] > C.RECENT_RETENTION_S:
        state.events.popleft()
    state.known_devices.add(event.get("device_id", ""))
    region = event.get("sender_region", "")
    state.region_counts[region] += 1
    # Last *located* event: only advanced when the event carries a region, so a
    # region-less event cannot reset the origin and mask an impossible journey.
    if region:
        state.last_region = region
        state.last_region_ts = now
    # Welford running amount baseline: full-history mean/std with O(1) memory.
    state.n_amt += 1
    delta = amount - state.mean_amt
    state.mean_amt += delta / state.n_amt
    state.m2_amt += delta * (amount - state.mean_amt)
    # Welford on LOG(secs) — matches the z computed in extract().
    log_secs = math.log1p(float(event.get("secs_login_to_confirm") or 0.0))
    state.n_secs += 1
    d = log_secs - state.mean_secs
    state.mean_secs += d / state.n_secs
    state.m2_secs += d * (log_secs - state.mean_secs)
