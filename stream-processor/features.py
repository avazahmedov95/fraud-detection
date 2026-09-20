"""The single train/serve feature contract: the SAME function builds the vector for
offline training and for online scoring, so the model cannot be served features it
was not trained on. Pure."""

import logging
import math
import datetime

import config as C
import geo as G
import capabilities as CAP

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
        # The cards: the payee is keyed by card, as the job keys it.
        "sender_card": row.get("sender_card", ""),
        "receiver_card": row.get("receiver_card", ""),
        "is_family_transfer": truthy(row.get("is_family_transfer")),
    }


def sender_key(event: dict) -> str:
    """The sender's own account in the SAME key space as payee_key: what the
    receiver-side store filed this sender under when they were last paid."""
    if CAP.mode("payee_identity") == "pinfl":
        pinfl = str(event.get("sender_pinfl", "") or "")
        if pinfl:
            return pinfl
    return str(event.get("sender_card", "") or "")


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


def extract(event: dict, state, now: float, receiver_state=None,
            sender_inbound_ts=None) -> dict:
    """Read-only feature extraction; does NOT mutate either state. `receiver_state`
    may be None when the shared store is down - inbound features then read as zero,
    the fail-open behaviour used elsewhere."""
    amount = float(event["amount_uzs"])
    payee = payee_key(event)
    region = event.get("sender_region", "")

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

    # Inbound concentration on the PAYEE - the fan-in shape sender-keyed state
    # cannot see. Distinct senders, not transfers: ten from one person is a habit.
    rcv_senders, rcv_inflow = 0, 0.0
    if receiver_state is not None:
        recent = [e for e in receiver_state.inbound
                  if now - e[0] <= C.RECEIVER_WINDOW_S]
        rcv_senders = len({e[1] for e in recent} | {event.get("sender_pinfl", "")})
        rcv_inflow = sum(e[2] for e in recent) + amount

    # The same counting over the windows the AML rules count over (a day and a
    # week, config.LINK_*), on both sides, plus the interval since the SENDER's
    # own account was last paid: a mule collects over days and passes the money
    # on. Fail open exactly like the fan-in features - a store that is down
    # reads as nothing seen, not as a small count.
    payee_payers_24h = payee_payers_7d = 0
    if receiver_state is not None:
        payer = event.get("sender_pinfl", "")
        payers = getattr(receiver_state, "payers", None) or {}
        payee_payers_24h = len({p for p, t in payers.items()
                                if now - t <= C.LINK_DAY_S} | {payer})
        payee_payers_7d = len({p for p, t in payers.items()
                               if now - t <= C.LINK_WEEK_S} | {payer})
    paid = getattr(state, "payee_times", None) or {}
    sender_payees_24h = len({p for p, t in paid.items()
                             if now - t <= C.LINK_DAY_S} | {payee})
    sender_payees_7d = len({p for p, t in paid.items()
                            if now - t <= C.LINK_WEEK_S} | {payee})
    # Capped, never NaN: 'never paid' and 'the store is down' are the same
    # observation here, and the cap keeps the column finite for every model.
    secs_since_sender_inbound = float(C.LINK_WEEK_S)
    if sender_inbound_ts:
        secs_since_sender_inbound = max(0.0, min(now - float(sender_inbound_ts),
                                                 float(C.LINK_WEEK_S)))

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
        "payee_payers_24h": payee_payers_24h,
        "payee_payers_7d": payee_payers_7d,
        "sender_payees_24h": sender_payees_24h,
        "sender_payees_7d": sender_payees_7d,
        "secs_since_sender_inbound": secs_since_sender_inbound,
        # MyID kinship; in the vector only when the myid_kinship capability is on.
        "is_family": truthy(event.get("is_family_transfer")),
        "vel_10m": win_count(C.VELOCITY_WINDOW_S),
        "vel_1h": win_count(C.STRUCTURING_WINDOW_S),
        "distinct_payees_10m": len(distinct),
        "sub_threshold_1h": sub,
        "active_call": active_call,
        "secs_login_z": secs_login_z,
        "geo_is_anomaly": geo_is_anomaly,
        "secs_since_last": secs_since_last,
        "daily_sum_ratio": daily_sum / C.LIMIT_DAILY,
        "hour": float(datetime.datetime.fromtimestamp(now, datetime.timezone.utc).hour),
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


def _prune_links(times: dict, now: float) -> None:
    """Drop counterparties older than the longest window. Memory only: extract
    filters by time, so a late prune cannot change a feature."""
    if len(times) <= C.LINK_PRUNE_AT:
        return
    cutoff = now - C.LINK_WEEK_S
    for k, t in list(times.items()):
        if t < cutoff:
            del times[k]


def update_receiver_state(receiver_state, event: dict, now: float) -> None:
    """Advance the payee's inbound history (call AFTER extract)."""
    if receiver_state is None:
        return
    receiver_state.inbound.append(
        (now, event.get("sender_pinfl", ""), float(event["amount_uzs"])))
    receiver_state.payers[event.get("sender_pinfl", "")] = now
    receiver_state.last_inbound_ts = now
    _prune_links(receiver_state.payers, now)
    while (receiver_state.inbound
           and now - receiver_state.inbound[0][0] > C.RECEIVER_WINDOW_S):
        receiver_state.inbound.popleft()


def update_state(state, event: dict, now: float) -> None:
    """Advance per-sender state with the current event (call AFTER extract)."""
    amount = float(event["amount_uzs"])
    # Same resolver as extract(): a differently-keyed write would make
    # is_new_payee read 1 on every event forever.
    payee = payee_key(event)
    state.seen_payees.add(payee)
    state.payee_times[payee] = now
    _prune_links(state.payee_times, now)
    state.events.append((now, amount, payee))
    # Bound the window deque (memory). Stale entries are time-filtered in extract anyway.
    while state.events and now - state.events[0][0] > C.RECENT_RETENTION_S:
        state.events.popleft()
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
