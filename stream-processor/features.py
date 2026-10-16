"""The features: one function builds the vector for training and for scoring, so the
model is never served features it was not trained on. Pure."""

import logging
import math
import datetime

import config as C
import geo as G
import capabilities as CAP

FEATURE_NAMES = CAP.feature_names()

_NO_PINFL = ("CAP_PAYEE_IDENTITY=pinfl but the events carry no receiver_pinfl; keying "
             "the payee by card. Only the offline harnesses have the payee's identity.")
_NO_PAYEE_KEY = ("no receiver_card on the event: every payee shares one receiver-side "
                 "state and fan-in fires on ordinary traffic. A source without cards "
                 "(PaySim) needs CAP_PAYEE_IDENTITY=pinfl.")
_warned = set()


def _warn_once(message):
    """Once per process: the condition never changes."""
    if message not in _warned:
        _warned.add(message)
        logging.getLogger("features").warning(message)


#: Values that mean False when a flag arrives as text.
_FALSEY_TEXT = {"", "0", "false", "f", "no", "n", "none", "null", "nan"}


def truthy(v) -> int:
    """A flag as 0/1; from Kafka it may be text, and "False" is a non-empty string."""
    if isinstance(v, str):
        return 0 if v.strip().lower() in _FALSEY_TEXT else 1
    return 1 if v else 0


def unusable(event: dict):
    """Why an event cannot be scored, or None. Raised in the operator, it would fail
    the task on every replay; a NaN amount would poison the sender's baseline."""
    try:
        amount = float(event["amount_uzs"])
        secs = float(event.get("secs_login_to_confirm") or 0.0)
    except (KeyError, TypeError, ValueError) as exc:
        return f"{type(exc).__name__}: {exc}"
    if not (math.isfinite(amount) and amount >= 0):
        return f"amount_uzs {amount!r}"
    if not (math.isfinite(secs) and secs >= 0):
        return f"secs_login_to_confirm {secs!r}"
    if event.get("event_time") is not None and not isinstance(event["event_time"], str):
        return f"event_time {event['event_time']!r}"
    return None


def event_from(row: dict) -> dict:
    """A generated CSV row as the event `rules.evaluate` expects (offline replays)."""
    return {
        "amount_uzs": row["amount_uzs"],
        "sender_pinfl": row["sender_pinfl"],
        "receiver_pinfl": row["receiver_pinfl"],
        "sender_region": row["sender_region"],
        "active_call": truthy(row.get("active_call")),
        "secs_login_to_confirm": row.get("secs_login_to_confirm", 0.0),
        "sender_card": row.get("sender_card", ""),
        "receiver_card": row.get("receiver_card", ""),
    }


def sender_key(event: dict) -> str:
    """The sender's own account in payee_key's key space."""
    if CAP.mode("payee_identity") == "pinfl":
        pinfl = str(event.get("sender_pinfl", "") or "")
        if pinfl:
            return pinfl
    return str(event.get("sender_card", "") or "")


def payee_key(event: dict) -> str:
    """The payee's key, card or pinfl, the same for every transfer of a deployment."""
    if CAP.mode("payee_identity") == "pinfl":
        pinfl = str(event.get("receiver_pinfl", "") or "")
        if pinfl:
            return pinfl
        _warn_once(_NO_PINFL)
    card = str(event.get("receiver_card", "") or "")
    if not card:
        _warn_once(_NO_PAYEE_KEY)
    return card


def extract(event: dict, state, now: float, receiver_state=None,
            sender_inbound_ts=None, confirmed=frozenset()) -> dict:
    """The features of one event; mutates nothing. `receiver_state` is None while
    Redis is down: the payee-side features then read as nothing seen. `confirmed`: the
    confirmed fraud accounts among the payee, the sender and the payee's contacts."""
    amount = float(event["amount_uzs"])
    payee = payee_key(event)
    region = event.get("sender_region", "")

    ev = state.events
    n_hist = state.n_amt
    has_history = n_hist >= C.AMOUNT_DEVIATION_MIN_HISTORY
    mean = state.mean_amt
    std = math.sqrt(state.m2_amt / n_hist) if n_hist > 0 else 0.0

    # Only events at or before this one: a replay started earlier in time than the last
    # one leaves later events in the state, which no window may count.
    def past(window):
        return [e for e in ev if 0 <= now - e[0] <= window]

    def win_count(window):                      # this transfer included
        return len(past(window)) + 1

    band_low = C.STRUCTURING_BAND_LOW * C.STRUCTURING_THRESHOLD
    sub = sum(1 for e in past(C.STRUCTURING_WINDOW_S)
              if band_low <= e[1] < C.STRUCTURING_THRESHOLD)
    if band_low <= amount < C.STRUCTURING_THRESHOLD:
        sub += 1

    distinct = {e[2] for e in past(C.DISTINCT_PAYEE_WINDOW_S)} | {payee}

    geo_is_anomaly = 0
    if state.region_counts:
        home = state.region_counts.most_common(1)[0][0]
        if sum(state.region_counts.values()) >= 3 and region != home:
            geo_is_anomaly = 1

    # For the IMPOSSIBLE_TRAVEL rule only, not the model.
    travel_kmh = 0.0
    travel_distance_km = 0.0
    if state.last_region and region and region != state.last_region \
            and now >= state.last_region_ts:
        d = G.region_distance_km(state.last_region, region)
        if d is not None:
            travel_distance_km = d
            travel_kmh = G.implied_speed_kmh(
                state.last_region, region, now - state.last_region_ts)

    earlier = [e[0] for e in ev if e[0] <= now]
    secs_since_last = (now - max(earlier)) if earlier else float(C.RECENT_RETENTION_S)
    daily_sum = sum(e[1] for e in past(C.DAILY_WINDOW_S)) + amount

    amount_to_mean = (amount / mean) if mean > 0 else float(C.NEW_PAYEE_AMOUNT_FACTOR + 1)
    amount_z = ((amount - mean) / std) if std > 0 else 0.0

    # Fan-in on the payee: distinct senders, since ten from one person is a habit.
    me = event.get("sender_pinfl", "")
    rcv_senders, rcv_inflow, day_inflow = 0, 0.0, 0.0
    if receiver_state is not None:
        recent = _within(receiver_state.inbound, now, C.RECEIVER_WINDOW_S)
        rcv_senders = len({e[1] for e in recent} | {me})
        rcv_inflow = sum(e[2] for e in recent) + amount
        day_inflow = sum(e[2] for e in _within(receiver_state.inbound, now, C.LINK_DAY_S)) + amount

    # Counterparties over a day and a week on both sides, and the time since the
    # sender was last paid: a mule collects over days and passes the money on.
    def _windows(times, self_key):
        """Distinct counterparties per window, this transfer's included; one pass,
        since a hub account can hold tens of thousands."""
        day = week = 0
        for other, t in times.items():
            if other == self_key:
                continue
            age = now - t
            if 0 <= age <= C.LINK_WEEK_S:
                week += 1
                if age <= C.LINK_DAY_S:
                    day += 1
        return day + 1, week + 1

    payee_payers_24h = payee_payers_7d = payee_new_payers_24h = 0
    if receiver_state is not None:
        payee_payers_24h, payee_payers_7d = _windows(receiver_state.payers, me)
        # Payers who had not paid this payee for a month; this one included.
        payee_new_payers_24h = (sum(1 for t in receiver_state.new_payers.values()
                                    if now - t <= C.LINK_DAY_S)
                                + (not _known_payer(receiver_state, me, now)))
    sender_payees_24h, sender_payees_7d = _windows(state.payee_times, payee)
    # The sender's payees paid for the first time within a day, and the money sent them.
    first = payee not in state.seen_payees
    to_new = [e[1] for e in state.new_payees if 0 <= now - e[0] <= C.LINK_DAY_S]
    # Capped, never NaN: "never paid" and "Redis down" read alike.
    secs_since_sender_inbound = float(C.LINK_WEEK_S)
    if sender_inbound_ts:
        secs_since_sender_inbound = max(0.0, min(now - float(sender_inbound_ts),
                                                 float(C.LINK_WEEK_S)))

    # The payee's contacts of the week among the confirmed fraud accounts.
    flagged_contacts = 0
    if receiver_state is not None:
        flagged_contacts = sum(1 for card, t in receiver_state.contacts.items()
                               if now - t <= C.LINK_WEEK_S and card in confirmed)

    active_call = truthy(event.get("active_call"))
    secs_login = float(event.get("secs_login_to_confirm") or 0.0)

    # In log space: session times are lognormal.
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
        "is_new_payee": int(first),
        "rcv_distinct_senders_1h": rcv_senders,
        "rcv_inflow_1h": math.log1p(rcv_inflow),
        "payee_payers_24h": payee_payers_24h,
        "payee_payers_7d": payee_payers_7d,
        "sender_payees_24h": sender_payees_24h,
        "sender_payees_7d": sender_payees_7d,
        "secs_since_sender_inbound": secs_since_sender_inbound,
        "payee_inflow_24h": math.log1p(day_inflow),
        "payee_new_payers_24h": payee_new_payers_24h,
        "sender_new_payees_24h": len(to_new) + first,
        "sender_new_payee_sum_24h": math.log1p(sum(to_new) + (amount if first else 0.0)),
        "payee_flagged": int(payee in confirmed),
        "sender_flagged": int(sender_key(event) in confirmed),
        "payee_flagged_contacts": flagged_contacts,
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
        # Rule helpers, not model features:
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


def _within(entries, now: float, window: float) -> list:
    """The entries of a time-ordered deque no older than `window`, newest first."""
    out = []
    for e in reversed(entries):
        if now - e[0] > window:
            break
        if e[0] <= now:
            out.append(e)
    return out


def _known_payer(receiver_state, payer: str, now: float) -> bool:
    """Whether `payer` paid this payee within the last month."""
    return now - receiver_state.payers.get(payer, -math.inf) <= C.LINK_MONTH_S


def _prune_links(times: dict, now: float, keep: float = C.LINK_WEEK_S) -> None:
    """Drop entries older than `keep` (memory only: extract filters by time)."""
    if len(times) <= C.LINK_PRUNE_AT:
        return
    cutoff = now - keep
    for k, t in list(times.items()):
        if t < cutoff:
            del times[k]


def add_contact(account_state, card: str, now: float) -> None:
    """File `card` among this account's contacts (after extract); each end of a
    transfer files the other."""
    if account_state is None or not card:
        return
    account_state.contacts[card] = now
    _prune_links(account_state.contacts, now)


def update_receiver_state(receiver_state, event: dict, now: float) -> None:
    """Advance the payee's inbound history (call AFTER extract)."""
    if receiver_state is None:
        return
    me = event.get("sender_pinfl", "")
    receiver_state.inbound.append((now, me, float(event["amount_uzs"])))
    if not _known_payer(receiver_state, me, now):
        receiver_state.new_payers[me] = now
    receiver_state.payers[me] = now
    receiver_state.last_inbound_ts = now
    _prune_links(receiver_state.payers, now, C.LINK_MONTH_S)
    _prune_links(receiver_state.new_payers, now, C.LINK_DAY_S)
    add_contact(receiver_state, sender_key(event), now)
    while (receiver_state.inbound
           and now - receiver_state.inbound[0][0] > C.LINK_DAY_S):
        receiver_state.inbound.popleft()


def update_state(state, event: dict, now: float) -> None:
    """Advance per-sender state with the current event (call AFTER extract)."""
    amount = float(event["amount_uzs"])
    payee = payee_key(event)
    if payee not in state.seen_payees:
        state.new_payees.append((now, amount))
    while state.new_payees and now - state.new_payees[0][0] > C.LINK_DAY_S:
        state.new_payees.popleft()
    state.seen_payees.add(payee)
    state.payee_times[payee] = now
    _prune_links(state.payee_times, now)
    state.events.append((now, amount, payee))
    while state.events and now - state.events[0][0] > C.RECENT_RETENTION_S:
        state.events.popleft()
    region = event.get("sender_region", "")
    state.region_counts[region] += 1
    # Only a located event moves the origin of the next journey, and not back in time.
    if region and now >= state.last_region_ts:
        state.last_region = region
        state.last_region_ts = now
    # Running mean and variance (Welford): the whole history in constant memory.
    state.n_amt += 1
    delta = amount - state.mean_amt
    state.mean_amt += delta / state.n_amt
    state.m2_amt += delta * (amount - state.mean_amt)
    log_secs = math.log1p(float(event.get("secs_login_to_confirm") or 0.0))
    state.n_secs += 1
    d = log_secs - state.mean_secs
    state.mean_secs += d / state.n_secs
    state.m2_secs += d * (log_secs - state.mean_secs)
