"""
Shared feature extraction — the single train/serve feature contract.

The SAME function produces the feature vector for:
  * offline model training (replay over the CSV), and
  * online scoring inside the Flink job (per event, from keyed state).

This guarantees the model sees identical features at train and serve time. The
CEP rule engine (rules.py) also consumes these features, so rules and ML never
drift apart. Pure: no Flink/Redis/Neo4j imports.
"""

import logging
import math
import datetime

import config as C
import geo as G
import capabilities as CAP
import bins as B

# The model's feature vector, derived from the capability registry rather than
# maintained by hand — so the train/serve contract cannot drift from what the
# deployment can actually observe. See capabilities.py.
FEATURE_NAMES = CAP.feature_names()

_CHANNEL_KEY = {"MOBILE_APP": "ch_mobile_app", "USSD": "ch_ussd",
                "WEB": "ch_web", "ATM": "ch_atm"}


_warned_no_pinfl = False


def _warn_pinfl_unavailable():
    """Once per process. This runs on the 300 ms path and the condition is a
    deployment mistake that does not change between events."""
    global _warned_no_pinfl
    if not _warned_no_pinfl:
        _warned_no_pinfl = True
        logging.getLogger("features").warning(
            "CAP_PAYEE_IDENTITY=pinfl but the events carry no receiver_pinfl; "
            "keying the payee by card instead. The live wire format does not "
            "carry the payee's identity - only the offline harnesses, which "
            "read the generated CSV, can run this mode.")


#: Values that mean False when a flag arrives as text.
_FALSEY_TEXT = {"", "0", "false", "f", "no", "n", "none", "null", "nan"}


def truthy(v) -> int:
    """Coerce a wire-shaped flag to 0/1.

    `1 if v else 0` is wrong at this boundary and was wrong in production. A
    Kafka record built from csv.DictReader carries every field as TEXT, so
    active_call arrived as the string "False" - which is a non-empty string and
    therefore true. The live job scored active_call = 1 on 100% of events while
    the model had been trained on 3.5%: not a missing feature, a constant one,
    and constant at the RARE value.

    Coercing here rather than only in the producer is deliberate. This module is
    the single train/serve feature contract; every caller - the Flink job, the
    offline replay, ml/dataset.py, the tests - reaches the model through it, and
    the contract should not depend on each of them having typed its input
    correctly. The producer is fixed too, but that fix protects one caller.
    """
    if isinstance(v, str):
        return 0 if v.strip().lower() in _FALSEY_TEXT else 1
    return 1 if v else 0


def _issuer(event: dict, side: str) -> str:
    """Card issuer for one side of the transfer, resolved from the PAN's BIN.

    This is what a deployment actually does, and it used to be faked: the two
    `*_bank_name` fields travelled in the Kafka message, which made the wire
    format carry something UzCard / HUMO does not carry and made the on-us test
    - and with it the whole receiver_age capability - depend on a convenience of
    the generator. The switch message carries the PAN; the bank resolves the
    issuer from its 6-digit BIN against its own table. See bins.py.
    """
    return B.issuer_of(event.get(f"{side}_card"))


def is_on_us(event: dict) -> bool:
    """True when both parties bank with the same issuer.

    Only then can the sending bank look the receiver's account age up in its own
    core system. Unknown issuers are treated as inter-bank: the pessimistic
    reading, since an unresolvable BIN is not evidence of a shared institution.
    """
    s, r = _issuer(event, "sender"), _issuer(event, "receiver")
    return bool(s) and bool(r) and s == r


def payee_key(event: dict) -> str:
    """The identity this deployment can pin the payee to.

    Everything receiver-side is keyed on this: the payee history behind
    `is_new_payee`, the inbound window behind the fan-in features, and the
    account-age lookup. It must therefore be an identifier present on EVERY
    event, which is what rules out resolving it only where the bank can.

    card  - the destination PAN, which is what a card-to-card transfer actually
            delivers to the sending bank. The default.
    pinfl - the person behind the PAN. Available to the switch or the CBU
            platform for everyone, and to a bank only for its own clients.
            Models a platform-level deployment.

    There is deliberately no per-transfer mode. Resolving to PINFL where the
    bank can and to PAN otherwise makes the key depend on the SENDER's bank,
    which splits one payee's inbound window into two by a property that is not
    about the payee at all. Measured: 24 MULE_FAN_IN hits under either uniform
    key, 19 under the mixed one - a 17.4% loss of the rule's true positives for
    no gain. See capabilities.payee_identity.
    """
    if CAP.mode("payee_identity") == "pinfl":
        pinfl = str(event.get("receiver_pinfl", "") or "")
        if pinfl:
            return pinfl
        # Asked for pinfl, given an event that does not carry one. This is the
        # normal state of the LIVE stream: receiver_pinfl left the wire because
        # a sending bank cannot resolve the destination PAN to a person, so the
        # mode is reachable only from the offline harnesses, which read the CSV.
        #
        # Returning "" here would be the quiet catastrophe: an empty key makes
        # ReceiverStore skip the write and the read, and the whole fan-in signal
        # disappears with the knob showing "on". So fall back to the card and
        # say so, once.
        _warn_pinfl_unavailable()
    return str(event.get("receiver_card", "") or "")


def visible_receiver_age(event: dict, receiver_age_days):
    """Apply RECEIVER_AGE_MODE: what the sending bank can actually see.

    Returns None when the age is not obtainable, which callers must treat as
    "unknown" rather than as a value.
    """
    mode = CAP.mode("receiver_age")
    if mode == "off":
        return None
    if mode == "on_us" and not is_on_us(event):
        return None
    return receiver_age_days


def extract(event: dict, receiver_age_days, state, now: float,
            receiver_state=None) -> dict:
    """Read-only feature extraction. Does NOT mutate either state.

    `state` is the sender's history (Flink keyed state); `receiver_state` is the
    payee's inbound history from the shared store, and may be None when that
    store is unavailable — the inbound features then read as zero, which is the
    fail-open behaviour the rest of the enrichment path also uses.
    """
    amount = float(event["amount_uzs"])
    payee = payee_key(event)
    device = event.get("device_id", "")
    region = event.get("sender_region", "")
    channel = event.get("channel", "MOBILE_APP")
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

    # Implied travel speed since the sender's previous transaction. Kept as a
    # rule helper rather than a model feature: it is near-zero for almost every
    # event, so it carries little gradient for the model, while as a rule it is
    # a deterministic physical contradiction the CEP layer can act on alone.
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

    # What the sending bank can actually see, per RECEIVER_AGE_MODE.
    age = visible_receiver_age(event, receiver_age_days)
    age_known = 1 if age is not None else 0
    if age is not None:
        receiver_age = float(age)
        receiver_is_fresh = 1.0 if age < C.FRESH_RECEIVER_DAYS else 0.0
    elif CAP.mode("receiver_age") == "on_us":
        # NaN, not a sentinel: LightGBM branches on missing values natively, so
        # "unknown" stays distinguishable from "known and unremarkable". A
        # sentinel such as -1 would be ordered against real ages instead.
        receiver_age = float("nan")
        receiver_is_fresh = float("nan")
    else:
        receiver_age = -1.0
        receiver_is_fresh = 0.0

    # Inbound concentration on the PAYEE — the fan-in shape sender-keyed state
    # cannot see. Counting distinct senders rather than transfers: ten transfers
    # from one person is a habit, one each from ten people is a collection point.
    rcv_senders, rcv_inflow = 0, 0.0
    if receiver_state is not None:
        recent = [e for e in receiver_state.inbound
                  if now - e[0] <= C.RECEIVER_WINDOW_S]
        rcv_senders = len({e[1] for e in recent} | {event.get("sender_pinfl", "")})
        rcv_inflow = sum(e[2] for e in recent) + amount

    active_call = truthy(event.get("active_call"))
    secs_login = float(event.get("secs_login_to_confirm") or 0.0)

    # z-score in LOG space: session latency is lognormal, so log() first —
    # otherwise the right tail dominates and z is meaningless.
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
        "receiver_age": receiver_age,
        "receiver_is_fresh": receiver_is_fresh,
        "receiver_age_known": age_known,
        # MyID-verified kinship between sender and payee. Only meaningful when
        # the myid_kinship capability is on; otherwise it is not in the vector.
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
        "ch_mobile_app": 0, "ch_ussd": 0, "ch_web": 0, "ch_atm": 0,
        # --- rule helpers (NOT model features) ---
        "travel_kmh": travel_kmh,
        "travel_distance_km": travel_distance_km,
        "amount": amount,
        "has_history": 1 if has_history else 0,
        "amount_gt_factor_mean": 1 if ((not has_history) or (amount > C.NEW_PAYEE_AMOUNT_FACTOR * mean)) else 0,
    }
    ch_key = _CHANNEL_KEY.get(channel)
    if ch_key:
        feat[ch_key] = 1
    return feat


def to_vector(feat: dict) -> list:
    """Feature dict -> ordered float vector matching FEATURE_NAMES."""
    return [float(feat[name]) for name in FEATURE_NAMES]


def update_receiver_state(receiver_state, event: dict, now: float) -> None:
    """Advance the payee's inbound history (call AFTER extract)."""
    if receiver_state is None:
        return
    receiver_state.inbound.append(
        (now, event.get("sender_pinfl", ""), float(event["amount_uzs"])))
    while (receiver_state.inbound
           and now - receiver_state.inbound[0][0] > C.RECEIVER_WINDOW_S):
        receiver_state.inbound.popleft()


def update_state(state, event: dict, now: float) -> None:
    """Advance per-sender state with the current event (call AFTER extract)."""
    amount = float(event["amount_uzs"])
    # The same resolver extract() used. Keying the write differently from the
    # read would leave `seen_payees` permanently missing what it just recorded,
    # and is_new_payee would read 1 on every event forever.
    payee = payee_key(event)
    state.seen_payees.add(payee)
    state.events.append((now, amount, payee, event.get("device_id", "")))
    # Bound the window deque (memory). Stale entries are time-filtered in extract anyway.
    while state.events and now - state.events[0][0] > C.RECENT_RETENTION_S:
        state.events.popleft()
    state.known_devices.add(event.get("device_id", ""))
    region = event.get("sender_region", "")
    state.region_counts[region] += 1
    # Last *located* event, for the travel-speed check. Only advanced when the
    # event actually carries a region, so a region-less event cannot reset the
    # origin point and mask an impossible journey around it.
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
