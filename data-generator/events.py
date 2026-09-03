"""Normal transaction behaviour: salary, rent, transfers to relatives, payments.
Spec for what each pattern should look like: docs/generator-spec.md 3."""

import uuid

EVENT_FIELDS = [
    "transaction_id", "event_time",
    "sender_pinfl", "sender_card", "sender_network",
    "sender_name", "sender_bank_code", "sender_bank_name",
    "receiver_pinfl", "receiver_card", "receiver_network",
    "receiver_name", "receiver_bank_code", "receiver_bank_name",
    "amount_uzs", "channel", "device_id",
    "sender_region", "receiver_region", "sender_balance_before",
    "active_call", "secs_login_to_confirm",
    # enriched (Flink-side in production)
    "is_new_payee", "receiver_account_age_days", "is_family_transfer",
    "label_is_fraud", "label_fraud_type",
]

import numpy as np
import config as C


def gen_session_signals(sender, fraud_type, rng):
    """Behavioural session signals: active call + login→confirm latency."""
    if fraud_type == "APP":
        stretch = float(rng.uniform(*C.APP_TIME_STRETCH))
        call = bool(rng.random() < C.ACTIVE_CALL_APP_RATE)
    elif fraud_type == "ATO":
        stretch = float(rng.uniform(*C.ATO_TIME_COMPRESS))
        call = bool(rng.random() < C.ACTIVE_CALL_BASE_RATE)
    else:                      # NONE / MULE / STRUCTURING — actor acts unpressured
        stretch = 1.0
        call = bool(rng.random() < C.ACTIVE_CALL_BASE_RATE)

    median = getattr(sender, "decision_time_median", C.DECISION_TIME_MEDIAN_SEC)
    secs = median * stretch * float(np.exp(rng.normal(0, C.DECISION_TIME_SIGMA)))
    return call, round(max(C.SECS_LOGIN_FLOOR, secs), 1)

def make_event(sender, receiver, amount, ts, channel, device_id,
               is_new_payee, balance_before,
               is_fraud=0, fraud_type="NONE", rng=None):
    active_call, secs_login = gen_session_signals(sender, fraud_type, rng)
    """Build one transaction event. `sender`/`receiver` are Person-like objects."""
    return {
        "transaction_id": str(uuid.uuid4()),
        "event_time": ts.isoformat(),
        "sender_pinfl": sender.pinfl,
        "sender_card": sender.card,
        "sender_network": sender.network,
        "sender_name": sender.full_name,
        "sender_bank_code": sender.bank_code,
        "sender_bank_name": sender.bank_name,
        "receiver_pinfl": receiver.pinfl,
        "receiver_card": receiver.card,
        "receiver_network": receiver.network,
        "receiver_name": receiver.full_name,
        "receiver_bank_code": receiver.bank_code,
        "receiver_bank_name": receiver.bank_name,
        "amount_uzs": int(amount),
        "channel": channel,
        "device_id": device_id,
        "sender_region": sender.region,
        "receiver_region": receiver.region,
        "sender_balance_before": int(balance_before),
        "active_call": bool(active_call),
        "secs_login_to_confirm": float(secs_login),
        "is_new_payee": bool(is_new_payee),
        "receiver_account_age_days": int(receiver.account_age_days),
        # Fraud accounts live in their own households, so this is False for them
        # unless a pattern deliberately routes through a real relative.
        "is_family_transfer": bool(
            sender.household_id == receiver.household_id),
        "label_is_fraud": int(is_fraud),
        "label_fraud_type": fraud_type,

    }