"""Normal transaction behaviour: salary, rent, transfers to relatives, payments.
Spec for what each pattern should look like: docs/generator-spec.md 3."""

import uuid

from config import SECOND_CARD_USE_RATE

EVENT_FIELDS = [
    "transaction_id", "event_time",
    "sender_pinfl", "sender_card", "sender_network",
    "sender_name", "sender_bank_code", "sender_bank_name",
    "receiver_pinfl", "receiver_card", "receiver_network",
    "receiver_name", "receiver_bank_code", "receiver_bank_name",
    "amount_uzs", "device_id",
    "sender_region", "receiver_region", "sender_balance_before",
    "active_call", "secs_login_to_confirm",
    # enriched (Flink-side in production)
    "is_new_payee", "receiver_account_age_days", "is_family_transfer",
    "label_is_fraud", "label_fraud_type",
]

import numpy as np
import config as C


def gen_session_signals(sender, fraud_type, rng):
    """Behavioural session signals: active call + login→confirm latency, at the
    rates of the profile being generated (config.PROFILE)."""
    prof = C.PROFILE
    if fraud_type == "APP":
        stretch = float(rng.uniform(*prof.app_time_stretch))
        call = bool(rng.random() < prof.active_call_app_rate)
    elif fraud_type == "ATO":
        stretch = float(rng.uniform(*C.ATO_TIME_COMPRESS))
        call = bool(rng.random() < prof.active_call_base_rate)
    else:                      # NONE / MULE / STRUCTURING — actor acts unpressured
        stretch = 1.0
        call = bool(rng.random() < prof.active_call_base_rate)

    median = getattr(sender, "decision_time_median", C.DECISION_TIME_MEDIAN_SEC)
    secs = median * stretch * float(np.exp(rng.normal(0, prof.decision_time_sigma)))
    return call, round(max(C.SECS_LOGIN_FLOOR, secs), 1)


def round_like_a_person(amount):
    """People send round sums - to 10,000, 50,000 or 100,000 UZS, the step growing
    with the amount (generator-spec.md 8, item 6)."""
    step = 10_000 if amount < 500_000 else 50_000 if amount < 5_000_000 else 100_000
    return float(max(step, round(amount / step) * step))

def _payee_card(receiver, rng):
    """Which of the payee's cards this transfer lands on - what lets PAN and PINFL
    keys differ (config.SECOND_CARD_*). Without `rng`, the primary card."""
    second = getattr(receiver, "card2", "")
    if second and rng is not None and rng.random() < SECOND_CARD_USE_RATE:
        return {"receiver_card": second,
                "receiver_network": receiver.network2,
                "receiver_name": receiver.full_name,
                "receiver_bank_code": receiver.bank_code2,
                "receiver_bank_name": receiver.bank_name2}
    return {"receiver_card": receiver.card,
            "receiver_network": receiver.network,
            "receiver_name": receiver.full_name,
            "receiver_bank_code": receiver.bank_code,
            "receiver_bank_name": receiver.bank_name}


def make_event(sender, receiver, amount, ts, device_id,
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
        **_payee_card(receiver, rng),
        "amount_uzs": int(amount),
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