"""
Unit tests for the RECEIVER_AGE_MODE ablation switch.

FEATURE_NAMES is fixed at import time, so the contract itself is tested through
`_age_block`; the per-event gating is tested by flipping `C.RECEIVER_AGE_MODE`,
which `visible_receiver_age` reads on every call.

Run: python -m pytest test_receiver_age_modes.py -q
"""

import math

import pytest

import capabilities as CAP
import features as F
from rules import SenderState, evaluate
from conftest import bank_card, payee_card


@pytest.fixture
def mode():
    """Restore the configured mode after each test."""
    original = CAP.MODES["receiver_age"]
    yield lambda m: CAP.MODES.__setitem__("receiver_age", m)
    CAP.MODES["receiver_age"] = original


def _ev(sender_bank="BankA", receiver_bank="BankA"):
    return {"amount_uzs": 500_000, "receiver_pinfl": "rcv", "device_id": "dev-1",
            "sender_region": "Tashkent City",
            "sender_card": bank_card(sender_bank),
            "receiver_card": payee_card("rcv", receiver_bank)}


# --- the feature contract ---------------------------------------------------

def test_contract_differs_per_mode(mode):
    mode("always")
    assert CAP.feature_names().count("receiver_age_known") == 0
    assert "receiver_age" in CAP.feature_names()

    mode("on_us")
    assert CAP.feature_names().count("receiver_age_known") == 1

    mode("off")
    names = CAP.feature_names()
    assert "receiver_age" not in names and "receiver_is_fresh" not in names


def test_unknown_mode_is_rejected_loudly(monkeypatch):
    """A typo must not silently fall back to a different experiment."""
    monkeypatch.setenv("CAP_RECEIVER_AGE", "on-us")
    with pytest.raises(ValueError):
        CAP._configured(CAP.BY_KEY["receiver_age"])


# --- the on-us test ---------------------------------------------------------

def test_same_issuer_is_on_us():
    assert F.is_on_us(_ev("BankA", "BankA")) is True


def test_different_issuer_is_not_on_us():
    assert F.is_on_us(_ev("BankA", "BankB")) is False


def test_unresolvable_issuer_is_treated_as_inter_bank():
    """An unknown BIN is not evidence of a shared institution."""
    assert F.is_on_us(_ev("BankA", "")) is False
    assert F.is_on_us(_ev("", "")) is False


# --- visibility gating ------------------------------------------------------

def test_always_mode_sees_every_age(mode):
    mode("always")
    assert F.visible_receiver_age(_ev("BankA", "BankB"), 42) == 42


def test_off_mode_sees_nothing(mode):
    mode("off")
    assert F.visible_receiver_age(_ev("BankA", "BankA"), 42) is None


def test_on_us_mode_sees_only_same_issuer(mode):
    mode("on_us")
    assert F.visible_receiver_age(_ev("BankA", "BankA"), 42) == 42
    assert F.visible_receiver_age(_ev("BankA", "BankB"), 42) is None


# --- what the model and the rules then see ----------------------------------

def test_unknown_age_is_nan_not_a_sentinel(mode):
    """A sentinel would be ordered against real ages; NaN is a separate branch."""
    mode("on_us")
    f = F.extract(_ev("BankA", "BankB"), 5, SenderState(), now=1000)
    assert math.isnan(f["receiver_age"])
    assert math.isnan(f["receiver_is_fresh"])
    assert f["receiver_age_known"] == 0


def test_fresh_receiver_rule_does_not_fire_on_unknown_age(mode):
    """NaN is truthy in Python, so this rule needs its own guard."""
    mode("on_us")
    res = evaluate(_ev("BankA", "BankB"), 5, SenderState(), now=1000)
    assert "FRESH_RECEIVER" not in res["rule_hits"]


def test_fresh_receiver_rule_still_fires_on_us(mode):
    mode("on_us")
    res = evaluate(_ev("BankA", "BankA"), 5, SenderState(), now=1000)
    assert "FRESH_RECEIVER" in res["rule_hits"]


def test_audit_trail_records_what_the_bank_could_see(mode):
    """The decision record must not carry ground truth the bank never had."""
    mode("on_us")
    res = evaluate(_ev("BankA", "BankB"), 5, SenderState(), now=1000)
    assert res["receiver_account_age_days"] is None
