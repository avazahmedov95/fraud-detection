"""
Unit tests for the pure CEP rule engine. Run: python -m pytest test_rules.py -q
(or: python test_rules.py for a plain run without pytest).
"""

import pytest

from rules import SenderState, evaluate
import config as C
import capabilities as CAP
from conftest import bank_card, payee_card

# A rule cannot fire when the capability behind it is switched off, so tests
# asserting on those rules are skipped rather than weakened.
needs_receiver_age = pytest.mark.skipif(
    not CAP.enabled("receiver_age"),
    reason="receiver age is disabled by CAP_RECEIVER_AGE=off")
needs_geo = pytest.mark.skipif(
    not CAP.enabled("geo_telemetry"),
    reason="geo is disabled by CAP_GEO_TELEMETRY=off")
needs_device = pytest.mark.skipif(
    not CAP.enabled("device_telemetry"),
    reason="device telemetry is disabled by CAP_DEVICE_TELEMETRY=off")


def _ev(amount, payee="rcv", device="dev-1", region="Tashkent City",
        sender_bank="BankA", receiver_bank="BankA"):
    # Both sides default to the SAME issuer so these tests behave identically
    # under every RECEIVER_AGE_MODE: on-us means the age is always visible, so
    # the rules under test here are exercised on their own terms. The effect of
    # the mode switch is covered in test_receiver_age_modes.py.
    return {"amount_uzs": amount, "receiver_pinfl": payee,
            "device_id": device, "sender_region": region,
            "sender_card": bank_card(sender_bank),
            "receiver_card": payee_card(payee, receiver_bank)}


def test_known_small_payment_is_allowed():
    st = SenderState()
    # establish a baseline of small transfers to a known payee
    for _ in range(6):
        evaluate(_ev(100_000, payee="friend"),  800, st, now=1000)
    res = evaluate(_ev(120_000, payee="friend"),  800, st, now=2000)
    assert res["decision"] == "ALLOW"
    assert res["is_new_payee"] is False



@needs_receiver_age
def test_app_pattern_new_large_fresh_payee_is_flagged():
    st = SenderState()
    for i in range(6):
        evaluate(_ev(150_000, payee="friend"),  800, st, now=1000 + i)
    res = evaluate(_ev(9_000_000, payee="fraudster"),
                   receiver_age_days=5, state=st, now=5000)
    assert res["decision"] in ("REVIEW", "BLOCK")
    assert "NEW_PAYEE_HIGH_AMOUNT" in res["rule_hits"]
    assert "FRESH_RECEIVER" in res["rule_hits"]


def test_velocity_burst_is_flagged():
    st = SenderState()
    res = None
    for i in range(7):  # 7 transfers inside the velocity window
        res = evaluate(_ev(200_000, payee=f"p{i}"),  800, st, now=1000 + i)
    assert "VELOCITY" in res["rule_hits"]


def test_structuring_is_flagged():
    st = SenderState()
    just_under = int(0.95 * C.STRUCTURING_THRESHOLD)
    res = None
    for i in range(3):
        res = evaluate(_ev(just_under, payee=f"p{i}"), 800, st, now=1000 + i * 60)
    assert "STRUCTURING" in res["rule_hits"]


@needs_geo
@needs_device
def test_device_and_geo_change_flagged():
    st = SenderState()
    for i in range(4):
        evaluate(_ev(150_000, payee="friend", device="dev-A", region="Samarkand"),
                  800, st, now=1000 + i)
    res = evaluate(_ev(150_000, payee="friend", device="dev-NEW", region="Andijan"),
                    800, st, now=2000)
    assert "DEVICE_CHANGE" in res["rule_hits"]
    assert "GEO_ANOMALY" in res["rule_hits"]


@needs_geo
def test_impossible_travel_is_flagged():
    """Tashkent -> Nukus (~800 km) ten minutes later: no journey achieves this."""
    st = SenderState()
    evaluate(_ev(150_000, payee="friend", region="Tashkent City"), 800, st, now=1000)
    res = evaluate(_ev(150_000, payee="friend", region="Karakalpakstan"),
                   800, st, now=1000 + 600)
    assert "IMPOSSIBLE_TRAVEL" in res["rule_hits"]
    # On its own the rule must be decisive enough to reach REVIEW.
    assert res["decision"] in ("REVIEW", "BLOCK")


@needs_geo
def test_real_travel_is_not_flagged():
    """Same journey twelve hours later is an ordinary trip, not a contradiction."""
    st = SenderState()
    evaluate(_ev(150_000, payee="friend", region="Tashkent City"), 800, st, now=1000)
    res = evaluate(_ev(150_000, payee="friend", region="Karakalpakstan"),
                   800, st, now=1000 + 12 * 3600)
    assert "IMPOSSIBLE_TRAVEL" not in res["rule_hits"]


@needs_geo
def test_adjacent_regions_are_never_impossible():
    """Tashkent City and Tashkent Region border each other; centre-to-centre
    distance is an artefact of the reference table, not a real journey."""
    st = SenderState()
    evaluate(_ev(150_000, payee="friend", region="Tashkent City"), 800, st, now=1000)
    res = evaluate(_ev(150_000, payee="friend", region="Tashkent Region"),
                   800, st, now=1001)
    assert "IMPOSSIBLE_TRAVEL" not in res["rule_hits"]


@needs_geo
def test_unknown_region_does_not_fire():
    """An unmapped region is 'not applicable', never a zero-distance move."""
    st = SenderState()
    evaluate(_ev(150_000, payee="friend", region="Atlantis"), 800, st, now=1000)
    res = evaluate(_ev(150_000, payee="friend", region="Karakalpakstan"),
                   800, st, now=1001)
    assert "IMPOSSIBLE_TRAVEL" not in res["rule_hits"]


@needs_geo
def test_impossible_travel_is_independent_of_home_region():
    """A sender with no established home has no GEO_ANOMALY, but physics still
    applies — the two rules must not be coupled."""
    st = SenderState()
    evaluate(_ev(150_000, payee="friend", region="Termez-like"), 800, st, now=1000)
    st.last_region = "Surkhandarya"
    res = evaluate(_ev(150_000, payee="friend", region="Khorezm"), 800, st, now=1060)
    assert "IMPOSSIBLE_TRAVEL" in res["rule_hits"]
    assert "GEO_ANOMALY" not in res["rule_hits"]
