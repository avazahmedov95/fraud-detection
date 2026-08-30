"""
Unit tests for capability-scaled decision thresholds.

The behaviour under test was found empirically: run on PaySim with two rules
available, the highest CEP score any fraud reached was 0.35 against a 0.40
review cutoff, so nothing was ever flagged — while the same rules separated the
classes 4:1. A fixed additive threshold turns a reduced deployment into a silent
one rather than a degraded one.

Run: python -m pytest test_scaled_thresholds.py -q
"""

import pytest

import capabilities as CAP
import config as C
import rules as R


@pytest.fixture
def profile():
    """Set capability modes and clear the threshold cache around each test."""
    saved = dict(CAP.MODES)
    R._THRESHOLD_CACHE.clear()
    yield lambda **kw: (CAP.MODES.update(kw), R._THRESHOLD_CACHE.clear())
    CAP.MODES.clear(); CAP.MODES.update(saved)
    R._THRESHOLD_CACHE.clear()


# --- reachability -----------------------------------------------------------

def test_every_pattern_is_reachable_at_full_capability(profile):
    profile(**{c.key: ("on" if "on" in c.modes else c.modes[0])
               for c in CAP.REGISTRY if not c.always_on})
    for pattern in CAP.PATTERN_SIGNATURES:
        assert CAP.reachable_score(pattern) > 0, pattern


def test_disabling_a_capability_lowers_what_its_pattern_can_reach(profile):
    profile(session_telemetry="on")
    with_session = CAP.reachable_score("APP")
    profile(session_telemetry="off")
    assert CAP.reachable_score("APP") < with_session


def test_signature_rules_all_exist_in_the_weight_table():
    """A renamed weight constant would silently contribute zero."""
    weights = CAP._rule_weights()
    for pattern, sig in CAP.PATTERN_SIGNATURES.items():
        for rule in sig:
            assert rule in weights, f"{pattern}: {rule} has no weight"


def test_signature_rules_are_all_known_to_the_registry():
    for pattern, sig in CAP.PATTERN_SIGNATURES.items():
        for rule in sig:
            assert rule in CAP.RULE_CAPABILITY, f"{pattern}: {rule} undeclared"


# --- scaling ----------------------------------------------------------------

def test_full_capability_leaves_the_calibrated_threshold_untouched(profile):
    """The hand-calibrated operating point must not move; only reduced
    deployments are meant to be affected."""
    profile(**{c.key: ("on" if "on" in c.modes else c.modes[0])
               for c in CAP.REGISTRY if not c.always_on})
    assert CAP.scaled_threshold(C.REVIEW_THRESHOLD) == pytest.approx(
        C.REVIEW_THRESHOLD, abs=1e-6)


def test_reduced_capability_lowers_the_threshold(profile):
    profile(session_telemetry="off", geo_telemetry="off", device_telemetry="off")
    assert CAP.scaled_threshold(C.REVIEW_THRESHOLD) < C.REVIEW_THRESHOLD


def test_threshold_never_goes_negative_or_above_the_base(profile):
    profile(**{c.key: "off" for c in CAP.REGISTRY if not c.always_on})
    t = CAP.scaled_threshold(C.REVIEW_THRESHOLD)
    assert 0.0 <= t <= C.REVIEW_THRESHOLD


def test_ordering_of_review_and_block_is_preserved(profile):
    profile(session_telemetry="off", geo_telemetry="off")
    assert (CAP.scaled_threshold(C.REVIEW_THRESHOLD)
            < CAP.scaled_threshold(C.BLOCK_THRESHOLD))


# --- the behaviour that motivated this --------------------------------------

def _ev(amount, payee, bank="BankA"):
    return {"amount_uzs": amount, "sender_pinfl": "S1", "receiver_pinfl": payee,
            "device_id": "dev-1", "sender_region": "Tashkent City",
            "sender_bank_name": bank, "receiver_bank_name": bank}


def test_single_rule_can_flag_when_it_is_all_that_is_available(profile):
    """The PaySim case. One rule at 0.35 is below the 0.40 fixed cutoff, but is
    the strongest signal a minimal deployment can produce, so it must act."""
    profile(receiver_age="off", myid_kinship="off", device_telemetry="off",
            geo_telemetry="off", session_telemetry="off", channel="off")

    st = R.SenderState()
    for i in range(6):
        R.evaluate(_ev(150_000, "friend"), None, st, now=1000 + i)
    res = R.evaluate(_ev(9_000_000, "fraudster"), None, st, now=5000)

    assert "NEW_PAYEE_HIGH_AMOUNT" in res["rule_hits"]
    assert res["cep_score"] == pytest.approx(C.W_NEW_PAYEE_HIGH, abs=1e-6)
    assert res["cep_score"] < C.REVIEW_THRESHOLD      # would be silent unscaled
    assert res["decision"] in ("REVIEW", "BLOCK")     # but is not


def test_scaling_can_be_switched_off(profile, monkeypatch):
    """The previous fixed-threshold behaviour stays available for comparison."""
    monkeypatch.setattr(C, "SCALE_THRESHOLDS_BY_CAPABILITY", False)
    profile(receiver_age="off", myid_kinship="off", device_telemetry="off",
            geo_telemetry="off", session_telemetry="off", channel="off")
    assert R._thresholds() == (C.REVIEW_THRESHOLD, C.BLOCK_THRESHOLD)


def test_scaling_does_not_change_the_full_profile_decision(profile):
    """Regression guard: the deployed configuration must behave as before."""
    profile(**{c.key: ("on" if "on" in c.modes else c.modes[0])
               for c in CAP.REGISTRY if not c.always_on})
    assert R._thresholds() == pytest.approx(
        (C.REVIEW_THRESHOLD, C.BLOCK_THRESHOLD), abs=1e-6)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
