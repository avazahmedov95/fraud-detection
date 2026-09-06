"""Unit tests for fusion.py."""

import pytest

import capabilities as CAP
import config as C
import fusion
from fusion import final_score, decide, classify_type


def test_final_score_is_model_with_cep_fallback():
    # model present -> graded risk is the model probability (not blended)
    assert final_score(0.9, 0.2) == 0.2
    assert final_score(0.0, 0.7) == 0.7
    # model down -> fall back to the CEP score
    assert final_score(0.55, None) == 0.55
    assert final_score(0.0, 1.4) == 1.0
    assert final_score(-0.3, None) == 0.0


def test_decision_thresholds():
    assert decide(0.90, []) == "BLOCK"
    assert decide(0.50, []) == "REVIEW"
    assert decide(0.10, []) == "ALLOW"


def test_mandatory_floor_forces_review():
    # compliance must-flags escalate even when the model score is low
    assert decide(0.10, ["STRUCTURING"]) == "REVIEW"
    assert decide(0.10, ["DAILY_LIMIT_BREACH"]) == "REVIEW"
    # a non-mandatory rule does NOT force escalation on its own
    assert decide(0.10, ["GEO_ANOMALY"]) == "ALLOW"


# --- the fallback path scales with capability, the fused path does not -------
#
# `scaled_threshold` used to be reachable only through rules._thresholds(), whose
# verdict fraud_job discards - so the fix for the PaySim silent-layer finding ran
# in the offline adapters and nowhere in the deployed job. These pin both halves:
# the fallback obeys the scaling, and the validated fused operating point does not
# move.

@pytest.fixture
def profile():
    """Set capability modes and clear the cutoff cache around each test."""
    saved = dict(CAP.MODES)
    fusion._CUTOFF_CACHE.clear()
    yield lambda **kw: (CAP.MODES.update(kw), fusion._CUTOFF_CACHE.clear())
    CAP.MODES.clear(); CAP.MODES.update(saved)
    fusion._CUTOFF_CACHE.clear()


def _full(profile):
    profile(**{c.key: ("on" if "on" in c.modes else c.modes[0])
               for c in CAP.REGISTRY if not c.always_on})


def _reduced(profile):
    """Own stream plus the fan-in window: the profile a single bank really has."""
    profile(receiver_age="off", myid_kinship="off", device_telemetry="off",
            geo_telemetry="off", session_telemetry="off", channel="off")


def test_full_capability_does_not_move_either_operating_point(profile):
    _full(profile)
    assert fusion.cutoffs(cep_only=False) == (
        C.FINAL_REVIEW_THRESHOLD, C.FINAL_BLOCK_THRESHOLD)
    assert fusion.cutoffs(cep_only=True) == pytest.approx(
        (C.FINAL_REVIEW_THRESHOLD, C.FINAL_BLOCK_THRESHOLD), abs=1e-6)


def test_a_probability_is_never_rescaled(profile):
    """Retraining recalibrates the model, so the fused cutoff must stay put."""
    _reduced(profile)
    assert fusion.cutoffs(cep_only=False) == (
        C.FINAL_REVIEW_THRESHOLD, C.FINAL_BLOCK_THRESHOLD)


def test_reduced_capability_lowers_the_fallback_cutoff(profile):
    _reduced(profile)
    review_at, block_at = fusion.cutoffs(cep_only=True)
    assert review_at < C.FINAL_REVIEW_THRESHOLD
    assert block_at < C.FINAL_BLOCK_THRESHOLD
    assert review_at < block_at              # ordering survives the rescale


def test_the_silent_layer_is_what_this_prevents(profile):
    """The PaySim shape: one rule at 0.35 against a 0.40 cutoff flags nothing."""
    _reduced(profile)
    lone_rule_score = C.W_NEW_PAYEE_HIGH          # 0.35
    assert lone_rule_score < C.FINAL_REVIEW_THRESHOLD
    # Fused, that is correctly ALLOW - the model was asked and said no.
    assert decide(lone_rule_score, ["NEW_PAYEE_HIGH_AMOUNT"]) == "ALLOW"
    # On the fallback there IS no model, and going silent is the failure.
    assert decide(lone_rule_score, ["NEW_PAYEE_HIGH_AMOUNT"],
                  cep_only=True) in ("REVIEW", "BLOCK")


def test_scaling_can_be_switched_off(profile, monkeypatch):
    """The previous fixed-cutoff behaviour stays available for comparison."""
    monkeypatch.setattr(C, "SCALE_THRESHOLDS_BY_CAPABILITY", False)
    _reduced(profile)
    assert fusion.cutoffs(cep_only=True) == (
        C.FINAL_REVIEW_THRESHOLD, C.FINAL_BLOCK_THRESHOLD)


def test_default_is_the_fused_path(profile):
    """A caller that forgets the flag gets today's behaviour, not a rescale."""
    _reduced(profile)
    assert decide(0.10, []) == decide(0.10, [], cep_only=False)


def test_type_priority():
    assert classify_type(["STRUCTURING", "VELOCITY"]) == "STRUCTURING"
    assert classify_type(["DEVICE_CHANGE"]) == "ATO"
    assert classify_type(["VELOCITY", "DISTINCT_PAYEE_BURST"]) == "MULE"
    assert classify_type(["NEW_PAYEE_HIGH_AMOUNT"]) == "APP"
    assert classify_type([]) is None


# --- the deployed call site, which no other test can reach -------------------
#
# fraud_job.py cannot be imported here: it needs PyFlink, which is not installed
# on the host. So these read its SOURCE. That is a weak form of test and it is
# used deliberately, because the alternative is no coverage at all of the one
# call that matters - `decide`'s cep_only default is safe, so a caller that stops
# passing it goes back to the pre-fix behaviour with nothing failing, and at full
# capability the two behaviours are identical (docs/irp-framing.md 8, fifteenth).

def _fraud_job_ast():
    import ast
    import pathlib
    src = pathlib.Path(__file__).resolve().parent.parent / "fraud_job.py"
    return ast.parse(src.read_text(encoding="utf-8")), src


def _calls_to(tree, attr):
    import ast
    return [n for n in ast.walk(tree)
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
            and n.func.attr == attr
            and isinstance(n.func.value, ast.Name) and n.func.value.id == "fusion"]


def test_the_job_decides_through_score_and_decide():
    """The combined form is the one that cannot mis-derive cep_only."""
    tree, src = _fraud_job_ast()
    assert _calls_to(tree, "score_and_decide"), (
        f"{src.name} no longer calls fusion.score_and_decide")


def test_the_job_does_not_split_the_two_steps_again():
    """Splitting them puts the cep_only derivation back at the call site, where
    dropping it is silent. If this ever needs to be split, the replacement must
    pass cep_only explicitly - and this test should be replaced, not deleted."""
    tree, src = _fraud_job_ast()
    for attr in ("decide", "final_score"):
        assert not _calls_to(tree, attr), (
            f"{src.name} calls fusion.{attr} directly again; the cep_only "
            f"derivation has moved back to the call site")


def test_score_and_decide_matches_the_split_form():
    """The combined form must be exactly the two steps, or the tests above guard
    the wrong thing."""
    for cep, ml in ((0.35, None), (0.35, 0.93), (0.9, 0.01), (0.0, None)):
        final, dec = fusion.score_and_decide(cep, ml, ["NEW_PAYEE_HIGH_AMOUNT"])
        assert final == final_score(cep, ml)
        assert dec == decide(final, ["NEW_PAYEE_HIGH_AMOUNT"],
                             cep_only=ml is None)


def test_no_model_reaches_the_scaled_cutoffs(profile):
    """End to end through the combined form: a reduced deployment with no model
    must not go silent - the failure the fifteenth entry is about."""
    _reduced(profile)
    lone = C.W_NEW_PAYEE_HIGH                      # 0.35, under the 0.40 cutoff
    final, dec = fusion.score_and_decide(lone, None, ["NEW_PAYEE_HIGH_AMOUNT"])
    assert final == lone
    assert dec in ("REVIEW", "BLOCK")
    # and with a model present the same score is correctly left alone
    _, fused = fusion.score_and_decide(lone, 0.05, ["NEW_PAYEE_HIGH_AMOUNT"])
    assert fused == "ALLOW"
