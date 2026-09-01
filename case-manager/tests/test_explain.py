"""Tests for the model's own reasons.

The risk here is not that an explanation is missing - it is that a WRONG one is
given confidently. A contribution labelled with the wrong feature, or computed
from a model other than the one that decided, reads as authoritative and is
worse than silence. Most of these tests are about refusing to speak.
"""

import pytest

import explain as E


class FakeBooster:
    """Contributions are [f0..fn, bias]; sum is the log-odds margin."""

    def __init__(self, contrib, n_features=3):
        self._c = contrib
        self._n = n_features

    def num_feature(self):
        return self._n

    def num_trees(self):
        return 1

    def predict(self, x, pred_contrib=False):
        import numpy as np
        assert pred_contrib
        return np.asarray([self._c])


def _explainer(contrib, names=("amount_z", "is_new_payee", "receiver_age")):
    ex = E.Explainer(feature_names=list(names))
    ex._booster = FakeBooster(contrib, n_features=len(names))
    ex._loaded = True
    return ex


def _proba(contrib):
    margin = sum(contrib)
    return 1.0 / (1.0 + pow(2.718281828, -margin))


# --- refusing to speak -------------------------------------------------------

def test_no_model_is_reported_not_hidden():
    ex = E.Explainer()
    ex._loaded = True
    ex._booster = None
    assert ex.explain([1.0], 0.9) == (E.NO_MODEL, [])


def test_missing_features_are_reported_even_with_no_model():
    """The two failures are fixed in different places - a missing artefact by
    rebuilding this service, a missing vector by redeploying the scoring job -
    so the more upstream one must not be hidden behind the other."""
    ex = E.Explainer()
    ex._loaded = True
    ex._booster = None
    assert ex.explain(None, 0.9) == (E.NO_FEATURES, [])


def test_missing_features_are_reported():
    """Alerts raised before the job published its feature vector cannot be
    explained, and must not be silently blank."""
    ex = _explainer([1.0, 0.0, 0.0, 0.0])
    assert ex.explain(None, 0.9)[0] == E.NO_FEATURES
    assert ex.explain([], 0.9)[0] == E.NO_FEATURES


def test_a_disagreeing_model_refuses_to_explain():
    """The guard that matters. If the explaining trees are not the deciding
    trees, a plausible story about the wrong model is worse than nothing."""
    contrib = [2.0, 1.0, 0.5, -0.25]
    ex = _explainer(contrib)
    status, lines = ex.explain([1.0, 1.0, 1.0], recorded_score=0.10)
    assert status == E.MODEL_MISMATCH
    assert lines == []


def test_an_agreeing_model_does_explain():
    contrib = [2.0, 1.0, 0.5, -0.25]
    ex = _explainer(contrib)
    status, lines = ex.explain([1.0, 1.0, 1.0], recorded_score=_proba(contrib))
    assert status == E.OK
    assert lines


def test_tolerance_is_tighter_than_a_real_divergence_and_looser_than_noise():
    """ONNX and the booster agree to 3.3e-07 over 50k events; the tolerance has
    to sit above that and well below anything a stale artefact would produce."""
    assert 1e-6 < E.TOLERANCE < 1e-2


def test_no_recorded_score_still_explains():
    """A record without ml_score is a CEP-only run. There is nothing to check
    against, so the check is skipped rather than failed - the contributions are
    still the model's own."""
    ex = _explainer([2.0, 1.0, 0.5, -0.25])
    assert ex.explain([1.0, 1.0, 1.0], recorded_score=None)[0] == E.OK


def test_mislabelling_is_refused(caplog):
    """Names and model out of step: better to say nothing than to attribute a
    contribution to the wrong feature."""
    ex = E.Explainer(feature_names=["only_one_name"])
    ex._booster = FakeBooster([1.0, 1.0, 1.0, 0.0], n_features=3)
    ex._loaded = False

    def _load():
        ex._loaded = True
        n = ex._booster.num_feature()
        if ex._names is None or len(ex._names) != n:
            ex._booster = None
    ex._load = _load
    assert ex.explain([1.0, 1.0, 1.0], 0.9)[0] == E.NO_MODEL


# --- what it says when it does speak -----------------------------------------

def test_only_upward_contributions_are_shown():
    """A negative contribution is a reason the model was LESS suspicious, which
    is not what an alert is about."""
    contrib = [3.0, -5.0, 1.0, 0.0]     # feature 1 argues strongly against
    ex = _explainer(contrib)
    _, lines = ex.explain([1.0, 1.0, 1.0], _proba(contrib))
    assert not any("payee never paid before" in ln for ln in lines)
    assert any("amount" in ln for ln in lines)


def test_strongest_contribution_comes_first():
    contrib = [1.0, 4.0, 2.0, 0.0]
    ex = _explainer(contrib)
    _, lines = ex.explain([1.0, 1.0, 1.0], _proba(contrib))
    assert lines[0].startswith("payee never paid before")


def test_at_most_top_n():
    contrib = [1.0, 2.0, 3.0, 0.0]
    ex = _explainer(contrib)
    _, lines = ex.explain([1.0, 1.0, 1.0], _proba(contrib))
    assert len(lines) <= E.TOP_N


# --- the phrasing ------------------------------------------------------------

def test_phrases_read_as_findings_not_column_names():
    """An analyst acts on 'payee's account is 2 days old', not on
    'receiver_age = 2.0'."""
    assert E.phrase("receiver_age", 2.0, 1.5) == \
        "payee's account age: 2 days (+1.50)"
    assert "yes" in E.phrase("active_call", 1.0, 0.4)


def test_unknown_age_is_not_rendered_as_a_number():
    """NaN means the bank could not see the age; -1 is the off-mode sentinel.
    Printing either as a quantity would invent a fact."""
    assert "unknown" in E.phrase("receiver_age", float("nan"), 1.0)
    assert "unknown" in E.phrase("receiver_age", -1.0, 1.0)


def test_an_unmapped_feature_still_renders():
    """Ugly is fine; crashing the queue over a new feature name is not."""
    out = E.phrase("some_new_feature", 3.5, 0.2)
    assert "some_new_feature" in out and "3.5" in out


def test_a_broken_formatter_does_not_take_down_the_queue():
    assert E.phrase("hour", float("inf"), 1.0)
