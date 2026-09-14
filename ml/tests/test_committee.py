"""committee.merge: the one booster the job serves is the members' mean, and its
tree contributions still add up to its own score."""

import lightgbm as lgb
import numpy as np
import pytest

import committee


def _fits(k=3):
    rng = np.random.default_rng(0)
    X = rng.normal(size=(600, 5))
    y = (X[:, 0] + rng.normal(size=600) > 1).astype(int)
    boosters = [lgb.LGBMClassifier(n_estimators=20, num_leaves=7, colsample_bytree=0.8,
                                   random_state=s, verbose=-1).fit(X, y).booster_
                for s in range(k)]
    return X, boosters


def test_the_merged_raw_score_is_the_members_mean():
    X, boosters = _fits()
    merged = committee.merge(boosters)
    want = np.mean([b.predict(X, raw_score=True) for b in boosters], axis=0)
    assert np.max(np.abs(merged.predict(X, raw_score=True) - want)) < 1e-9
    assert merged.num_trees() == sum(b.num_trees() for b in boosters)


def test_the_merged_contributions_add_up_to_its_score():
    X, boosters = _fits()
    merged = committee.merge(boosters)
    contrib = merged.predict(X, pred_contrib=True)
    assert np.allclose(contrib.sum(axis=1), merged.predict(X, raw_score=True), atol=1e-9)


def test_check_refuses_a_model_that_is_not_the_mean():
    X, boosters = _fits()
    with pytest.raises(RuntimeError):
        committee.check(boosters[0], boosters, X)
