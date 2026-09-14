"""The realistic profile, and the promise that the baseline profile is untouched."""

import pytest

import config as C
from generator import build_dataset


@pytest.fixture
def restore_profile():
    yield
    C.PROFILE = C.GeneratorConfig()


def test_the_baseline_config_is_the_documented_one():
    cfg = C.GeneratorConfig()
    assert cfg.profile == "baseline"
    assert (cfg.active_call_base_rate, cfg.active_call_app_rate) == (
        C.ACTIVE_CALL_BASE_RATE, C.ACTIVE_CALL_APP_RATE)
    assert cfg.mule_recruited_share == C.MULE_RECRUITED_SHARE
    assert (cfg.collector_share, cfg.split_payment_share, cfg.phone_change_share,
            cfg.round_amount_share, cfg.unreported_fraud_share) == (0, 0, 0, 0, 0)


def test_realistic_takes_overrides_and_refuses_unknown_ones():
    cfg = C.realistic(n_transactions=1000)
    assert (cfg.profile, cfg.n_transactions, cfg.fraud_rate) == ("realistic", 1000, 0.002)
    with pytest.raises(TypeError):
        C.realistic(no_such_knob=1)


def test_a_small_realistic_dataset_carries_what_the_profile_adds(restore_profile):
    df, _ = build_dataset(C.realistic(n_persons=800, n_transactions=8000,
                                      fraud_rate=0.02, seed=3))
    legit = df[df.label_is_fraud == 0]
    assert legit.device_id.str.endswith("-n").any()                # phones changed
    assert (legit.amount_uzs % 10_000 == 0).mean() > 0.2           # round sums
    # a tenth of fraud goes unreported, so fewer rows are labelled than injected
    assert 0.010 < df.label_is_fraud.mean() < 0.021
