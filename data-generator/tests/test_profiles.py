"""The dataset of record's configuration and what it adds to plain traffic."""

import pytest

import config as C
from generator import build_dataset


@pytest.fixture
def restore_profile():
    yield
    C.PROFILE = C.GeneratorConfig()


def test_the_config_is_the_dataset_of_record_and_refuses_unknown_knobs():
    cfg = C.GeneratorConfig()
    assert (cfg.n_persons, cfg.n_transactions, cfg.fraud_rate, cfg.seed) == (
        50_000, 500_000, 0.002, 42)
    with pytest.raises(TypeError):
        C.GeneratorConfig(no_such_knob=1)


def test_a_small_dataset_carries_what_the_config_adds(restore_profile):
    df, _ = build_dataset(C.GeneratorConfig(n_persons=800, n_transactions=8000,
                                            fraud_rate=0.02, seed=3))
    legit = df[df.label_is_fraud == 0]
    assert legit.device_id.str.endswith("-n").any()                # phones changed
    assert (legit.amount_uzs % 1_000 == 0).mean() > 0.85           # round sums
    # a tenth of fraud goes unreported, so fewer rows are labelled than injected
    assert 0.010 < df.label_is_fraud.mean() < 0.021


def test_roundness_does_not_mark_the_class(restore_profile):
    """A person sends round sums whether or not they are being defrauded. If the
    two classes rounded differently, a model could separate them on that alone."""
    df, _ = build_dataset(C.GeneratorConfig(n_persons=800, n_transactions=8000,
                                            fraud_rate=0.05, seed=3))
    rounded = df.amount_uzs % 1_000 == 0
    legit = rounded[df.label_is_fraud == 0].mean()
    fraud = rounded[df.label_is_fraud == 1].mean()
    assert abs(legit - fraud) < 0.10, (legit, fraud)
