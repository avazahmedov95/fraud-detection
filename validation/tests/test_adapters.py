"""Tests for the external-dataset adapters, on small fixtures shaped like the
real files: the mapping onto this project's event contract, not the result."""

import os
import sys

import numpy as np
import pandas as pd
import pytest

# conftest.py puts the package on sys.path; this adds stream-processor's modules.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", "..", "stream-processor"))

import paysim_adapter as PS      # noqa: E402
import capabilities as CAP       # noqa: E402
import harness as RP             # noqa: E402


@pytest.fixture
def paysim_df():
    """A file in PaySim's schema: legitimate pairs plus fan-in to drop accounts."""
    rng = np.random.default_rng(0)
    rows = []
    for step in range(1, 80):
        for _ in range(4):
            rows.append(dict(
                step=step, type="TRANSFER",
                amount=float(np.exp(rng.normal(9.0, 0.6))),
                nameOrig=f"C{rng.integers(1, 60)}",
                nameDest=f"C{rng.integers(100, 140)}",
                isFraud=0))
    for i in range(20):
        step = int(rng.integers(1, 80))
        for _ in range(5):
            rows.append(dict(
                step=step, type="TRANSFER",
                amount=float(np.exp(rng.normal(12.5, 0.4))),
                nameOrig=f"C{rng.integers(1, 60)}",
                nameDest=f"C{900 + i % 6}",
                isFraud=1))
    return pd.DataFrame(rows)


def test_scale_factor_normalises_the_median():
    """Without rescaling, PaySim amounts sit ~1000x below the UZS thresholds."""
    amounts = pd.Series([100.0, 200.0, 300.0])
    s = PS.scale_factor(amounts, our_median_uzs=2000.0)
    assert abs(float(amounts.median()) * s - 2000.0) < 1e-6


def test_scale_factor_survives_a_degenerate_median():
    assert PS.scale_factor(pd.Series([0.0, 0.0])) == 1.0


def test_events_carry_only_fields_paysim_actually_has(paysim_df):
    """A fabricated default would let a rule fire on data that does not exist."""
    e = next(PS.to_events(paysim_df.head(1), 1.0))
    assert set(e.ev) == {"amount_uzs", "sender_pinfl", "receiver_pinfl"}
    for absent in ("device_id", "sender_region", "active_call",
                   "is_family_transfer"):
        assert absent not in e.ev


def test_step_becomes_an_hourly_timestamp(paysim_df):
    evs = list(PS.to_events(paysim_df.head(3), 1.0))
    assert all(e.ts % 3600 == 0 for e in evs)


def test_rules_fire_on_foreign_data(paysim_df, tmp_path):
    """Rules written against our generator must hit a foreign dataset untuned."""
    path = tmp_path / "paysim.csv"
    paysim_df.to_csv(path, index=False)

    saved = dict(CAP.MODES)
    try:
        for key in ("myid_kinship", "geo_telemetry", "session_telemetry"):
            CAP.MODES[key] = "off"
        CAP.MODES["payee_identity"] = "pinfl"
        res, hits = PS.run(str(path), ["TRANSFER"], None)
    finally:
        CAP.MODES.clear(); CAP.MODES.update(saved)

    assert len(res) == len(paysim_df)
    assert hits["fraud"], "no rule fired on fraud — the adapter is not wiring up"
    # Separation, not a rate: the fixture is not calibrated, so a threshold here
    # would be meaningless. Fraud must simply be flagged more often than legit.
    flagged = res.decision.isin(["REVIEW", "BLOCK"])
    fraud_rate = flagged[res.label == 1].mean()
    legit_rate = flagged[res.label == 0].mean()
    assert fraud_rate > legit_rate


def test_capabilities_without_data_are_off_in_the_run(paysim_df, tmp_path):
    """PaySim has no device, geo or session; a rule firing means invented data."""
    path = tmp_path / "paysim.csv"
    paysim_df.to_csv(path, index=False)

    saved = dict(CAP.MODES)
    try:
        for key in ("myid_kinship", "geo_telemetry", "session_telemetry"):
            CAP.MODES[key] = "off"
        CAP.MODES["payee_identity"] = "pinfl"
        _, hits = PS.run(str(path), ["TRANSFER"], None)
    finally:
        CAP.MODES.clear(); CAP.MODES.update(saved)

    forbidden = {"GEO_ANOMALY", "IMPOSSIBLE_TRAVEL",
                 "COACHED_SESSION"}
    fired = set(hits["fraud"]) | set(hits["legit"])
    assert not (fired & forbidden), f"fired without data: {fired & forbidden}"


def test_replay_refuses_a_stream_with_no_payee_key():
    """With no payee key every row shares one key: is_new_payee, DISTINCT_PAYEE_BURST
    and NEW_PAYEE_HIGH_AMOUNT would measure the adapter, so the replay refuses."""
    saved = dict(CAP.MODES)
    try:
        CAP.MODES["payee_identity"] = "card"      # the deployed default
        ev = RP.Event(ev={"amount_uzs": 1.0, "sender_pinfl": "A",
                          "receiver_pinfl": "B"}, ts=0, label=0)
        with pytest.raises(SystemExit):
            RP.replay([ev])
    finally:
        CAP.MODES.clear(); CAP.MODES.update(saved)


def test_the_shared_profile_sets_the_payee_key(capsys):
    """capability_profile is the one place that gets this right, so every adapter
    must go through it rather than setting modes itself."""
    saved = dict(CAP.MODES)
    try:
        RP.capability_profile("geo_telemetry")
        assert CAP.MODES["payee_identity"] == "pinfl"
        assert CAP.MODES["geo_telemetry"] == "off"
    finally:
        CAP.MODES.clear(); CAP.MODES.update(saved)


# --- the shared extractor ----------------------------------------------------------

def _foreign_profile():
    saved = dict(CAP.MODES)
    for key in ("myid_kinship", "geo_telemetry", "session_telemetry"):
        CAP.MODES[key] = "off"
    CAP.MODES["payee_identity"] = "pinfl"
    return saved


def test_available_features_drops_exactly_what_the_profile_switched_off():
    """A column computed from data the dataset lacks is a fabricated zero; this is
    the list that keeps it out of a model."""
    saved = _foreign_profile()
    try:
        idx, names = RP.available_features()
        for gone in ("geo_is_anomaly", "active_call", "secs_login_z"):
            assert gone not in names
        assert "rcv_distinct_senders_1h" in names and "vel_10m" in names
        assert len(idx) == len(names) == 13
    finally:
        CAP.MODES.clear(); CAP.MODES.update(saved)


def test_extract_features_accepts_what_paysim_to_events_yields(paysim_df):
    """The regression that motivated sharing the loop: PaySim's model mode unpacked
    dict events after `to_events` had started yielding `Event`s."""
    saved = _foreign_profile()
    try:
        X, y, _ = RP.extract_features(PS.to_events(paysim_df, 1.0),
                                      total=len(paysim_df))
        assert X.shape[0] == len(paysim_df)
        assert list(y) == list(paysim_df.isFraud)
    finally:
        CAP.MODES.clear(); CAP.MODES.update(saved)
