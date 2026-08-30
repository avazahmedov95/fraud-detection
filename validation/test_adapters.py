"""
Tests for the external-dataset adapters.

These run against small fixtures in the shape of the real files, so the harness
is known to work before anyone downloads 470 MB. They test the ADAPTER — the
mapping from a foreign schema onto this project's event contract — not the
detection result, which is the point of the actual run.

Run: python -m pytest test_adapters.py -q
"""

import os
import sys

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", "stream-processor"))

import paysim_adapter as PS      # noqa: E402
import capabilities as CAP       # noqa: E402


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
    """Amounts are rescaled so absolute-threshold rules are meaningful. Without
    it, PaySim amounts sit ~1000x below the UZS thresholds and no absolute rule
    could ever fire."""
    amounts = pd.Series([100.0, 200.0, 300.0])
    s = PS.scale_factor(amounts, our_median_uzs=2000.0)
    assert abs(float(amounts.median()) * s - 2000.0) < 1e-6


def test_scale_factor_survives_a_degenerate_median():
    assert PS.scale_factor(pd.Series([0.0, 0.0])) == 1.0


def test_events_carry_only_fields_paysim_actually_has(paysim_df):
    """A fabricated default would let a rule fire on data that does not exist."""
    ev = next(PS.to_events(paysim_df.head(1), 1.0))
    assert set(ev) == {"amount_uzs", "sender_pinfl", "receiver_pinfl",
                       "_ts", "_label", "_type"}
    for absent in ("device_id", "sender_region", "channel", "active_call",
                   "is_family_transfer"):
        assert absent not in ev


def test_step_becomes_an_hourly_timestamp(paysim_df):
    evs = list(PS.to_events(paysim_df.head(3), 1.0))
    assert all(e["_ts"] % 3600 == 0 for e in evs)


def test_rules_fire_on_foreign_data(paysim_df, tmp_path):
    """The substantive check: rules written against our generator produce hits on
    a dataset built by someone else, without retraining or tuning."""
    path = tmp_path / "paysim.csv"
    paysim_df.to_csv(path, index=False)

    saved = dict(CAP.MODES)
    try:
        for key in ("receiver_age", "myid_kinship", "device_telemetry",
                    "geo_telemetry", "session_telemetry", "channel"):
            CAP.MODES[key] = "off"
        res, hits = PS.run(str(path), ["TRANSFER"], None)
    finally:
        CAP.MODES.clear(); CAP.MODES.update(saved)

    assert len(res) == len(paysim_df)
    assert hits["fraud"], "no rule fired on fraud — the adapter is not wiring up"
    # Separation, not a specific rate: the fixture is not calibrated, so a
    # threshold here would be meaningless. What must hold is that fraud is
    # flagged more often than legitimate traffic.
    flagged = res.decision.isin(["REVIEW", "BLOCK"])
    fraud_rate = flagged[res.label == 1].mean()
    legit_rate = flagged[res.label == 0].mean()
    assert fraud_rate > legit_rate


def test_capabilities_without_data_are_off_in_the_run(paysim_df, tmp_path):
    """PaySim has no device, geo, session or channel. Any rule from those
    capabilities firing would mean the run invented data."""
    path = tmp_path / "paysim.csv"
    paysim_df.to_csv(path, index=False)

    saved = dict(CAP.MODES)
    try:
        for key in ("receiver_age", "myid_kinship", "device_telemetry",
                    "geo_telemetry", "session_telemetry", "channel"):
            CAP.MODES[key] = "off"
        _, hits = PS.run(str(path), ["TRANSFER"], None)
    finally:
        CAP.MODES.clear(); CAP.MODES.update(saved)

    forbidden = {"DEVICE_CHANGE", "GEO_ANOMALY", "IMPOSSIBLE_TRAVEL",
                 "COACHED_SESSION", "FRESH_RECEIVER"}
    fired = set(hits["fraud"]) | set(hits["legit"])
    assert not (fired & forbidden), f"fired without data: {fired & forbidden}"


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
