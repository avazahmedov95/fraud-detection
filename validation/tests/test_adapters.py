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

# The package itself is put on sys.path by tests/conftest.py; this adds the
# stream-processor modules the adapters replay through.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", "..", "stream-processor"))

import paysim_adapter as PS      # noqa: E402
import amlsim_adapter as AS      # noqa: E402
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



# ---------------------------------------------------------------------------
# AMLSim. Fixtures in the shape of AMLSim's three output files, so the harness
# is known to work before anyone builds a Java simulator. AMLSim supports one
# capability PaySim does not — receiver_age, from accounts.open_dt — so the
# "capabilities without data" test differs from PaySim's by exactly that entry.
# ---------------------------------------------------------------------------

@pytest.fixture
def amlsim_dir(tmp_path):
    """accounts.csv + transactions.csv + alert_transactions.csv, with fan_in and
    fan_out labelled separately — the reason this dataset was chosen."""
    rng = np.random.default_rng(0)
    base = pd.Timestamp("2017-01-01")

    accts = []
    for i in range(1, 181):
        fresh = rng.random() < 0.25
        off = int(rng.integers(1, 25)) if fresh else int(rng.integers(1, 900))
        accts.append(dict(acct_id=f"A{i}", dsply_nm=f"n{i}", type="I",
                          acct_stat="A",
                          open_dt=(base - pd.Timedelta(days=off)).date(),
                          initial_deposit=50000, tx_behavior_id=1, bank_id=0))

    tx, al, tid = [], [], [0]

    def add(step, orig, bene, amt, sar, aid="", atype=""):
        tid[0] += 1
        row = dict(tran_id=f"T{tid[0]}",
                   tran_timestamp=(base + pd.Timedelta(days=int(step))).date(),
                   base_amt=round(float(amt), 2), tx_type="TRANSFER",
                   orig_acct=orig, bene_acct=bene, is_sar=bool(sar), alert_id=aid)
        tx.append(row)
        if aid:
            al.append(dict(alert_id=aid, alert_type=atype, is_sar=True,
                           tran_id=row["tran_id"], orig_acct=orig, bene_acct=bene,
                           tx_type="TRANSFER", base_amt=row["base_amt"],
                           tran_timestamp=row["tran_timestamp"]))

    for step in range(1, 120):
        for _ in range(6):
            add(step, f"A{rng.integers(1, 90)}", f"A{rng.integers(90, 180)}",
                np.exp(rng.normal(5.5, 0.6)), 0)
    for k in range(12):                       # collection stage: 6 senders -> 1 drop
        drop = f"A{170 + k % 10}"
        for _ in range(6):
            add(rng.integers(1, 119), f"A{rng.integers(1, 90)}", drop,
                np.exp(rng.normal(7.4, 0.3)), 1, f"FI{k}", "fan_in")
    for k in range(12):                       # dispersal stage: 1 mule -> 6 dests
        mule, st = f"A{rng.integers(1, 90)}", int(rng.integers(1, 119))
        for _ in range(6):
            add(st, mule, f"A{90 + rng.integers(0, 80)}",
                np.exp(rng.normal(7.4, 0.3)), 1, f"FO{k}", "fan_out")

    pd.DataFrame(accts).to_csv(tmp_path / "accounts.csv", index=False)
    pd.DataFrame(tx).to_csv(tmp_path / "transactions.csv", index=False)
    pd.DataFrame(al).to_csv(tmp_path / "alert_transactions.csv", index=False)
    return str(tmp_path)


def test_amlsim_missing_directory_is_refused(tmp_path):
    """A silently empty run would look like 'the rules found nothing'."""
    with pytest.raises(SystemExit):
        AS.load(str(tmp_path / "nope"))


def test_amlsim_daily_timestamps_become_epoch_seconds(amlsim_dir):
    tx, _, _ = AS.load(amlsim_dir)
    ts = AS._epoch(tx["tran_timestamp"])
    assert (ts % 86400 == 0).all(), "AMLSim steps are whole days"
    assert ts.is_monotonic_increasing or ts.min() > 0


def test_amlsim_receiver_age_is_read_from_accounts(amlsim_dir):
    """The capability PaySim could not support. If open_dt were ignored,
    receiver_age would be None everywhere and FRESH_RECEIVER could never fire."""
    saved = dict(CAP.MODES)
    try:
        for key in ("myid_kinship", "device_telemetry", "geo_telemetry",
                    "session_telemetry", "channel"):
            CAP.MODES[key] = "off"
        _, hits = AS.run(amlsim_dir, None)
    finally:
        CAP.MODES.clear(); CAP.MODES.update(saved)
    fired = set(hits["fraud"]) | set(hits["legit"])
    assert "FRESH_RECEIVER" in fired


def test_amlsim_typology_labels_survive_to_the_result(amlsim_dir):
    """Section B of the report exists only if alert_type reaches the rows. The
    fan_in/fan_out split is the whole reason for using this dataset."""
    saved = dict(CAP.MODES)
    try:
        for key in ("myid_kinship", "device_telemetry", "geo_telemetry",
                    "session_telemetry", "channel"):
            CAP.MODES[key] = "off"
        res, _ = AS.run(amlsim_dir, None)
    finally:
        CAP.MODES.clear(); CAP.MODES.update(saved)
    assert {"fan_in", "fan_out"} <= set(res.typology.unique())
    assert (res[res.typology == "fan_in"].label == 1).all()


def test_amlsim_capabilities_without_data_are_off(amlsim_dir):
    """AMLSim has no device, geo, session or channel. FRESH_RECEIVER is NOT in
    the forbidden set here — unlike the PaySim test — because open_dt is real."""
    saved = dict(CAP.MODES)
    try:
        for key in ("myid_kinship", "device_telemetry", "geo_telemetry",
                    "session_telemetry", "channel"):
            CAP.MODES[key] = "off"
        _, hits = AS.run(amlsim_dir, None)
    finally:
        CAP.MODES.clear(); CAP.MODES.update(saved)
    forbidden = {"DEVICE_CHANGE", "GEO_ANOMALY", "IMPOSSIBLE_TRAVEL",
                 "COACHED_SESSION"}
    fired = set(hits["fraud"]) | set(hits["legit"])
    assert not (fired & forbidden), f"fired without data: {fired & forbidden}"

if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
