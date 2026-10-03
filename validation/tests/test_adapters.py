"""Tests for the public-dataset adapters, on small fixtures shaped like the real
files: the mapping onto this project's event contract and the scoring, not the
result."""

import numpy as np
import pandas as pd
import pytest

import harness as RP
import ibm_aml_adapter as IB
import paysim_adapter as PS

CAP = RP.CAP


@pytest.fixture
def foreign_profile():
    saved = dict(CAP.MODES)
    RP.capability_profile("geo_telemetry", "session_telemetry")
    yield
    CAP.MODES.clear()
    CAP.MODES.update(saved)


@pytest.fixture
def paysim_df():
    """A file in PaySim's schema: legitimate pairs plus fan-in to drop accounts."""
    rng = np.random.default_rng(0)
    rows = [dict(step=step, type="TRANSFER", amount=float(np.exp(rng.normal(9.0, 0.6))),
                 nameOrig=f"C{rng.integers(1, 60)}", nameDest=f"C{rng.integers(100, 140)}",
                 isFraud=0)
            for step in range(1, 80) for _ in range(4)]
    rows += [dict(step=int(step), type="TRANSFER", amount=float(np.exp(rng.normal(12.5, 0.4))),
                  nameOrig=f"C{rng.integers(1, 60)}", nameDest=f"C{900 + i % 6}", isFraud=1)
             for i, step in enumerate(rng.integers(1, 80, 20)) for _ in range(5)]
    return pd.DataFrame(rows).sort_values("step", kind="stable").reset_index(drop=True)


def test_scale_factor_normalises_the_median():
    amounts = pd.Series([100.0, 200.0, 300.0])
    assert abs(200.0 * RP.scale_factor(amounts, our_median_uzs=2000.0) - 2000.0) < 1e-6
    assert RP.scale_factor(pd.Series([0.0, 0.0])) == 1.0


def test_paysim_events_carry_only_what_paysim_has_on_an_hourly_clock(paysim_df):
    """A fabricated default would let a feature read data that does not exist."""
    evs = list(PS.to_events(paysim_df.head(3), 1.0))
    assert set(evs[0].ev) == {"amount_uzs", "sender_pinfl", "receiver_pinfl"}
    assert all(e.ts % 3600 == 0 for e in evs)


def test_the_profile_drops_exactly_what_the_dataset_cannot_supply(foreign_profile):
    idx, names = RP.available_features()
    for gone in ("geo_is_anomaly", "active_call", "secs_login_z", "payee_flagged"):
        assert gone not in names
    assert "rcv_distinct_senders_1h" in names and "payee_payers_7d" in names
    assert len(idx) == len(names) == 18
    assert CAP.MODES["payee_identity"] == "pinfl"


def test_a_stream_with_no_payee_key_is_refused(paysim_df):
    """Keyed by card, every account payee of a foreign file would share one key."""
    saved = dict(CAP.MODES)
    try:
        CAP.MODES["payee_identity"] = "card"
        with pytest.raises(SystemExit):
            RP.extract_features(PS.to_events(paysim_df, 1.0), total=len(paysim_df))
    finally:
        CAP.MODES.clear()
        CAP.MODES.update(saved)


def test_extraction_and_scoring_run_end_to_end(paysim_df, foreign_profile, tmp_path):
    X, y, ts = RP.extract_features(PS.to_events(paysim_df, 1.0), total=len(paysim_df))
    assert X.shape[0] == len(y) == len(paysim_df) and list(y) == list(paysim_df.isFraud)
    assert (np.diff(ts) >= 0).all()
    cache = str(tmp_path / "m.npz")
    Xk, yk = RP.cached_matrix(cache, lambda: (X, y))
    assert Xk.shape[1] == 18
    Xc, _ = RP.cached_matrix(cache, lambda: pytest.fail("the cache was not read"))
    assert np.array_equal(Xk, Xc)
    n = len(yk)
    s = RP.fit_and_score(Xk, yk, fit=int(n * 0.6), cut=int(n * 0.8))
    assert s["frauds"] == int(yk[int(n * 0.8):].sum())
    assert 0 <= s["recall"] <= 1 and 0 <= s["precision"] <= 1 and 0 <= s["f1"] <= 1


def test_a_cache_of_other_columns_is_refused(tmp_path, foreign_profile):
    cache = str(tmp_path / "old.npz")
    np.savez(cache, X=np.zeros((2, 2)), y=np.zeros(2), names=np.array(["a", "b"]))
    with pytest.raises(SystemExit):
        RP.cached_matrix(cache, lambda: None)


IBM_HEADER = ["Timestamp", "From Bank", "Account", "To Bank", "Account",
              "Amount Received", "Receiving Currency", "Amount Paid",
              "Payment Currency", "Payment Format", "Is Laundering"]


def _ibm_row(ts, src, dst, amount, currency="US Dollar", fmt="ACH", label=0):
    return [ts, "010", src, "020", dst, amount, currency, amount, currency, fmt, label]


@pytest.fixture
def ibm_file(tmp_path):
    """Six senders converging on one receiver inside a minute, background traffic,
    a self-transfer and a second currency."""
    rows = [_ibm_row("2022/09/01 00:0%d" % i, f"S{i}", "DROP", 900.0, label=1)
            for i in range(6)]
    rows += [_ibm_row("2022/09/01 01:%02d" % i, f"L{i}", f"R{i}", 100.0) for i in range(10)]
    rows.append(_ibm_row("2022/09/01 02:00", "SELF", "SELF", 5000.0, fmt="Reinvestment"))
    rows += [_ibm_row("2022/09/01 03:%02d" % i, f"Y{i}", f"Z{i}", 120000.0, currency="Yen")
             for i in range(4)]
    path = tmp_path / "HI-Small_Trans.csv"
    with open(path, "w", encoding="utf-8", newline="") as fh:
        fh.write(",".join(IBM_HEADER) + "\n")
        for r in rows:
            fh.write(",".join(str(v) for v in r) + "\n")
    return str(path)


def test_ibm_load_drops_self_transfers_and_keeps_time_order(ibm_file):
    d, n_self = IB.load(ibm_file)
    assert n_self == 1 and not (d.sender == d.receiver).any()
    assert d.ts.is_monotonic_increasing


def test_ibm_minutes_survive_and_each_currency_has_its_own_scale(ibm_file):
    """One global factor would put a yen and a dollar amount on opposite sides of
    a threshold for no reason but their denomination."""
    d, _ = IB.load(ibm_file)
    evs = list(IB.to_events(d))
    assert any(e.ts % 3600 != 0 for e in evs)
    assert all(RP.OUR_MEDIAN_UZS / 20 < e.ev["amount_uzs"] < RP.OUR_MEDIAN_UZS * 20
               for e in evs)


def test_ibm_a_file_of_the_wrong_shape_is_refused(tmp_path):
    path = tmp_path / "wrong.csv"
    pd.DataFrame({"step": [1], "amount": [1.0]}).to_csv(path, index=False)
    with pytest.raises(SystemExit):
        IB.load(str(path))
