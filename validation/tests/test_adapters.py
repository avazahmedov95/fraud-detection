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
import amlsim_adapter as AS      # noqa: E402
import ibm_aml_adapter as IB    # noqa: E402
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
        for key in ("receiver_age", "myid_kinship", "device_telemetry",
                    "geo_telemetry", "session_telemetry"):
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
        for key in ("receiver_age", "myid_kinship", "device_telemetry",
                    "geo_telemetry", "session_telemetry"):
            CAP.MODES[key] = "off"
        CAP.MODES["payee_identity"] = "pinfl"
        _, hits = PS.run(str(path), ["TRANSFER"], None)
    finally:
        CAP.MODES.clear(); CAP.MODES.update(saved)

    forbidden = {"DEVICE_CHANGE", "GEO_ANOMALY", "IMPOSSIBLE_TRAVEL",
                 "COACHED_SESSION", "FRESH_RECEIVER"}
    fired = set(hits["fraud"]) | set(hits["legit"])
    assert not (fired & forbidden), f"fired without data: {fired & forbidden}"



# --- AMLSim ----------------------------------------------------------------
# Fixtures shaped like its output files; AMLSim also supplies receiver_age.

@pytest.fixture
def amlsim_dir(tmp_path):
    """AMLSim's three files, with fan_in and fan_out labelled separately."""
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
    """If open_dt were ignored, receiver_age would be None and FRESH_RECEIVER never fire."""
    saved = dict(CAP.MODES)
    try:
        for key in ("myid_kinship", "device_telemetry", "geo_telemetry",
                    "session_telemetry"):
            CAP.MODES[key] = "off"
        CAP.MODES["payee_identity"] = "pinfl"
        _, hits = AS.run(amlsim_dir, None)
    finally:
        CAP.MODES.clear(); CAP.MODES.update(saved)
    fired = set(hits["fraud"]) | set(hits["legit"])
    assert "FRESH_RECEIVER" in fired


def test_amlsim_typology_labels_survive_to_the_result(amlsim_dir):
    """Section B needs alert_type on the rows; the fan_in/fan_out split is why this dataset."""
    saved = dict(CAP.MODES)
    try:
        for key in ("myid_kinship", "device_telemetry", "geo_telemetry",
                    "session_telemetry"):
            CAP.MODES[key] = "off"
        CAP.MODES["payee_identity"] = "pinfl"
        res, _ = AS.run(amlsim_dir, None)
    finally:
        CAP.MODES.clear(); CAP.MODES.update(saved)
    assert {"fan_in", "fan_out"} <= set(res.typology.unique())
    assert (res[res.typology == "fan_in"].label == 1).all()


def test_amlsim_capabilities_without_data_are_off(amlsim_dir):
    """FRESH_RECEIVER is NOT forbidden here, unlike the PaySim test: open_dt is real."""
    saved = dict(CAP.MODES)
    try:
        for key in ("myid_kinship", "device_telemetry", "geo_telemetry",
                    "session_telemetry"):
            CAP.MODES[key] = "off"
        CAP.MODES["payee_identity"] = "pinfl"
        _, hits = AS.run(amlsim_dir, None)
    finally:
        CAP.MODES.clear(); CAP.MODES.update(saved)
    forbidden = {"DEVICE_CHANGE", "GEO_ANOMALY", "IMPOSSIBLE_TRAVEL",
                 "COACHED_SESSION"}
    fired = set(hits["fraud"]) | set(hits["legit"])
    assert not (fired & forbidden), f"fired without data: {fired & forbidden}"


# --- IBM AML ---------------------------------------------------------------
# The translation, not the result: dropped rows, currencies, minute timestamps.

IBM_HEADER = ["Timestamp", "From Bank", "Account", "To Bank", "Account",
              "Amount Received", "Receiving Currency", "Amount Paid",
              "Payment Currency", "Payment Format", "Is Laundering"]


def _ibm_row(ts, src, dst, amount, currency="US Dollar", fmt="ACH", label=0):
    return [ts, "010", src, "020", dst, amount, currency, amount, currency,
            fmt, label]


@pytest.fixture
def ibm_file(tmp_path):
    """Six senders converging on one receiver inside a minute, plus background."""
    rows = [_ibm_row("2022/09/01 00:0%d" % i, f"S{i}", "DROP", 900.0, label=1)
            for i in range(6)]
    rows += [_ibm_row("2022/09/01 01:%02d" % i, f"L{i}", f"R{i}", 100.0)
             for i in range(10)]
    # a self-transfer, which must not reach the replay
    rows.append(_ibm_row("2022/09/01 02:00", "SELF", "SELF", 5000.0,
                         fmt="Reinvestment"))
    # a second currency, three orders of magnitude away
    rows += [_ibm_row("2022/09/01 03:%02d" % i, f"Y{i}", f"Z{i}", 120000.0,
                      currency="Yen") for i in range(4)]
    path = tmp_path / "HI-Small_Trans.csv"
    with open(path, "w", encoding="utf-8", newline="") as fh:
        fh.write(",".join(IBM_HEADER) + "\n")
        for r in rows:
            fh.write(",".join(str(v) for v in r) + "\n")
    return str(path)


def test_ibm_self_transfers_are_dropped_and_counted(ibm_file):
    """12% of the real file is an account paying itself. Left in, every one of them
    becomes a fan-in edge from an account to itself."""
    d, n_all, n_self = IB.load(ibm_file)
    assert n_self == 1
    assert len(d) == n_all - 1
    assert not (d.sender == d.receiver).any()


def test_ibm_rows_arrive_in_time_order(ibm_file):
    d, _, _ = IB.load(ibm_file)
    assert d.ts.is_monotonic_increasing


def test_ibm_minute_timestamps_are_not_rounded_to_the_hour(ibm_file):
    """The reason this dataset was fetched. PaySim's clock is hourly and AMLSim's is
    daily, so neither can exercise a sub-hour window at all; if this collapsed to
    the hour the run would answer nothing the other two did not."""
    evs = list(IB.to_events(*_loaded(ibm_file)))
    assert any(e.ts % 3600 != 0 for e in evs)


def _loaded(path):
    d, _, _ = IB.load(path)
    return d, IB._scales(d)


def test_ibm_each_currency_is_scaled_by_its_own_median(ibm_file):
    """One global factor would put a yen amount and a dollar amount on opposite
    sides of the structuring threshold for no reason but their denomination."""
    d, scales = _loaded(ibm_file)
    assert set(scales) == {"US Dollar", "Yen"}
    assert scales["US Dollar"] != scales["Yen"]
    amounts = [e.ev["amount_uzs"] for e in IB.to_events(d, scales)]
    # every converted amount lands within an order of magnitude of our own median
    assert all(RP.OUR_MEDIAN_UZS / 20 < a < RP.OUR_MEDIAN_UZS * 20 for a in amounts)


def test_ibm_a_file_of_the_wrong_shape_is_refused(tmp_path):
    """A schema mismatch must not read as 'the rules found nothing'."""
    path = tmp_path / "wrong.csv"
    pd.DataFrame({"step": [1], "amount": [1.0]}).to_csv(path, index=False)
    with pytest.raises(SystemExit):
        IB.load(str(path))


def test_ibm_fan_in_is_visible_to_the_rules(ibm_file):
    """Six senders onto one receiver inside a minute is what MULE_FAN_IN is for, and
    the fixture exists to prove the wiring reaches it - not to claim a result."""
    saved = dict(CAP.MODES)
    try:
        for key in ("receiver_age", "myid_kinship", "device_telemetry",
                    "geo_telemetry", "session_telemetry"):
            CAP.MODES[key] = "off"
        CAP.MODES["payee_identity"] = "pinfl"
        d, scales = _loaded(ibm_file)
        _, hits = RP.replay(IB.to_events(d, scales))
    finally:
        CAP.MODES.clear(); CAP.MODES.update(saved)
    assert "MULE_FAN_IN" in (set(hits["fraud"]) | set(hits["legit"]))


def test_ibm_capabilities_without_data_are_off(ibm_file):
    """The released files carry no device, geo, session or account-opening date."""
    saved = dict(CAP.MODES)
    try:
        for key in ("receiver_age", "myid_kinship", "device_telemetry",
                    "geo_telemetry", "session_telemetry"):
            CAP.MODES[key] = "off"
        CAP.MODES["payee_identity"] = "pinfl"
        d, scales = _loaded(ibm_file)
        _, hits = RP.replay(IB.to_events(d, scales))
    finally:
        CAP.MODES.clear(); CAP.MODES.update(saved)
    forbidden = {"DEVICE_CHANGE", "GEO_ANOMALY", "IMPOSSIBLE_TRAVEL",
                 "COACHED_SESSION", "FRESH_RECEIVER"}
    fired = set(hits["fraud"]) | set(hits["legit"])
    assert not (fired & forbidden), f"fired without data: {fired & forbidden}"


def test_ibm_patterns_sidecar_labels_the_edges(tmp_path):
    """Typologies live in a separate file the Hugging Face mirror does not carry.
    These rows are from the real HI-Small file: a suffix on some markers, and one
    account pair in two attempts of different types."""
    import pandas as pd
    p = tmp_path / "HI-Small_Patterns.txt"
    p.write_text(
        "BEGIN LAUNDERING ATTEMPT - FAN-OUT:  Max 16-degree Fan-Out\n"
        "2022/09/01 00:06,021174,800737690,012,80011F990,2848.96,Euro,2848.96,Euro,ACH,1\n"
        "END LAUNDERING ATTEMPT - FAN-OUT\n\n"
        "BEGIN LAUNDERING ATTEMPT - STACK\n"
        "2022/09/02 17:28,023691,8021353D0,015231,80266F880,1413.09,Euro,1413.09,Euro,ACH,1\n"
        "END LAUNDERING ATTEMPT - STACK\n\n"
        "BEGIN LAUNDERING ATTEMPT - BIPARTITE\n"
        "2022/09/05 08:25,023691,8021353D0,015231,80266F880,946.84,Euro,946.84,Euro,ACH,1\n"
        "END LAUNDERING ATTEMPT - BIPARTITE\n", encoding="utf-8")
    typ = IB.read_patterns(str(p))
    assert typ == {("2022/09/01 00:06", "800737690", "80011F990"): "fan-out",
                   ("2022/09/02 17:28", "8021353D0", "80266F880"): "stack",
                   ("2022/09/05 08:25", "8021353D0", "80266F880"): "bipartite"}
    # to_events must build the same key from a loaded row.
    d = pd.DataFrame({"ts": pd.to_datetime(["2022/09/05 08:25"], format="%Y/%m/%d %H:%M"),
                      "sender": ["8021353D0"], "receiver": ["80266F880"],
                      "amount": [946.84], "currency": ["Euro"], "from_bank": ["023691"],
                      "to_bank": ["015231"], "label": [1]})
    assert next(IB.to_events(d, {}, typ)).typology == "bipartite"


def test_ibm_window_stats_measure_the_span(ibm_file):
    """Section D quotes this rather than asserting a caveat; on AMLSim the same
    quantity turned out to be 363 days and changed the reading of the result."""
    d, _, _ = IB.load(ibm_file)
    stats = IB.window_stats(d)
    assert stats["receivers"] == 1          # DROP, six inbound
    assert stats["median_h"] < 1.0
    assert stats["within_window"] == 1.0


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
        RP.capability_profile("device_telemetry")
        assert CAP.MODES["payee_identity"] == "pinfl"
        assert CAP.MODES["device_telemetry"] == "off"
    finally:
        CAP.MODES.clear(); CAP.MODES.update(saved)


# --- the shared extractor ----------------------------------------------------------

def _ibm_profile():
    saved = dict(CAP.MODES)
    for key in ("receiver_age", "myid_kinship", "device_telemetry",
                "geo_telemetry", "session_telemetry"):
        CAP.MODES[key] = "off"
    CAP.MODES["payee_identity"] = "pinfl"
    return saved


def test_extract_features_returns_one_row_per_event_and_the_labels(ibm_file):
    saved = _ibm_profile()
    try:
        d, scales = _loaded(ibm_file)
        X, y, ts = RP.extract_features(IB.to_events(d, scales), total=len(d))
        import features as F
        assert X.shape == (len(d), len(F.FEATURE_NAMES))
        assert list(y) == list(d.label)
        assert (ts[1:] >= ts[:-1]).all(), "rows must stay in stream order"
    finally:
        CAP.MODES.clear(); CAP.MODES.update(saved)


def test_available_features_drops_exactly_what_the_profile_switched_off():
    """A column computed from data the dataset lacks is a fabricated zero; this is
    the list that keeps it out of a model."""
    saved = _ibm_profile()
    try:
        idx, names = RP.available_features()
        for gone in ("receiver_age", "receiver_is_fresh", "device_is_new",
                     "geo_is_anomaly", "active_call", "secs_login_z"):
            assert gone not in names
        assert "rcv_distinct_senders_1h" in names and "vel_10m" in names
        # link_history stays: these datasets name the account on both sides.
        assert "money_back_96h" in names
        assert len(idx) == len(names) == 19
    finally:
        CAP.MODES.clear(); CAP.MODES.update(saved)


def test_extract_features_accepts_what_paysim_to_events_yields(paysim_df):
    """The regression that motivated sharing the loop: PaySim's model mode unpacked
    dict events after `to_events` had started yielding `Event`s."""
    saved = _ibm_profile()
    try:
        X, y, _ = RP.extract_features(PS.to_events(paysim_df, 1.0),
                                      total=len(paysim_df))
        assert X.shape[0] == len(paysim_df)
        assert list(y) == list(paysim_df.isFraud)
    finally:
        CAP.MODES.clear(); CAP.MODES.update(saved)


def test_extract_matrix_caches_what_a_fit_needs(ibm_file, tmp_path):
    saved = _ibm_profile()
    try:
        cache = tmp_path / "f.npz"
        IB.extract_matrix(ibm_file, str(cache))
        import numpy as np
        z = np.load(cache)
        assert z["X"].shape == (len(z["y"]), len(z["names"]))
        assert len(z["ts"]) == len(z["y"]) == len(z["fmt"]) == len(z["currency"])
    finally:
        CAP.MODES.clear(); CAP.MODES.update(saved)


def test_ibm_our_model_scores_every_configuration_on_a_temporal_split(tmp_path):
    """Wiring, not quality: a synthetic cache in the shape extract_matrix writes, with
    positives in every third of the timeline so each split has some."""
    import numpy as np
    rng = np.random.default_rng(0)
    n = 3000
    names = ["log_amount", "vel_1h", "rcv_distinct_senders_1h", "rcv_inflow_1h"]
    y = (rng.random(n) < 0.05).astype("int8")
    X = rng.normal(size=(n, len(names))).astype("float32")
    X[:, 2] += 2.0 * y                          # make fan-in informative
    cache = tmp_path / "c.npz"
    np.savez(cache, X=X, y=y, ts=np.arange(n), names=np.array(names),
             fmt=rng.integers(0, 3, n).astype("int8"),
             fmt_names=np.array(["ACH", "Cheque", "Wire"]),
             currency=rng.integers(0, 2, n).astype("int8"),
             currency_names=np.array(["Euro", "US Dollar"]))
    res = IB.our_model(str(cache), seeds=1)
    assert {k[0] for k in res} == {"this project, all it can compute",
                                   "without receiver aggregation",
                                   "+ the file's own format and currency"}
    agg, _ = res[("this project, all it can compute", False)]
    assert 0.0 <= agg["f1_tuned"][0] <= 100.0 and 0.5 < agg["roc_auc"][0] <= 1.0


def test_ibm_events_carry_the_bank_on_each_side(ibm_file):
    """cross_network compares the issuers behind the two cards; an interbank
    transfer is the same distinction, and without the banks the feature was a
    constant zero on a file that names both."""
    saved = _ibm_profile()
    try:
        d, scales = _loaded(ibm_file)
        e = next(IB.to_events(d, scales))
        assert (e.ev["sender_network"], e.ev["receiver_network"]) == ("010", "020")
        import features as F
        X, _, _ = RP.extract_features(IB.to_events(d, scales), total=len(d))
        assert X[:, F.FEATURE_NAMES.index("cross_network")].min() == 1.0
    finally:
        CAP.MODES.clear(); CAP.MODES.update(saved)


def test_ibm_receiver_ablation_reports_both_methods_and_a_verdict(tmp_path):
    """Wiring: a synthetic cache where fan-in is informative, two seeds, a few
    resamples. The verdict must be a boolean and every interval ordered."""
    import numpy as np
    rng = np.random.default_rng(1)
    n = 3000
    names = ["log_amount", "vel_1h", "rcv_distinct_senders_1h", "rcv_inflow_1h"]
    y = (rng.random(n) < 0.05).astype("int8")
    X = rng.normal(size=(n, len(names))).astype("float32")
    X[:, 2] += 2.0 * y
    cache = tmp_path / "c.npz"
    np.savez(cache, X=X, y=y, ts=np.arange(n), names=np.array(names),
             fmt=rng.integers(0, 3, n).astype("int8"),
             fmt_names=np.array(["ACH", "Cheque", "Wire"]),
             currency=rng.integers(0, 2, n).astype("int8"),
             currency_names=np.array(["Euro", "US Dollar"]))
    out = IB.receiver_ablation(str(cache), seeds=2, boots=20)
    assert isinstance(out["established"], bool)
    r = out["fourteen features"]
    assert r["f1_ci"][0] <= r["f1_ci"][1] and r["ap_ci"][0] <= r["ap_ci"][1]

