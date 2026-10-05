"""retrain.py's pieces that decide: the split, the cut-off that keeps the workload,
what an alert list makes of the known frauds, the drift measure, and the outcome
each run leaves for the demo page."""

import json

import numpy as np
import pytest

import retrain as R


def test_every_run_leaves_its_outcome_for_the_page(tmp_path, monkeypatch):
    monkeypatch.setattr(R, "STATUS", str(tmp_path / "status.json"))
    R.write_status({"outcome": "too_few_fraud", "known_fraud": 2}, 24)
    s = json.loads((tmp_path / "status.json").read_text(encoding="utf-8"))
    assert (s["outcome"], s["known_fraud"], s["every_hours"]) == ("too_few_fraud", 2, 24)
    assert s["at"] > 0


def test_the_logged_rows_split_as_train_py_splits_its_data():
    assert R.split(1000) == (640, 800)


def test_the_new_cut_off_raises_as_many_alerts_as_the_served_one():
    old = np.array([0.9, 0.8, 0.1, 0.05, 0.01])
    new = np.array([0.2, 0.7, 0.6, 0.3, 0.1])
    cut = R.same_workload_cut(new, old, 0.5)
    assert int((new >= cut).sum()) == int((old >= 0.5).sum()) == 2


def test_no_served_alert_means_no_new_one():
    new = np.array([0.2, 0.7])
    assert int((new >= R.same_workload_cut(new, np.array([0.1, 0.2]), 0.5)).sum()) == 0


def test_an_alert_list_is_read_against_the_known_frauds():
    y = np.array([1, 0, 1, 0, 1])
    r = R.read(y, np.array([0.9, 0.8, 0.2, 0.1, 0.7]), 0.5)
    assert (r["alerts"], r["caught"], r["fraud"]) == (3, 2, 3)
    assert r["precision"] == pytest.approx(2 / 3) and r["recall"] == pytest.approx(2 / 3)


def test_an_unmoved_feature_reads_near_zero_and_a_moved_one_far_from_it():
    rng = np.random.default_rng(0)
    ref = rng.normal(size=20_000)
    assert R.psi(ref, rng.normal(size=20_000)) < 0.01
    assert R.psi(ref, rng.normal(loc=1.0, size=20_000)) > R.PSI_MOVED


def test_a_feature_that_stops_being_computed_counts_as_moved():
    ref = np.arange(1000, dtype=float)
    assert R.psi(ref, np.full(1000, np.nan)) > R.PSI_MOVED
