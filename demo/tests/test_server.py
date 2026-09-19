"""What the demo sends: which rows make an episode, and how they are replayed."""

import csv
import random
from datetime import datetime, timedelta

import server as S  # first: it puts case-manager, where explain lives, on the path
import explain as EX  # noqa: E402

COLUMNS = ["transaction_id", "event_time", "sender_pinfl", "sender_card", "sender_network",
           "receiver_card", "receiver_network", "amount_uzs", "device_id", "sender_region",
           "sender_balance_before", "active_call", "secs_login_to_confirm",
           "label_is_fraud", "label_fraud_type"]
T0 = datetime(2025, 3, 1, 9, 0)
VICTIM, SCAMMER, MULE = "8600330000000001", "9860030000000009", "8600030000000777"


def _row(i, minutes, sender, receiver, fraud=""):
    return {"transaction_id": f"t{i}", "event_time": (T0 + timedelta(minutes=minutes)).isoformat(),
            "sender_pinfl": "3" * 14, "sender_card": sender, "sender_network": "UZCARD",
            "receiver_card": receiver, "receiver_network": "UZCARD", "amount_uzs": "500000",
            "device_id": "dev-1", "sender_region": "Tashkent City",
            "sender_balance_before": "9000000", "active_call": "False",
            "secs_login_to_confirm": "20.0", "label_is_fraud": "1" if fraud else "0",
            "label_fraud_type": fraud or "NONE"}


def _filler(n):
    """Early rows from other senders, so the rows under test fall past the cut."""
    return [_row(900 + k, -40 * 1440 + k, f"86000300000{k:05d}", "8600030000009999")
            for k in range(n)]


def _library(tmp_path, rows):
    path = tmp_path / "t.csv"
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=COLUMNS)
        w.writeheader()
        w.writerows(rows)
    return S.Episodes(str(path), held_out_from=0.5)


def _app_library(tmp_path):
    history = [_row(k, k * 1440, VICTIM, f"86003300000001{k % 2}") for k in range(10)]
    return _library(tmp_path, _filler(12) + history
                    + [_row(99, 10 * 1440 + 60, VICTIM, SCAMMER, "APP")])


def test_an_app_episode_is_the_senders_history_then_the_fraud(tmp_path):
    items = _app_library(tmp_path).pick("APP")
    assert [role for _, role in items] == ["history"] * S.HISTORY_ROWS + ["episode"]
    assert all(row["sender_card"] == VICTIM for row, _ in items)
    assert items[-1][0]["receiver_card"] == SCAMMER


def test_replay_gives_the_sender_a_new_card_and_keeps_everything_else(tmp_path):
    items = _app_library(tmp_path).pick("APP")
    now = 1_800_000_000.0
    out = S.replay_messages(items, now, random.Random(1))
    msgs = [o["message"] for o in out]
    new = msgs[0]["sender_card"]
    assert new != VICTIM and new[:6] == VICTIM[:6] and len(new) == len(VICTIM)
    assert {m["sender_card"] for m in msgs} == {new}
    assert msgs[-1]["receiver_card"] == SCAMMER                  # Neo4j knows this card
    assert len({m["transaction_id"] for m in msgs}) == len(msgs)
    assert not {m["transaction_id"] for m in msgs} & {row["transaction_id"] for row, _ in items}
    assert msgs[-1]["event_time"] == S._utc_iso(now)
    gap = (datetime.fromisoformat(msgs[1]["event_time"])
           - datetime.fromisoformat(msgs[0]["event_time"])).total_seconds()
    assert abs(gap - 86400) < 0.01                               # one day, as in the data
    assert "label_is_fraud" not in msgs[-1] and out[-1]["kind"] == "APP"


def test_a_mule_keeps_its_card_on_both_sides_of_the_episode(tmp_path):
    fan_in = [_row(k, 20 * 1440 + 3 * k, f"98600300000000{k:02d}", MULE, "MULE") for k in range(6)]
    out_rows = [_row(50 + k, 20 * 1440 + 30 + k, MULE, f"86003300000055{k}", "MULE") for k in range(2)]
    items = _library(tmp_path, _filler(10) + fan_in + out_rows).pick("MULE", random.Random(3))
    assert len(items) == 8
    msgs = [o["message"] for o in S.replay_messages(items, 1_800_000_000.0, random.Random(2))]
    assert [m["receiver_card"] for m in msgs[:6]] == [MULE] * 6
    assert [m["sender_card"] for m in msgs[6:]] == [MULE] * 2
    assert not {m["sender_card"] for m in msgs[:6]} & {r["sender_card"] for r in fan_in}


def test_an_ordinary_scenario_is_a_legitimate_transfer_with_history(tmp_path):
    items = _app_library(tmp_path).pick("NORMAL", random.Random(0))
    assert items[-1][1] == "episode" and not S._is_fraud(items[-1][0])
    assert sum(role == "history" for _, role in items) == S.HISTORY_ROWS


def test_case_manager_phrases_split_back_into_feature_value_weight():
    phrases = [EX.phrase("rcv_distinct_senders_1h", 3.0, 0.42), EX.phrase("hour", 14.0, 0.1)]
    assert S.split_phrases(phrases) == [
        {"feature": "rcv_distinct_senders_1h", "label_en": "distinct senders paying this payee in an hour", "shown": "3", "weight": 0.42},
        {"feature": "hour", "label_en": "hour of day (UTC)", "shown": "14:00", "weight": 0.1}]


def test_times_with_and_without_a_fraction_both_load(tmp_path):
    # isoformat() drops a zero fraction, so the generated file mixes both forms;
    # the realistic dataset failed to load on exactly this.
    rows = _filler(12) + [_row(k, k * 1440, VICTIM, "8600330000000100") for k in range(10)]
    rows[5]["event_time"] = rows[5]["event_time"] + ".742039"
    assert len(_library(tmp_path, rows).df) == len(rows)


def test_a_masked_card_shows_the_bin_head_and_the_last_four():
    assert S.mask("8600031234562655") == "8600 03** **** 2655"


def test_the_results_file_says_everything_in_both_languages():
    import json
    import os
    with open(os.path.join(os.path.dirname(S.__file__), "results.json"), encoding="utf-8") as fh:
        datasets = json.load(fh)["datasets"]
    for ds in datasets:
        assert ds["status"] in ("used", "inconclusive", "rejected"), ds["key"]
        assert ds["quotes"], ds["key"]
        fields = ("name", "findings") + (() if ds["status"] == "rejected"
                                         else ("what", "how", "verdict"))
        for field in fields:
            assert set(ds[field]) == {"ru", "en"}, (ds["key"], field)
        assert len(ds["findings"]["ru"]) == len(ds["findings"]["en"]), ds["key"]
