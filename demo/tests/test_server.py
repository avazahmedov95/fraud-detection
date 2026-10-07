"""What the demo sends: which rows make an episode, and how they are replayed."""

import csv
import random
import re
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest

import explain as EX
import server as S

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
    assert msgs[-1]["receiver_card"] == SCAMMER   # the confirmed accounts are kept by card
    assert len({m["transaction_id"] for m in msgs}) == len(msgs)
    assert not {m["transaction_id"] for m in msgs} & {row["transaction_id"] for row, _ in items}
    assert msgs[-1]["event_time"] == S._utc_iso(now)
    gap = (datetime.fromisoformat(msgs[1]["event_time"])
           - datetime.fromisoformat(msgs[0]["event_time"])).total_seconds()
    assert abs(gap - 86400) < 0.01                               # one day, as in the data
    assert "label_is_fraud" not in msgs[-1] and out[-1]["kind"] == "APP"


def _mule_library(tmp_path, payers=6):
    """`payers` people pay the mule within minutes, then the mule pays out twice."""
    fan_in = [_row(k, 20 * 1440 + 3 * k, f"98600300000000{k:02d}", MULE, "MULE") for k in range(payers)]
    out_rows = [_row(50 + k, 20 * 1440 + 30 + k, MULE, f"86003300000055{k}", "MULE") for k in range(2)]
    return _library(tmp_path, _filler(10) + fan_in + out_rows)


def test_a_mule_keeps_its_card_on_both_sides_of_the_episode(tmp_path):
    items = _mule_library(tmp_path).pick("MULE", random.Random(3))
    assert len(items) == 8
    msgs = [o["message"] for o in S.replay_messages(items, 1_800_000_000.0, random.Random(2))]
    assert [m["receiver_card"] for m in msgs[:6]] == [MULE] * 6
    assert [m["sender_card"] for m in msgs[6:]] == [MULE] * 2
    assert not {m["sender_card"] for m in msgs[:6]} & {row["sender_card"] for row, _ in items}


def test_the_show_picks_a_mule_paid_by_several_people(tmp_path):
    items = _mule_library(tmp_path).pick_story(random.Random(3))
    assert sum(row["receiver_card"] == MULE for row, _ in items) >= S.STORY_PAYERS
    with pytest.raises(LookupError):
        _mule_library(tmp_path, payers=S.STORY_PAYERS - 1).pick_story(random.Random(3))


def test_the_shows_replay_gives_every_card_a_new_number_the_same_on_both_sides(tmp_path):
    """So the case can be shown again: the mule is new to the system each time."""
    items = _mule_library(tmp_path).pick_story(random.Random(3))
    msgs = [o["message"] for o in S.replay_messages(items, 1_800_000_000.0, random.Random(2), fresh=True)]
    mule = msgs[0]["receiver_card"]
    assert mule != MULE and mule[:6] == MULE[:6] and len(mule) == len(MULE)
    assert [m["receiver_card"] for m in msgs[:6]] == [mule] * 6
    assert [m["sender_card"] for m in msgs[6:]] == [mule] * 2
    original = {row[k] for row, _ in items for k in ("sender_card", "receiver_card")}
    assert not original & {m[k] for m in msgs for k in ("sender_card", "receiver_card")}


class _Sent:
    def __init__(self):
        self.messages = []

    def send(self, messages, gap_s=0.15):
        self.messages += [m["message"] for m in messages]


def test_the_show_sends_its_case_one_transfer_at_a_time(tmp_path):
    from collections import OrderedDict
    app = S.App.__new__(S.App)
    app.library, app.sender, app.decisions, app.runs = _mule_library(tmp_path), _Sent(), S.Decisions(), OrderedDict()
    app.names, app._why = ["payee_flagged", "sender_flagged"], lambda rec: None
    run_id = app.start_story()["id"]
    assert app.sender.messages == []
    assert app.send_next(run_id) == {"ok": True}
    first = app.sender.messages[0]
    app.decisions.add({"transaction_id": first["transaction_id"], "decision": "REVIEW", "features": [1, 0]})
    run = app.run(run_id)
    assert [r["sent"] for r in run["rows"][:2]] == [True, False]
    assert run["rows"][0]["decision"] == "REVIEW"
    assert run["rows"][0]["marks"] == {"payee": True, "sender": False}
    assert "decision" not in run["rows"][1] and "_msgs" not in run
    while app.send_next(run_id)["ok"]:
        pass
    assert len(app.sender.messages) == len(run["rows"])


def test_the_memory_marks_are_what_the_job_read():
    names = ["log_amount", "payee_flagged", "sender_flagged"]
    assert S.marks({"features": [9.1, 1.0, 0.0]}, names) == {"payee": True, "sender": False}
    assert S.marks({"features": None}, names) is None


def test_the_show_reads_the_key_the_second_look_renews():
    config = _repo("second-look", "config.py")
    assert re.search(r'^ALIVE_KEY = "([^"]+)"', config, re.M).group(1) == S.SECOND_LOOK_ALIVE


def test_an_ordinary_scenario_is_a_legitimate_transfer_with_history(tmp_path):
    items = _app_library(tmp_path).pick("NORMAL", random.Random(0))
    assert items[-1][1] == "episode" and not S._is_fraud(items[-1][0])
    assert sum(role == "history" for _, role in items) == S.HISTORY_ROWS


def test_explanation_phrases_split_back_into_feature_value_weight():
    phrases = [EX.phrase("rcv_distinct_senders_1h", 3.0, 0.42), EX.phrase("hour", 14.0, 0.1)]
    assert S.split_phrases(phrases) == [
        {"feature": "rcv_distinct_senders_1h", "label_en": "distinct senders paying this payee in an hour", "shown": "3", "weight": 0.42},
        {"feature": "hour", "label_en": "hour of day (UTC)", "shown": "14:00", "weight": 0.1}]


def test_the_case_queue_reads_only_what_a_case_stores():
    """The queue once read a column the cases had dropped, and failed on every
    open case."""
    import case as CASE
    row = dict.fromkeys(CASE.CASE_COLUMNS, "")
    row.update(opened_at=T0, rule_hits=[], explanation=[], disposition="NEW")

    class Store:
        def _ensure(self):
            return True

        def open_cases(self, limit, since=None):
            return [row]

        def stats(self):
            return {}

        def holds(self):
            return {}

    app = SimpleNamespace(started=0, store=Store)
    assert len(S.App.cases(app)["cases"]) == 1


def test_a_store_that_cannot_connect_is_an_error_not_an_empty_queue():
    """It once answered [] for a missing schema file, and the page said "no new
    alerts" while 33 were waiting."""
    class Store:
        def _ensure(self):
            return False

        def open_cases(self, limit, since=None):
            return []

        def stats(self):
            return {}

    out = S.App.cases(SimpleNamespace(started=0, store=Store))
    assert out["error"] and out["cases"] == []


def test_the_verdicts_of_this_showing_come_with_the_queue():
    """Listed so that a mistaken verdict can be changed from the page."""
    import case as CASE
    row = dict.fromkeys(CASE.CASE_COLUMNS, "")
    row.update(case_id="t_1", amount_uzs=5_000_000, receiver_card="8600000000001234",
               disposition="CONFIRMED_FRAUD", resolved_by="demo")

    class Store:
        def _ensure(self):
            return True

        def open_cases(self, limit, since=None):
            return []

        def resolved_cases(self, since, limit):
            return [row]

        def stats(self):
            return {}

        def holds(self):
            return {}

    out = S.App.cases(SimpleNamespace(started=0, store=Store), new_only=True)
    assert [(d["id"], d["verdict"]) for d in out["done"]] == [("t_1", "CONFIRMED_FRAUD")]


def test_times_with_and_without_a_fraction_both_load(tmp_path):
    # isoformat() drops a zero fraction, so the generated file mixes both forms;
    # the realistic dataset failed to load on exactly this.
    rows = _filler(12) + [_row(k, k * 1440, VICTIM, "8600330000000100") for k in range(10)]
    rows[5]["event_time"] = rows[5]["event_time"] + ".742039"
    assert len(_library(tmp_path, rows).df) == len(rows)


def test_a_masked_card_shows_the_bin_head_and_the_last_four():
    assert S.mask("8600031234562655") == "8600 03** **** 2655"


def test_the_background_replay_starts_in_the_part_the_model_never_saw(tmp_path):
    lib = _app_library(tmp_path)
    rng = random.Random(4)
    assert all(lib.replay_start(rng) >= lib.cut for _ in range(50))


def test_the_page_opens_only_when_the_fraud_job_itself_is_running():
    job = {"name": S.JOB_NAME, "state": "RUNNING"}
    assert S.job_running({"jobs": [job]})
    assert not S.job_running({"jobs": [{**job, "state": "RESTARTING"}]})
    assert not S.job_running({"jobs": [{**job, "name": "some-other-job"}]})
    assert not S.job_running(None)                     # Flink not answering


def test_the_system_is_ready_only_with_every_part_up():
    health = S.Health(data_error="")
    health.parts.update(kafka=True, clickhouse=True, job=True)
    assert health.ready
    health.parts["job"] = False
    assert not health.ready
    assert not S.Health(data_error="no such file").parts["data"]


def test_decision_time_runs_from_arrival_to_decision():
    assert S.decision_ms({"ingested_at": 100.0, "scored_at_job": 100.086}) == pytest.approx(86.0)
    assert S.decision_ms({"scored_at_job": 100.0}) is None      # a producer that stamps nothing


def test_the_stage_averages_add_up_and_an_empty_window_says_so():
    row = [1000, 1.2, 71.5, 0.02, 1.58, 0.84, 0.32, 0.21, 0.01, 84, 197]
    out = S.stage_summary(row)
    assert [s["name"] for s in out["stages"]] == list(S.STAGES)
    assert out["total_ms"] == pytest.approx(sum(row[1:9]))
    assert (out["median_ms"], out["p99_ms"]) == (84, 197)
    empty = S.stage_summary([0] + [float("nan")] * 8 + [0, 0])
    assert empty["total_ms"] is None and empty["median_ms"] is None
    assert all(s["ms"] is None for s in empty["stages"])


def test_the_demo_names_the_same_stages_as_the_job():
    import ast
    import os
    job = os.path.join(os.path.dirname(S.__file__), "..", "stream-processor", "fraud_job.py")
    tree = ast.parse(open(job, encoding="utf-8").read())
    stages = next(n.value for n in tree.body if isinstance(n, ast.Assign)
                  and getattr(n.targets[0], "id", "") == "STAGES")
    assert ast.literal_eval(stages) == S.STAGES


def _repo(*parts):
    import os
    return open(os.path.join(os.path.dirname(os.path.dirname(S.__file__)), *parts), encoding="utf-8").read()


def test_the_about_tab_describes_the_jobs_rules_with_the_jobs_numbers():
    """Every rule rules.py runs, in its order, and every number the tab quotes for
    one, as stream-processor/config.py sets it."""
    block = _repo("demo", "about.js").split("const RULE_FACTS = {", 1)[1].split("};", 1)[0]
    rules = re.findall(r'hits\.append\("(\w+)"\)', _repo("stream-processor", "rules.py"))
    assert re.findall(r"^\s*(\w+): \{", block, re.M) == rules
    config = _repo("stream-processor", "config.py")
    for name, value in re.findall(r"\b([A-Z][A-Z_]+): ([\d.]+)", block):
        set_to = re.search(rf"^{name}\s*=\s*([\d_.]+)", config, re.M)
        assert set_to, name
        assert float(set_to.group(1).replace("_", "")) == float(value), name


def test_the_about_tab_lists_the_models_features_in_order_and_describes_each():
    import json
    about = _repo("demo", "about.js")
    block = about.split("const FEATURE_GROUPS = [", 1)[1].split("];", 1)[0]
    groups = re.findall(r'\["(\w+)", \[', block)
    names = [n for n in re.findall(r'"(\w+)"', block) if n not in groups]
    assert names == json.loads(_repo("ml", "models", "feature_names.json"))
    described = [re.findall(r'^\s*(\w+): "', part.split("}", 1)[0], re.M)
                 for part in about.split("feature: {")[1:]]
    assert described == [names, names]                           # Russian and English


def test_the_about_tab_names_alerts_as_the_job_does():
    import ast
    import json
    tree = ast.parse(_repo("stream-processor", "fusion.py"))
    priority = next(n.value for n in tree.body if isinstance(n, ast.Assign)
                    and getattr(n.targets[0], "id", "") == "_TYPE_PRIORITY")
    shown = _repo("demo", "about.js").split("const NAMED_BY = ", 1)[1].split(";", 1)[0]
    assert json.loads(shown) == [[kind, list(rules)] for kind, rules in ast.literal_eval(priority)]


def test_the_stream_sends_a_page_and_says_how_many_there_are():
    decisions = S.Decisions()
    for i in range(30):
        decisions.add({"transaction_id": f"t{i}", "decision": "ALLOW"})
    app = SimpleNamespace(decisions=decisions, runs={}, _why=lambda rec: None)
    out = S.App.stream(app, limit=S.PAGE)
    assert len(out["rows"]) == S.PAGE and out["total"] == 30
    assert out["rows"][0]["id"] == "t29"                         # newest first


def test_the_queue_sends_a_page_and_says_how_many_are_open():
    import case as CASE
    rows = [dict(dict.fromkeys(CASE.CASE_COLUMNS, ""), case_id=f"c{i}", opened_at=T0,
                 rule_hits=[], explanation=[], disposition="NEW") for i in range(25)]

    class Store:
        def _ensure(self):
            return True

        def open_cases(self, limit, since=None):
            return rows

        def stats(self):
            return {}

        def holds(self):
            return {}

    out = S.App.cases(SimpleNamespace(started=0, store=Store), limit=S.PAGE)
    assert len(out["cases"]) == S.PAGE and out["total"] == 25


def test_the_filter_reads_what_the_page_shows_of_a_card():
    f = S.parse_filter({"sender": ["8600 03"], "receiver": ["2655"], "min": ["1000"],
                        "max": ["many"], "decision": ["REVIEW"], "type": [""]})
    assert f == {"sender": "860003", "receiver": "2655", "min": 1000.0, "decision": "REVIEW"}
    card = "8600031234562655"
    assert S._card_matches(card, "860003") and S._card_matches(card, "2655")
    assert S._card_matches(card, card)
    assert not S._card_matches(card, "1234")                    # the hidden middle stays hidden


def test_the_filter_checks_every_condition():
    row = ("8600031234562655", "9860449876543210", 9_600_000, 1000.0, ("REVIEW",), "STRUCTURING")
    assert S.passes({}, *row)
    assert S.passes({"min": 9e6, "max": 1e7, "since": 999.0, "until": 1001.0,
                     "decision": "REVIEW", "type": "STRUCTURING"}, *row)
    assert not S.passes({"max": 9e6}, *row)
    assert not S.passes({"since": 1001.0}, *row)
    assert not S.passes({"decision": "ALLOW"}, *row)
    assert S.passes({"type": "NONE"}, *row[:5], None)            # the model alone named nothing


def test_a_filtered_stream_counts_only_what_passes():
    decisions = S.Decisions()
    for i in range(30):
        decisions.add({"transaction_id": f"t{i}", "decision": "REVIEW" if i % 3 == 0 else "ALLOW"})
    app = SimpleNamespace(decisions=decisions, runs={}, _why=lambda rec: None)
    out = S.App.stream(app, flt={"decision": "REVIEW"}, limit=S.PAGE)
    assert out["total"] == 10 and all(r["decision"] == "REVIEW" for r in out["rows"])


def test_a_page_size_stays_within_what_the_server_keeps():
    assert S._limit({"limit": ["40"]}) == 40
    assert S._limit({"limit": ["100000"]}) == 400
    assert S._limit({"limit": ["many"]}) == S._limit({}) == S.PAGE


def test_the_page_marks_the_same_rules_mandatory_as_the_job():
    import os
    root = os.path.dirname(os.path.dirname(S.__file__))
    config = open(os.path.join(root, "stream-processor", "config.py"), encoding="utf-8").read()
    page = open(os.path.join(root, "demo", "index.html"), encoding="utf-8").read()
    job = re.search(r"MANDATORY_REVIEW_RULES = \(([^)]*)\)", config).group(1)
    shown = re.search(r"const MUST = new Set\(\[([^\]]*)\]\)", page).group(1)
    assert re.findall(r'"(\w+)"', job) == re.findall(r'"(\w+)"', shown) == list(S.MANDATORY)


def test_the_queue_filter_finds_what_held_each_transfer():
    import case as CASE
    blank = dict.fromkeys(CASE.CASE_COLUMNS, "")
    rows = [{**blank, "case_id": f"c{i}", "opened_at": T0, "rule_hits": [], "explanation": [],
             "disposition": "NEW", **kw} for i, kw in enumerate([
                 {"ml_score": 0.5},                                         # the model
                 {"ml_score": 0.02, "rule_hits": ["STRUCTURING"]},          # a hard rule
                 {"ml_score": 0.5, "rule_hits": ["DAILY_LIMIT_BREACH"]},    # both
                 {"ml_score": 0.07, "final_score": 0.95, "model_version": "second-look:t"},
                 {"ml_score": None, "rule_hits": ["VELOCITY"]}])]           # no model: the rules

    class Store:
        def _ensure(self):
            return True

        def open_cases(self, limit, since=None):
            return rows

        def stats(self):
            return {}

        def holds(self):
            return {}

    app = SimpleNamespace(started=0, store=Store, cut=0.1)
    held = {by: [c["id"] for c in S.App.cases(app, flt={"held": by})["cases"]]
            for by in ("MODEL", "RULE", "SECOND_LOOK")}
    assert held == {"MODEL": ["c0", "c2"], "RULE": ["c1", "c2", "c4"], "SECOND_LOOK": ["c3"]}


def test_the_results_file_says_everything_in_both_languages():
    import json
    import os
    with open(os.path.join(os.path.dirname(S.__file__), "results.json"), encoding="utf-8") as fh:
        quoted = json.load(fh)
    for ds in quoted["datasets"]:
        assert ds["quotes"], ds["key"]
        for field in ("name", "what", "ours"):
            assert set(ds[field]) == {"ru", "en"}, (ds["key"], field)
    assert quoted["own"]["quotes"] and set(quoted["own"]["second"]) == {"ru", "en"}
    for table in quoted["research"]:
        for field in ("title", "note", "head"):
            assert set(table[field]) == {"ru", "en"}, (table["key"], field)
        width = len(table["head"]["en"])
        assert len(table["head"]["ru"]) == width, table["key"]
        for row in table["rows"]:
            assert set(row["name"]) == {"ru", "en"} and len(row["values"]) == width, table["key"]


def test_a_transfer_decided_twice_keeps_its_row_and_counts_once():
    """A transfer just under the cut-off is decided by the job, then by the second look."""
    d = S.Decisions()
    first = {"transaction_id": "t1", "decision": "SECOND_LOOK", "ingested_at": 1.0,
             "scored_at_job": 1.1, "ml_score": 0.07, "final_score": 0.07}
    d.add(first)
    d.add({"transaction_id": "t2", "decision": "ALLOW"})
    held = dict(first, decision="REVIEW", final_score=0.94, model_version="second-look:tabpfn")
    d.add(held)
    assert [r["transaction_id"] for r in d.recent] == ["t1", "t2"]
    assert d.counts["REVIEW"] == 1 and d.counts["SECOND_LOOK"] == 0
    assert len(d.ms) == 1                       # its time to decision is the job's, once
    # Every row's risk is the served model's; TabPFN's, on its own scale, comes beside it.
    v = S.view(d.by_id["t1"])
    assert v["second"] and (v["score"], v["second_score"]) == (0.07, 0.94)
    unscored = dict(held, final_score=0.07, model_version="second-look:tabpfn:unscored")
    assert S._scores(unscored) == {"score": 0.07, "second_score": None}
    # Its row has left SECOND_LOOK, and the filter still finds it there and as held.
    app = SimpleNamespace(decisions=d, runs={}, _why=lambda rec: None)
    for asked in ("SECOND_LOOK", "REVIEW"):
        assert [r["id"] for r in S.App.stream(app, flt={"decision": asked})["rows"]] == ["t1"]
