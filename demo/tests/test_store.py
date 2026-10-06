"""Store behaviour against a fake ClickHouse: the schema applied, FINAL on every
read, a resolution that finds its case first."""

import pytest

import case as CASE
import store as S

ALERT = {
    "transaction_id": "t_1", "event_time": "2026-03-14T19:22:41",
    "scored_at_job": 1_772_000_000.5, "sender_card": "8600330000000001",
    "receiver_card": "8600030000000002", "amount_uzs": 4_800_000,
    "final_score": 0.91, "decision": "REVIEW", "predicted_type": "APP",
    "rule_hits": ["VELOCITY"],
}


ALERT_TIME = CASE._dt(ALERT["event_time"])


class FakeResult:
    def __init__(self, rows):
        self.result_rows = rows


class FakeClient:
    """Records calls; last write per key wins, as ReplacingMergeTree converges."""

    def __init__(self):
        self.commands = []
        self.inserts = []
        self.queries = []
        self.rows = {}
        self.transfers = {}
        self.closed = False

    def ping(self):
        return True

    def command(self, stmt):
        self.commands.append(stmt)

    def insert(self, table, rows, column_names, database=None):
        self.inserts.append((table, len(rows)))
        for r in rows:
            key = r[column_names.index("case_id")]
            prev = self.rows.get(key)
            if prev is None or r[column_names.index("version")] >= \
                    prev[column_names.index("version")]:
                self.rows[key] = list(r)

    def query(self, q, parameters=None):
        self.queries.append(q)
        rows = list(self.rows.values())
        decision = CASE.CASE_COLUMNS.index("decision")
        if f"NOT ({S.HELD})" in q:
            return FakeResult([[sum(
                1 for r in rows if r[decision] != "REVIEW"
                and r[CASE.CASE_COLUMNS.index("disposition")] == "CONFIRMED_FRAUD")]])
        if f"WHERE {S.HELD}" in q:
            rows = [r for r in rows if r[decision] == "REVIEW"]
        if "transactions_scored" in q:
            t = self.transfers.get(parameters["t"])
            return FakeResult([[t[c] for c in CASE.REPORT_SOURCE]] if t else [])
        if "disposition = 'NEW'" in q:
            rows = [r for r in rows
                    if r[CASE.CASE_COLUMNS.index("disposition")] == "NEW"]
            if parameters and "since" in parameters:
                rows = [r for r in rows if r[CASE.CASE_COLUMNS.index("opened_at")]
                        .timestamp() * 1000 >= parameters["since"]]
            rows.sort(key=lambda r: (-r[CASE.CASE_COLUMNS.index("amount_uzs")],
                                     -r[CASE.CASE_COLUMNS.index("final_score")]))
        elif "disposition != 'NEW'" in q:
            at = CASE.CASE_COLUMNS.index("resolved_at")
            rows = [r for r in rows if r[CASE.CASE_COLUMNS.index("disposition")] != "NEW"
                    and r[at].timestamp() * 1000 >= parameters["since"]]
            rows.sort(key=lambda r: r[at], reverse=True)
        elif "case_id = " in q:
            cid = (parameters or {}).get("cid")
            rows = [r for r in rows if r[0] == cid]
        elif "receiver_card = " in q:
            return FakeResult([[sum(
                1 for r in rows
                if r[CASE.CASE_COLUMNS.index("receiver_card")] == parameters["card"]
                and r[CASE.CASE_COLUMNS.index("disposition")] == "CONFIRMED_FRAUD")]])
        elif "GROUP BY explanation_status" in q:
            counts = {}
            for r in rows:
                st = r[CASE.CASE_COLUMNS.index("explanation_status")]
                counts[st] = counts.get(st, 0) + 1
            return FakeResult(list(counts.items()))
        elif "SELECT disposition, opened_at, resolved_at, amount_uzs" in q:
            cols = [CASE.CASE_COLUMNS.index(c) for c in
                    ("disposition", "opened_at", "resolved_at", "amount_uzs")]
            return FakeResult([[r[i] for i in cols] for r in rows])
        elif "max(opened_at)" in q:
            opened = [r[CASE.CASE_COLUMNS.index("opened_at")] for r in rows]
            return FakeResult([[max(opened)]] if opened else [])
        elif "GROUP BY disposition" in q:
            counts = {}
            for r in rows:
                d = r[CASE.CASE_COLUMNS.index("disposition")]
                counts[d] = counts.get(d, 0) + 1
            return FakeResult(list(counts.items()))
        return FakeResult(rows)

    def close(self):
        self.closed = True


class FakeRedis:
    def __init__(self):
        self.sets = {}

    def sadd(self, key, member):
        self.sets.setdefault(key, set()).add(member)

    def srem(self, key, member):
        self.sets.get(key, set()).discard(member)

    def sismember(self, key, member):
        return member in self.sets.get(key, set())


def _open(store, alert, explanation=None, status=""):
    """A case as the sink writer opens it (sink-writer/ch_writer.py)."""
    store._insert(CASE.case_row(alert, explanation, status))


@pytest.fixture
def store(monkeypatch):
    fake = FakeClient()
    st = S.CaseStore("h", 1, "u", "p", "fraud")
    monkeypatch.setattr(st, "open", lambda: setattr(st, "_client", fake))
    st.open()
    st._fake = fake
    st._redis = FakeRedis()
    return st


# --- the read that must not forget FINAL -------------------------------------

def test_open_cases_query_uses_final(store):
    store.open_cases()
    assert " FINAL " in store._fake.queries[-1], (
        "without FINAL, ReplacingMergeTree can return both the open row and "
        "its resolution, showing a closed case as open")


def test_get_query_uses_final(store):
    store.get("t_1")
    assert "FINAL" in store._fake.queries[-1]


# --- the round trip ----------------------------------------------------------

def test_an_opened_case_is_in_the_queue(store):
    _open(store, ALERT)
    rows = store.open_cases()
    assert [r["case_id"] for r in rows] == ["t_1"]
    assert rows[0]["disposition"] == "NEW"


def test_queue_orders_by_exposure_when_scores_tie(store):
    """The failure the live queue exposed: with the model saturated at 1.000 score
    orders nothing, so the largest amount must come first."""
    assert "amount_uzs DESC" in _order_clause(store)
    for i, amt in enumerate([1_000_000, 9_000_000, 4_000_000]):
        _open(store, dict(ALERT, transaction_id=f"t_{i}", amount_uzs=amt, final_score=1.0))
    assert [r["amount_uzs"] for r in store.open_cases()] == \
        [9_000_000, 4_000_000, 1_000_000]


def _order_clause(store):
    store.open_cases()
    return store._fake.queries[-1]


def test_since_keeps_only_the_cases_opened_from_then_on(store):
    """Asked of the store, not filtered after it: the queue's first rows by amount
    can all be older than a case opened a second ago."""
    t = ALERT["scored_at_job"]
    _open(store, dict(ALERT, transaction_id="old", amount_uzs=9_000_000, scored_at_job=t - 60))
    _open(store, dict(ALERT, transaction_id="new", amount_uzs=100_000, scored_at_job=t))
    assert [r["case_id"] for r in store.open_cases(limit=1, since=t)] == ["new"]
    assert "opened_at >=" in store._fake.queries[-1]


def test_resolved_case_leaves_the_queue(store):
    _open(store, ALERT)
    assert store.resolve("t_1", "CONFIRMED_FRAUD", "analyst.k") is True
    assert store.open_cases() == []
    assert store.get("t_1")["resolved_by"] == "analyst.k"


def test_a_replayed_alert_does_not_reopen_a_resolved_case(store):
    """End to end: an alert redelivered after the verdict must not reopen the case."""
    _open(store, ALERT)
    store.resolve("t_1", "FALSE_POSITIVE", "analyst.k")
    _open(store, ALERT)                                # redelivered
    assert store.open_cases() == []
    assert store.get("t_1")["disposition"] == "FALSE_POSITIVE"


def test_resolving_an_unknown_case_reports_it(store):
    assert store.resolve("nope", "CONFIRMED_FRAUD", "analyst.k") is False


# --- the verdict the job learns from -----------------------------------------

def test_a_confirmed_fraud_adds_its_payee_to_the_accounts_the_job_reads(store):
    for i, d in enumerate(["CONFIRMED_FRAUD", "FALSE_POSITIVE"]):
        _open(store, dict(ALERT, transaction_id=f"t_{i}", receiver_card=f"card_{i}"))
        store.resolve(f"t_{i}", d, "analyst.k")
    assert store._redis.sets == {S.CONFIRMED_KEY: {"card_0"}}


def _confirmed(store, case_id, card, at):
    _open(store, dict(ALERT, transaction_id=case_id, receiver_card=card))
    store.resolve(case_id, "CONFIRMED_FRAUD", "analyst.k", at_epoch=at)


def test_a_withdrawn_confirmation_takes_the_payee_out(store):
    _confirmed(store, "t_1", "card_1", 1_000)
    store.resolve("t_1", "FALSE_POSITIVE", "analyst.m", at_epoch=2_000)
    assert store._redis.sets[S.CONFIRMED_KEY] == set()
    assert store.get("t_1")["disposition"] == "FALSE_POSITIVE"


def test_another_confirmed_case_keeps_the_payee_in(store):
    _confirmed(store, "t_1", "card_1", 1_000)
    _confirmed(store, "t_2", "card_1", 1_000)
    store.resolve("t_1", "FALSE_POSITIVE", "analyst.m", at_epoch=2_000)
    assert store._redis.sets[S.CONFIRMED_KEY] == {"card_1"}


def test_the_history_keeps_its_accounts(store):
    store._redis.sadd(S.HISTORY_KEY, "card_1")
    _confirmed(store, "t_1", "card_1", 1_000)
    store.resolve("t_1", "FALSE_POSITIVE", "analyst.m", at_epoch=2_000)
    assert "card_1" in store._redis.sets[S.CONFIRMED_KEY]


def test_resolved_cases_are_the_latest_verdicts_first(store):
    for i, at in enumerate([1_000, 3_000, 2_000]):
        _confirmed(store, f"t_{i}", f"card_{i}", at)
    assert [c["case_id"] for c in store.resolved_cases(since=1.5e3)] == ["t_1", "t_2"]


LET_GO = {"transaction_id": "t_9", "event_time": ALERT_TIME, "sender_card": "card_s",
          "receiver_card": "card_9", "amount_uzs": 2_000_000, "final_score": 0.01,
          "decision": "ALLOW", "predicted_type": "", "model_version": "cep+ml-fusion-v2",
          "ml_score": 0.01}


def test_a_report_on_a_transfer_let_go_is_a_confirmed_case_kept_out_of_the_holds(store):
    store._fake.transfers["t_9"] = LET_GO
    assert store.report("t_9", "analyst.k", at_epoch=5_000) is True
    case = store.get("t_9")
    assert (case["disposition"], case["decision"]) == ("CONFIRMED_FRAUD", "ALLOW")
    assert store._redis.sets[S.CONFIRMED_KEY] == {"card_9"}
    s = store.stats()
    assert s["_reported"] == 1 and s.get("CONFIRMED_FRAUD", 0) == 0 and s["_precision"] is None
    assert store.holds(now=6_000)["CONFIRMED_FRAUD"]["n"] == 0


def test_a_report_on_a_held_transfer_confirms_its_case(store):
    _open(store, ALERT)
    assert store.report("t_1", "analyst.k", at_epoch=5_000) is True
    assert store.get("t_1")["disposition"] == "CONFIRMED_FRAUD"
    assert store.stats()["_reported"] == 0


def test_a_report_on_a_transfer_not_yet_stored_says_so(store):
    assert store.report("t_unknown", "analyst.k") is False


def test_redis_down_costs_the_mark_not_the_verdict(store, caplog):
    class Down:
        def sadd(self, *a):
            raise ConnectionError("refused")
    store._redis = Down()
    _open(store, ALERT)
    with caplog.at_level("ERROR"):
        assert store.resolve("t_1", "CONFIRMED_FRAUD", "analyst.k") is True
    assert store.get("t_1")["disposition"] == "CONFIRMED_FRAUD"
    assert S.CONFIRMED_KEY in caplog.text


# --- what the queue is for ---------------------------------------------------

def test_precision_is_computed_over_resolved_cases_only(store):
    """Open cases are not 'not fraud'; folding them in drifts precision upward."""
    for i, (d, by) in enumerate([("CONFIRMED_FRAUD", "a"), ("CONFIRMED_FRAUD", "a"),
                                 ("FALSE_POSITIVE", "a")]):
        _open(store, dict(ALERT, transaction_id=f"t_{i}"))
        store.resolve(f"t_{i}", d, by)
    _open(store, dict(ALERT, transaction_id="t_open"))  # still NEW

    s = store.stats()
    assert s["_resolved"] == 3
    assert s["_precision"] == pytest.approx(2 / 3)


def test_precision_is_undefined_before_anyone_resolves_anything(store):
    _open(store, ALERT)
    assert store.stats()["_precision"] is None


def test_stats_report_why_explanations_are_missing(store):
    _open(store, ALERT, status="NO_FEATURES")
    s = store.stats()
    assert s["_explanation"] == {"NO_FEATURES": 1}
    assert s["_last_opened"] is not None


def test_holds_measure_the_wait_from_decision_to_verdict(store):
    _open(store, ALERT)
    store.resolve("t_1", "FALSE_POSITIVE", "analyst.k", at_epoch=ALERT["scored_at_job"] + 240)
    h = store.holds(now=ALERT["scored_at_job"] + 999)
    assert "FINAL" in store._fake.queries[-1]
    assert h["FALSE_POSITIVE"]["n"] == 1
    assert h["FALSE_POSITIVE"]["median_s"] == pytest.approx(240)
    assert h["NEW"]["n"] == 0
