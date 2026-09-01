"""Store behaviour, against a fake ClickHouse.

The cluster is not reachable from the test runner, and the parts most likely to
go wrong here are not ClickHouse's - they are ours: the schema that never gets
applied, the read that forgets FINAL, and the resolution that has to find its
case first. All three are checkable without a server.
"""

import pytest

import case as CASE
import store as S

ALERT = {
    "transaction_id": "t_1", "event_time": "2026-03-14T19:22:41",
    "scored_at_job": 1_772_000_000.5, "sender_card": "8600330000000001",
    "receiver_card": "8600030000000002", "amount_uzs": 4_800_000,
    "final_score": 0.91, "decision": "BLOCK", "predicted_type": "APP",
    "rule_hits": ["FRESH_RECEIVER"],
}


class FakeResult:
    def __init__(self, rows):
        self.result_rows = rows


class FakeClient:
    """Records what it was asked to do. Stores rows; last write wins per key,
    which is what ReplacingMergeTree converges to."""

    def __init__(self):
        self.commands = []
        self.inserts = []
        self.queries = []
        self.rows = {}
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
        if "disposition = 'NEW'" in q:
            rows = [r for r in rows
                    if r[CASE.CASE_COLUMNS.index("disposition")] == "NEW"]
            rows.sort(key=lambda r: (r[CASE.CASE_COLUMNS.index("priority")],
                                     -r[CASE.CASE_COLUMNS.index("amount_uzs")],
                                     -r[CASE.CASE_COLUMNS.index("final_score")]))
        elif "case_id = " in q:
            cid = (parameters or {}).get("cid")
            rows = [r for r in rows if r[0] == cid]
        elif "GROUP BY explanation_status" in q:
            counts = {}
            for r in rows:
                st = r[CASE.CASE_COLUMNS.index("explanation_status")]
                counts[st] = counts.get(st, 0) + 1
            return FakeResult(list(counts.items()))
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


@pytest.fixture
def store(monkeypatch):
    fake = FakeClient()
    st = S.CaseStore("h", 1, "u", "p", "fraud")
    monkeypatch.setattr(st, "open", lambda: (
        setattr(st, "_client", fake), st._apply_schema()))
    st.open()
    st._fake = fake
    return st


# --- the schema that would otherwise never be applied ------------------------

def test_the_shipped_ddl_parses_into_whole_statements():
    """_statements() strips line comments by hand. The first version split on
    ";" without doing so and tore the CREATE TABLE apart at a semicolon inside
    a comment. This pins the assumption against the real file."""
    with open(S._ddl_file(), encoding="utf-8") as fh:
        stmts = list(S._statements(fh.read()))
    creates = [s for s in stmts if s.startswith("CREATE TABLE")]
    assert len(creates) == 1
    assert "ReplacingMergeTree(version)" in creates[0]
    # Every other statement must be an idempotent migration, never a bare ALTER
    # that fails the second time the file is applied.
    for s in stmts:
        if s.startswith("ALTER TABLE"):
            assert "IF NOT EXISTS" in s, s
        else:
            assert s.startswith("CREATE TABLE"), s


def test_schema_is_applied_on_connect(store):
    """ClickHouse runs init scripts only on an empty data directory, so on any
    cluster that has been up before the table must be created by the service."""
    assert any("CREATE TABLE IF NOT EXISTS fraud.cases" in c
               for c in store._fake.commands)


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

def test_alert_becomes_an_open_case(store):
    store.add(ALERT)
    assert store.pending() == 1
    store.flush()
    assert store.pending() == 0
    rows = store.open_cases()
    assert [r["case_id"] for r in rows] == ["t_1"]
    assert rows[0]["disposition"] == "NEW"


def test_queue_orders_by_exposure_when_scores_tie(store):
    """The failure the live queue exposed. With the model saturated at 1.000
    across the alert set, score orders nothing; the largest amount must come
    first so the queue is triaged by what it costs to be wrong about."""
    assert "amount_uzs DESC" in _order_clause(store)
    for i, amt in enumerate([1_000_000, 9_000_000, 4_000_000]):
        store.add(dict(ALERT, transaction_id=f"t_{i}", amount_uzs=amt,
                       final_score=1.0))
    store.flush()
    assert [r["amount_uzs"] for r in store.open_cases()] == \
        [9_000_000, 4_000_000, 1_000_000]


def _order_clause(store):
    store.open_cases()
    return store._fake.queries[-1]


def test_a_block_still_outranks_a_bigger_review(store):
    """Exposure orders WITHIN a band, never across it."""
    store.add(dict(ALERT, transaction_id="small_block",
                   decision="BLOCK", amount_uzs=100_000))
    store.add(dict(ALERT, transaction_id="huge_review",
                   decision="REVIEW", amount_uzs=99_000_000))
    store.flush()
    assert [r["case_id"] for r in store.open_cases()] == \
        ["small_block", "huge_review"]


def test_resolved_case_leaves_the_queue(store):
    store.add(ALERT)
    store.flush()
    assert store.resolve("t_1", "CONFIRMED_FRAUD", "analyst.k") is True
    assert store.open_cases() == []
    assert store.get("t_1")["resolved_by"] == "analyst.k"


def test_a_replayed_alert_does_not_reopen_a_resolved_case(store):
    """End to end, through the version rule: the alert comes back after the
    verdict and the case must stay closed."""
    store.add(ALERT)
    store.flush()
    store.resolve("t_1", "FALSE_POSITIVE", "analyst.k")
    store.add(ALERT)                                   # redelivered
    store.flush()
    assert store.open_cases() == []
    assert store.get("t_1")["disposition"] == "FALSE_POSITIVE"


def test_resolving_an_unknown_case_reports_it(store):
    assert store.resolve("nope", "CONFIRMED_FRAUD", "analyst.k") is False


# --- failure is loud, not silent ---------------------------------------------

def test_cases_are_not_discarded_quietly(store, caplog):
    """An alert queue that drops silently is worse than one that is down: the
    operator sees an empty queue and reads it as 'nothing to work on'."""
    store._client = None
    store._last_attempt = 1e18            # block the reconnect attempt
    store.add(ALERT)
    with caplog.at_level("ERROR"):
        store.flush()
    assert "DISCARDED" in caplog.text
    assert store._dropped == 1


# --- what the queue is for ---------------------------------------------------

def test_precision_is_computed_over_resolved_cases_only(store):
    """Open cases are not 'not fraud'. Folding them in would make the number
    drift upward as the queue grows."""
    for i, (d, by) in enumerate([("CONFIRMED_FRAUD", "a"), ("CONFIRMED_FRAUD", "a"),
                                 ("FALSE_POSITIVE", "a")]):
        alert = dict(ALERT, transaction_id=f"t_{i}")
        store.add(alert)
        store.flush()
        store.resolve(f"t_{i}", d, by)
    store.add(dict(ALERT, transaction_id="t_open"))     # still NEW
    store.flush()

    s = store.stats()
    assert s["_resolved"] == 3
    assert s["_precision"] == pytest.approx(2 / 3)


def test_precision_is_undefined_before_anyone_resolves_anything(store):
    store.add(ALERT)
    store.flush()
    assert store.stats()["_precision"] is None


def test_stats_report_why_explanations_are_missing(store):
    """A queue full of unexplained cases has three different causes that look
    identical from the queue itself: a build predating the column, a scoring job
    not publishing features, or nothing consumed at all. The operator has to be
    able to read which."""
    store.add(ALERT)                       # no "features" key -> NO_FEATURES
    store.flush()
    s = store.stats()
    assert s["_explanation"] == {"NO_FEATURES": 1}
    assert s["_last_opened"] is not None
