"""Unit tests for the sink-writer mapping + batching. Run: python test_record.py"""

import json

import record as R
from ch_writer import ClickHouseWriter
from neo4j_writer import Neo4jWriter

SCORED = {
    "transaction_id": "tx-1", "event_time": "2025-01-15T10:30:00",
    "sender_card": "8600...1", "receiver_card": "9860...2",
    "sender_pinfl": "S1", "receiver_pinfl": "R1",
    "amount_uzs": 9_000_000, "channel": "MOBILE_APP",
    "sender_region": "Tashkent City", "receiver_region": "Andijan",
    "is_new_payee": True,
    "cep_score": 0.5, "ml_score": 0.93, "final_score": 0.93,
    "decision": "BLOCK", "predicted_type": "APP",
    "rule_hits": ["NEW_PAYEE_HIGH_AMOUNT", "FRESH_RECEIVER"],
    "model_version": "cep+ml-fusion-v1",
}
ALLOW = {**SCORED, "transaction_id": "tx-2", "decision": "ALLOW",
         "predicted_type": None, "ml_score": None, "rule_hits": []}


def test_scored_row_matches_columns():
    row = R.scored_row(SCORED)
    assert len(row) == len(R.SCORED_COLUMNS)
    d = dict(zip(R.SCORED_COLUMNS, row))
    assert d["amount_uzs"] == 9_000_000 and isinstance(d["amount_uzs"], int)
    assert d["is_new_payee"] == 1
    assert d["decision"] == "BLOCK"


def test_ml_score_none_becomes_zero():
    d = dict(zip(R.SCORED_COLUMNS, R.scored_row(ALLOW)))
    assert d["ml_score"] == 0.0           # ClickHouse Float32 has no NULL
    assert d["predicted_type"] == ""      # None -> empty for LowCardinality


def test_audit_payload_is_full_json():
    row = dict(zip(R.AUDIT_CORE_COLUMNS, R.audit_core(SCORED)))
    assert row["rule_hits"] == ["NEW_PAYEE_HIGH_AMOUNT", "FRESH_RECEIVER"]
    assert json.loads(row["payload"])["transaction_id"] == "tx-1"


def test_is_alert():
    assert R.is_alert(SCORED) is True
    assert R.is_alert(ALLOW) is False


def test_alert_params_links_by_pinfl():
    p = R.alert_params(SCORED)
    assert p["sender"] == "S1" and p["receiver"] == "R1"
    assert p["decision"] == "BLOCK" and p["ptype"] == "APP"


# --- batching, with injected fake clients (no real ClickHouse/Neo4j) ---
class _FakeCH:
    def __init__(self): self.inserts = []
    def insert(self, table, rows, column_names=None): self.inserts.append((table, len(rows)))
    def close(self): pass


class _FakeSession:
    def __init__(self, sink): self._sink = sink
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def run(self, q, rows=None): self._sink.append(len(rows) if rows is not None else 0)


class _FakeDriver:
    def __init__(self): self.runs = []
    def session(self): return _FakeSession(self.runs)
    def close(self): pass


def test_ch_writer_batches_scored_and_audit():
    w = ClickHouseWriter("h", 1, "u", "p", "fraud", audit_all=True)
    w._client = _FakeCH()
    for _ in range(3):
        w.add(SCORED)
    assert w.pending() == 3
    w.flush()
    tables = {t for t, _ in w._client.inserts}
    assert tables == {"fraud.transactions_scored", "fraud.audit_log"}
    assert all(n == 3 for _, n in w._client.inserts)
    assert w.pending() == 0


def test_neo4j_writer_only_buffers_alerts():
    w = Neo4jWriter("bolt://x", "u", "p")
    w._driver = _FakeDriver()
    w.add(SCORED)   # alert
    w.add(ALLOW)    # not an alert
    assert w.pending() == 1
    w.flush()
    assert w._driver.runs == [1]
    assert w.pending() == 0


# --- schema alignment -------------------------------------------------------
# Rows are inserted positionally, so a column added to one side only shifts every
# value after it into the wrong column. ClickHouse accepts that silently when the
# types happen to line up, which makes it a data-corruption bug rather than an
# error. These tests keep the two definitions honest.

import os
import re

_SCHEMA = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                       "..", "infra", "clickhouse", "init", "01-schema.sql")


def _schema_columns(table):
    sql = open(_SCHEMA, encoding="utf-8").read()
    body = sql.split(f"CREATE TABLE IF NOT EXISTS fraud.{table}")[1]
    body = body.split("ENGINE")[0]
    cols = []
    for line in body.splitlines():
        line = line.strip()
        if not line or line.startswith("--") or line in ("(", ")"):
            continue
        m = re.match(r"^(\w+)\s+\S", line)
        if m:
            cols.append(m.group(1))
    return cols


def test_scored_columns_match_the_schema_in_order():
    # `scored_at` is filled by the column DEFAULT at insert time, so the writer
    # deliberately does not send it.
    schema = [c for c in _schema_columns("transactions_scored") if c != "scored_at"]
    assert schema == R.SCORED_COLUMNS


def test_audit_columns_match_the_schema_in_order():
    schema = [c for c in _schema_columns("audit_log")
              if c not in ("audit_id", "recorded_at")]
    assert schema == R.AUDIT_COLUMNS


def test_row_builders_emit_one_value_per_column():
    assert len(R.scored_row(SCORED)) == len(R.SCORED_COLUMNS)
    # audit_core produces the content columns; the writer appends the 3 chain
    # columns to reach the full AUDIT_COLUMNS.
    assert len(R.audit_core(SCORED)) == len(R.AUDIT_CORE_COLUMNS)
    assert len(R.AUDIT_COLUMNS) == len(R.AUDIT_CORE_COLUMNS) + 3


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print(f"PASS  {fn.__name__}")
    print(f"\n{len(fns)} tests passed")
