"""Tests for the case row and the properties the queue depends on."""

import os
import re

import pytest

import case as CASE

ALERT = {
    "transaction_id": "t_0041237",
    "event_time": "2026-03-14T19:22:41",
    "scored_at_job": 1_772_000_000.5,
    "sender_card": "8600330000000001",
    "receiver_card": "8600030000000002",
    "amount_uzs": 4_800_000,
    "final_score": 0.91,
    "decision": "BLOCK",
    "predicted_type": "APP",
    "rule_hits": ["FRESH_RECEIVER", "NEW_PAYEE_HIGH_AMOUNT"],
}


def _as_dict(row):
    return dict(zip(CASE.CASE_COLUMNS, row))


# --- determinism, which idempotence rests on --------------------------------

def test_the_same_alert_produces_the_same_row():
    """AT_LEAST_ONCE means this function runs more than once per alert. If any
    field came from now(), the duplicate would be a second row the merge cannot
    collapse and the analyst would see one case twice."""
    assert CASE.case_row(ALERT) == CASE.case_row(dict(ALERT))


def test_opened_at_comes_from_the_pipeline_not_the_clock():
    row = _as_dict(CASE.case_row(ALERT))
    assert row["opened_at"].timestamp() == pytest.approx(ALERT["scored_at_job"])


# --- the versioning property that protects a human's work -------------------

def test_open_writes_version_zero():
    assert _as_dict(CASE.case_row(ALERT))["version"] == CASE.OPEN_VERSION == 0


def test_an_explained_open_outranks_an_unexplained_one():
    """The same transaction is re-alerted whenever the producer replays a CSV it
    has already sent. If both open rows carried the same version, which one
    survived the merge would be luck - and it was: 16 cases picked up an
    explanation on replay while 212 did not. More information about the same
    event must win deterministically."""
    bare = _as_dict(CASE.case_row(ALERT))
    explained = _as_dict(CASE.case_row(ALERT, ["amount: big (+9.9)"], "OK"))
    assert explained["version"] > bare["version"]
    assert explained["case_id"] == bare["case_id"]


def test_an_explained_open_still_loses_to_a_resolution():
    """The whole reason open versions are tiny constants. Bumping one must not
    let a replay overwrite a human's verdict."""
    bare = _as_dict(CASE.case_row(ALERT))
    resolved = _as_dict(CASE.resolution_row(
        bare, "CONFIRMED_FRAUD", "analyst.k", at_epoch=1_772_000_100.0))
    explained = _as_dict(CASE.case_row(ALERT, ["amount: big (+9.9)"], "OK"))
    assert resolved["version"] > explained["version"]


def test_a_replayed_alert_cannot_revert_a_resolution():
    """The property the whole engine choice exists for.

    ReplacingMergeTree keeps the highest version per case_id. An alert
    redelivered after an analyst resolved its case re-inserts the OPEN row; if
    that row could outrank the resolution, the verdict would silently vanish and
    the case would reappear in the queue as unworked.
    """
    opened = _as_dict(CASE.case_row(ALERT))
    resolved = _as_dict(CASE.resolution_row(
        opened, "CONFIRMED_FRAUD", "analyst.k", at_epoch=1_772_000_100.0))
    replayed = _as_dict(CASE.case_row(ALERT))          # same alert, later

    assert resolved["version"] > replayed["version"]
    assert resolved["case_id"] == replayed["case_id"]  # same key, so they race


def test_resolution_keeps_the_case_identity_and_facts():
    opened = _as_dict(CASE.case_row(ALERT))
    resolved = _as_dict(CASE.resolution_row(
        opened, "FALSE_POSITIVE", "analyst.k", at_epoch=1_772_000_100.0))
    for field in ("case_id", "transaction_id", "amount_uzs", "final_score",
                  "decision", "rule_hits", "event_time"):
        assert resolved[field] == opened[field], field
    assert resolved["disposition"] == "FALSE_POSITIVE"
    assert resolved["resolved_by"] == "analyst.k"


# --- what a verdict must carry ----------------------------------------------

def test_a_verdict_must_name_its_author():
    """A disposition is a label a model may be retrained on. An unattributed
    label cannot be audited, questioned, or withdrawn."""
    opened = _as_dict(CASE.case_row(ALERT))
    with pytest.raises(ValueError, match="name who"):
        CASE.resolution_row(opened, "CONFIRMED_FRAUD", "", at_epoch=1.0)


@pytest.mark.parametrize("bad", ["NEW", "MAYBE", "", "confirmed_fraud"])
def test_only_terminal_dispositions_can_be_written(bad):
    opened = _as_dict(CASE.case_row(ALERT))
    with pytest.raises(ValueError):
        CASE.resolution_row(opened, bad, "analyst.k", at_epoch=1.0)


# --- queue order -------------------------------------------------------------

def test_block_outranks_every_review():
    """A blocked transfer has a customer waiting on it; a review does not. The
    cost of delay is a different quantity, so no review score overtakes a
    block."""
    worst_block = CASE.priority_of({"decision": "BLOCK", "final_score": 0.0})
    best_review = CASE.priority_of({"decision": "REVIEW", "final_score": 1.0})
    assert worst_block < best_review


def test_priority_is_the_band_and_nothing_else():
    """Score is deliberately absent from this column.

    The live queue showed 89.1% of alerts carrying a probability that rounds to
    1.000 - only 35 distinct rounded values across the whole alert set - so
    score-within-band ordered nothing and every case arrived at priority 0.
    Ordering by exposure happens in the query instead; see
    CaseStore.open_cases.
    """
    for score in (0.0, 0.42, 0.95, 1.0):
        assert CASE.priority_of({"decision": "BLOCK", "final_score": score}) == 0
        assert CASE.priority_of({"decision": "REVIEW", "final_score": score}) == 1


def test_missing_or_malformed_score_does_not_crash_the_queue():
    for bad in (None, "", "n/a", float("nan")):
        assert isinstance(CASE.priority_of({"decision": "REVIEW",
                                            "final_score": bad}), int)


# --- drift guard -------------------------------------------------------------

def test_columns_match_the_clickhouse_schema():
    """CASE_COLUMNS is passed to insert() as column_names, so a column added to
    the DDL and not here is inserted into the wrong position - silently, since
    ClickHouse will happily coerce a String into a String.

    The DDL is a migration, not just a CREATE: columns added after the table
    first existed arrive through ALTER ... ADD COLUMN, because the file is
    re-applied on every connect and a widened CREATE would do nothing on a
    cluster that already has the table. Both forms are read here.
    """
    ddl = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..",
                       "infra", "clickhouse", "init", "02-cases.sql")
    with open(ddl, encoding="utf-8") as fh:
        sql = fh.read()

    body = sql.split("CREATE TABLE IF NOT EXISTS fraud.cases", 1)[1]
    body = body.split("ENGINE", 1)[0]
    declared = []
    for line in body.splitlines():
        line = line.strip()
        if not line or line.startswith("--") or line in ("(", ")"):
            continue
        m = re.match(r"^([a-z_]+)\s+[A-Za-z]", line)
        if m:
            declared.append(m.group(1))

    declared += re.findall(r"ADD COLUMN IF NOT EXISTS\s+([a-z_]+)\s", sql)
    assert declared == CASE.CASE_COLUMNS
