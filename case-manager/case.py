"""Pure mapping of an alert into a case row, and of a verdict into its
replacement. Column order matches infra/clickhouse/init/02-cases.sql."""

from datetime import datetime, timezone

#: NEW is the open state; the other two are terminal retrain labels.
DISPOSITIONS = ("NEW", "CONFIRMED_FRAUD", "FALSE_POSITIVE")

#: Versions written when a case is OPENED. Never wall clock - see the engine note in
#: 02-cases.sql: a redelivered alert must never outrank a resolution, and a resolution
#: is versioned by epoch milliseconds. Two of them because ReplacingMergeTree keeps only
#: the HIGHEST version and an arbitrary row among ties: a re-alert (the producer
#: replaying a CSV it already sent) raced an open row carrying an explanation against an
#: older one carrying none - 16 cases picked up their explanation on a replay and 212 did
#: not. An explanation is strictly more information about the same event, so it wins
#: deterministically; both remain astronomically below any resolution version.
OPEN_VERSION = 0
OPEN_VERSION_EXPLAINED = 1

_EPOCH = datetime.fromtimestamp(0, timezone.utc)


def _dt(iso):
    if not iso:
        return _EPOCH
    try:
        return datetime.fromisoformat(iso)
    except ValueError:
        return _EPOCH


def _epoch_dt(v):
    try:
        return datetime.fromtimestamp(float(v), timezone.utc)
    except (TypeError, ValueError, OSError, OverflowError):
        return _EPOCH


def priority_of(alert: dict) -> int:
    """0 = work this first. The BAND only - deliberately not the score.

    Bucketing the score inside each band ordered nothing: 89.1% of alerts carry a
    model probability that rounds to 1.000 and only 35 distinct rounded values exist
    across the whole alert set, so every case landed on priority 0 and the queue
    degenerated to arrival order. The queue orders by EXPOSURE after the band instead
    (see CaseStore.open_cases); amount varies over four orders of magnitude. A BLOCK
    still outranks every REVIEW - a blocked transfer has a customer waiting on it."""
    return 0 if alert.get("decision") == "BLOCK" else 1


CASE_COLUMNS = [
    "case_id", "transaction_id", "event_time", "opened_at",
    "sender_card", "receiver_card", "amount_uzs", "final_score",
    "decision", "predicted_type", "rule_hits", "priority",
    "disposition", "resolved_by", "resolved_at", "version",
    "explanation", "explanation_status",
]


def case_row(alert: dict, explanation=None, explanation_status="") -> list:
    """One alert -> one case row, deterministically. The alert topic is AT_LEAST_ONCE,
    so this runs more than once per alert and every field must come out identical - else
    the duplicate is a row ReplacingMergeTree cannot collapse and the queue shows the case
    twice. Hence `opened_at` from the pipeline's own stamp rather than now()."""
    # Explanation is passed in, not computed here: store.py owns the Explainer.
    return [
        alert.get("transaction_id", "") or "",     # case_id: one case per alert
        alert.get("transaction_id", "") or "",
        _dt(alert.get("event_time")),
        _epoch_dt(alert.get("scored_at_job")),     # when the decision existed
        alert.get("sender_card", "") or "",
        alert.get("receiver_card", "") or "",
        int(alert.get("amount_uzs") or 0),
        float(alert.get("final_score") or 0.0),
        alert.get("decision", "") or "",
        alert.get("predicted_type") or "",
        list(alert.get("rule_hits") or []),
        priority_of(alert),
        "NEW",
        "",
        _EPOCH,
        OPEN_VERSION_EXPLAINED if explanation else OPEN_VERSION,
        list(explanation or []),
        explanation_status or "",
    ]


def resolution_row(case: dict, disposition: str, by: str, at_epoch: float) -> list:
    """An existing case, re-written with a verdict: ClickHouse has no row update
    and ReplacingMergeTree collapses by ORDER BY key on merge. The caller reads the
    current case and passes it back in, so a resolution cannot be written for a case
    that was never opened."""
    if disposition not in DISPOSITIONS or disposition == "NEW":
        raise ValueError(
            f"{disposition!r} is not a terminal disposition; expected one of "
            f"{[d for d in DISPOSITIONS if d != 'NEW']}")
    if not by:
        raise ValueError(
            "a resolution must name who made it: the disposition is a label a "
            "model may later be retrained on, and an unattributed label cannot "
            "be audited or withdrawn")
    row = [case[c] for c in CASE_COLUMNS]
    row[CASE_COLUMNS.index("disposition")] = disposition
    row[CASE_COLUMNS.index("resolved_by")] = by
    row[CASE_COLUMNS.index("resolved_at")] = _epoch_dt(at_epoch)
    # Strictly greater than OPEN_VERSION for any real timestamp, so a replayed
    # alert re-inserting the open row can never win the merge.
    row[CASE_COLUMNS.index("version")] = int(at_epoch * 1000)
    return row
