"""A hold as a case row, and a verdict or a report as a newer row; no I/O. Column
order matches infra/clickhouse/init/02-cases.sql."""

from datetime import datetime, timezone
from statistics import median

#: NEW: still held; CONFIRMED_FRAUD blocks it, and the money stays with the payer;
#: FALSE_POSITIVE releases it. The verdicts are the labels the model retrains on.
DISPOSITIONS = ("NEW", "CONFIRMED_FRAUD", "FALSE_POSITIVE")

#: An opened case's versions: never the clock, so a redelivered hold cannot outrank
#: a verdict (versioned in epoch ms). The explained row wins a tie.
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


CASE_COLUMNS = [
    "case_id", "transaction_id", "event_time", "opened_at",
    "sender_card", "receiver_card", "amount_uzs", "final_score",
    "decision", "predicted_type", "rule_hits",
    "disposition", "resolved_by", "resolved_at", "version",
    "explanation", "explanation_status", "model_version", "ml_score",
]


def case_row(alert: dict, explanation=None, explanation_status="") -> list:
    """One hold -> one case row, the same on redelivery (so duplicates collapse in
    ReplacingMergeTree): `opened_at` is the job's stamp, not now()."""
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
        "NEW",
        "",
        _EPOCH,
        OPEN_VERSION_EXPLAINED if explanation else OPEN_VERSION,
        list(explanation or []),
        explanation_status or "",
        alert.get("model_version") or "",           # the second look's, when it held it
        alert.get("ml_score"),                      # the served model's, under TabPFN's
    ]


def resolution_row(case: dict, disposition: str, by: str, at_epoch: float) -> list:
    """An existing case, re-written with a verdict (ClickHouse has no row update).
    The caller passes the current case, so an unopened case cannot be resolved."""
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
    # Above the open versions, so a redelivered hold never wins the merge.
    row[CASE_COLUMNS.index("version")] = int(at_epoch * 1000)
    return row


#: What a client's report reads of the transfer, from fraud.transactions_scored.
REPORT_SOURCE = ["transaction_id", "event_time", "sender_card", "receiver_card",
                 "amount_uzs", "final_score", "decision", "predicted_type",
                 "model_version", "ml_score"]


def report_row(transfer: dict, by: str, at_epoch: float) -> list:
    """A transfer the system let go, reported as fraud by the client: a case opened
    and confirmed at once. It keeps the system's own decision, which is what keeps it
    out of the queue's hold figures and precision: nobody held it."""
    if not by:
        raise ValueError("a report must name who recorded it")
    at = _epoch_dt(at_epoch)
    row = dict(transfer, case_id=transfer["transaction_id"], opened_at=at,
               rule_hits=[], disposition="CONFIRMED_FRAUD", resolved_by=by,
               resolved_at=at, version=int(at_epoch * 1000), explanation=[],
               explanation_status="")
    return [row[c] for c in CASE_COLUMNS]


def _seconds(dt):
    """A stored DateTime as epoch seconds; the client returns it naive, in UTC."""
    return (dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt).timestamp()


def held_seconds(case: dict, now: float) -> float:
    """How long the transfer was held: from the decision until the verdict, or until
    `now` while nobody has decided."""
    end = now if case["disposition"] == "NEW" else _seconds(case["resolved_at"])
    return max(0.0, end - _seconds(case["opened_at"]))


def hold_summary(cases, now) -> dict:
    """Per disposition: how many, how much money, the median and the longest hold.
    The released are honest customers kept waiting - what holding costs; the
    blocked are the fraud money it kept."""
    out = {}
    for d in DISPOSITIONS:
        group = [c for c in cases if c["disposition"] == d]
        waits = [held_seconds(c, now) for c in group]
        out[d] = {"n": len(group), "amount": sum(int(c["amount_uzs"]) for c in group),
                  "median_s": median(waits) if waits else None,
                  "max_s": max(waits) if waits else None}
    return out
