"""A decision (the JSON the job emits) as ClickHouse rows; no I/O. Column orders
match infra/clickhouse/init/."""

import json
from datetime import datetime, timezone


def _dt(iso):
    if not iso:
        return datetime.now(timezone.utc)
    try:
        return datetime.fromisoformat(iso)
    except ValueError:
        return datetime.now(timezone.utc)


def _i(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return 0


def _f(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _b(v):
    return 1 if v else 0


_EPOCH = datetime.fromtimestamp(0, timezone.utc)


def _epoch_dt(v):
    """Unix seconds -> datetime; a missing stamp is the epoch, never a plausible now."""
    try:
        return datetime.fromtimestamp(float(v), timezone.utc)
    except (TypeError, ValueError, OSError, OverflowError):
        return _EPOCH


#: The job's stages (fraud_job.py), one column each.
STAGES = ("kafka", "handoff", "decode", "state", "redis", "rules", "model", "decide")
STAGE_COLUMNS = [f"stage_{s}_ms" for s in STAGES]

SCORED_COLUMNS = [
    "transaction_id", "event_time", "sender_card", "receiver_card", "amount_uzs",
    "sender_region", "is_new_payee",
    "cep_score", "ml_score", "final_score", "decision",
    "predicted_type", "model_version",
    "active_call", "secs_login_to_confirm", "secs_login_z",
    "ingested_at", "scored_at_job", "scoring_ms",
] + STAGE_COLUMNS + ["features"]


def _stage_times(e: dict) -> list:
    """One value per stage; NULL where the job did not time it, never 0."""
    stages = e.get("stage_ms") or {}
    out = []
    for s in STAGES:
        try:
            out.append(float(stages[s]))
        except (KeyError, TypeError, ValueError):
            out.append(None)
    return out


def scored_row(e: dict) -> list:
    return [
        e.get("transaction_id", "") or "",
        _dt(e.get("event_time")),
        e.get("sender_card", "") or "",
        e.get("receiver_card", "") or "",
        _i(e.get("amount_uzs")),
        e.get("sender_region", "") or "",
        _b(e.get("is_new_payee")),
        _f(e.get("cep_score")),
        _f(e.get("ml_score")),               # None -> 0.0 (model-down)
        _f(e.get("final_score")),
        e.get("decision", "") or "",
        e.get("predicted_type") or "",       # None -> "" for LowCardinality(String)
        e.get("model_version", "") or "",
        _b(e.get("active_call")),
        _f(e.get("secs_login_to_confirm")),
        _f(e.get("secs_login_z")),
        _epoch_dt(e.get("ingested_at")),
        _epoch_dt(e.get("scored_at_job")),
        _f(e.get("scoring_ms")),
    ] + _stage_times(e) + [
        # A feature the job did not compute is NaN, as the model reads it.
        [float("nan") if v is None else float(v) for v in e.get("features") or []],
    ]


# fraud.audit_log is append-only; the writer adds the chain columns.
AUDIT_CORE_COLUMNS = [
    "transaction_id", "event_time", "decision", "final_score",
    "model_version", "rule_hits", "payload", "ingress_hash",
]
AUDIT_CHAIN_COLUMNS = ["seq", "prev_hash", "record_hash"]
AUDIT_COLUMNS = AUDIT_CORE_COLUMNS + AUDIT_CHAIN_COLUMNS

_PAYLOAD_IDX = AUDIT_CORE_COLUMNS.index("payload")
_INGRESS_IDX = AUDIT_CORE_COLUMNS.index("ingress_hash")


def audit_core(e: dict) -> list:
    """Content columns of one audit record (no chain fields yet)."""
    return [
        e.get("transaction_id", "") or "",
        _dt(e.get("event_time")),
        e.get("decision", "") or "",
        _f(e.get("final_score")),
        e.get("model_version", "") or "",
        list(e.get("rule_hits") or []),
        json.dumps(e, ensure_ascii=False, separators=(",", ":")),
        # Stamped by the producer, carried through the job untouched.
        e.get("ingress_hash", "") or "",
    ]


def audit_signed_values(core: list) -> list:
    """What the chain hash binds: the ingress hash and the payload, the record's
    authoritative copy (Float32 columns do not read back byte for byte)."""
    return [core[_INGRESS_IDX], core[_PAYLOAD_IDX]]


def is_alert(e: dict) -> bool:
    """A held transfer; a SECOND_LOOK becomes REVIEW or ALLOW in a later record."""
    return e.get("decision") == "REVIEW"

