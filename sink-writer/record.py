"""
Pure mapping of a scored event (the JSON the Flink job emits) into storage rows.
No I/O here, so it is fully unit-testable. Column orders match the ClickHouse
schema in infra/clickhouse/init/01-schema.sql.
"""

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
    """Unix seconds (float) -> datetime, for the latency stamps.

    Missing stamps become the epoch rather than "now": a zero is visibly wrong
    in a latency query, whereas a plausible-looking `now` would silently report
    a few milliseconds and hide the fact that nothing was measured.
    """
    try:
        return datetime.fromtimestamp(float(v), timezone.utc)
    except (TypeError, ValueError, OSError, OverflowError):
        return _EPOCH


# --- fraud.transactions_scored ---------------------------------------------
SCORED_COLUMNS = [
    "transaction_id", "event_time", "sender_card", "receiver_card", "amount_uzs",
    "channel", "sender_region", "receiver_region", "is_new_payee",
    "cep_score", "ml_score", "final_score", "decision",
    "predicted_type", "model_version",
    "active_call", "secs_login_to_confirm", "secs_login_z",
    "ingested_at", "scored_at_job", "scoring_ms",
]

def scored_row(e: dict) -> list:
    return [
        e.get("transaction_id", "") or "",
        _dt(e.get("event_time")),
        e.get("sender_card", "") or "",
        e.get("receiver_card", "") or "",
        _i(e.get("amount_uzs")),
        e.get("channel", "") or "",
        e.get("sender_region", "") or "",
        e.get("receiver_region", "") or "",
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
    ]


# --- fraud.audit_log (append-only / WORM) ----------------------------------
# The record content, in schema order. The chain columns (seq, prev_hash,
# record_hash) are appended by the writer, which is where the chain state lives.
AUDIT_CORE_COLUMNS = [
    "transaction_id", "event_time", "decision", "final_score",
    "model_version", "rule_hits", "payload", "ingress_hash",
]
AUDIT_CHAIN_COLUMNS = ["seq", "prev_hash", "record_hash"]
AUDIT_COLUMNS = AUDIT_CORE_COLUMNS + AUDIT_CHAIN_COLUMNS

# Positions of the two columns the chain hash binds, derived from the column
# list rather than written as literals: reordering the columns then cannot
# silently change what is signed.
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
        # Computed at ingress by the producer, carried through Flink untouched.
        # Empty when a producer predates the integrity chain — visible in a
        # verify pass rather than silently treated as valid.
        e.get("ingress_hash", "") or "",
    ]


def audit_signed_values(core: list) -> list:
    """The values the chain hash actually binds: [ingress_hash, payload].

    The payload is the authoritative JSON snapshot of the whole event, so hashing
    it covers decision, score, rule hits and every id at once. The flat columns
    (decision, final_score, ...) are a queryable PROJECTION of the payload, not a
    second source of truth — the verifier re-derives them from the payload and
    checks they match, so editing a flat column without editing the payload is
    caught too.

    Only strings enter the hash. That matters for round-trip: ClickHouse returns
    a String column byte-for-byte, whereas final_score is Float32 and would come
    back as 0.9300000071 — hashing typed columns directly would make every record
    fail verification after a read.

    Takes the audit ROW, not the event, on purpose: the hash has to cover
    exactly the values that will be written and read back, so it is taken from
    the row `audit_core` produced rather than recomputed from the event beside
    it. The two agree today; taking them from one place is what keeps them
    agreeing.

    This function had been defined and called by nowhere, while the writer
    indexed the same two columns itself and the verifier looked them up by name.
    The contract was therefore stated in three places and executed in two, and
    the one that named it could drift from the other two in silence.
    """
    return [core[_INGRESS_IDX], core[_PAYLOAD_IDX]]
    return [e.get("ingress_hash", "") or "",
            json.dumps(e, ensure_ascii=False, separators=(",", ":"))]


# --- Neo4j alert graph ------------------------------------------------------
def is_alert(e: dict) -> bool:
    return e.get("decision") not in (None, "ALLOW")


def alert_params(e: dict) -> dict:
    return {
        "txid": e.get("transaction_id", "") or "",
        "sender": e.get("sender_pinfl", "") or "",
        "receiver": e.get("receiver_pinfl", "") or "",
        "amount": _i(e.get("amount_uzs")),
        "event_time": e.get("event_time") or "",
        "final_score": _f(e.get("final_score")),
        "decision": e.get("decision", "") or "",
        "ptype": e.get("predicted_type") or "UNKNOWN",
        "rule_hits": list(e.get("rule_hits") or []),
    }
