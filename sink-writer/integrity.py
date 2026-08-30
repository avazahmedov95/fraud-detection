"""
Tamper-evidence for the audit trail: an ingress hash and a hash chain.

Two independent guarantees, because they defend different things:

  * ingress_hash binds a stored decision to the exact event that produced it.
    Computed by the producer over the raw fields, carried through Flink
    untouched, stored in the audit row. An auditor holding the original event
    (from the raw archive) can recompute it and confirm the decision was made on
    that event and not a substituted one.

  * record_hash chains the audit records: each record's hash folds in the
    previous record's hash, so altering, deleting or reordering any record
    breaks every link after it. ClickHouse WORM grants make the log
    append-only; the chain makes a bypass of those grants *evident*.

Why a chain and not a per-row hash: a standalone hash is recomputed by whoever
edits the row, so it proves nothing against someone who can write the table. A
chain forces an attacker who edits one record to recompute all later ones, and a
periodically published head hash (external anchor — see verify_audit.py) closes
even that.

THIS FILE IS DUPLICATED in data-generator/ and sink-writer/ because they deploy
as separate units with no shared package. The two copies MUST stay byte-identical
— test_integrity.py in each pins the same known-answer vector, so a drift in
either copy fails a test rather than silently producing an unverifiable chain.
"""

import hashlib

# Raw fields covered by the ingress hash, in this exact order. The producer and
# any verifier must agree on the list and the order, so changing it is a
# breaking change to every hash already stored. These are the fields the switch
# emits and that survive unchanged through the pipeline.
INGRESS_FIELDS = (
    "transaction_id", "event_time",
    "sender_pinfl", "sender_card", "receiver_pinfl", "receiver_card",
    "amount_uzs", "channel", "sender_region", "receiver_region",
)

GENESIS = "0" * 64          # prev_hash of the first record in a chain


def _canonical(values) -> bytes:
    """Deterministic bytes from an ordered list of values.

    Unit-separated (0x1f) rather than JSON: no key ordering can enter the bytes,
    numbers cannot pick up incidental float formatting, and the separator cannot
    occur in any of the string fields here (ids, cards, ISO timestamps, region
    names). Every value is normalised to str first so 9000000 and "9000000" hash
    alike — the producer sends an int, a verifier reading CSV sees a string.
    """
    parts = []
    for v in values:
        parts.append("" if v is None else str(v))
    return "\x1f".join(parts).encode("utf-8")


def ingress_hash(event: dict) -> str:
    """SHA-256 over the raw event's INGRESS_FIELDS."""
    values = [event.get(f) for f in INGRESS_FIELDS]
    return hashlib.sha256(_canonical(values)).hexdigest()


def record_hash(prev_hash: str, seq: int, core_values) -> str:
    """Chain link: SHA-256(prev_hash || seq || the record's content).

    `core_values` is the ordered content of the audit record (everything except
    the chain columns themselves). Folding `seq` in as well means a record cannot
    be moved to a different position without changing its hash.
    """
    h = hashlib.sha256()
    h.update((prev_hash or GENESIS).encode("ascii"))
    h.update(b"\x1f")
    h.update(str(seq).encode("ascii"))
    h.update(b"\x1f")
    h.update(_canonical(core_values))
    return h.hexdigest()
