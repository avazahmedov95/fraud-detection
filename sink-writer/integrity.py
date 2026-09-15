"""Tamper-evidence for the audit trail: an ingress hash and a hash chain.

`ingress_hash` binds a stored decision to the event that produced it;
`record_hash` folds in the previous hash, so altering, deleting or reordering a
record breaks every later link, and a published head hash (verify_audit.py)
anchors the chain.

The copies in data-generator/ and sink-writer/ must stay byte-identical.
"""

import hashlib

# BREAKING to change this list or its order: it invalidates every stored hash
# (docs/audit-anchors.md records each anchored head with its field list).
INGRESS_FIELDS = (
    "transaction_id", "event_time",
    "sender_pinfl", "sender_card", "receiver_card",
    "amount_uzs", "sender_region",
)

GENESIS = "0" * 64          # prev_hash of the first record in a chain


def _canonical(values) -> bytes:
    """Deterministic bytes from an ordered list of values: unit-separated (0x1f), not
    JSON, and str() so 9000000 and "9000000" hash alike."""
    parts = []
    for v in values:
        parts.append("" if v is None else str(v))
    return "\x1f".join(parts).encode("utf-8")


def ingress_hash(event: dict) -> str:
    """SHA-256 over the raw event's INGRESS_FIELDS."""
    values = [event.get(f) for f in INGRESS_FIELDS]
    return hashlib.sha256(_canonical(values)).hexdigest()


def record_hash(prev_hash: str, seq: int, core_values) -> str:
    """Chain link: SHA-256(prev_hash || seq || content); `seq` pins the position."""
    h = hashlib.sha256()
    h.update((prev_hash or GENESIS).encode("ascii"))
    h.update(b"\x1f")
    h.update(str(seq).encode("ascii"))
    h.update(b"\x1f")
    h.update(_canonical(core_values))
    return h.hexdigest()
