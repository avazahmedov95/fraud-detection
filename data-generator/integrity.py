"""Tamper-evidence for the audit trail: an ingress hash and a hash chain.

`ingress_hash` binds a stored decision to the event that produced it;
`record_hash` folds in the previous hash, so altering, deleting or reordering a
record breaks every link after it. A standalone per-row hash would prove nothing
against someone who can write the table - they recompute it. A chain forces
recomputing the whole tail, and a published head hash (verify_audit.py) closes
even that.

DUPLICATED in data-generator/ and sink-writer/, which deploy separately. The
copies MUST stay byte-identical - a known-answer test in each pins the same
vector, so drift fails a test instead of producing an unverifiable chain.
"""

import hashlib

# BREAKING to change this list or its order: it invalidates every stored hash.
# receiver_pinfl left on 01.09.2026 and receiver_region on 03.09.2026, both
# when they left the wire - docs/audit-anchors.md records each anchored head
# with the field list behind it.
INGRESS_FIELDS = (
    "transaction_id", "event_time",
    "sender_pinfl", "sender_card", "receiver_card",
    "amount_uzs", "channel", "sender_region",
)

GENESIS = "0" * 64          # prev_hash of the first record in a chain


def _canonical(values) -> bytes:
    """Deterministic bytes from an ordered list of values.

    Unit-separated (0x1f) rather than JSON, so no key ordering or float
    formatting enters the bytes. Values become str so 9000000 and "9000000" hash
    alike: the producer sends an int, a verifier reading CSV sees a string.
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
    """Chain link: SHA-256(prev_hash || seq || content).

    `seq` is folded in so a record cannot be moved without changing its hash.
    """
    h = hashlib.sha256()
    h.update((prev_hash or GENESIS).encode("ascii"))
    h.update(b"\x1f")
    h.update(str(seq).encode("ascii"))
    h.update(b"\x1f")
    h.update(_canonical(core_values))
    return h.hexdigest()
