"""
Tamper-evidence for the audit trail: an ingress hash and a hash chain.

  * ingress_hash binds a stored decision to the event that produced it. An
    auditor holding the original event recomputes it and confirms the decision
    was made on that event and not a substituted one.

  * record_hash chains the records: each folds in the previous hash, so
    altering, deleting or reordering any record breaks every link after it.
    WORM grants make the log append-only; the chain makes a bypass evident.

A standalone per-row hash would prove nothing against someone who can write the
table - they recompute it. A chain forces recomputing the whole tail, and a
periodically published head hash (verify_audit.py) closes even that.

DUPLICATED in data-generator/ and sink-writer/, which deploy as separate units.
The copies MUST stay byte-identical: test_integrity.py in each pins the same
known-answer vector, so a drift fails a test rather than silently producing an
unverifiable chain.
"""

import hashlib

# Changing this list or its order is a BREAKING change to every hash already
# stored. receiver_pinfl was removed from it on 01.09.2026 when it left the
# wire; see docs/audit-anchors.md, where the anchored head is recorded together
# with the field list it was computed over.
INGRESS_FIELDS = (
    "transaction_id", "event_time",
    "sender_pinfl", "sender_card", "receiver_card",
    "amount_uzs", "channel", "sender_region", "receiver_region",
)

GENESIS = "0" * 64          # prev_hash of the first record in a chain


def _canonical(values) -> bytes:
    """Deterministic bytes from an ordered list of values.

    Unit-separated (0x1f) rather than JSON: no key ordering enters the bytes and
    numbers pick up no incidental float formatting. Values are normalised to str
    so 9000000 and "9000000" hash alike - the producer sends an int, a verifier
    reading CSV sees a string.
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

    Folding `seq` in means a record cannot be moved to a different position
    without changing its hash.
    """
    h = hashlib.sha256()
    h.update((prev_hash or GENESIS).encode("ascii"))
    h.update(b"\x1f")
    h.update(str(seq).encode("ascii"))
    h.update(b"\x1f")
    h.update(_canonical(core_values))
    return h.hexdigest()
