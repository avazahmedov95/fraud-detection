"""
Unit tests for the audit integrity chain.

The known-answer vectors are the mechanism that keeps the two copies of
integrity.py (here and in data-generator/) in step: both files carry this test,
and both must produce the same fixed hex for the same input. If a copy drifts, a
vector fails rather than the chain silently becoming unverifiable.

Run: python -m pytest test_integrity.py -q
"""

import pytest

import integrity
import record as R
from verify_audit import verify


EVENT = {
    "transaction_id": "tx-1", "event_time": "2025-01-15T10:30:00",
    "sender_pinfl": "S1", "sender_card": "8600000000000001",
    "receiver_pinfl": "R1", "receiver_card": "9860000000000002",
    "amount_uzs": 9_000_000, "channel": "MOBILE_APP",
    "sender_region": "Tashkent City", "receiver_region": "Andijan",
}


# --- known-answer vectors (pin the two copies together) ---------------------

# Frozen vectors. These exact strings must also appear in
# data-generator/test_integrity.py; equal vectors on both sides prove the two
# integrity.py copies agree without a runtime dependency between them.
# Changed when receiver_pinfl left the wire: the hash can only bind
# fields the event carries. Both copies of integrity.py must produce
# this same value - that is what these two test files are for.
INGRESS_VECTOR = "80f4245868803adc2b0324e7a0a3b5ef43ec6cfcff45011f752eb3083c1591c6"
RECORD_VECTOR = "0b78fd33b284062eb5b9f6dc32f1c4507ed4fbb4c1d00c2a48cc4b885c964cad"


def test_ingress_hash_known_answer():
    """Fixed input -> fixed hex. If this changes, every stored hash is invalid,
    and the two copies of integrity.py have diverged."""
    assert integrity.ingress_hash(EVENT) == INGRESS_VECTOR
    assert integrity.ingress_hash(dict(EVENT)) == INGRESS_VECTOR   # order-independent


def test_record_hash_known_answer():
    assert integrity.record_hash("prev", 5, ["ing", "payload"]) == RECORD_VECTOR


def test_ingress_hash_ignores_unlisted_fields():
    """ingested_at, labels and the hash itself must not feed the hash."""
    noisy = dict(EVENT, ingested_at=123.456, label_is_fraud=1, ingress_hash="x")
    assert integrity.ingress_hash(noisy) == integrity.ingress_hash(EVENT)


def test_ingress_hash_changes_when_a_signed_field_changes():
    tampered = dict(EVENT, amount_uzs=1_000)
    assert integrity.ingress_hash(tampered) != integrity.ingress_hash(EVENT)


def test_int_and_str_amount_hash_alike():
    """The producer sends an int; a CSV verifier sees a string. Same hash."""
    assert (integrity.ingress_hash(dict(EVENT, amount_uzs=9_000_000))
            == integrity.ingress_hash(dict(EVENT, amount_uzs="9000000")))


def test_record_hash_depends_on_every_input():
    base = integrity.record_hash("prev", 5, ["ing", "payload"])
    assert base != integrity.record_hash("PREV", 5, ["ing", "payload"])
    assert base != integrity.record_hash("prev", 6, ["ing", "payload"])
    assert base != integrity.record_hash("prev", 5, ["ing", "PAYLOAD"])


# --- chain construction, mirroring the writer -------------------------------

def _build_chain(events):
    """Reproduce what ClickHouseWriter.add does, without ClickHouse."""
    rows = []
    seq, prev = 0, integrity.GENESIS
    pay_i = R.AUDIT_CORE_COLUMNS.index("payload")
    ing_i = R.AUDIT_CORE_COLUMNS.index("ingress_hash")
    for e in events:
        core = R.audit_core(e)
        rh = integrity.record_hash(prev, seq, [core[ing_i], core[pay_i]])
        rows.append({
            "seq": seq, "prev_hash": prev, "record_hash": rh,
            "ingress_hash": core[ing_i], "payload": core[pay_i],
            "decision": e.get("decision", ""),
            "transaction_id": e.get("transaction_id", ""),
        })
        seq, prev = seq + 1, rh
    return rows


def _events(n):
    return [dict(EVENT, transaction_id=f"tx-{i}", decision="BLOCK",
                 final_score=0.9, ingress_hash=integrity.ingress_hash(EVENT))
            for i in range(n)]


def test_clean_chain_verifies():
    assert verify(_build_chain(_events(10))) == []


def test_altered_payload_is_caught():
    chain = _build_chain(_events(10))
    chain[4]["payload"] = chain[4]["payload"].replace("BLOCK", "ALLOW")
    findings = verify(chain)
    assert any("record_hash does not recompute" in f for f in findings)


def test_deleted_record_is_caught():
    chain = _build_chain(_events(10))
    del chain[5]
    findings = verify(chain)
    assert findings                                     # gap and/or broken link


def test_reordered_records_are_caught():
    chain = _build_chain(_events(10))
    chain[3], chain[4] = chain[4], chain[3]
    assert verify(chain)


def test_edited_flat_column_is_caught():
    """Change the queryable decision column but not the signed payload."""
    chain = _build_chain(_events(10))
    chain[6]["decision"] = "ALLOW"
    findings = verify(chain)
    assert any("disagrees with payload" in f for f in findings)


def test_missing_ingress_hash_still_chains():
    """A pre-integrity producer sends no ingress_hash; the chain still forms and
    verifies, the ingress binding is simply absent."""
    events = [dict(EVENT, transaction_id=f"tx-{i}", decision="ALLOW")
              for i in range(5)]
    assert verify(_build_chain(events)) == []
