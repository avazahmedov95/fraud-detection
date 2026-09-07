"""Known-answer vectors for data-generator's copy of integrity.py.

The same vectors appear in sink-writer/test_integrity.py: equal vectors on both
sides keep the two copies byte-identical without a runtime dependency.
"""

import integrity

EVENT = {
    "transaction_id": "tx-1", "event_time": "2025-01-15T10:30:00",
    "sender_pinfl": "S1", "sender_card": "8600000000000001",
    "receiver_pinfl": "R1", "receiver_card": "9860000000000002",
    "amount_uzs": 9_000_000,
    "sender_region": "Tashkent City",
}

# Re-anchored whenever a field leaves the wire, because the hash binds only
# what the event carries: receiver_pinfl (01.09), receiver_region (03.09),
# channel (07.09). These tests FAILING on such a change is the point of them.
# Both copies of integrity.py must produce these same values.
INGRESS_VECTOR = "2a1db4b7be624d44b152ad0eb1dc41d293db2a0955057477304ed6dd9173fb43"
RECORD_VECTOR = "0b78fd33b284062eb5b9f6dc32f1c4507ed4fbb4c1d00c2a48cc4b885c964cad"


def test_ingress_hash_known_answer():
    assert integrity.ingress_hash(EVENT) == INGRESS_VECTOR
    assert integrity.ingress_hash(dict(EVENT)) == INGRESS_VECTOR


def test_record_hash_known_answer():
    assert integrity.record_hash("prev", 5, ["ing", "payload"]) == RECORD_VECTOR


def test_ingress_hash_ignores_unlisted_fields():
    noisy = dict(EVENT, ingested_at=123.456, label_is_fraud=1, ingress_hash="x")
    assert integrity.ingress_hash(noisy) == integrity.ingress_hash(EVENT)
