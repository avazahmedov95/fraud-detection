"""
Known-answer vectors for data-generator's copy of integrity.py.

The same vectors appear in sink-writer/test_integrity.py. Equal vectors on both
sides are what guarantee the two copies of integrity.py stay byte-identical
without a runtime dependency between the two deploy units.

Run: python -m pytest test_integrity.py -q
"""

import integrity

EVENT = {
    "transaction_id": "tx-1", "event_time": "2025-01-15T10:30:00",
    "sender_pinfl": "S1", "sender_card": "8600000000000001",
    "receiver_pinfl": "R1", "receiver_card": "9860000000000002",
    "amount_uzs": 9_000_000, "channel": "MOBILE_APP",
    "sender_region": "Tashkent City", "receiver_region": "Andijan",
}

# Changed when receiver_pinfl left the wire: the hash can only bind
# fields the event carries. Both copies of integrity.py must produce
# this same value - that is what these two test files are for.
INGRESS_VECTOR = "80f4245868803adc2b0324e7a0a3b5ef43ec6cfcff45011f752eb3083c1591c6"
RECORD_VECTOR = "0b78fd33b284062eb5b9f6dc32f1c4507ed4fbb4c1d00c2a48cc4b885c964cad"


def test_ingress_hash_known_answer():
    assert integrity.ingress_hash(EVENT) == INGRESS_VECTOR
    assert integrity.ingress_hash(dict(EVENT)) == INGRESS_VECTOR


def test_record_hash_known_answer():
    assert integrity.record_hash("prev", 5, ["ing", "payload"]) == RECORD_VECTOR


def test_ingress_hash_ignores_unlisted_fields():
    noisy = dict(EVENT, ingested_at=123.456, label_is_fraud=1, ingress_hash="x")
    assert integrity.ingress_hash(noisy) == integrity.ingress_hash(EVENT)
