"""What the producer actually puts on the wire.

csv.DictReader returns every column as a string. Numbers were cast from the
start, booleans were not, and `"active_call": "False"` reads as TRUE to every
consumer testing truthiness. The live job scored active_call = 1 on 100% of
events against a model trained on 3.5%.
"""

import csv
import os

import pytest

import kafka_producer as P

_ROW = {
    "transaction_id": "t-1", "event_time": "2026-03-14T19:22:41",
    "sender_pinfl": "S1", "sender_card": "8600330000000001",
    "sender_network": "UZCARD", "receiver_pinfl": "R1",
    "receiver_card": "8600030000000002", "receiver_network": "UZCARD",
    "amount_uzs": "4800000", "channel": "MOBILE_APP", "device_id": "dev-1",
    "sender_region": "Tashkent City",
    "sender_balance_before": "9000000", "active_call": "False",
    "secs_login_to_confirm": "41.2",
}


def test_booleans_leave_as_booleans():
    msg = P._row_to_message(dict(_ROW), include_labels=False)
    assert msg["active_call"] is False
    assert isinstance(msg["active_call"], bool)


def test_true_survives_too():
    msg = P._row_to_message(dict(_ROW, active_call="True"), include_labels=False)
    assert msg["active_call"] is True


def test_numbers_leave_as_numbers():
    msg = P._row_to_message(dict(_ROW), include_labels=False)
    assert msg["amount_uzs"] == 4_800_000 and isinstance(msg["amount_uzs"], int)
    assert msg["sender_balance_before"] == 9_000_000
    assert msg["secs_login_to_confirm"] == pytest.approx(41.2)


def test_a_malformed_latency_does_not_stop_the_stream():
    """One unparseable cell must not take down a replay of 50,000 rows."""
    msg = P._row_to_message(dict(_ROW, secs_login_to_confirm="n/a"),
                            include_labels=False)
    assert msg["secs_login_to_confirm"] == 0.0


def test_no_field_leaves_as_a_stringified_bool():
    """The general form: a field holding the text of a Python bool was never cast."""
    msg = P._row_to_message(dict(_ROW), include_labels=False)
    offenders = [k for k, v in msg.items()
                 if isinstance(v, str) and v.strip() in ("True", "False")]
    assert not offenders, f"still travelling as text: {offenders}"


def test_the_payee_identity_is_not_on_the_wire():
    """receiver_pinfl must not travel: a card-to-card transfer reaches the sending
    bank as a destination PAN, and the person behind it is a core-banking lookup
    for the bank's own clients only — 6.85% of transfers at the measured market
    concentration — so carrying it asserts knowledge no deployment has, and every
    receiver-side signal built on it inherits that claim. sender_pinfl stays: the
    sender IS the bank's client."""
    msg = P._row_to_message(dict(_ROW), include_labels=False)
    assert "receiver_pinfl" not in msg
    assert msg["sender_pinfl"] == "S1"
    assert msg["receiver_card"] == "8600030000000002"


def test_the_ingress_hash_covers_only_fields_that_travel():
    """A hash over a field the message omits binds "", weakening it silently."""
    import integrity
    msg = P._row_to_message(dict(_ROW), include_labels=False)
    missing = [f for f in integrity.INGRESS_FIELDS
               if f not in msg and f not in ("event_time",)]
    assert not missing, f"hashed but never sent: {missing}"


def test_the_issuer_is_not_on_the_wire():
    """The issuer is derived from the PAN's BIN, never carried by the message."""
    msg = P._row_to_message(dict(_ROW, sender_bank_name="X",
                                 receiver_bank_name="Y"), include_labels=False)
    assert "sender_bank_name" not in msg and "receiver_bank_name" not in msg


def test_every_generated_row_survives_the_conversion():
    """Against the real dataset: a cast that throws on row 40,000 is not a cast."""
    here = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(here, "..", "out", "transactions.csv")
    if not os.path.exists(csv_path):
        pytest.skip("dataset not generated")
    with open(csv_path, newline="", encoding="utf-8") as fh:
        for i, row in enumerate(csv.DictReader(fh)):
            msg = P._row_to_message(row, include_labels=False)
            assert isinstance(msg["active_call"], bool), i
            assert isinstance(msg["amount_uzs"], int), i
