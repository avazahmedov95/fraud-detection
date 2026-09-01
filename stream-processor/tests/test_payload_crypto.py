"""
Known-answer test pinning the encrypted wire format.

payload_crypto.py is duplicated between data-generator/ and stream-processor/,
which deploy as separate units. If the copies drift, the producer writes records
the job cannot read - and because the job's decoder falls back to plaintext JSON
on anything without the magic prefix, a format drift could surface as mangled
events rather than as an error. This test makes drift fail loudly in both places
instead.

The vector is a DECRYPT vector, not an encrypt one: AES-GCM uses a fresh random
nonce per record, so encryption is deliberately non-deterministic and cannot be
pinned by output comparison. Fixing the key, the nonce and the resulting
envelope pins everything that matters - magic, routing key placement, base64
framing, serialisation of the plaintext, and the associated data.

    python -m pytest test_payload_crypto.py -q
"""

import base64
import json

import pytest

import payload_crypto as pc

KAT_KEY = bytes.fromhex(
    "00112233445566778899aabbccddeeff102132435465768798a9bacbdcedfe0f")
KAT_ENVELOPE = (
    'FDE1:8600123456789012:AAECAwQFBgcICQoLQ1/xHfJHBqlaA/gMiOVLz72O0H'
    'vE5FLNhG3qkTpc/xUzEmBQHG+C4YERWwzjulUQCA+NjgQaWq6c+/npa6/MeNxM7w'
    'buUO8PGSd+908pUXMh9rqhW2hGfSDTyQqNgnD4F59QWBmKw3vHqykU5Mm3u78IZQ'
    'xLZw==')
KAT_EVENT = {"amount_uzs": 9000000, "channel": "mobile",
             "sender_card": "8600123456789012", "transaction_id": "kat-0001"}


def test_known_answer_decrypts():
    """The pinned envelope must decrypt to the pinned event."""
    assert pc.decrypt(KAT_ENVELOPE, KAT_KEY) == KAT_EVENT


def test_magic_is_detectable_without_the_key():
    """A consumer must be able to route a record before it can decrypt it."""
    assert pc.is_encrypted(KAT_ENVELOPE)
    assert not pc.is_encrypted(json.dumps(KAT_EVENT).encode())


def test_routing_key_needs_no_key_and_no_decryption():
    """This is what key_by calls, once per record, before scoring. If it needed
    the key or the plaintext, every event would be decrypted twice and the
    security-overhead measurement would be measuring the wrong thing."""
    assert pc.routing_key(KAT_ENVELOPE) == "8600123456789012"


def test_routing_key_works_on_the_plaintext_arm_too():
    plain = json.dumps(KAT_EVENT)
    assert pc.routing_key(plain) == "8600123456789012"


def test_routing_key_is_authenticated():
    """Re-pointing a record at another sender must break it, not misroute it: a
    record landing in the wrong key group would corrupt that sender's velocity
    and structuring windows."""
    routing, body = KAT_ENVELOPE[len(pc.PREFIX):].split(":", 1)
    forged = pc.PREFIX + "9860000000000001" + ":" + body
    assert pc.routing_key(forged) == "9860000000000001"   # split still parses
    with pytest.raises(Exception):                        # but GCM rejects it
        pc.decrypt(forged, KAT_KEY)


@pytest.mark.parametrize("bad", [
    "",                                   # empty record
    "not json at all",
    "{}",                                 # valid JSON, no routing field
    '{"sender_card": ',                   # truncated JSON
    "FDE1:no-separator-after-this",       # an OLDER envelope format
    "FDE1:",                              # prefix only
])
def test_routing_key_never_raises(bad):
    """key_by has no error handling: an exception there crash-loops the job on
    that record for as long as it is in the topic. The 'older envelope format'
    case is not hypothetical - it is what happens whenever the wire format
    changes while records are still unconsumed."""
    assert pc.routing_key(bad) == pc.POISON_KEY


def test_poison_records_still_fail_to_decode():
    """Routing must be lenient; decoding must not be. A record that cannot be
    read has to be counted and dropped, never scored as if it were valid."""
    with pytest.raises(Exception):
        pc.loads_maybe_encrypted("FDE1:no-separator-after-this", KAT_KEY)


def test_roundtrip():
    blob = pc.encrypt(KAT_EVENT, KAT_KEY)
    assert pc.decrypt(blob, KAT_KEY) == KAT_EVENT
    assert pc.routing_key(blob) == KAT_EVENT["sender_card"]


def test_nonce_is_fresh_per_record():
    """Reusing a nonce under one key breaks GCM catastrophically, so two
    encryptions of one event must differ."""
    assert pc.encrypt(KAT_EVENT, KAT_KEY) != pc.encrypt(KAT_EVENT, KAT_KEY)


def test_tampering_is_detected():
    # Flip one bit inside the ciphertext, keeping the base64 well-formed, so the
    # failure comes from the GCM tag rather than from a decoding error.
    routing, body = KAT_ENVELOPE[len(pc.PREFIX):].split(":", 1)
    raw = bytearray(base64.b64decode(body))
    raw[-17] ^= 0x01
    bad = pc.PREFIX + routing + ":" + base64.b64encode(bytes(raw)).decode()
    with pytest.raises(Exception):
        pc.decrypt(bad, KAT_KEY)


def test_wrong_key_is_rejected():
    with pytest.raises(Exception):
        pc.decrypt(KAT_ENVELOPE, bytes.fromhex("cd" * 32))


def test_bytes_and_str_are_both_accepted():
    """Kafka hands the job a str via SimpleStringSchema; the producer and the
    tests deal in both. Neither form may change the result."""
    assert pc.decrypt(KAT_ENVELOPE.encode(), KAT_KEY) == KAT_EVENT
    assert pc.decrypt(KAT_ENVELOPE, KAT_KEY) == KAT_EVENT


def test_both_arms_share_one_decode_path():
    """The measurement compares encrypted against plaintext; if the two used
    different decoders, the difference would not be the cost of crypto."""
    plain = json.dumps(KAT_EVENT).encode()
    assert pc.loads_maybe_encrypted(plain) == KAT_EVENT
    assert pc.loads_maybe_encrypted(KAT_ENVELOPE, KAT_KEY) == KAT_EVENT


def test_missing_key_is_an_error_not_a_default():
    with pytest.raises(pc.PayloadCryptoError):
        pc.key_from_env("DEFINITELY_NOT_SET_PAYLOAD_KEY")


def test_encrypted_record_without_a_key_is_loud():
    """A missing key must not read as 'plaintext record' and vanish."""
    with pytest.raises(pc.PayloadCryptoError):
        pc.loads_maybe_encrypted(KAT_ENVELOPE, None)
