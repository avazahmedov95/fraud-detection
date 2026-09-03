"""Known-answer test pinning the encrypted wire format.

payload_crypto.py is duplicated between data-generator/ and stream-processor/,
which deploy separately. On drift the producer writes records the job cannot
read, and because the decoder falls back to plaintext JSON without the magic
prefix, drift could surface as mangled events, not an error. A DECRYPT vector:
AES-GCM uses a fresh random nonce per record, so encryption is non-deterministic;
fixing key, nonce and envelope pins magic, routing-key placement, base64 framing,
plaintext serialisation and the associated data.
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
    """key_by calls this per record; needing the key would decrypt every event twice."""
    assert pc.routing_key(KAT_ENVELOPE) == "8600123456789012"


def test_routing_key_works_on_the_plaintext_arm_too():
    plain = json.dumps(KAT_EVENT)
    assert pc.routing_key(plain) == "8600123456789012"


def test_routing_key_is_authenticated():
    """A record in the wrong key group corrupts that sender's velocity windows."""
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
    case is what happens when the format changes with records still unconsumed."""
    assert pc.routing_key(bad) == pc.POISON_KEY


def test_poison_records_still_fail_to_decode():
    """An unreadable record must be counted and dropped, never scored as valid."""
    with pytest.raises(Exception):
        pc.loads_maybe_encrypted("FDE1:no-separator-after-this", KAT_KEY)


def test_roundtrip():
    blob = pc.encrypt(KAT_EVENT, KAT_KEY)
    assert pc.decrypt(blob, KAT_KEY) == KAT_EVENT
    assert pc.routing_key(blob) == KAT_EVENT["sender_card"]


def test_nonce_is_fresh_per_record():
    """Reusing a nonce under one key breaks GCM: two encryptions must differ."""
    assert pc.encrypt(KAT_EVENT, KAT_KEY) != pc.encrypt(KAT_EVENT, KAT_KEY)


def test_tampering_is_detected():
    # Flip a bit inside the ciphertext, base64 still valid: GCM tag fails, not decode.
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
    """Kafka hands the job a str via SimpleStringSchema; both forms must decode alike."""
    assert pc.decrypt(KAT_ENVELOPE.encode(), KAT_KEY) == KAT_EVENT
    assert pc.decrypt(KAT_ENVELOPE, KAT_KEY) == KAT_EVENT


def test_both_arms_share_one_decode_path():
    """Both arms must share a decoder, or the measured difference is not crypto."""
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
