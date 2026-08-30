"""
Payload encryption at rest and in transit on the event topics (AES-256-GCM).

Reviewer point 3 asks for the measured cost of mTLS and payload encryption. The
two are deliberately separable here: transport security protects the channel,
payload encryption protects the record even from someone holding the broker's
disk or a topic-read grant. A bank's own operators are inside the TLS boundary;
they are not inside this one.

WIRE FORMAT
-----------
    "FDE1:" + routing_key + ":" + base64( nonce(12) || ciphertext || tag(16) )

The magic prefix means a consumer can tell an encrypted record from a plaintext
one by looking at it, so the two sides do not have to be switched over
atomically and a topic containing both remains readable. That property is what
makes the A/B measurement possible at all: the same job binary consumes both
arms of the experiment. `{` is the only other first byte a record can have, so
the discrimination is unambiguous.

WHY A CLEAR ROUTING KEY
-----------------------
The Flink job keys the stream by sender before scoring:

    .key_by(lambda v: ...["sender_card"])

which means the record is read TWICE - once to partition it, once to score it.
Decrypting in both places would double the cryptographic cost of every event and
corrupt the very measurement this module exists to produce, so the partitioning
field travels in clear and `routing_key()` extracts it with a string split.

It is authenticated, not merely appended: the routing key is the GCM associated
data, so altering it makes the record undecryptable rather than silently
misrouted. That matters here beyond tidiness - a record steered to the wrong key
group would accumulate into another sender's velocity and structuring windows,
which is an integrity failure in the detection logic rather than a privacy one.

WHAT IS AND IS NOT PROTECTED
----------------------------
The routing key discloses no more than the topic already does: Kafka partitions
on the message key, which is `sender_card`, so an adversary with topic-read
access already learns which cards transacted and how often. What payload
encryption adds is that amounts, counterparties, regions and session telemetry
stay closed. Stating that limit is part of the answer: payload encryption on a
keyed topic buys confidentiality of the record, not of the metadata.

WHY TEXT AND NOT RAW BYTES
--------------------------
The envelope is base64 rather than binary because the Flink source deserialises
with `SimpleStringSchema`, which decodes the record as UTF-8 - and AES-GCM
ciphertext is not valid UTF-8, so a binary envelope would be corrupted in
transit rather than rejected. PyFlink 1.19 exposes no byte-array deserialiser to
`set_value_only_deserializer` without dropping to Java.

This costs roughly 33% in message size ON TOP of the envelope's fixed overhead,
and that inflation must be reported as part of the measured cost rather than
quietly excluded. It is an artefact of the deserialiser, not of encryption: a
pipeline with a binary schema pays the same CPU and none of the base64
expansion. The measurement therefore gives an UPPER bound on the transport cost
and an accurate figure for the compute cost.

One asymmetry to disclose when reporting: on the encrypted arm `key_by` does a
string split, while on the plaintext arm it parses JSON. That makes partitioning
marginally CHEAPER under encryption, slightly flattering the encrypted arm - in
the opposite direction to the effect being measured, so it cannot manufacture
the result, but it should be stated rather than discovered.

ORDER OF OPERATIONS (important)
-------------------------------
`ingress_hash` is computed over the PLAINTEXT fields, before encryption, and
travels inside the encrypted payload. Encrypting first and hashing the
ciphertext would break the audit guarantee: an auditor holding the original
event could no longer recompute the hash, since GCM nonces make every encryption
of the same event distinct. Hash the plaintext, then encrypt; decrypt, then
verify.

KEY MANAGEMENT
--------------
The key comes from PAYLOAD_KEY_HEX (64 hex characters = 32 bytes). There is no
default: a hard-coded fallback key is worse than a startup failure, because it
encrypts everything under a value that is in the source tree. A real deployment
would source this from a KMS or HSM with rotation; the version digit in the
magic prefix is what a rotation scheme would extend.

THIS FILE IS DUPLICATED in data-generator/ and stream-processor/ because they
deploy as separate units with no shared package. The two copies MUST stay
byte-identical - test_payload_crypto.py in each pins the same known-answer
vector, so a drift in either copy fails a test rather than silently producing
records the other side cannot read.
"""

import base64
import json
import os

MAGIC = b"FDE1"
PREFIX = "FDE1:"
SEP = ":"
NONCE_BYTES = 12          # GCM standard; 96-bit nonces avoid an internal rehash
KEY_BYTES = 32            # AES-256
ROUTING_FIELD = "sender_card"   # what the Flink job keys the stream by

# Key given to records whose routing field cannot be read at all. They are
# dropped downstream; what matters is that they are dropped rather than raised,
# because key_by has no error handling and an exception there stops the job on
# that record permanently - a poison pill. One malformed event must not be able
# to halt a payment pipeline, and "malformed" includes the entirely mundane case
# of an envelope format changing while records are still in the topic.
POISON_KEY = "__undecodable__"


class PayloadCryptoError(RuntimeError):
    pass


def key_from_env(var="PAYLOAD_KEY_HEX"):
    """Load the 32-byte key, or raise. No default, deliberately."""
    raw = os.getenv(var, "").strip()
    if not raw:
        raise PayloadCryptoError(
            f"{var} is not set. Payload encryption was requested but no key was "
            f"supplied; refusing to fall back to a hard-coded key.")
    try:
        key = bytes.fromhex(raw)
    except ValueError as exc:
        raise PayloadCryptoError(f"{var} is not valid hex: {exc}") from None
    if len(key) != KEY_BYTES:
        raise PayloadCryptoError(
            f"{var} must be {KEY_BYTES * 2} hex characters "
            f"({KEY_BYTES} bytes); got {len(key)}")
    return key


def _aesgcm(key):
    # Imported lazily so the module can be loaded (and its constants used) on a
    # host without `cryptography` installed - the plaintext arm of the
    # experiment must not require the crypto dependency.
    try:
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    except ImportError as exc:                          # pragma: no cover
        raise PayloadCryptoError(
            "payload encryption requires the `cryptography` package") from exc
    return AESGCM(key)


def _aad(routing) -> bytes:
    """Associated data: the magic plus the clear routing key.

    Binding the routing key means a record cannot be re-pointed at another
    sender's key group without invalidating its own authentication tag.
    """
    return MAGIC + b"|" + routing.encode("utf-8")


def _as_text(blob) -> str:
    if isinstance(blob, (bytes, bytearray)):
        return bytes(blob).decode("utf-8")
    return blob


def _split(text):
    """-> (routing_key, base64_body). Raises on a malformed envelope."""
    rest = text[len(PREFIX):]
    routing, sep, body = rest.partition(SEP)
    if not sep:
        raise PayloadCryptoError("envelope has no routing key separator")
    return routing, body


def is_encrypted(blob) -> bool:
    """True if this record carries the encrypted envelope.

    Decidable without the key, which is what lets one consumer read a topic
    holding both arms of the experiment.
    """
    try:
        return _as_text(blob).startswith(PREFIX)
    except (UnicodeDecodeError, AttributeError):
        return False


def routing_key(blob, field=ROUTING_FIELD) -> str:
    """The partitioning field, WITHOUT decrypting.

    Used by the job's key_by. On an encrypted record this is a string split; on
    a plaintext one it parses the JSON, exactly as the job did before encryption
    existed.

    NEVER RAISES. key_by runs before any of the job's own error handling, and an
    exception raised there fails the task, restarts it, and meets the same
    record again - the job crash-loops for as long as that record is in the
    topic, which is forever. So an unreadable record is routed to POISON_KEY and
    dropped by the scorer, which counts and logs it. Returning a value here is
    not leniency about bad data; it is the difference between losing one event
    and losing the stream.
    """
    try:
        text = _as_text(blob)
        if text.startswith(PREFIX):
            return _split(text)[0]
        return str(json.loads(text)[field])
    except Exception:                                   # noqa: BLE001
        return POISON_KEY


def encrypt(event: dict, key: bytes, field=ROUTING_FIELD) -> str:
    """Serialise and encrypt one event into the wire envelope."""
    routing = str(event.get(field, ""))
    if SEP in routing:
        # Would make the envelope ambiguous to parse. Card numbers are digits,
        # so this is a guard against a future field choice rather than a case
        # that occurs today.
        raise PayloadCryptoError(
            f"routing field {field!r} contains the separator {SEP!r}: {routing!r}")
    plaintext = json.dumps(event, separators=(",", ":")).encode("utf-8")
    nonce = os.urandom(NONCE_BYTES)
    ct = _aesgcm(key).encrypt(nonce, plaintext, _aad(routing))
    return PREFIX + routing + SEP + base64.b64encode(nonce + ct).decode("ascii")


def decrypt(blob, key: bytes) -> dict:
    """Reverse of encrypt(). Raises if the record is not a valid envelope."""
    text = _as_text(blob)
    if not text.startswith(PREFIX):
        raise PayloadCryptoError("not an encrypted payload (bad prefix)")
    routing, body = _split(text)
    try:
        raw = base64.b64decode(body, validate=True)
    except Exception as exc:                            # noqa: BLE001
        raise PayloadCryptoError(f"envelope is not valid base64: {exc}") from None
    if len(raw) < NONCE_BYTES + 16:
        raise PayloadCryptoError("truncated envelope")
    nonce, ct = raw[:NONCE_BYTES], raw[NONCE_BYTES:]
    plaintext = _aesgcm(key).decrypt(nonce, ct, _aad(routing))
    return json.loads(plaintext.decode("utf-8"))


def loads_maybe_encrypted(blob, key=None) -> dict:
    """Decode a record that may or may not be encrypted.

    This is what the consuming side calls. It keeps the two arms of the
    measurement on one code path, so any difference between them is the cost of
    the cryptography rather than the cost of a different deserialiser.
    """
    if is_encrypted(blob):
        if key is None:
            raise PayloadCryptoError(
                "record is encrypted but no key is configured "
                "(set PAYLOAD_KEY_HEX)")
        return decrypt(blob, key)
    return json.loads(_as_text(blob))
