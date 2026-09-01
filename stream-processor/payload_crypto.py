"""AES-256-GCM envelope for the event topics, and the plaintext routing key.

Wire format:  "FDE1:" + routing_key + ":" + base64(nonce(12) || ct || tag(16))

Two invariants a change here must not break: hash the plaintext BEFORE
encrypting (see integrity.py), and keep the two copies of this file - in
data-generator/ and stream-processor/ - byte-identical, since each package
deploys separately and test_payload_crypto.py in each pins the same vector.
Rationale and the measured cost: docs/irp-framing.md 7.4.
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
