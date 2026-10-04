"""Replays a generated CSV into transactions.raw, one JSON message per row.
Keyed by sender_card so keyBy(sender) in Flink gets an ordered per-sender stream;
stamps ingested_at (t0 for every latency figure) and ingress_hash."""

import argparse
import csv
import json
import time
import warnings
from datetime import datetime

import integrity
import payload_crypto

# kafka-python warns for any serializer that is not its own ABC subclass; a plain
# callable is the supported form, and PowerShell shows the warning as an error.
warnings.filterwarnings("ignore", message=".*does not implement kafka.serializer.Serializer",
                        category=DeprecationWarning)

from kafka import KafkaProducer


# Raw fields the switch would actually emit; everything else - the labels, the
# payee's account age - is dropped here.
RAW_FIELDS = [
    "transaction_id", "event_time", "sender_pinfl", "sender_card", "sender_network",
    "receiver_card", "receiver_network", "amount_uzs",
    "device_id", "sender_region", "sender_balance_before",
    # Session signals the mobile app sends with the confirmation - raw, so the live
    # job scores the values the model was trained on.
    "active_call", "secs_login_to_confirm",
]
# NOT sent: receiver_pinfl (the sending bank sees only the destination PAN) and the
# bank names (a switch message carries the PAN, whose BIN names the issuer).


#: Fields that are booleans, not text: csv.DictReader gives strings, and "False"
#: is truthy to every consumer.
_BOOL_FIELDS = ("active_call",)
_INT_FIELDS = ("amount_uzs", "sender_balance_before")
_FLOAT_FIELDS = ("secs_login_to_confirm",)
_FALSEY = {"", "0", "false", "f", "no", "n", "none", "null", "nan"}


def _row_to_message(row):
    msg = {k: row[k] for k in RAW_FIELDS if k in row}
    for k in _INT_FIELDS:
        if k in msg:
            msg[k] = int(msg[k])
    for k in _FLOAT_FIELDS:
        if k in msg:
            try:
                msg[k] = float(msg[k])
            except (TypeError, ValueError):
                msg[k] = 0.0
    for k in _BOOL_FIELDS:
        if k in msg:
            v = msg[k]
            msg[k] = (bool(v) if isinstance(v, bool)
                      else str(v).strip().lower() not in _FALSEY)
    return msg


def main():
    ap = argparse.ArgumentParser(description="Replay transactions.csv into Kafka")
    ap.add_argument("--file", required=True)
    ap.add_argument("--bootstrap", default="127.0.0.1:29092")  # compose EXTERNAL listener
    ap.add_argument("--topic", default="transactions.raw")
    # Unpaced, the whole file lands in the topic at once.
    ap.add_argument("--realtime", action="store_true",
                    help="pace to the original inter-event gaps")
    ap.add_argument("--speed", type=float, default=200.0,
                    help="time-compression factor for --realtime")
    ap.add_argument("--limit", type=int, default=None, help="stop after N messages")
    ap.add_argument("--skip", type=int, default=0, help="drop the first N rows")
    ap.add_argument("--encrypt", action="store_true",
                    help="AES-256-GCM the payload; needs PAYLOAD_KEY_HEX")
    ap.add_argument("--tls", action="store_true",
                    help="connect over mutual TLS (broker listener :9094)")
    ap.add_argument("--ssl-ca", default="/certs/ca.crt")
    ap.add_argument("--ssl-cert", default="/certs/client.crt")
    ap.add_argument("--ssl-key", default="/certs/client.key")
    args = ap.parse_args()

    # Resolved before the loop so a missing key fails at startup, not mid-run.
    crypto_key = payload_crypto.key_from_env() if args.encrypt else None

    def _serialize(v):
        """The encrypted envelope is base64 text, so both forms are UTF-8 on the wire."""
        if crypto_key is None:
            return json.dumps(v).encode()
        return payload_crypto.encrypt(v, crypto_key).encode()

    tls = {}
    if args.tls:
        tls = dict(security_protocol="SSL", ssl_check_hostname=True,
                   ssl_cafile=args.ssl_ca, ssl_certfile=args.ssl_cert,
                   ssl_keyfile=args.ssl_key)
    producer = KafkaProducer(bootstrap_servers=args.bootstrap,
                             key_serializer=lambda k: k.encode(),
                             value_serializer=_serialize, **tls)

    sent, skipped, prev_dt = 0, 0, None
    interrupted = False
    try:
        with open(args.file, newline="") as f:
            for row in csv.DictReader(f):
                # Before the pacing clock: the first row SENT sets prev_dt.
                if skipped < args.skip:
                    skipped += 1
                    continue
                if args.realtime:
                    dt = datetime.fromisoformat(row["event_time"])
                    if prev_dt is not None:
                        gap = (dt - prev_dt).total_seconds() / args.speed
                        if gap > 0:
                            time.sleep(min(gap, 5.0))
                    prev_dt = dt

                msg = _row_to_message(row)
                # Wall clock at ingress: `event_time` is simulated time spread over weeks.
                msg["ingested_at"] = time.time()
                # Integrity hash of the raw event, including ingested_at.
                msg["ingress_hash"] = integrity.ingress_hash(msg)
                producer.send(args.topic, key=row["sender_card"], value=msg)
                sent += 1
                if args.limit is not None and sent >= args.limit:
                    break
    except KeyboardInterrupt:
        interrupted = True

    producer.flush()
    print(f"produced {sent:,} messages to '{args.topic}'"
          + (f" (rows {args.skip:,}..{args.skip + sent:,})" if args.skip else "")
          + (" (stopped by hand)" if interrupted else ""))


if __name__ == "__main__":
    main()
