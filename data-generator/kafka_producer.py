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


try:
    from kafka import KafkaProducer
except ImportError:  # allow --dry-run without the dependency installed
    KafkaProducer = None


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


def _row_to_message(row, include_labels):
    fields = RAW_FIELDS + (["label_is_fraud", "label_fraud_type"] if include_labels else [])
    msg = {k: row[k] for k in fields if k in row}
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
    ap.add_argument("--bootstrap", default="localhost:29092")  # compose EXTERNAL listener
    ap.add_argument("--topic", default="transactions.raw")

    # Pacing. Unpaced, the whole file lands in the topic at once and every
    # latency figure measured afterwards is queue depth, not decision time.
    ap.add_argument("--realtime", action="store_true",
                    help="pace to the original inter-event gaps")
    ap.add_argument("--speed", type=float, default=200.0,
                    help="time-compression factor for --realtime")
    ap.add_argument("--rate", type=float, default=0.0, metavar="TPS",
                    help="pace at a fixed events/s; 0 sends as fast as the "
                         "client can. Excludes --realtime")

    # Slicing: arms equal in length and disjoint in ids, so no row replays as a
    # duplicate. Skipped rows are dropped before the pacing clock starts.
    ap.add_argument("--limit", type=int, default=None,
                    help="stop after N messages")
    ap.add_argument("--skip", type=int, default=0,
                    help="drop the first N rows before sending")

    # Security arms, docs/irp-framing.md 7.4-7.5a.
    ap.add_argument("--encrypt", action="store_true",
                    help="AES-256-GCM the payload; needs PAYLOAD_KEY_HEX")
    ap.add_argument("--tls", action="store_true",
                    help="connect over mutual TLS (broker listener :9094)")
    ap.add_argument("--ssl-ca", default="/certs/ca.crt")
    ap.add_argument("--ssl-cert", default="/certs/client.crt")
    ap.add_argument("--ssl-key", default="/certs/client.key")
    # Reconnect every N messages, so the handshake recurs instead of amortising.
    ap.add_argument("--reconnect-every", type=int, default=0, metavar="N",
                    help="reopen the producer every N messages")

    ap.add_argument("--include-labels", action="store_true",
                    help="carry label_is_fraud through, for offline scoring")
    ap.add_argument("--dry-run", action="store_true",
                    help="print messages instead of producing to Kafka")
    args = ap.parse_args()

    # Resolved before the loop so a missing key fails at startup, not mid-run.
    crypto_key = payload_crypto.key_from_env() if args.encrypt else None
    if args.encrypt:
        print("payload encryption: ON (AES-256-GCM)")

    def _serialize(v):
        """One serialiser for both arms: the encrypted envelope is base64 text, so
        both are UTF-8 on the wire."""
        if crypto_key is None:
            return json.dumps(v).encode()
        return payload_crypto.encrypt(v, crypto_key).encode()

    producer = None
    if not args.dry_run:
        if KafkaProducer is None:
            raise SystemExit("kafka-python not installed; use --dry-run or pip install -r requirements.txt")
        tls = {}
        if args.tls:
            # ssl_check_hostname stays TRUE: turning it off removes part of the handshake
            # work this arm measures, giving a "security overhead" for a weaker posture.
            tls = dict(security_protocol="SSL",
                       ssl_check_hostname=True,
                       ssl_cafile=args.ssl_ca,
                       ssl_certfile=args.ssl_cert,
                       ssl_keyfile=args.ssl_key)
            print(f"transport: mutual TLS -> {args.bootstrap}")
        def _new_producer():
            return KafkaProducer(
                bootstrap_servers=args.bootstrap,
                key_serializer=lambda k: k.encode(),
                value_serializer=_serialize,
                **tls,
            )

        producer = _new_producer()

    sent, skipped, reconnects, prev_dt = 0, 0, 0, None
    pace_t0, behind_max, behind_n = None, 0.0, 0
    interrupted = False
    try:
        with open(args.file, newline="") as f:
            for row in csv.DictReader(f):
                # Before the pacing clock: prev_dt must be set by the first row actually
                # SENT, or the slice opens by sleeping out a gap belonging to skipped rows.
                if skipped < args.skip:
                    skipped += 1
                    continue
                # Deadline pacing, not sleep(1/rate): per-message sleeps drift, while an
                # absolute schedule keeps the average rate and shows falling behind.
                if args.rate > 0:
                    if pace_t0 is None:
                        pace_t0 = time.time()
                    due = pace_t0 + sent / args.rate
                    ahead = due - time.time()
                    if ahead > 0:
                        time.sleep(ahead)
                    else:
                        behind_max = max(behind_max, -ahead)
                        behind_n += 1
                elif args.realtime:
                    dt = datetime.fromisoformat(row["event_time"])
                    if prev_dt is not None:
                        gap = (dt - prev_dt).total_seconds() / args.speed
                        if gap > 0:
                            time.sleep(min(gap, 5.0))
                    prev_dt = dt

                msg = _row_to_message(row, args.include_labels)
                # Wall clock at ingress. `event_time` is SIMULATED time spread over
                # weeks, so it cannot measure anything about the pipeline.
                msg["ingested_at"] = time.time()
                # Integrity hash of the raw event, including ingested_at;
                # it excludes itself and the labels.
                msg["ingress_hash"] = integrity.ingress_hash(msg)
                if args.dry_run:
                    print(msg["sender_card"], "->", json.dumps(msg))
                else:
                    producer.send(args.topic, key=row["sender_card"], value=msg)
                sent += 1
                if (args.reconnect_every and producer is not None
                        and sent % args.reconnect_every == 0):
                    # flush() before close(): buffered messages belong to the run, and
                    # dropping them looks like loss in the experiment ruling loss out.
                    producer.flush()
                    producer.close(timeout=10)
                    reconnects += 1
                    producer = _new_producer()
                    # The reconnect follows the send and blocks until bootstrap,
                    # so the handshake stays outside every latency figure.
                if args.limit is not None and sent >= args.limit:
                    break
    except KeyboardInterrupt:
        # Ctrl+C still reaches the flush below, and the count printed is what
        # experiments/outage.py --expect needs.
        interrupted = True

    if producer is not None:
        producer.flush()
    slice_note = f" (rows {args.skip:,}..{args.skip + sent:,})" if args.skip else ""
    slice_note += f", {reconnects} reconnects" if reconnects else ""
    print(f"produced {sent:,} messages to '{args.topic}'" + slice_note
          + (" (stopped by hand)" if interrupted else ""))

    # The achieved rate, always: an arm that cannot reach the requested rate measures
    # the producer, not the pipeline.
    if pace_t0 is not None and sent:
        elapsed = time.time() - pace_t0
        achieved = sent / elapsed if elapsed > 0 else float("inf")
        print(f"rate: requested {args.rate:,.0f}/s, achieved {achieved:,.0f}/s "
              f"over {elapsed:.1f}s")
        if behind_n:
            print(f"  BEHIND SCHEDULE on {behind_n:,} of {sent:,} messages "
                  f"({behind_n / sent:.1%}), worst lag {behind_max * 1000:.0f} ms")
        if achieved < args.rate * 0.95:
            print(f"  SATURATED: the client could not sustain the requested "
                  f"rate. Latency from this arm measures the producer, not the "
                  f"pipeline - report it as the saturation point, not as a "
                  f"pipeline figure.")


if __name__ == "__main__":
    main()
