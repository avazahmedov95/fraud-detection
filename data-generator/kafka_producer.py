"""
Replay generated transactions into a Kafka topic for the Flink pipeline.

Reads transactions.csv (already sorted by event_time) and produces one JSON
message per row. Messages are keyed by `sender_card`, so a keyBy(sender) in Flink
receives an ordered per-sender stream — exactly what the velocity / structuring
CEP patterns need.

  python kafka_producer.py --file out/transactions.csv \
      --bootstrap localhost:9092 --topic transactions.raw

  # simulate a live stream paced to the original inter-event gaps (200x faster):
  python kafka_producer.py --file out/transactions.csv --realtime --speed 200

Requires a running Kafka broker and `kafka-python` (see requirements.txt).
"""

import argparse
import csv
import json
import time
from datetime import datetime

import integrity
import payload_crypto

try:
    from kafka import KafkaProducer
except ImportError:  # allow --dry-run without the dependency installed
    KafkaProducer = None


# Raw fields the switch would actually emit; enrichment/labels are dropped here
# so the stream resembles production input. Flip --include-labels to keep labels
# for offline evaluation harnesses.
RAW_FIELDS = [
    "transaction_id", "event_time", "sender_pinfl", "sender_card", "sender_network",
    "receiver_pinfl", "receiver_card", "receiver_network", "amount_uzs", "channel",
    "device_id", "sender_region", "receiver_region", "sender_balance_before",
    # Session signals the mobile app observes and sends with the confirmation.
    # These are raw, not enrichment: omitting them silently scores every live
    # event as "no call, average hesitation" while the model was trained on the
    # real values — the train/serve skew this project has already been bitten by
    # twice. The session_telemetry capability is the second most valuable one
    # measured, so dropping it here would quietly discard that.
    "active_call", "secs_login_to_confirm",
    # Issuer identity. In production it is derived from the PAN's BIN, which the
    # switch message carries; materialised here so the job need not carry a BIN
    # table. Used for the on-us test behind the receiver_age capability.
    "sender_bank_name", "receiver_bank_name",
]


def _row_to_message(row, include_labels):
    fields = RAW_FIELDS + (["label_is_fraud", "label_fraud_type"] if include_labels else [])
    msg = {k: row[k] for k in fields if k in row}
    for k in ("amount_uzs", "sender_balance_before"):
        if k in msg:
            msg[k] = int(msg[k])
    return msg


def main():
    ap = argparse.ArgumentParser(description="Replay transactions.csv into Kafka")
    ap.add_argument("--file", required=True)
    ap.add_argument("--bootstrap", default="localhost:29092")  # matches docker-compose EXTERNAL listener
    ap.add_argument("--topic", default="transactions.raw")
    ap.add_argument("--realtime", action="store_true",
                    help="pace messages to original inter-event gaps")
    ap.add_argument("--speed", type=float, default=200.0,
                    help="time-compression factor when --realtime")
    ap.add_argument("--include-labels", action="store_true")
    ap.add_argument("--dry-run", action="store_true",
                    help="print messages instead of producing to Kafka")
    ap.add_argument("--encrypt", action="store_true",
                    help="AES-256-GCM the payload (needs PAYLOAD_KEY_HEX); "
                         "the security-overhead arm of reviewer point 3")
    ap.add_argument("--tls", action="store_true",
                    help="connect over mutual TLS (broker listener :9094); "
                         "the transport arm of reviewer point 3")
    ap.add_argument("--ssl-ca", default="/certs/ca.crt")
    ap.add_argument("--ssl-cert", default="/certs/client.crt")
    ap.add_argument("--ssl-key", default="/certs/client.key")
    ap.add_argument("--limit", type=int, default=None,
                    help="stop after N messages. Use for A/B runs: stopping by "
                         "hand gives each arm a different length and a "
                         "different amount of cache warming, which moves the "
                         "figures by more than the effect being measured")
    ap.add_argument("--skip", type=int, default=0,
                    help="drop the first N rows before sending anything. A "
                         "repeated experiment on one dataset needs DISJOINT "
                         "slices: replaying the same transaction ids makes "
                         "every row after the first pass indistinguishable "
                         "from a duplicate, and duplication is exactly what "
                         "fault_injection.py measures. Skipped rows are "
                         "dropped before the pacing clock starts, so the "
                         "slice is paced from its own first row rather than "
                         "sleeping out the gap it was never going to send")
    args = ap.parse_args()

    # Resolved once, before the loop, so a missing key fails at startup rather
    # than part-way through a measurement run.
    crypto_key = payload_crypto.key_from_env() if args.encrypt else None
    if args.encrypt:
        print("payload encryption: ON (AES-256-GCM)")

    def _serialize(v):
        """One serialiser for both arms of the experiment.

        The encrypted form is text - see payload_crypto for why the envelope is
        base64 rather than binary - so both arms end up as UTF-8 on the wire and
        the difference measured between them is cryptography plus framing, not a
        change of transport type.
        """
        if crypto_key is None:
            return json.dumps(v).encode()
        return payload_crypto.encrypt(v, crypto_key).encode()

    producer = None
    if not args.dry_run:
        if KafkaProducer is None:
            raise SystemExit("kafka-python not installed; use --dry-run or pip install -r requirements.txt")
        tls = {}
        if args.tls:
            # ssl_check_hostname stays TRUE. Turning it off is the usual
            # shortcut in a prototype, and it would quietly remove part of the
            # handshake work this arm exists to measure - producing a
            # "security overhead" figure for a weaker security posture than the
            # one being claimed.
            tls = dict(security_protocol="SSL",
                       ssl_check_hostname=True,
                       ssl_cafile=args.ssl_ca,
                       ssl_certfile=args.ssl_cert,
                       ssl_keyfile=args.ssl_key)
            print(f"transport: mutual TLS -> {args.bootstrap}")
        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap,
            key_serializer=lambda k: k.encode(),
            value_serializer=_serialize,
            **tls,
        )

    sent, skipped, prev_dt = 0, 0, None
    interrupted = False
    try:
        with open(args.file, newline="") as f:
            for row in csv.DictReader(f):
                # Before the pacing clock, deliberately: prev_dt must be set by
                # the first row actually SENT, or the slice would open by
                # sleeping out a gap belonging to rows it skipped.
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

                msg = _row_to_message(row, args.include_labels)
                # Wall clock at the moment the event enters the pipeline. `event_time`
                # is the SIMULATED time the transaction happened, spread over weeks,
                # so it cannot be used to measure anything about the pipeline. This
                # is the t0 every latency figure is measured from.
                msg["ingested_at"] = time.time()
                # Integrity hash of the raw event, computed here at ingress and
                # carried unchanged to the audit store. Binds the recorded decision
                # to exactly this event. Set before the hash so ingested_at cannot be
                # altered without detection, but excludes itself and the labels.
                msg["ingress_hash"] = integrity.ingress_hash(msg)
                if args.dry_run:
                    print(msg["sender_card"], "->", json.dumps(msg))
                else:
                    producer.send(args.topic, key=row["sender_card"], value=msg)
                sent += 1
                if args.limit is not None and sent >= args.limit:
                    break
    except KeyboardInterrupt:
        # A paced stream is meant to be stopped by hand, so Ctrl+C is a normal
        # exit rather than an error — but it must still reach the flush below.
        # Messages buffered and never flushed are messages the pipeline never
        # received, and in the fault-injection run they would be counted as
        # transactions LOST after the kill: the script would report the exact
        # correctness failure it exists to rule out. The count printed here is
        # also what `fault_injection.py --expect` needs, and it is only knowable
        # on this side of the wire.
        interrupted = True

    if producer is not None:
        producer.flush()
    slice_note = f" (rows {args.skip:,}..{args.skip + sent:,})" if args.skip else ""
    print(f"produced {sent:,} messages to '{args.topic}'" + slice_note
          + (" (stopped by hand)" if interrupted else ""))


if __name__ == "__main__":
    main()
