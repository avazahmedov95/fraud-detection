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

# kafka-python 3.0.10 warns for any serializer that is not a subclass of its own
# Serializer ABC - a plain callable is the documented, supported form. Silenced
# narrowly: PowerShell renders a native stderr line as a red NativeCommandError block.
warnings.filterwarnings("ignore", message=".*does not implement kafka.serializer.Serializer",
                        category=DeprecationWarning)


try:
    from kafka import KafkaProducer
except ImportError:  # allow --dry-run without the dependency installed
    KafkaProducer = None


# Raw fields the switch would actually emit; enrichment and labels are dropped here.
RAW_FIELDS = [
    "transaction_id", "event_time", "sender_pinfl", "sender_card", "sender_network",
    "receiver_card", "receiver_network", "amount_uzs", "channel",
    "device_id", "sender_region", "sender_balance_before",
    # Session signals the mobile app sends with the confirmation. Raw, not enrichment:
    # omitting them scores every live event as "no call, average hesitation" while the
    # model was trained on real values - the train/serve skew this project has been
    # bitten by twice. session_telemetry is the second most valuable capability measured.
    "active_call", "secs_login_to_confirm",
]
# NOT sent: receiver_pinfl. A card-to-card transfer reaches the sending bank as a destination
# PAN; the person behind it is a core-banking lookup available for the bank's own clients
# only (6.85% of transfers at the measured market concentration). Carrying it made the wire
# format assert knowledge no deployment has. sender_pinfl stays: the sender IS the client.
# NOT sent: sender_bank_name / receiver_bank_name - the issuer is derived from the PAN's BIN
# (stream-processor/bins.py). Sending them carried a field UzCard / HUMO does not and put the
# on-us test on a convenience of the generator, not on data a deployment holds.


#: Fields that are booleans, not text. csv.DictReader returns every column as a string,
#: so without this `active_call` travelled as "False" - a non-empty string, therefore TRUE
#: to every consumer that tested truthiness. The live job scored active_call = 1 on 100%
#: of events while the model had been trained on 3.5%. Numbers were cast from the start,
#: booleans were not, and the omission was invisible because the JSON looked right.
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
    ap.add_argument("--bootstrap", default="localhost:29092")  # matches docker-compose EXTERNAL listener
    ap.add_argument("--topic", default="transactions.raw")
    ap.add_argument("--realtime", action="store_true",
                    help="pace messages to original inter-event gaps")
    ap.add_argument("--speed", type=float, default=200.0,
                    help="time-compression factor when --realtime")
    ap.add_argument("--rate", type=float, default=0.0, metavar="TPS",
                    help="pace at a fixed events/s. 0 (default) sends as fast "
                         "as the client can. Mutually exclusive with --realtime, "
                         "which paces to the original inter-event gaps instead.")
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
    ap.add_argument("--reconnect-every", type=int, default=0, metavar="N",
                    help="close and reopen the producer every N messages. The "
                         "transport arms in docs/irp-framing.md 7.5 each hold "
                         "ONE long-lived connection, so the handshake is "
                         "amortised over the whole arm and what they measure is "
                         "mostly record framing. A payment switch does not work "
                         "that way. This makes the handshake recur, which is "
                         "the only condition under which the mutual-TLS answer "
                         "could change.")
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

    # Resolved before the loop so a missing key fails at startup, not mid-run.
    crypto_key = payload_crypto.key_from_env() if args.encrypt else None
    if args.encrypt:
        print("payload encryption: ON (AES-256-GCM)")

    def _serialize(v):
        """One serialiser for both arms of the experiment.

        The encrypted form is text (base64 envelope, see payload_crypto), so both arms are
        UTF-8 on the wire; the difference is cryptography plus framing, not transport.
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
                # Deadline pacing, not sleep(1/rate): per-message sleeps drift and cannot
                # resolve the interval above a few hundred events/s - OS timer granularity
                # is 1-15 ms and 5000 TPS needs 0.2 ms. An absolute schedule keeps the
                # AVERAGE rate correct and makes falling behind measurable.
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
                # Integrity hash of the raw event, carried unchanged to the audit store.
                # Stamped after ingested_at so that cannot be altered undetected; it
                # excludes itself and the labels.
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
                    # SCOPE: the reconnect happens AFTER the send and _new_producer()
                    # blocks until bootstrap, so the next row's ingested_at is on the far
                    # side of the handshake - client-side handshake cost is EXCLUDED from
                    # every decision-latency figure by construction (handshake_bench.py
                    # measures it). A churn arm shows broker-side spillover; the confound
                    # the other way is that the pause drains buffers and the TLS arm pauses
                    # longer, biasing TLS to look FASTER (both arms reconnect).
                if args.limit is not None and sent >= args.limit:
                    break
    except KeyboardInterrupt:
        # Ctrl+C is a normal exit for a paced stream but must still reach the flush
        # below: messages buffered and never flushed would be counted in the
        # fault-injection run as transactions LOST after the kill. The count printed here
        # is what `fault_injection.py --expect` needs, knowable only on this side.
        interrupted = True

    if producer is not None:
        producer.flush()
    slice_note = f" (rows {args.skip:,}..{args.skip + sent:,})" if args.skip else ""
    slice_note += f", {reconnects} reconnects" if reconnects else ""
    print(f"produced {sent:,} messages to '{args.topic}'" + slice_note
          + (" (stopped by hand)" if interrupted else ""))

    # The achieved rate, always, and loudly when it is not the requested one: if the client
    # cannot reach the requested rate, every latency figure from that arm describes the
    # PRODUCER, not the pipeline, and the arm still looks successful unless the shortfall is
    # printed. `ingested_at` is stamped before send(), so client-buffer time is inside it.
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
