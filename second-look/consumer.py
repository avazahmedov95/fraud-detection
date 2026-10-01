"""Second-look service: fraud.second_look -> the final decision on the transfers just
under the review cut-off. Scope and design: second-look/README.md."""

import json
import logging
import os
import signal
import sys
import time

import config as C
from decide import verdict

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("second-look")

_running = True


def _stop(*_):
    global _running
    _running = False


def handle(event, look, send):
    """Score one waiting transfer and publish its decision: every decision to the
    scored topic, which the warehouse and the audit chain read, and a REVIEW to the
    alert topic as well, which opens the case that holds it."""
    try:
        score = look.score(event["features"]) if event.get("features") else None
    except Exception as exc:                           # noqa: BLE001 - held, and said so
        log.error("TabPFN failed on %s, holding it unscored: %s",
                  event.get("transaction_id"), exc)
        score = None
    out = verdict(event, score, look.spec["cut"], look.spec["checkpoint"])
    send(C.TOPIC_SCORED, out)
    if out["decision"] == "REVIEW":
        send(C.TOPIC_ALERTS, out)
    return out


def main():
    if not os.path.isfile(C.CHECKPOINT):
        log.error("no TabPFN checkpoint at %s: set TABPFN_CHECKPOINT in .env to the file "
                  "fetched from the vendor's page. Band transfers wait until it is.",
                  C.CHECKPOINT)
        sys.exit(1)
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    from kafka import KafkaConsumer, KafkaProducer
    from look import SecondLook

    started = time.time()
    look = SecondLook(C.MODELS_DIR, C.CHECKPOINT)
    log.info("context cached in %.0f s: %d pieces, holding at %.4f, band from %.4f",
             time.time() - started, len(look.members), look.spec["cut"], look.spec["from"])

    producer = KafkaProducer(bootstrap_servers=C.KAFKA_BOOTSTRAP, acks="all",
                             value_serializer=lambda v: json.dumps(v).encode("utf-8"))
    consumer = KafkaConsumer(
        C.TOPIC_SECOND_LOOK,
        bootstrap_servers=C.KAFKA_BOOTSTRAP,
        group_id=C.CONSUMER_GROUP,
        # Committed only after the decision is published: a crash re-decides a
        # transfer rather than losing it, and the case store collapses the repeat.
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=1000,
    )
    log.info("second-look started: %s -> %s, %s", C.TOPIC_SECOND_LOOK, C.TOPIC_SCORED,
             C.TOPIC_ALERTS)
    total = 0
    while _running:
        for msg in consumer:
            began = time.time()
            out = handle(msg.value, look, producer.send)
            producer.flush()
            consumer.commit()
            total += 1
            log.info("%s %s at %s in %.0f ms", out.get("transaction_id"), out["decision"],
                     out.get("final_score"), 1000 * (time.time() - began))
            if not _running:
                break
    consumer.close()
    producer.close()
    log.info("second-look stopped (%d decided)", total)


if __name__ == "__main__":
    main()
