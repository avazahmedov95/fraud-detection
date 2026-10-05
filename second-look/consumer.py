"""Second-look service: fraud.second_look -> the final decision on the transfers just
under the review cut-off. Scope and design: second-look/README.md."""

import json
import logging
import os
import signal
import sys
import time

import config as C
from decide import verdict, waited

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("second-look")

_running = True


def _stop(*_):
    global _running
    _running = False


def handle(event, look, send, now=None):
    """Score one waiting transfer and publish its decision to the scored topic. Past
    the deadline it is held unscored at once, so nothing queued behind waits longer."""
    age = waited(event, time.time() if now is None else now)
    score = None
    if age >= C.DEADLINE_S:
        log.warning("%s waited %.1f s, past the %.0f s deadline: held unscored",
                    event.get("transaction_id"), age, C.DEADLINE_S)
    else:
        try:
            score = look.score(event["features"]) if event.get("features") else None
        except Exception as exc:                       # noqa: BLE001 - held, and said so
            log.error("TabPFN failed on %s, holding it unscored: %s",
                      event.get("transaction_id"), exc)
    out = verdict(event, score, look.spec["cut"], look.spec["checkpoint"])
    send(C.TOPIC_SCORED, out)
    return out


def _renew(alive):
    """Tell the job the service is answering; DEADLINE_S after the last renewal the
    key lapses and the job holds band transfers itself."""
    try:
        alive.set(C.ALIVE_KEY, int(time.time()), px=int(C.DEADLINE_S * 1000))
    except Exception as exc:                           # noqa: BLE001 - the job holds instead
        log.warning("cannot renew %s in Redis, the job will hold band transfers: %s",
                    C.ALIVE_KEY, exc)


def main():
    if not os.path.isfile(C.CHECKPOINT):
        log.error("no TabPFN checkpoint at %s: set TABPFN_CHECKPOINT in .env to the file "
                  "fetched from the vendor's page. Band transfers wait until it is.",
                  C.CHECKPOINT)
        sys.exit(1)
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    import redis
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
        # Committed after publishing: a crash re-decides a transfer, never loses it.
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=1000,
    )
    alive = redis.Redis(host=C.REDIS_HOST, port=C.REDIS_PORT, socket_timeout=1)
    log.info("second-look started: %s -> %s; deadline %.0f s", C.TOPIC_SECOND_LOOK,
             C.TOPIC_SCORED, C.DEADLINE_S)
    total = 0
    while _running:
        _renew(alive)
        for msg in consumer:
            began = time.time()
            out = handle(msg.value, look, producer.send)
            producer.flush()
            consumer.commit()
            total += 1
            log.info("%s %s at %s in %.0f ms", out.get("transaction_id"), out["decision"],
                     out.get("final_score"), 1000 * (time.time() - began))
            _renew(alive)
            if not _running:
                break
    consumer.close()
    producer.close()
    log.info("second-look stopped (%d decided)", total)


if __name__ == "__main__":
    main()
