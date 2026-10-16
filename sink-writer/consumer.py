"""Sink-writer service: transactions.scored -> ClickHouse: every decision, its audit
record, and a case for each hold. Batched by size and time. The offsets are committed
only once a batch is stored: while ClickHouse is down the writer waits and tries
again, and a crash reads again what was not stored."""

import json
import logging
import signal
import time

from kafka import KafkaConsumer

import config as C
from ch_writer import ClickHouseWriter

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("sink")

POLL_MS = 200

_running = True


def _stop(*_):
    global _running
    _running = False


signal.signal(signal.SIGINT, _stop)
signal.signal(signal.SIGTERM, _stop)


def _commit(consumer):
    try:
        consumer.commit()
    except Exception as exc:                           # noqa: BLE001 - read again at worst
        log.warning("offset commit failed, these decisions may be stored twice: %s", exc)


def _store(ch, consumer):
    """Flush, trying again while ClickHouse is down, then commit."""
    while not ch.flush():
        if not _running:
            return
        time.sleep(1)
    _commit(consumer)


def main():
    ch = ClickHouseWriter(C.CH_HOST, C.CH_PORT, C.CH_USER, C.CH_PASSWORD, C.CH_DB, C.AUDIT_ALL)
    ch.open()

    consumer = KafkaConsumer(
        C.TOPIC_SCORED,
        bootstrap_servers=C.KAFKA_BOOTSTRAP,
        group_id=C.CONSUMER_GROUP,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    log.info("sink-writer started: %s -> ClickHouse(%s) (batch=%d, flush=%.0fs)",
             C.TOPIC_SCORED, C.CH_DB, C.BATCH_SIZE, C.FLUSH_INTERVAL_S)

    last_flush, total = time.time(), 0
    while _running:
        for records in consumer.poll(timeout_ms=POLL_MS, max_records=C.BATCH_SIZE).values():
            for msg in records:
                ch.add(msg.value)
                total += 1
        if ch.pending() and (ch.pending() >= C.BATCH_SIZE
                             or time.time() - last_flush >= C.FLUSH_INTERVAL_S):
            _store(ch, consumer)
            last_flush = time.time()

    if ch.close():
        _commit(consumer)
    consumer.close()
    log.info("sink-writer stopped (processed %d events)", total)


if __name__ == "__main__":
    main()
