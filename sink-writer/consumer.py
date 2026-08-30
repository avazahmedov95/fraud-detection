"""
Sink-writer service: consumes transactions.scored from Kafka and persists every
event to ClickHouse (analytics + WORM audit) and each alert to the Neo4j graph.

Batched by size and time; commits offsets after processing. A clean SIGINT/SIGTERM
flushes and closes so no buffered rows are lost on shutdown.

  python consumer.py
"""

import json
import logging
import signal
import time

from kafka import KafkaConsumer

import config as C
from ch_writer import ClickHouseWriter
from neo4j_writer import Neo4jWriter

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("sink")

_running = True


def _stop(*_):
    global _running
    _running = False


signal.signal(signal.SIGINT, _stop)
signal.signal(signal.SIGTERM, _stop)


def main():
    ch = ClickHouseWriter(C.CH_HOST, C.CH_PORT, C.CH_USER, C.CH_PASSWORD, C.CH_DB, C.AUDIT_ALL)
    neo = Neo4jWriter(C.NEO4J_URI, C.NEO4J_USER, C.NEO4J_PASSWORD)
    ch.open()
    neo.open()

    consumer = KafkaConsumer(
        C.TOPIC_SCORED,
        bootstrap_servers=C.KAFKA_BOOTSTRAP,
        group_id=C.CONSUMER_GROUP,
        enable_auto_commit=True,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=1000,
    )
    log.info("sink-writer started: %s -> ClickHouse(%s) + Neo4j (batch=%d, flush=%.0fs)",
             C.TOPIC_SCORED, C.CH_DB, C.BATCH_SIZE, C.FLUSH_INTERVAL_S)

    last_flush = time.time()
    total = 0
    while _running:
        received = False
        for msg in consumer:
            received = True
            event = msg.value
            ch.add(event)
            neo.add(event)
            total += 1
            if ch.pending() >= C.BATCH_SIZE:
                ch.flush(); neo.flush()
                last_flush = time.time()
                log.info("flushed batch (total processed: %d)", total)
            if not _running:
                break
        if (time.time() - last_flush >= C.FLUSH_INTERVAL_S) or not received:
            ch.flush(); neo.flush()
            last_flush = time.time()

    ch.close()
    neo.close()
    consumer.close()
    log.info("sink-writer stopped (processed %d events)", total)


if __name__ == "__main__":
    main()
