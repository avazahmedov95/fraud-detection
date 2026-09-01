"""Case-manager service: consumes fraud.alerts and opens a case per alert.

This is the consumer the alert topic did not have. Before it existed, the
pipeline computed a decision, wrote it to fraud.alerts, and nothing read it -
so BLOCK and REVIEW were strings in a warehouse rather than work anyone did.
It still is not enforcement (nothing here declines a transfer or challenges a
customer; see docs/irp-framing.md on scope), but the verdict now lands somewhere
a person acts on, and their verdict is recorded.

  python consumer.py
"""

import json
import logging
import signal
import time

from kafka import KafkaConsumer

import config as C
from store import CaseStore

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("case-manager")

_running = True


def _stop(*_):
    global _running
    _running = False


signal.signal(signal.SIGINT, _stop)
signal.signal(signal.SIGTERM, _stop)


def main():
    store = CaseStore(C.CH_HOST, C.CH_PORT, C.CH_USER, C.CH_PASSWORD, C.CH_DB)
    store.open()

    consumer = KafkaConsumer(
        C.TOPIC_ALERTS,
        bootstrap_servers=C.KAFKA_BOOTSTRAP,
        group_id=C.CONSUMER_GROUP,
        enable_auto_commit=True,
        # From the beginning on first run: an alert raised before this service
        # existed is still an alert nobody worked. Re-consumption is safe -
        # case_row() is deterministic and the open version is 0, so a case that
        # was already resolved cannot be reopened by a replay.
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=1000,
    )
    log.info("case-manager started: %s -> ClickHouse(%s.cases) "
             "(batch=%d, flush=%.0fs)",
             C.TOPIC_ALERTS, C.CH_DB, C.BATCH_SIZE, C.FLUSH_INTERVAL_S)

    last_flush = time.time()
    total = 0
    while _running:
        received = False
        for msg in consumer:
            received = True
            alert = msg.value
            # Defensive: the topic is fed by a filter on decision != ALLOW, but
            # a filter is a line of code and this is the only reader. An ALLOW
            # reaching the queue would be an analyst asked to investigate a
            # transfer the system approved.
            if alert.get("decision") == "ALLOW":
                log.warning("ALLOW on the alert topic (transaction %s) - "
                            "skipped; check the alerts-sink filter",
                            alert.get("transaction_id"))
                continue
            store.add(alert)
            total += 1
            if store.pending() >= C.BATCH_SIZE:
                store.flush()
                last_flush = time.time()
                log.info("opened batch (total this run: %d)", total)
            if not _running:
                break
        if (time.time() - last_flush >= C.FLUSH_INTERVAL_S) or not received:
            store.flush()
            last_flush = time.time()

    store.close()
    consumer.close()
    log.info("case-manager stopped (opened %d cases)", total)


if __name__ == "__main__":
    main()
