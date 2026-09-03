"""Batched Neo4j writer for ALERT transactions, as
(s:Person)-[:SENT]->(t:Transaction)-[:TO]->(r:Person). Both ends match by CARD:
the payee's PINFL is not on the wire. Fails open, with a running total - never quietly."""

import logging
import time

import record as R

log = logging.getLogger("neo4j_writer")

RECONNECT_INTERVAL_S = 10.0  # same throttle as ch_writer

_MERGE = """
UNWIND $rows AS row
MATCH (s:Person {card: row.sender})
MATCH (r:Person {card: row.receiver})
MERGE (t:Transaction {id: row.txid})
  SET t.amount         = row.amount,
      t.final_score    = row.final_score,
      t.decision       = row.decision,
      t.predicted_type = row.ptype,
      t.event_time     = row.event_time,
      t.rule_hits      = row.rule_hits
MERGE (s)-[:SENT]->(t)
MERGE (t)-[:TO]->(r)
"""

_CONSTRAINT = ("CREATE CONSTRAINT transaction_id IF NOT EXISTS "
               "FOR (t:Transaction) REQUIRE t.id IS UNIQUE")


class Neo4jWriter:
    def __init__(self, uri, user, password):
        self._uri = uri
        self._auth = (user, password)
        self._driver = None
        self._buf = []
        self._dropped = 0
        self._last_attempt = 0.0

    def open(self):
        self._last_attempt = time.time()
        try:
            from neo4j import GraphDatabase
            self._driver = GraphDatabase.driver(self._uri, auth=self._auth)
            self._driver.verify_connectivity()
            with self._driver.session() as s:
                s.run(_CONSTRAINT)
            log.info("Neo4j connected (%s)", self._uri)
        except Exception as exc:                       # noqa: BLE001
            log.warning("Neo4j unavailable, graph sink disabled: %s", exc)
            self._driver = None
        return self._driver is not None

    def _reconnect_due(self) -> bool:
        return (time.time() - self._last_attempt) >= RECONNECT_INTERVAL_S

    def _discard(self, reason):
        """Drop buffered alerts, but never quietly."""
        self._dropped += len(self._buf)
        log.error("Neo4j sink DOWN (%s) - DISCARDING %d alert(s). Total lost this "
                  "run: %d. Kafka offsets have already advanced, so these alerts "
                  "will not be re-delivered.", reason, len(self._buf), self._dropped)
        self._buf.clear()

    def add(self, event: dict):
        if R.is_alert(event):
            self._buf.append(R.alert_params(event))

    def pending(self) -> int:
        return len(self._buf)

    def flush(self):
        if not self._buf:
            return
        if self._driver is None:
            if not self._reconnect_due():
                self._discard("waiting to retry")
                return
            if not self.open():
                self._discard("reconnect failed")
                return
        try:
            with self._driver.session() as s:
                s.run(_MERGE, rows=self._buf)
        except Exception as exc:                       # noqa: BLE001
            self._dropped += len(self._buf)
            log.error("Neo4j write FAILED (dropped %d alert(s); total lost this "
                      "run: %d): %s", len(self._buf), self._dropped, exc)
            # As likely a dead session as a bad statement; revalidate on the next flush.
            self._driver = None
        finally:
            self._buf.clear()

    def dropped(self) -> int:
        """Alerts lost this run. Non-zero means the graph is incomplete."""
        return self._dropped

    def close(self):
        self.flush()
        if self._dropped:
            log.error("graph sink shutting down having LOST %d alert(s)", self._dropped)
        if self._driver is not None:
            self._driver.close()
