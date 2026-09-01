"""
Batched Neo4j writer: persists ALERT transactions (decision != ALLOW) as
:Transaction nodes linking the sender and receiver :Person nodes.

  (s:Person)-[:SENT]->(t:Transaction)-[:TO]->(r:Person)

Both ends are matched by CARD. The payee's PINFL is not on the wire - a sending
bank does not hold it - so the money-flow network is the card-to-card graph the
switch actually sees.

This puts the flagged-flow network next to the account population, so mule
fan-in/out and transfer rings become graph-queryable. Only alerts are written to
keep the graph focused. Fails open if Neo4j is unreachable.

The Transaction is created only when both Persons exist, so no orphan nodes are
left for alerts that reference an unknown PINFL.

"Fails open" means the pipeline keeps running, NOT that losses go unrecorded.
The original version disabled the sink permanently when the driver could not be
opened and cleared its buffer with no log on every flush thereafter — the same
defect that made the ClickHouse sink discard 331 consumed events in silence
while Kafka offsets advanced. Alerts are the output this system exists to
produce, so losing them quietly is the worst available failure mode.
"""

import logging
import time

import record as R

log = logging.getLogger("neo4j_writer")

# Matches ch_writer: retry a dead connection at most this often.
RECONNECT_INTERVAL_S = 10.0

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
            # Revalidate the connection on the next flush: a write that fails
            # against a supposedly-live driver is as likely to be a dead session
            # as a bad statement.
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
