"""Batched ClickHouse writer: every decision, its audit record, and a case for each
hold. Fails open, but every discarded row is logged with a running total."""

import logging
import os
import time

import case as CASE
import integrity
import record as R
from explain import Explainer

log = logging.getLogger("ch_writer")

RECONNECT_INTERVAL_S = 10.0
COLUMNS = {"transactions_scored": R.SCORED_COLUMNS, "audit_log": R.AUDIT_COLUMNS,
           "cases": CASE.CASE_COLUMNS}

#: Columns added after the tables were made. ClickHouse runs its init scripts only on
#: an empty data directory, so the writer applies this file on every connect.
_HERE = os.path.dirname(os.path.abspath(__file__))
_MIGRATION_CANDIDATES = (
    os.path.join(_HERE, "03-columns.sql"),                                       # in the image
    os.path.join(_HERE, "..", "infra", "clickhouse", "init", "03-columns.sql"),  # in the repo
)


def _migration_sql():
    """03-columns.sql as one query: comments stripped, no trailing semicolon."""
    path = next((p for p in _MIGRATION_CANDIDATES if os.path.exists(p)), None)
    if path is None:
        raise FileNotFoundError("03-columns.sql is missing; check the COPY in "
                                "infra/sink-writer/Dockerfile")
    with open(path, encoding="utf-8") as fh:
        sql = "\n".join(ln.split("--", 1)[0] for ln in fh.read().splitlines())
    return sql.strip().rstrip(";")


class ClickHouseWriter:
    def __init__(self, host, port, user, password, database, audit_all=True):
        self._cfg = dict(host=host, port=port, username=user,
                         password=password, database=database)
        self._db = database
        self._audit_all = audit_all
        self._client = None
        self._rows = {table: [] for table in COLUMNS}
        self._lost = dict.fromkeys(COLUMNS, 0)
        self._explainer = Explainer()
        # The audit chain follows arrival order, not storage order.
        self._seq, self._prev_hash = 0, integrity.GENESIS
        self._last_attempt = 0.0

    def open(self):
        self._last_attempt = time.time()
        try:
            import clickhouse_connect
            self._client = clickhouse_connect.get_client(**self._cfg)
            self._client.ping()
            self._client.command(_migration_sql())
            log.info("ClickHouse connected (%s:%s/%s)",
                     self._cfg["host"], self._cfg["port"], self._db)
            self._resume_chain()
        except Exception as exc:                       # noqa: BLE001
            log.warning("ClickHouse unavailable: %s", exc)
            self._client = None
        return self._client is not None

    def _resume_chain(self):
        """Continue the audit chain across restarts, as the verifier expects. One
        writer only: a second would need a chain of its own."""
        try:
            rows = self._client.query(f"SELECT seq, record_hash FROM {self._db}.audit_log "
                                      f"ORDER BY seq DESC LIMIT 1").result_rows
            if rows:
                self._seq, self._prev_hash = int(rows[0][0]) + 1, rows[0][1]
                log.info("audit chain resumed from seq=%d", rows[0][0])
        except Exception as exc:                       # noqa: BLE001
            log.warning("could not resume the audit chain, starting fresh: %s", exc)

    def add(self, event: dict):
        self._rows["transactions_scored"].append(R.scored_row(event))
        if self._audit_all or R.is_alert(event):
            core = R.audit_core(event)
            rh = integrity.record_hash(self._prev_hash, self._seq, R.audit_signed_values(core))
            self._rows["audit_log"].append(core + [self._seq, self._prev_hash, rh])
            self._seq, self._prev_hash = self._seq + 1, rh
        if R.is_alert(event):
            status, lines = self._explainer.explain(event.get("features"), event.get("ml_score"))
            self._rows["cases"].append(CASE.case_row(event, lines, status))

    def pending(self) -> int:
        return len(self._rows["transactions_scored"])

    def _lose(self, reason):
        """Drop the buffer, never quietly: Kafka has moved on and will not redeliver."""
        for table, rows in self._rows.items():
            self._lost[table] += len(rows)
            rows.clear()
        log.error("ClickHouse %s - rows lost this run: %s", reason, self._lost)

    def flush(self):
        reconnected = self._client is not None or (
            time.time() - self._last_attempt >= RECONNECT_INTERVAL_S and self.open())
        if not reconnected:
            if any(self._rows.values()):
                self._lose("down")
            return
        try:
            for table, rows in self._rows.items():
                if rows:
                    self._client.insert(f"{self._db}.{table}", rows, column_names=COLUMNS[table])
        except Exception as exc:                       # noqa: BLE001
            self._lose(f"insert failed: {exc}")
            self._client = None                        # the next flush reconnects
        for rows in self._rows.values():
            rows.clear()

    def close(self):
        self.flush()
        if any(self._lost.values()):
            log.error("sink stopping having LOST rows: %s", self._lost)
        if self._client is not None:
            self._client.close()
