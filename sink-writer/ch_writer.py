"""Batched ClickHouse writer: every decision, its audit record, and a case for each
hold. While ClickHouse is down it keeps the batch for the next try, so a hold never
loses its case; only rows a working ClickHouse refuses are dropped, and counted."""

import logging
import time

import case as CASE
import integrity
import record as R
from explain import Explainer

log = logging.getLogger("ch_writer")

RECONNECT_INTERVAL_S = 10.0
COLUMNS = {"transactions_scored": R.SCORED_COLUMNS, "audit_log": R.AUDIT_COLUMNS,
           "cases": CASE.CASE_COLUMNS}


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
        # The stored chain's tail: the audit records are linked onto it at the insert,
        # so a failed insert leaves no gap, and it moves only when they are stored.
        self._seq, self._prev_hash = 0, integrity.GENESIS
        self._last_attempt = 0.0

    def open(self):
        self._last_attempt = time.time()
        try:
            import clickhouse_connect
            self._client = clickhouse_connect.get_client(**self._cfg)
            if not self._client.ping():
                raise ConnectionError("no answer to ping")
            self._resume_chain()
            log.info("ClickHouse connected (%s:%s/%s)",
                     self._cfg["host"], self._cfg["port"], self._db)
        except Exception as exc:                       # noqa: BLE001
            log.warning("ClickHouse unavailable: %s", exc)
            self._client = None
        return self._client is not None

    def _resume_chain(self):
        """Continue the stored audit chain. Raises when its tail cannot be read, so the
        writer never starts a second chain beside it."""
        rows = self._client.query(f"SELECT seq, record_hash FROM {self._db}.audit_log "
                                  f"ORDER BY seq DESC LIMIT 1").result_rows
        if rows:
            self._seq, self._prev_hash = int(rows[0][0]) + 1, rows[0][1]
            log.info("audit chain resumed from seq=%d", rows[0][0])

    def add(self, event: dict):
        self._rows["transactions_scored"].append(R.scored_row(event))
        if self._audit_all or R.is_alert(event):
            self._rows["audit_log"].append(R.audit_core(event))
        if R.is_alert(event):
            status, lines = self._explainer.explain(event.get("features"), event.get("ml_score"))
            self._rows["cases"].append(CASE.case_row(event, lines, status))

    def pending(self) -> int:
        return max(len(rows) for rows in self._rows.values())

    def _lose(self, reason):
        """Drop the buffer, never quietly."""
        for table, rows in self._rows.items():
            self._lost[table] += len(rows)
            rows.clear()
        log.error("ClickHouse %s - rows lost this run: %s", reason, self._lost)

    def _answers(self):
        try:
            return bool(self._client.ping())
        except Exception:                              # noqa: BLE001
            return False

    def _chained(self, cores):
        """The pending audit records linked onto the stored chain's tail."""
        rows, seq, prev = [], self._seq, self._prev_hash
        for core in cores:
            rh = integrity.record_hash(prev, seq, R.audit_signed_values(core))
            rows.append(core + [seq, prev, rh])
            seq, prev = seq + 1, rh
        return rows

    def flush(self) -> bool:
        """Store what is pending. False while ClickHouse is down: the rows not yet
        stored stay for the next try."""
        if not any(self._rows.values()):
            return True
        if self._client is None and (time.time() - self._last_attempt < RECONNECT_INTERVAL_S
                                     or not self.open()):
            return False
        audit = self._chained(self._rows["audit_log"])
        try:
            for table, rows in self._rows.items():
                rows = audit if table == "audit_log" else rows
                if rows:
                    self._client.insert(f"{self._db}.{table}", rows, column_names=COLUMNS[table])
                    if table == "audit_log":
                        self._seq, self._prev_hash = rows[-1][-3] + 1, rows[-1][-1]
                    self._rows[table].clear()
        except Exception as exc:                       # noqa: BLE001
            if self._answers():
                self._lose(f"refused the rows: {exc}")
                return True
            log.warning("ClickHouse down, %d decisions kept for the next try: %s",
                        self.pending(), exc)
            self._client = None                        # the reconnect re-reads the tail
            return False
        return True

    def close(self) -> bool:
        stored = self.flush()
        if any(self._lost.values()):
            log.error("sink stopping having LOST rows: %s", self._lost)
        if self._client is not None:
            self._client.close()
        return stored
