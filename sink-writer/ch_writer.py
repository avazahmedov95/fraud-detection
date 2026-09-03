"""Batched ClickHouse writer for the scored rows and the audit log. Fails open,
but every discarded row is logged with a running total, never silently."""

import logging
import time

import record as R
import integrity

log = logging.getLogger("ch_writer")

RECONNECT_INTERVAL_S = 10.0  # retry throttle; every flush would hammer a down server



class ClickHouseWriter:
    def __init__(self, host, port, user, password, database, audit_all=True):
        self._cfg = dict(host=host, port=port, username=user,
                         password=password, database=database)
        self._db = database
        self._audit_all = audit_all
        self._client = None
        self._scored = []
        self._audit = []
        # Chain follows arrival order, not ClickHouse storage order.
        self._seq = 0
        self._prev_hash = integrity.GENESIS
        # Non-zero at shutdown invalidates any stored-row measurement.
        self._dropped_scored = 0
        self._dropped_audit = 0
        self._last_attempt = 0.0

    def open(self):
        self._last_attempt = time.time()
        try:
            import clickhouse_connect
            self._client = clickhouse_connect.get_client(**self._cfg)
            self._client.ping()
            log.info("ClickHouse connected (%s:%s/%s)",
                     self._cfg["host"], self._cfg["port"], self._db)
            self._resume_chain()
        except Exception as exc:                       # noqa: BLE001
            log.warning("ClickHouse unavailable, CH sink disabled: %s", exc)
            self._client = None
        return self._client is not None

    def _reconnect_due(self) -> bool:
        return (time.time() - self._last_attempt) >= RECONNECT_INTERVAL_S

    def _resume_chain(self):
        """Continue the audit chain across restarts: a fresh chain each boot
        would look like tampering to the verifier. Requires a single writer; a
        scaled-out sink would need a per-writer chain id."""
        try:
            res = self._client.query(
                f"SELECT seq, record_hash FROM {self._db}.audit_log "
                f"ORDER BY seq DESC LIMIT 1")
            if res.result_rows:
                last_seq, last_hash = res.result_rows[0]
                self._seq = int(last_seq) + 1
                self._prev_hash = last_hash
                log.info("audit chain resumed from seq=%d", last_seq)
        except Exception as exc:                       # noqa: BLE001
            log.warning("could not resume audit chain, starting fresh: %s", exc)

    def add(self, event: dict):
        self._scored.append(R.scored_row(event))
        if self._audit_all or R.is_alert(event):
            core = R.audit_core(event)
            seq, prev = self._seq, self._prev_hash
            rh = integrity.record_hash(prev, seq, R.audit_signed_values(core))
            self._audit.append(core + [seq, prev, rh])
            self._seq, self._prev_hash = seq + 1, rh

    def pending(self) -> int:
        return len(self._scored)

    def _discard(self, reason):
        """Drop the buffer, but never quietly."""
        self._dropped_scored += len(self._scored)
        self._dropped_audit += len(self._audit)
        log.error("ClickHouse sink DOWN (%s) - DISCARDING %d scored / %d audit rows. "
                  "Total lost this run: %d scored / %d audit. Kafka offsets have "
                  "already advanced, so these events will not be re-delivered.",
                  reason, len(self._scored), len(self._audit),
                  self._dropped_scored, self._dropped_audit)
        self._scored.clear(); self._audit.clear()

    def flush(self):
        if self._client is None:
            # ClickHouse not being ready at boot is transient; rows written after a
            # successful reconnect used to be lost outright.
            if not self._reconnect_due():
                if self._scored or self._audit:
                    self._discard("waiting to retry")
                return
            if not self.open():
                self._discard("reconnect failed")
                return

        try:
            if self._scored:
                self._client.insert(f"{self._db}.transactions_scored",
                                    self._scored, column_names=R.SCORED_COLUMNS)
            if self._audit:
                self._client.insert(f"{self._db}.audit_log",
                                    self._audit, column_names=R.AUDIT_COLUMNS)
        except Exception as exc:                       # noqa: BLE001
            self._dropped_scored += len(self._scored)
            self._dropped_audit += len(self._audit)
            log.error("ClickHouse insert FAILED (dropped %d scored / %d audit; "
                      "total lost this run: %d / %d): %s",
                      len(self._scored), len(self._audit),
                      self._dropped_scored, self._dropped_audit, exc)
            # A failure on a live connection is usually schema or serialisation, not a
            # dead server; drop the client so the next flush revalidates it.
            self._client = None
        finally:
            self._scored.clear(); self._audit.clear()

    def dropped(self):
        """(scored, audit) rows lost this run. Non-zero invalidates counts."""
        return self._dropped_scored, self._dropped_audit

    def close(self):
        self.flush()
        if self._dropped_scored or self._dropped_audit:
            log.error("sink shutting down having LOST %d scored / %d audit rows",
                      self._dropped_scored, self._dropped_audit)
        if self._client is not None:
            self._client.close()
