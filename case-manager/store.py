"""The analyst's queue in ClickHouse: read the cases the sink writer opens, give or
change a verdict, record a client's report, count. Shared by the demo and the CLI."""

import logging
import os
import time

import case as CASE

log = logging.getLogger("case_store")

RECONNECT_INTERVAL_S = 10.0
_TABLE = "cases"
#: The accounts in confirmed frauds the job reads (stream-processor/config.py).
CONFIRMED_KEY = "confirmed:accounts"
#: The history's confirmed accounts (ml/seed_confirmed.py): no withdrawal removes them.
HISTORY_KEY = "confirmed:history"
#: Cases a hold opened; a client's report on a transfer let go is not one.
HELD = "decision = 'REVIEW'"
#: Applied on every connect: ClickHouse runs its init scripts only on an empty data
#: directory.
_DDL = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "infra", "clickhouse",
                    "init", "02-cases.sql")


def _statements(sql: str):
    """The DDL file's statements, comments stripped first: a ";" in a comment would
    otherwise split one."""
    stripped = "\n".join(ln.split("--", 1)[0] for ln in sql.splitlines())
    return [chunk.strip() for chunk in stripped.split(";") if chunk.strip()]


class CaseStore:
    def __init__(self, host, port, user, password, database):
        self._cfg = dict(host=host, port=port, username=user,
                         password=password, database=database)
        self._db = database
        self._client = None
        self._redis = None
        self._last_attempt = 0.0

    def open(self):
        self._last_attempt = time.time()
        try:
            import clickhouse_connect
            self._client = clickhouse_connect.get_client(**self._cfg)
            self._client.ping()
            self._apply_schema()
        except Exception as exc:                       # noqa: BLE001
            log.warning("ClickHouse unavailable, will retry: %s", exc)
            self._client = None

    def _apply_schema(self):
        with open(_DDL, encoding="utf-8") as fh:
            for stmt in _statements(fh.read()):
                self._client.command(stmt)

    def _ensure(self):
        if self._client is None and time.time() - self._last_attempt >= RECONNECT_INTERVAL_S:
            self.open()
        return self._client is not None

    def _select(self, where, params=None, tail=""):
        """Cases as dicts. FINAL: until parts merge, a plain SELECT can return a case's
        open row beside its verdict."""
        q = (f"SELECT {', '.join(CASE.CASE_COLUMNS)} FROM {self._db}.{_TABLE} FINAL "
             f"WHERE {where} {tail}")
        return [dict(zip(CASE.CASE_COLUMNS, r))
                for r in self._client.query(q, parameters=params).result_rows]

    def _insert(self, row):
        self._client.insert(_TABLE, [row], column_names=CASE.CASE_COLUMNS, database=self._db)

    def open_cases(self, limit=20, since=None):
        """The held transfers, the largest amount first; with `since` (epoch seconds),
        those opened from then on."""
        if not self._ensure():
            return []
        where = "disposition = 'NEW'"
        if since is not None:
            where += " AND opened_at >= fromUnixTimestamp64Milli(%(since)s)"
        return self._select(where, None if since is None else {"since": int(since * 1000)},
                            f"ORDER BY amount_uzs DESC, final_score DESC, opened_at ASC "
                            f"LIMIT {int(limit)}")

    def get(self, case_id):
        if not self._ensure():
            return None
        rows = self._select("case_id = %(cid)s", {"cid": case_id})
        return rows[0] if rows else None

    def resolved_cases(self, since, limit=20):
        """The verdicts given from `since` (epoch seconds) on, the latest first."""
        if not self._ensure():
            return []
        return self._select("disposition != 'NEW' AND resolved_at >= "
                            "fromUnixTimestamp64Milli(%(since)s)", {"since": int(since * 1000)},
                            f"ORDER BY resolved_at DESC LIMIT {int(limit)}")

    def resolve(self, case_id, disposition, by, at_epoch=None):
        """Give a verdict, or change one. False when there is no such case."""
        current = self.get(case_id)
        if current is None:
            return False
        self._insert(CASE.resolution_row(current, disposition, by,
                                         time.time() if at_epoch is None else at_epoch))
        if disposition == "CONFIRMED_FRAUD":
            self._confirm(current["receiver_card"])
        elif current["disposition"] == "CONFIRMED_FRAUD":
            self._withdraw(current["receiver_card"])
        return True

    def report(self, transaction_id, by, at_epoch=None):
        """A client reports a transfer as fraud: its case is confirmed, or, for one the
        system let go, opened confirmed from the warehouse. False when the warehouse
        does not have it yet."""
        if self.get(transaction_id) is not None:
            return self.resolve(transaction_id, "CONFIRMED_FRAUD", by, at_epoch)
        if not self._ensure():
            return False
        # The latest row: a transfer sent for the second look has its answer after it.
        q = (f"SELECT {', '.join(CASE.REPORT_SOURCE)} FROM {self._db}.transactions_scored "
             f"WHERE transaction_id = %(t)s ORDER BY scored_at DESC LIMIT 1")
        rows = self._client.query(q, parameters={"t": transaction_id}).result_rows
        if not rows:
            return False
        transfer = dict(zip(CASE.REPORT_SOURCE, rows[0]))
        self._insert(CASE.report_row(transfer, by, time.time() if at_epoch is None else at_epoch))
        self._confirm(transfer["receiver_card"])
        return True

    def _redis_client(self):
        if self._redis is None:
            import redis
            self._redis = redis.Redis(host=os.getenv("REDIS_HOST", "redis"),
                                      port=int(os.getenv("REDIS_PORT", "6379")),
                                      socket_timeout=2)
        return self._redis

    def _confirm(self, card):
        """The payee joins the confirmed accounts. Redis down costs the mark, not the
        verdict, which is already stored."""
        try:
            self._redis_client().sadd(CONFIRMED_KEY, card)
        except Exception as exc:                       # noqa: BLE001
            log.error("confirmed %s, but could not add it to %s: %s", card, CONFIRMED_KEY, exc)

    def _withdraw(self, card):
        """The payee leaves the confirmed accounts, unless another confirmed case or the
        history still names it."""
        q = (f"SELECT count() FROM {self._db}.{_TABLE} FINAL "
             f"WHERE receiver_card = %(card)s AND disposition = 'CONFIRMED_FRAUD'")
        if self._client.query(q, parameters={"card": card}).result_rows[0][0]:
            return
        try:
            r = self._redis_client()
            if not r.sismember(HISTORY_KEY, card):
                r.srem(CONFIRMED_KEY, card)
        except Exception as exc:                       # noqa: BLE001
            log.error("withdrew %s, but could not take it out of %s: %s",
                      card, CONFIRMED_KEY, exc)

    def stats(self):
        """Counts per verdict and the precision they imply, over resolved holds only:
        an open case is not "not fraud"."""
        if not self._ensure():
            return {}
        q = (f"SELECT disposition, count() FROM {self._db}.{_TABLE} FINAL "
             f"WHERE {HELD} GROUP BY disposition")
        counts = {d: n for d, n in self._client.query(q).result_rows}
        confirmed, false_pos = counts.get("CONFIRMED_FRAUD", 0), counts.get("FALSE_POSITIVE", 0)
        counts["_resolved"] = confirmed + false_pos
        counts["_precision"] = confirmed / counts["_resolved"] if counts["_resolved"] else None
        q = (f"SELECT count() FROM {self._db}.{_TABLE} FINAL "
             f"WHERE NOT ({HELD}) AND disposition = 'CONFIRMED_FRAUD'")
        counts["_reported"] = self._client.query(q).result_rows[0][0]
        q = (f"SELECT explanation_status, count() FROM {self._db}.{_TABLE} FINAL "
             f"WHERE {HELD} GROUP BY explanation_status")
        counts["_explanation"] = {(d or "(none)"): n for d, n in self._client.query(q).result_rows}
        q = f"SELECT max(opened_at) FROM {self._db}.{_TABLE} FINAL WHERE {HELD}"
        rows = self._client.query(q).result_rows
        counts["_last_opened"] = rows[0][0] if rows else None
        return counts

    def holds(self, now=None):
        """What holding costs and keeps (case.hold_summary)."""
        if not self._ensure():
            return {}
        cols = ("disposition", "opened_at", "resolved_at", "amount_uzs")
        q = (f"SELECT {', '.join(cols)} FROM {self._db}.{_TABLE} FINAL "
             f"WHERE {HELD} AND toUnixTimestamp64Milli(opened_at) > 0")
        return CASE.hold_summary([dict(zip(cols, r)) for r in self._client.query(q).result_rows],
                                 time.time() if now is None else now)
