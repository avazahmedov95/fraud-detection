"""ClickHouse access for the case queue: open cases, read them, resolve them.

Kept separate from the consumer so the analyst CLI can use the same code paths
and cannot invent a second opinion about how a case is stored.
"""

import logging
import os
import time

import case as CASE
from explain import Explainer

log = logging.getLogger("case_store")

RECONNECT_INTERVAL_S = 10.0
_TABLE = "cases"

#: The table DDL, shipped into the image beside this module.
#:
#: Applied on every connect rather than left to ClickHouse's initdb. Scripts in
#: docker-entrypoint-initdb.d run ONLY when the data directory is empty, so on
#: any cluster that has been up before - which is every cluster that has
#: produced a measurement - a newly added schema file is never executed. The
#: failure is quiet in the worst way: the service starts, consumes the alert
#: topic, commits offsets, and every insert fails against a table that does not
#: exist. That is the same shape as the 331 events the sink-writer once
#: discarded in silence, and it is avoidable here for the cost of one idempotent
#: statement per connect.
_HERE = os.path.dirname(os.path.abspath(__file__))
_DDL_CANDIDATES = (
    os.path.join(_HERE, "02-cases.sql"),                       # in the image
    os.path.join(_HERE, "..", "infra", "clickhouse", "init",   # in the repo
                 "02-cases.sql"),
)


def _ddl_file():
    for p in _DDL_CANDIDATES:
        if os.path.exists(p):
            return p
    return _DDL_CANDIDATES[0]


def _statements(sql: str):
    """Split a DDL file into executable statements.

    Comments are stripped BEFORE splitting, not after. The first version dropped
    only whole comment lines and then split on ";" - which silently tore the
    CREATE TABLE in half at the semicolon inside an inline comment
    ("-- one case per alert; see note below"), producing three fragments of
    which none was valid SQL. Comment text is not syntax, so it has to be gone
    before the delimiter is looked for.

    Line comments only: this file has no /* */ and no string literal containing
    "--", so a real SQL parser would be more machinery than the job needs. That
    is an assumption about THIS file, which is why test_store.py asserts the
    shipped schema parses to exactly one statement.
    """
    stripped = "\n".join(ln.split("--", 1)[0] for ln in sql.splitlines())
    for chunk in stripped.split(";"):
        chunk = chunk.strip()
        if chunk:
            yield chunk


class CaseStore:
    def __init__(self, host, port, user, password, database):
        self._cfg = dict(host=host, port=port, username=user,
                         password=password, database=database)
        self._db = database
        self._client = None
        self._buf = []
        self._dropped = 0
        self._last_attempt = 0.0
        # Lazily loads its model on first use; an absent artefact disables
        # explanations without stopping the queue, because an unexplained case
        # is still a case worth working.
        self._explainer = Explainer()

    # --- connection ---------------------------------------------------------

    def open(self):
        self._last_attempt = time.time()
        try:
            import clickhouse_connect
            self._client = clickhouse_connect.get_client(**self._cfg)
            self._client.ping()
            self._apply_schema()
            log.info("ClickHouse connected (%s:%s/%s)",
                     self._cfg["host"], self._cfg["port"], self._db)
        except Exception as exc:                       # noqa: BLE001
            log.warning("ClickHouse unavailable, will retry: %s", exc)
            self._client = None

    def _apply_schema(self):
        """CREATE TABLE IF NOT EXISTS, every connect. Idempotent by construction.

        Failing here is fatal to this connection rather than tolerated: a store
        that cannot guarantee its table exists would spend the run discarding
        cases and reporting itself connected.
        """
        ddl = _ddl_file()
        if not os.path.exists(ddl):
            raise FileNotFoundError(
                f"{ddl} is missing from the image. The case table cannot "
                "be assumed to exist: ClickHouse only runs the init scripts on "
                "an empty data directory. Check the COPY in "
                "infra/case-manager/Dockerfile.")
        with open(ddl, encoding="utf-8") as fh:
            sql = fh.read()
        for stmt in _statements(sql):
            self._client.command(stmt)

    def _ensure(self):
        if self._client is None and (
                time.time() - self._last_attempt >= RECONNECT_INTERVAL_S):
            self.open()
        return self._client is not None

    # --- writing ------------------------------------------------------------

    def add(self, alert: dict):
        status, lines = self._explainer.explain(alert.get("features"),
                                                alert.get("ml_score"))
        self._buf.append(CASE.case_row(alert, lines, status))

    def pending(self):
        return len(self._buf)

    def flush(self):
        if not self._buf:
            return
        if not self._ensure():
            # Loud, with a running total. An alert queue that drops silently is
            # worse than one that is down: the operator sees an empty queue and
            # reads it as "nothing to work on".
            self._dropped += len(self._buf)
            log.error("ClickHouse down, DISCARDED %d cases (%d total this run)",
                      len(self._buf), self._dropped)
            self._buf.clear()
            return
        rows, self._buf = self._buf, []
        try:
            self._client.insert(_TABLE, rows, column_names=CASE.CASE_COLUMNS,
                                database=self._db)
        except Exception as exc:                       # noqa: BLE001
            self._dropped += len(rows)
            log.error("case insert failed, DISCARDED %d (%d total): %s",
                      len(rows), self._dropped, exc)
            self._client = None

    # --- reading ------------------------------------------------------------

    def open_cases(self, limit=20):
        """The work queue: unresolved cases, most urgent first.

        FINAL is required, not optional. ReplacingMergeTree collapses duplicate
        keys only when parts merge, which is background work on no schedule; a
        plain SELECT can return both the open row and its resolution and show a
        closed case as open.
        """
        if not self._ensure():
            return []
        q = (f"SELECT {', '.join(CASE.CASE_COLUMNS)} "
             f"FROM {self._db}.{_TABLE} FINAL "
             f"WHERE disposition = 'NEW' "
             # Exposure, not score. The model's probability is near-constant
             # across the alert set (89.1% round to 1.000), so ordering by it
             # is ordering by nothing; amount spans four orders of magnitude.
             # Score stays as a later tiebreaker for the minority that differ.
             f"ORDER BY priority ASC, amount_uzs DESC, final_score DESC, "
             f"opened_at ASC "
             f"LIMIT {int(limit)}")
        return [dict(zip(CASE.CASE_COLUMNS, r))
                for r in self._client.query(q).result_rows]

    def get(self, case_id):
        if not self._ensure():
            return None
        q = (f"SELECT {', '.join(CASE.CASE_COLUMNS)} "
             f"FROM {self._db}.{_TABLE} FINAL WHERE case_id = %(cid)s")
        rows = self._client.query(q, parameters={"cid": case_id}).result_rows
        return dict(zip(CASE.CASE_COLUMNS, rows[0])) if rows else None

    def resolve(self, case_id, disposition, by, at_epoch=None):
        """Write a verdict. Returns False when there is no such case."""
        current = self.get(case_id)
        if current is None:
            return False
        row = CASE.resolution_row(current, disposition, by,
                                  time.time() if at_epoch is None else at_epoch)
        self._client.insert(_TABLE, [row], column_names=CASE.CASE_COLUMNS,
                            database=self._db)
        return True

    def stats(self):
        """Counts per disposition, and the precision they imply.

        This is the only place in the system where precision is computed from
        something other than generated ground truth. It is reported over
        RESOLVED cases only: open cases are not "not fraud", and folding them in
        would make the number drift upward as the queue grows.
        """
        if not self._ensure():
            return {}
        q = (f"SELECT disposition, count() FROM {self._db}.{_TABLE} FINAL "
             f"GROUP BY disposition")
        counts = {d: n for d, n in self._client.query(q).result_rows}
        confirmed = counts.get("CONFIRMED_FRAUD", 0)
        false_pos = counts.get("FALSE_POSITIVE", 0)
        resolved = confirmed + false_pos
        counts["_resolved"] = resolved
        counts["_precision"] = (confirmed / resolved) if resolved else None

        # Explanation coverage, and when the queue last received anything.
        #
        # Added after a live run where the answer to "are explanations working?"
        # had to be deduced from an ABSENT log line. An operator should be able
        # to read it: an empty status means the row was written by a build that
        # predates the column, NO_FEATURES means the scoring job is not
        # publishing its feature vector yet, and a stale max(opened_at) means
        # nothing has been consumed at all - three different problems that all
        # look identical in the queue itself.
        q = (f"SELECT explanation_status, count() FROM {self._db}.{_TABLE} FINAL "
             f"GROUP BY explanation_status")
        counts["_explanation"] = {(d or "(written before the column existed)"): n
                                  for d, n in self._client.query(q).result_rows}
        q = f"SELECT max(opened_at) FROM {self._db}.{_TABLE} FINAL"
        rows = self._client.query(q).result_rows
        counts["_last_opened"] = rows[0][0] if rows else None
        return counts

    def close(self):
        self.flush()
        if self._client is not None:
            try:
                self._client.close()
            except Exception:                          # noqa: BLE001
                pass
        if self._dropped:
            log.error("SHUTDOWN WITH LOSS: %d cases were never stored. Any "
                      "queue-derived number from this run is invalid.",
                      self._dropped)
