"""
Enrichment: the Neo4j account lookup, cached in Redis.

For each transaction the pipeline needs one fact that is NOT in the raw switch
message:
  * receiver_account_age_days — how old is the receiver's account?

Results are cached in Redis (most lookups then hit the cache, keeping per-event
latency low). If Redis or Neo4j is unreachable, lookups fail open (unknown age)
and the pipeline keeps running on behavioural signals alone.

An earlier design also looked up a MyID-style kinship edge (`is_family_transfer`).
It was removed: on synthetic data it ranked first in SHAP, but as an artefact of
how the data was constructed rather than as a real signal.

NOTE: synchronous lookups are fine for a prototype; in production this stage
would use Flink async I/O.
"""

import logging

log = logging.getLogger("enrichment")

_AGE_QUERY = """
MATCH (r:Person {pinfl: $receiver})
RETURN r.account_age_days AS receiver_age
"""


class EnrichmentClient:
    def __init__(self, neo4j_uri, neo4j_user, neo4j_password,
                 redis_host, redis_port, cache_ttl_s):
        self._cfg = dict(neo4j_uri=neo4j_uri, neo4j_user=neo4j_user,
                         neo4j_password=neo4j_password, redis_host=redis_host,
                         redis_port=redis_port, cache_ttl_s=cache_ttl_s)
        self._driver = None
        self._redis = None

    def open(self):
        """Connect lazily; tolerate either backend being unavailable."""
        try:
            from neo4j import GraphDatabase
            self._driver = GraphDatabase.driver(
                self._cfg["neo4j_uri"],
                auth=(self._cfg["neo4j_user"], self._cfg["neo4j_password"]))
            self._driver.verify_connectivity()
        except Exception as exc:                       # noqa: BLE001
            log.warning("Neo4j unavailable, age lookups will fail open: %s", exc)
            self._driver = None
        try:
            import redis
            self._redis = redis.Redis(host=self._cfg["redis_host"],
                                      port=self._cfg["redis_port"],
                                      decode_responses=True)
            self._redis.ping()
        except Exception as exc:                       # noqa: BLE001
            log.warning("Redis unavailable, enrichment cache disabled: %s", exc)
            self._redis = None

    def lookup(self, receiver_pinfl):
        """Return receiver_age_days (int | None)."""
        key = f"age:{receiver_pinfl}"
        if self._redis is not None:
            try:
                cached = self._redis.get(key)
                if cached is not None:
                    return int(cached) if cached != "" else None
            except Exception:                          # noqa: BLE001
                pass

        receiver_age = None
        if self._driver is not None:
            try:
                with self._driver.session() as session:
                    rec = session.run(_AGE_QUERY, receiver=receiver_pinfl).single()
                if rec is not None:
                    receiver_age = rec["receiver_age"]
            except Exception as exc:                   # noqa: BLE001
                log.warning("age lookup failed, failing open: %s", exc)

        if self._redis is not None:
            try:
                self._redis.setex(key, self._cfg["cache_ttl_s"],
                                  "" if receiver_age is None else receiver_age)
            except Exception:                          # noqa: BLE001
                pass
        return receiver_age

    def close(self):
        if self._driver is not None:
            self._driver.close()
