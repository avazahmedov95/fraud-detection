"""Receiver account-age lookup: Neo4j, cached in Redis, keyed by payee identity.
Fails open - an unreachable backend means "age unknown", not a stalled pipeline."""

import logging

import capabilities as CAP

log = logging.getLogger("enrichment")

#: One query per payee identity, chosen by the payee_identity capability: the
#: age lookup must key on the same identity as the rest of the receiver-side
#: state. Two literals, not an f-string - Neo4j cannot bind a property name.
_AGE_QUERY_BY_FIELD = {
    "card": """
MATCH (r:Person {card: $payee})
RETURN r.account_age_days AS receiver_age
""",
    "pinfl": """
MATCH (r:Person {pinfl: $payee})
RETURN r.account_age_days AS receiver_age
""",
}


class EnrichmentClient:
    def __init__(self, neo4j_uri, neo4j_user, neo4j_password,
                 redis_host, redis_port, cache_ttl_s):
        self._cfg = dict(neo4j_uri=neo4j_uri, neo4j_user=neo4j_user,
                         neo4j_password=neo4j_password, redis_host=redis_host,
                         redis_port=redis_port, cache_ttl_s=cache_ttl_s)
        self._driver = None
        self._redis = None
        # Resolved once: the mode is read from the environment at import.
        self._payee_field = CAP.mode("payee_identity")
        self._query = _AGE_QUERY_BY_FIELD[self._payee_field]

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

    def lookup(self, payee):
        """Return receiver_age_days (int | None) for a resolved payee key; the
        caller passes features.payee_key(event), not a raw field.
        """
        # Identity is part of the cache key: without it, flipping the mode against a
        # warm Redis would read a card's age out of an entry written for a PINFL.
        key = f"age:{self._payee_field}:{payee}"
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
                    rec = session.run(self._query, payee=payee).single()
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
