# sink-writer — phase 7 ✅

Consumes `transactions.scored` from Kafka and persists every event to ClickHouse
and each alert to Neo4j. Runs as its own service so sink failures never backpressure
the scoring job, and the scored stream can be replayed from Kafka at any time.

```
transactions.scored  -->  ClickHouse fraud.transactions_scored   (all events, analytics)
                     -->  ClickHouse fraud.audit_log             (all decisions, WORM)
                     -->  Neo4j (s:Person)-[:SENT]->(:Transaction)-[:TO]->(r:Person)
                                                                  (alerts only)
```

## Files

```
record.py        pure mapping: scored event -> CH rows + Neo4j params (testable)
ch_writer.py     batched ClickHouse writer (transactions_scored + audit_log)
neo4j_writer.py  batched Neo4j alert-graph writer
consumer.py      Kafka consumer loop: batch by size/time, clean shutdown
config.py        connections + batch settings (env-driven)
test_record.py   unit tests (mapping + batching with fake clients)
```

## Design

- **Batched inserts.** ClickHouse strongly prefers batches, so rows buffer and
  flush every `SINK_BATCH_SIZE` (default 500) or `SINK_FLUSH_INTERVAL_S` (5s).
- **WORM audit.** Every decision is appended to `fraud.audit_log` with the full
  event JSON and the CEP `rule_hits`. Immutability is enforced at the grant level
  (INSERT/SELECT only — see the schema). Set `SINK_AUDIT_ALL=false` to audit only
  flagged (REVIEW/BLOCK) decisions.
- **Alert graph.** Only alerts go to Neo4j, as `:Transaction` nodes between the
  sender and receiver `:Person`. This puts flagged flows next to the account
  population, so mule fan-in/out and transfer rings become graph-queryable.
  Transactions are created only when both Persons exist (no orphan nodes).
- **Fails open.** If ClickHouse or Neo4j is down, that sink is disabled and the
  consumer keeps running rather than blocking the pipeline.

> Alternative for the ClickHouse leg: a native Kafka-engine table + materialized
> view ingests `transactions.scored` with SQL only. We use an explicit consumer so
> ClickHouse and the Neo4j graph are driven by one component with shared batching.

## Run

Comes up with the stack (`make up`). Useful checks:

```bash
make sink-logs        # tail the writer
make query-scored     # decision counts in fraud.transactions_scored
```

Example Neo4j query — top mule receivers (fan-in of alerts):

```cypher
MATCH (:Transaction)-[:TO]->(r:Person)
RETURN r.pinfl, count(*) AS incoming_alerts
ORDER BY incoming_alerts DESC LIMIT 10;
```

## Verify without the stack

```bash
pip install -r requirements.txt
python test_record.py     # mapping + batching unit tests (no DB needed)
```
