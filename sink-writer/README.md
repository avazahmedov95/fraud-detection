# sink-writer

Consumes `transactions.scored` from Kafka and persists every event to ClickHouse.
Runs as its own service so sink failures never backpressure the scoring job, and
the scored stream can be replayed from Kafka at any time.

```
transactions.scored  -->  ClickHouse fraud.transactions_scored   (all events, analytics)
                     -->  ClickHouse fraud.audit_log             (all decisions, WORM)
```

## Files

```
record.py        pure mapping: scored event -> ClickHouse rows (testable)
ch_writer.py     batched ClickHouse writer (transactions_scored + audit_log)
consumer.py      Kafka consumer loop: batch by size/time, clean shutdown
config.py        connections + batch settings (env-driven)
integrity.py     the audit hash chain; byte-identical to data-generator's copy
verify_audit.py  recompute the chain over the warehouse and find any break
tests/           run with `python -m pytest sink-writer -q`
```

## Design

- **Batched inserts.** ClickHouse strongly prefers batches, so rows buffer and
  flush every `SINK_BATCH_SIZE` (default 500) or `SINK_FLUSH_INTERVAL_S` (5s).
- **WORM audit.** Every decision is appended to `fraud.audit_log` with the full
  event JSON and the CEP `rule_hits`. Immutability is enforced at the grant level
  (INSERT/SELECT only — see the schema). Set `SINK_AUDIT_ALL=false` to audit only
  REVIEW decisions.
- **Fails open.** If ClickHouse is down, the sink is disabled and the consumer
  keeps running rather than blocking the pipeline.
- **No alert graph since 2026-10-03.** Alerts also went to Neo4j as a graph for
  investigations; no decision read it, and it was removed (`ml/README.md`).

> Alternative: a native Kafka-engine table + materialized
> view ingests `transactions.scored` with SQL only. We use an explicit consumer
> because the audit chain's hashes are computed here (`integrity.py`).

## Run

Comes up with the stack (`make up`). Useful checks:

```bash
make sink-logs        # tail the writer
make query-scored     # decision counts in fraud.transactions_scored
```

## Verify without the stack

```bash
pip install -r requirements.txt
python -m pytest ../sink-writer -q    # mapping, batching and the hash chain
```
