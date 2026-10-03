# sink-writer

Persists every decision from `transactions.scored` to ClickHouse: the decision with
its stage times (`fraud.transactions_scored`) and the audit record
(`fraud.audit_log`, append-only, each record chained to the previous one by a hash).
A service of its own, so a slow or absent warehouse never holds back the job.

| File | What it does |
|---|---|
| `consumer.py` | the service loop: batch by size and time |
| `ch_writer.py` | batched inserts and the audit hash chain |
| `record.py` | a decision as ClickHouse rows; no I/O |
| `integrity.py` | the hashes; byte-identical to data-generator's copy |
| `verify_audit.py` | recomputes the chain over the warehouse and reports any break |
| `config.py` | connections and batch settings, from the environment |
| `tests/` | `python -m pytest sink-writer -q` |

When ClickHouse is down the writer keeps consuming and counts what it discards.
`run.ps1 verify-audit` checks the chain; `run.ps1 query-scored` counts decisions.
