# sink-writer

Persists every decision from `transactions.scored` to ClickHouse: the decision with
its stage times and the feature values it was taken on (`fraud.transactions_scored`,
which `ml/retrain.py` learns from), the audit record (`fraud.audit_log`, append-only,
each record chained to the previous one by a hash), and for a hold a case for the
analyst (`fraud.cases`) with its reasons in words, by `explain.py` over the served
model (`ml/models`, mounted).
A service of its own, so a slow or absent warehouse never holds back the job.

| File | What it does |
|---|---|
| `consumer.py` | the service loop: batch by size and time |
| `ch_writer.py` | batched inserts and the audit hash chain (data-generator's `integrity.py`) |
| `record.py` | a decision as ClickHouse rows; no I/O |
| `case.py` | a hold as a case row, a verdict or a client's report as a newer row; no I/O |
| `explain.py` | a hold's reasons: the model's exact tree contributions, in words |
| `verify_audit.py` | recomputes the chain over the warehouse and reports any break |
| `config.py` | connections and batch settings, from the environment |
| `tests/` | `python -m pytest sink-writer -q` |

When ClickHouse is down the writer stops reading and tries again every 10 seconds:
its offsets are committed only once a batch is stored, so a held transfer never
loses its case, and a crash reads again what was not stored. Rows a working
ClickHouse refuses are dropped and counted. The audit records are numbered at the
insert, so the chain has no gaps. The writer connects as `fraud_app`, which can only
add and read rows.
`run.ps1 verify-audit` checks the chain; `run.ps1 query-scored` counts decisions.
