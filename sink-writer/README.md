# sink-writer

Persists every decision from `transactions.scored` to ClickHouse: the decision with
its stage times and the feature values it was taken on (`fraud.transactions_scored`,
which `ml/retrain.py` learns from), the audit record (`fraud.audit_log`, append-only,
each record chained to the previous one by a hash), and for a hold a case for the
analyst (`fraud.cases`) with its reasons in words, by case-manager's `explain.py`
over the served model (`ml/models`, mounted).
A service of its own, so a slow or absent warehouse never holds back the job.

| File | What it does |
|---|---|
| `consumer.py` | the service loop: batch by size and time |
| `ch_writer.py` | batched inserts and the audit hash chain (data-generator's `integrity.py`) |
| `record.py` | a decision as ClickHouse rows; no I/O |
| `verify_audit.py` | recomputes the chain over the warehouse and reports any break |
| `config.py` | connections and batch settings, from the environment |
| `tests/` | `python -m pytest sink-writer -q` |

When ClickHouse is down the writer keeps consuming and counts what it discards.
`run.ps1 verify-audit` checks the chain; `run.ps1 query-scored` counts decisions.
