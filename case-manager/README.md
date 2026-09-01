# case-manager

The consumer `fraud.alerts` did not have.

Before this service the pipeline computed a decision, wrote it to the alert
topic, and nothing read it — `BLOCK` and `REVIEW` were strings in a warehouse,
not work anyone did. This turns each alert into a **case**, gives it a priority,
and records what a human decided about it.

## What it is not

It is not enforcement. Nothing here declines a transfer, holds an account, or
challenges a customer. The system detects and queues; a production deployment
would additionally answer the authorisation flow synchronously and trigger
step-up authentication. That boundary is deliberate and is declared in
`docs/irp-framing.md` rather than left to be discovered.

## Why the disposition matters more than the queue

`CONFIRMED_FRAUD` / `FALSE_POSITIVE` is the **only real label this system can
ever produce**. Every figure reported elsewhere is measured against generated
ground truth. What an analyst confirms is what a production model would actually
be retrained on, so the column exists even in a prototype with no analysts: it
is the shape of the feedback loop, and the place a real deployment would start
collecting.

`queue_cli.py stats` computes precision from those verdicts — and says, in its
own output, why that number reads high: analysts work the top of the queue, so
the resolved set is biased towards high scores.

## Files

| File | What it does |
|---|---|
| `case.py` | pure alert → case row; priority; the resolution rule. No I/O |
| `store.py` | ClickHouse access: open, read, resolve, count |
| `explain.py` | exact tree contributions, in words, for alerts no rule explains |
| `consumer.py` | the service: `fraud.alerts` → `fraud.cases` |
| `queue_cli.py` | the analyst surface: `list` / `show` / `resolve` / `stats` |
| `test_case.py` | 14 tests, incl. the replay-cannot-revert-a-verdict property |
| `test_store.py` | tests against a fake ClickHouse: schema, FINAL, round trip |
| `test_explain.py` | 14 tests, mostly about refusing to give a wrong reason |

## The schema is applied by the service, not by ClickHouse

`docker-entrypoint-initdb.d` scripts run **only when the data directory is
empty**. Every cluster that has ever produced a measurement has a populated
`clickhouse_data` volume, so a newly added schema file would never execute
there — and the failure is the quiet kind: the service starts, consumes the
alert topic, commits offsets, and every insert fails against a table that does
not exist.

So `store.open()` applies `02-cases.sql` on every connect. `CREATE TABLE IF NOT
EXISTS` makes that free, and the file is the same one ClickHouse would have run,
copied into the image by the Dockerfile — one source of truth, not two.

## Why the model's reasons are computed here and not in Flink

14.9% of alerts fire with **no rule hit at all** — the model alone. Those used
to reach the queue as an automated adverse decision with nothing to tell a
customer or an auditor. Now each carries the exact tree contributions that
pushed its score up, in words:

```
money into this payee in an hour: 3 208 945 UZS (+9.86)
payee's account age: 35 days (+7.09)
distinct senders paying this payee in an hour: 3 (+5.12)
```

Exact contributions cost **1.89 ms per event** (measured: 400 trees, 24
features, LightGBM `pred_contrib`). Restricted to alerts that is ~1.5% of
traffic, so the scoring path could have afforded it — and it was still the wrong
place. It would put a second copy of the model in the serving worker beside
`model.onnx`, and two artefacts of one model is how a system starts explaining a
different model than the one that decided. Nobody consumes an explanation at
decision time either: the analyst reads it from the case, the auditor from the
record. So the job publishes the feature vector it scored on (alerts only), and
the explanation is computed here, where a millisecond costs nothing.

**The guard.** This service loads `model.txt`; the pipeline serves `model.onnx`.
They agree to 3.3e-07 across 50 000 events today. Every explanation recomputes
the probability and compares it to the `ml_score` the job recorded — beyond
`1e-4` the explanation is refused and stored as `MODEL_MISMATCH`. A confident,
specific, wrong reason is worse than an admitted absence, so most of
`test_explain.py` is about refusing to speak.

## Two things in here that are easy to get wrong

**A replay must not undo a human's verdict.** The alert topic is
`AT_LEAST_ONCE`, so an alert can be redelivered after its case was resolved.
`fraud.cases` is a `ReplacingMergeTree` keyed on `case_id`, which keeps the
highest `version`. Opening a case therefore always writes `version = 0`, and a
resolution writes epoch milliseconds — so a replayed alert can never outrank a
resolution. Were the open row versioned by wall clock, a redelivery would
silently revert the verdict and put the case back in the queue as unworked.

**Reads need `FINAL`.** `ReplacingMergeTree` collapses duplicates only when
parts merge, which is background work on no schedule. A plain `SELECT` can
return both the open row and its resolution, and show a closed case as open.

## Use

```powershell
.\run.ps1 cases                                                  # the queue
.\run.ps1 cases -Case t_0041237                                  # one case in full
.\run.ps1 cases -Case t_0041237 -Verdict CONFIRMED_FRAUD -By analyst.k
.\run.ps1 cases -Stats                                           # dispositions + precision
```

`-By` is required for a resolution: a label with no author cannot be audited or
withdrawn.

## Known limitation: granularity

One case per alert. A mule receiving from twelve senders produces up to twelve
cases where an investigator wants one. Grouping alerts into an investigation (by
payee, within a window) is the obvious next step and is deliberately not done
here — it changes what a "false positive" counts, and the counting is what the
disposition exists for.
