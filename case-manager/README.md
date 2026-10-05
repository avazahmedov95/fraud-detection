# case-manager

Turns every alert into a case for an analyst: `fraud.alerts` into the ClickHouse
table `fraud.cases`, each case with its reasons in words. A REVIEW holds the
transfer until the analyst decides: CONFIRMED_FRAUD blocks it and the money stays
with the payer, FALSE_POSITIVE releases it. The verdict is the only real label the
system produces, and a CONFIRMED_FRAUD also adds the payee's card at once to the
confirmed fraud accounts the job reads (Redis, `confirmed:accounts`). A verdict can
be changed: withdrawing a CONFIRMED_FRAUD takes the card out again, unless another
confirmed case or the labelled history (`confirmed:history`) still names it.

A client can also report a transfer the system let go: `report` opens a case for it,
confirmed at once, so the payee joins the confirmed accounts and a retrain
(`ml/retrain.py`) learns the miss. Such a case keeps the system's decision, ALLOW,
and stays out of the hold figures and the precision: nobody held it.

| File | What it does |
|---|---|
| `consumer.py` | the service: `fraud.alerts` -> `fraud.cases` |
| `case.py` | an alert as a case row, the verdict as a new row, how long a transfer was held; no I/O |
| `store.py` | ClickHouse access - open, read, resolve, count, holds - and the confirmed payee into and out of Redis |
| `explain.py` | each alert's reasons: the model's exact tree contributions, in words |
| `queue_cli.py` | the analyst's queue on the command line: `list`, `show`, `resolve`, `report`, `stats` |
| `config.py` | connections, from the environment |
| `tests/` | `python -m pytest case-manager -q` |

The service applies `02-cases.sql` on every connect: ClickHouse runs its init
scripts only on an empty data directory. A redelivered alert cannot reopen a case
already decided.

```bash
.\run.ps1 cases                                                   # the queue
.\run.ps1 cases -Case <id> -Verdict CONFIRMED_FRAUD -By analyst.k  # block it
.\run.ps1 cases -Case <id> -Report -By analyst.k                  # a client's report
.\run.ps1 cases -Stats                                            # verdicts, precision, holds
```

The demo page's "Block" and "Release" buttons write the same verdicts through the
same `store.py`, its list of verdicts changes one, and an allowed transfer has a
button for the client's report; on the command line, resolve the case again.
