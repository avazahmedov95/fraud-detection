# case-manager

The analyst's queue: the cases in the ClickHouse table `fraud.cases`, and what an
analyst does with them. Not a service of its own: the sink writer opens a case for
every hold, with its reasons in words, and the demo page and the command line work
the queue through `store.py`.

A hold waits until the analyst decides: CONFIRMED_FRAUD blocks the transfer and the
money stays with the payer, FALSE_POSITIVE releases it. A confirmation also puts the
payee's card into the confirmed fraud accounts the job reads (Redis,
`confirmed:accounts`); a withdrawn one takes it out, unless another confirmed case or
the history (`confirmed:history`) still names it. A client can report a transfer the
system let go: it becomes a confirmed case, kept out of the hold figures, since
nobody held it. Verdicts and reports are the labels the model is retrained on.

| File | What it does |
|---|---|
| `case.py` | a hold as a case row, a verdict or a report as a newer row, how long a transfer was held; no I/O |
| `store.py` | the queue in ClickHouse - read, resolve, report, count, holds - and the confirmed accounts in Redis |
| `explain.py` | a hold's reasons: the model's exact tree contributions, in words (run by the sink writer) |
| `queue_cli.py` | the queue on the command line: `list`, `show`, `resolve`, `report`, `stats` |
| `config.py` | the CLI's warehouse connection |
| `tests/` | `python -m pytest case-manager -q` |

```powershell
.\run.ps1 cases                                                   # the queue
.\run.ps1 cases -Case <id> -Verdict CONFIRMED_FRAUD -By analyst.k  # block it
.\run.ps1 cases -Case <id> -Report -By analyst.k                  # a client's report
.\run.ps1 cases -Stats                                            # verdicts, precision, holds
```

The CLI runs in the demo's container, beside the same `store.py` the page uses.
