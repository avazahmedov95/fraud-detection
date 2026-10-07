# demo

One page over the running system, in Russian and English, at
http://localhost:8090 (this machine only). It shows the running system or nothing:
it opens once Kafka, ClickHouse and the Flink job answer, and every figure on it
comes from them.

| File | What it does |
|---|---|
| `server.py` | the page's server: sends transfers, reads decisions from `transactions.scored`, the case queue through `store.py` |
| `store.py` | the analyst's queue in ClickHouse - read, resolve, report, count, holds - and the confirmed fraud accounts in Redis |
| `queue_cli.py` | the same queue on the command line: `list`, `show`, `resolve`, `report`, `stats` |
| `index.html` | the page |
| `about.js` | the "About the project" tab |
| `show.html` | the page for an audience, http://localhost:8090/show |
| `results.json` | the figures both pages quote, each with the line of the document it came from |
| `tests/` | `python -m pytest demo -q` |

Tabs:

- **Desk** - a live stream of decisions, real fraud cases from the part of the data
  the model never saw sent through the system, the held transfers to block or
  release, the verdicts given, each of which can be changed, and on an allowed
  transfer the client's report that it was fraud.
- **About the project** - every component, the path of one real decision with its
  stage times, the 24 features and the 10 rules.
- **Grafana** - the dashboard (Grafana asks for its login).
- **Data & results** - the live stage times and verdicts, the retrainer's latest
  run, the model's figures from `ml/models/metrics.json`, the second look, and the two
  public datasets.

The page for an audience (`/show`) hides the technical parts and has two tabs:

- **Live show** - is everything ready (the system, the second look, the stream; one
  button starts the stream), the main figures in large type, and one money-mule case
  from the part of the data the model never saw, told in five steps: the usual stream,
  transfers to the mule one by one until one is held, the reasons, the analyst's
  block, and the next transfer to the same card held because of the memory. Every
  card in the case gets a new number, so it can be shown again.
- **Research results** - before and after on our data and on twenty datasets, the
  speed, retraining on a new scheme, the public datasets and the 2% alert budget, all
  quoted from `thesis/results.md`.

A verdict is the analyst's: CONFIRMED_FRAUD blocks the held transfer and puts the
payee's card into the confirmed fraud accounts the job reads (Redis,
`confirmed:accounts`); FALSE_POSITIVE releases it; a withdrawn confirmation takes the
card out unless another case or the history (`confirmed:history`) names it. Verdicts
and clients' reports are the labels the model is retrained on.

```powershell
.\run.ps1 cases                                                   # the queue
.\run.ps1 cases -Case <id> -Verdict CONFIRMED_FRAUD -By analyst.k  # block it
.\run.ps1 cases -Case <id> -Report -By analyst.k                  # a client's report
.\run.ps1 cases -Stats                                            # verdicts, precision, holds
```

It runs as the `demo` container (`docker compose up`); the code is mounted, so a
restart of the container picks up a change. The command line runs in it too.
