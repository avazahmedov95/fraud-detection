# demo

One page over the running system, in Russian and English, at
http://localhost:8090 (this machine only). It shows the running system or nothing:
it opens once Kafka, ClickHouse and the Flink job answer, and every figure on it
comes from them.

| File | What it does |
|---|---|
| `server.py` | the page's server: sends transfers, reads decisions from `transactions.scored`, the case queue through case-manager's `store.py` |
| `index.html` | the page |
| `about.js` | the "About the project" tab |
| `results.json` | the figures the results tab quotes, each with the line of the document it came from |
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

It runs as the `demo` container (`docker compose up`); the code is mounted, so a
restart of the container picks up a change.
