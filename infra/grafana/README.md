# Grafana provisioning

Provisioned when the stack starts (`run.ps1 up`):

- **Datasource** (`datasources/clickhouse.yml`) — ClickHouse over the native
  protocol (`clickhouse:9000`, database `fraud`), uid `clickhouse`.
- **Dashboard** (`dashboards/json/fraud-overview.json`) — loaded by the file
  provider into the "Fraud Detection" folder.

## Dashboard: Fraud Detection — Overview

Open Grafana at http://localhost:3000 (`admin` / `.env` password). It shows the
last hour and refreshes every 10 seconds; every figure comes from
`fraud.transactions_scored`, `fraud.cases` and `fraud.retrain_runs`:

- **Top row** — transfers checked, held for the analyst, the share held, the
  median decision time and the time 99% of decisions stay under (red from the
  300 ms target).
- **Speed** — the share of decisions under 25, 50, 100 and 300 ms, and where a
  decision's time goes, by service.
- **Transfers** — checked and held over time, and who held them: the rules
  (named by fraud type), the model, the second look, or no answer from it.
- **The analyst** — holds waiting now, blocked, released, the share of holds that
  were fraud, the money kept with the clients, the median wait for a verdict,
  missed fraud reported by clients, and the latest 50 holds with their reasons.
- **Second look (TabPFN)** — transfers sent to it, held by it, and held because
  it did not answer within 5 seconds.
- **Retraining (weekly)** — the retrainer's last ten runs from `fraud.retrain_runs`,
  whatever the time range: the current model against the retrained one (alerts,
  frauds caught, F1), and the features that moved. A person reads it and switches a
  better model on with `run.ps1 promote-model`.

Each query names its format: 1 for a table or a number, 0 for the time chart.
Without it the ClickHouse plugin reads every result as a time series and shows
nothing for a table sorted newest first; the reason is only in
`docker logs grafana`.

## Embedded in the demo

`demo/` shows this dashboard in a frame, in kiosk mode, which is why
`GF_SECURITY_ALLOW_EMBEDDING` is set in docker-compose.yml: without it Grafana
refuses to be framed. It changes nothing else - the frame asks for the same login.
