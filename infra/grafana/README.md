# Grafana provisioning — phase 8 ✅

Auto-provisions on `make up`:

- **Datasource** (`datasources/clickhouse.yml`) — ClickHouse over the native
  protocol (`clickhouse:9000`, database `fraud`), uid `clickhouse`.
- **Dashboard** (`dashboards/json/fraud-overview.json`) — loaded by the file
  provider into the "Fraud Detection" folder.

## Dashboard: Fraud Detection — Overview

Open Grafana at http://localhost:3000 (`admin` / `.env` password). Panels over
`fraud.transactions_scored` and `fraud.audit_log`:

- **Stat tiles** — total transactions, alert rate %, new-payee share %, blocked count.
- **Alerts over time** — REVIEW vs BLOCK counts bucketed by the dashboard interval.
- **Alerts by predicted type** — APP / ATO / MULE / STRUCTURING split.
- **Risk score distribution** — histogram of `final_score`.
- **Alerts by sender region**. (The channel panel went with the `channel`
  column on 07.09.2026 - `infra/clickhouse/init/01-schema.sql` says why.)
- **Recent alerts** — latest flagged transactions with their CEP reason codes
  (`rule_hits`) from the WORM audit log.

Every panel query was validated against the real schema with an embedded
ClickHouse engine. The default time range is `now-1y` so synthetic data shows
regardless of when it was generated — narrow it once live.

> The `grafana-clickhouse-datasource` query schema has shifted across plugin
> versions. If a provisioned panel shows a datasource/query error, re-pick the
> ClickHouse datasource on the panel (the SQL itself is correct); see the note in
> `datasources/clickhouse.yml`.

## Embedded in the demo

`demo/` shows this dashboard in a frame, in kiosk mode, which is why
`GF_SECURITY_ALLOW_EMBEDDING` is set in docker-compose.yml: without it Grafana
refuses to be framed. It changes nothing else - the frame asks for the same login.
