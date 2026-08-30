# Real-Time Fraud Detection for Instant P2P Payments

Streaming architecture that scores instant P2P transfers **before settlement**,
combining Kafka, Flink + CEP, a gradient-boosting model with SHAP explainability,
and a money-flow graph — grounded in Uzbekistan's regulatory framework
and the UzCard / HUMO networks.

> Research prototype. Any detection metrics are **design targets**, not measured
> findings, until validated against real integration data.

## Project layout

```
fraud-detection/
├── docker-compose.yml      full local stack (phase 2)
├── .env                    image versions, ports, dev credentials
├── Makefile                make up / generate / produce / load-graph ...
│
├── infra/                  infrastructure configuration
│   ├── kafka/              topic creation
│   ├── redis/              feature-store config
│   ├── neo4j/              graph import Cypher
│   ├── clickhouse/init/    warehouse + WORM audit schema
│   ├── flink/              PyFlink-ready Flink image
│   └── grafana/            datasource + dashboard provisioning
│
├── data-generator/   ◀──   PUT THE GENERATOR FILES HERE   (phase 1 ✅)
│   ├── config.py  events.py  persons.py  fraud_patterns.py
│   ├── generator.py  kafka_producer.py  requirements.txt  README.md
│   └── out/                generated CSVs (gitignored)
│
├── stream-processor/       PyFlink: enrich + CEP + ONNX + fusion   (phases 4,6)
├── ml/                     LightGBM + SHAP -> ONNX                 (phase 5)
├── sink-writer/            transactions.scored -> ClickHouse + Neo4j (phase 7)
└── docs/                   architecture & regulatory mapping
```

## Prerequisites

- Docker + Docker Compose v2
- Python 3.10+ on the host (to run the generator / producer)

## Quickstart

```bash
# 1. bring the stack up (first run builds the Flink image — a few minutes)
make up
make ps

# 2. generate the dataset, then load the graph and stream the events
make generate
make load-graph
make produce            # or: make produce-stream  (paced live stream)

# 3. train the model, then submit the scoring job (CEP + ML fusion)
#    (the sink-writer service comes up with `make up` and persists results)
cd ml && python train.py && python export_onnx.py && cd ..
make submit-job         # serves model.onnx inside Flink and starts scoring

# 4. watch it: Flink UI (8081), Grafana dashboard (3000), or:
make query-scored       # decision counts in ClickHouse
```

## Service endpoints

| Service | URL / port | Credentials |
|---|---|---|
| Kafka (host clients) | `localhost:29092` | — |
| Flink UI | http://localhost:8081 | — |
| Neo4j Browser | http://localhost:7474 | `neo4j` / `.env` password |
| ClickHouse HTTP | http://localhost:8123 | `.env` user / password |
| Grafana | http://localhost:3000 | `admin` / `.env` password |

From inside the Docker network use service names: `kafka:9092`, `redis:6379`,
`neo4j:7687`, `clickhouse:9000`.

## Topics

| Topic | Purpose |
|---|---|
| `transactions.raw` | events from the switch (keyed by sender) |
| `transactions.scored` | enriched + scored transactions |
| `fraud.alerts` | high-risk decisions |
| `cbu.reports` | outbound Central Bank platform integration |

## Roadmap

1. ✅ Synthetic data generator
2. ✅ Infrastructure (this stack)
3. ✅ Kafka ingestion wiring
4. ✅ Flink job: enrichment + CEP
5. ✅ ML: training, SHAP, ONNX export
6. ✅ ONNX serving inside Flink + score fusion
7. ✅ ClickHouse + Neo4j sinks
8. ✅ Grafana dashboards

All eight phases are implemented. Pipeline logic is validated offline (unit tests,
CEP replay, ML/fusion evaluation, dashboard-query checks against the schema); the
Flink runtime and full stack run via Docker. All metrics are design targets on
synthetic data, not validated production findings.

## Notes on image tags

All image versions are pinned in `.env`. If a tag is unavailable in your
registry, bump it there — nothing else needs to change. Neo4j is the Community
Edition; APOC downloads on first boot, so the first `make up` needs internet.
