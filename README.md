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
├── data-generator/         synthetic population + transactions   (phase 1)
│   ├── config.py  events.py  persons.py  travel.py  fraud_patterns.py
│   ├── generator.py  kafka_producer.py  handshake_bench.py
│   └── out/                generated CSVs (gitignored)
│
├── stream-processor/       PyFlink: enrich + CEP + ONNX + fusion   (phases 4,6)
├── ml/                     LightGBM + SHAP -> ONNX                 (phase 5)
├── sink-writer/            transactions.scored -> ClickHouse + Neo4j (phase 7)
├── case-manager/           fraud.alerts -> the analyst work queue
│   ├── case.py  store.py   an alert becomes a case; a verdict becomes a label
│   ├── explain.py          exact tree contributions, for alerts no rule explains
│   └── queue_cli.py        list / show / resolve / stats
├── validation/             the deployed rules run on FOREIGN datasets
│   ├── paysim_adapter.py   PaySim, and what transfers from it
│   ├── amlsim_adapter.py   IBM AMLSim + amlsim.Dockerfile toolchain
│   ├── amlsim_ablation.py  leakage and drift screens
│   └── zenodo_provenance.py  why one published dataset was rejected
└── docs/                   the evidence base — read irp-framing.md first
```

## Where the results live

The code runs; the argument lives in `docs/`. In rough order of importance to a
reader:

| document | what it holds |
|---|---|
| `docs/irp-framing.md` | the research question, every measurement with its interval, a line-by-line answer to the seven review points, twelve silent failure modes, and what a real work queue exposed that no metric did |
| `docs/threat-model.md` | three adversaries, what each control assumes, and what evading it costs — one of those costs is now measured rather than argued |
| `docs/generator-spec.md` | the generator as a specification, the dataset of record with its hashes, and why the data is generated at all |
| `validation/README.md` | four foreign datasets, what each could and could not test, and the screens that came out of it |
| `docs/related-work.md` | fifteen sources, each with what it does **not** support — and §9, which maps every source to the file it actually reaches |
| `ml/README.md` | model, SHAP, and the capability ablation |
| `case-manager/README.md` | the alert consumer: the analyst queue, the disposition as the only real label this system can produce, and why the model's reasons are computed off the scoring path |

Numbers quoted anywhere else in this repository are subordinate to those files.

## Tests

Each package keeps its tests in its own `tests/` directory, and the packages are
run **one at a time**:

```bash
python -m pytest stream-processor -q     # 153
python -m pytest data-generator   -q     #  43
python -m pytest sink-writer      -q     #  23
python -m pytest validation       -q     #  11
python -m pytest case-manager     -q     #  45
```

Not all five in one invocation: three module names occur twice across packages
(`config.py`, `integrity.py`, `payload_crypto.py`), because the packages deploy
as separate units, and pytest cannot import two modules of the same name.

Nine of those files were written after a defect that had already happened —
`test_wire_types.py` after a boolean that travelled as the string `"False"` and
scored 1 on 100% of live events, `test_payload_crypto.py` after the risk of two
copies of one module drifting, `test_bins.py` after a bank that closed. They are
regression evidence, not coverage.

```bash
python tools/boundary_audit.py           # 17 joins between components
```

checks what the tests cannot: that what one component *produces* is what the
next one *expects*. It found three warehouse columns that were constant zero.

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
