# Real-time fraud detection for instant P2P payments

Checks every instant card-to-card transfer **before the money leaves**. Built for
Uzbekistan's UzCard and HUMO networks from Kafka, a PyFlink job with hard rules and
a LightGBM model, a memory of confirmed fraud accounts, and a second look by
TabPFN. A suspicious transfer is held until an analyst blocks or releases it.

> A research prototype on generated data: the figures are design targets, not
> results from a real bank.

## Components

```
data-generator/    synthetic transfers, and the producer into Kafka
stream-processor/  the PyFlink job: features, rules, model, decision
ml/                trains and retrains the model, exports ONNX, prepares the second look
second-look/       TabPFN for the transfers just under the cut-off
sink-writer/       every decision into ClickHouse, the audit chain, a case for each hold
                   with its reasons in words
demo/              one page over the running system, and the analyst's queue on it and
                   on the command line; /show, the same system for an audience
validation/        the model on the public datasets PaySim and IBM AML
infra/             Docker images, Kafka, ClickHouse schema, Redis, Grafana
tools/             boundary_audit.py and the diagram generators
docs/              the diagrams: architecture, integration (how the services talk),
                   the BPMN process, one transfer step by step
thesis/            notes for the thesis in plain English - not used by the system
```

```
bank app -> Kafka transactions.raw -> Flink job (Redis: payee side, confirmed accounts)
         -> transactions.scored -> sink-writer -> ClickHouse (decisions, audit, cases)
            -> Grafana, demo; analyst (demo or CLI; a block puts the payee into Redis)
         -> fraud.second_look -> second-look (TabPFN) -> transactions.scored
ClickHouse (decisions with features, verdicts, clients' reports) -> the retrainer
         (ml/retrain.py, every day) -> a candidate model, served once a person promotes it
```

## Run

Needs Docker Desktop and Python 3.11 (`.venv311`). On Windows use `run.ps1`; the
Makefile carries the same targets.

```powershell
copy .env.example .env               # then set the passwords in it
.\run.ps1 make-certs                 # Kafka's TLS listener needs them before the first up
.\run.ps1 up                         # build and start the stack
.\run.ps1 generate                   # the dataset of record
cd ml; python train.py; cd ..
.\run.ps1 export-model               # the model to ONNX, in a container
.\run.ps1 seed-confirmed             # the history's confirmed fraud accounts into Redis
.\run.ps1 submit-job                 # the Flink job
```

Then the demo at http://localhost:8090 (for an audience: http://localhost:8090/show).
`.\run.ps1 help` lists every target;
`.\run.ps1 status` checks the whole path.

| Service | Address |
|---|---|
| Kafka, from the host | `127.0.0.1:29092` |
| Flink UI | http://localhost:8081 |
| ClickHouse HTTP | http://localhost:8123 (`.env` user and password) |
| Grafana | http://localhost:3000 (`admin` / `.env` password) |
| Demo | http://localhost:8090 |

| Topic | What it carries |
|---|---|
| `transactions.raw` | transfers from the switch, keyed by sender |
| `transactions.scored` | every decision |
| `fraud.second_look` | transfers just under the cut-off, waiting for TabPFN |

## Results

Before and after the latest changes, in plain words: `thesis/results.md`. The
model's exact figures are in `ml/README.md` (and `ml/models/metrics.json`), the
public datasets in `validation/README.md`.

## Tests

Each package is tested on its own (module names repeat across packages):

```bash
python -m pytest stream-processor -q
python -m pytest data-generator   -q
python -m pytest sink-writer      -q
python -m pytest second-look      -q
python -m pytest demo             -q
python -m pytest ml               -q
python -m pytest validation       -q
python tools/boundary_audit.py       # what each component hands the next
```
