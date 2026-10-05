# stream-processor

The PyFlink job that decides every transfer before it settles. It reads
`transactions.raw`, keeps each sender's history in Flink state and each payee's side
in Redis, computes 24 features, runs 10 hard rules, scores the model (ONNX) and
decides: ALLOW, REVIEW - held for the analyst - or SECOND_LOOK, just under the
cut-off, sent to TabPFN (`second-look/`). Every decision goes to
`transactions.scored` with its feature values, where the sink writer opens a case
for each hold.

| File | What it does |
|---|---|
| `fraud_job.py` | the job: read, state, Redis, features, rules, model, decision, stage times |
| `features.py` | the 24 features; training replays the same code (`ml/dataset.py`) |
| `rules.py` | the 10 hard rules and the sender and payee state they read |
| `fusion.py` | the decision from the model's score and the rules, and the alert's type |
| `capabilities.py` | what the bank can observe, and so which features and rules are on |
| `receiver_store.py` | the payee's side in Redis: recent payers, counterparties, contacts, confirmed fraud accounts |
| `geo.py` | region coordinates and distances for GEO_ANOMALY and IMPOSSIBLE_TRAVEL |
| `payload_crypto.py` | AES-256-GCM payloads, when `PAYLOAD_KEY_HEX` is set; the generator encrypts with it |
| `config.py` | windows, thresholds, topics, Redis keys |
| `tests/` | `python -m pytest stream-processor -q` |

## The decision

A transfer is held when the model's risk is at or above the cut-off chosen in
training (`thresholds.json`), or when a rule the regulator requires fired
(STRUCTURING, DAILY_LIMIT_BREACH). Just under the cut-off it waits for the second
look; when the second look is not answering, the job holds it itself. The system
never blocks on its own: a person blocks or releases what it holds. The other rules
name the alert's type and give the analyst a reason.

## Capabilities

`capabilities.py` is the one place that decides which features and rules are on;
the feature vector follows its order, so changing a mode means retraining.

```
CAP_RECEIVER_VELOCITY=on|off      payee fan-in over the hour (Redis)
CAP_GEO_TELEMETRY=on|off          the operation's region
CAP_SESSION_TELEMETRY=on|off      a call during confirmation, confirmation time
CAP_PAYEE_IDENTITY=card|pinfl     what the payee is keyed by
CAP_COUNTERPARTY_HISTORY=on|off   counterparties per day and week
CAP_CONFIRMED_CASES=on|off        confirmed fraud accounts (Redis) and their contacts
```

When Redis is down the job keeps deciding: the payee-side features read as nothing
seen, and the confirmed accounts as none.

## Speed

Three copies of the job run side by side (`JOB_PARALLELISM`), each handing Python
a bundle at least every 20 ms (`PY_BUNDLE_TIME_MS`). Arrival to decision in ms,
3,000 generated transfers per rate (1,000 at 10 a second, 6,000 at 50), producer
inside Docker, measured on 5 October 2026 on battery - mains power is faster:

| transfers a second | one copy, 50 ms: mean / p95 / p99 | three copies, 20 ms: mean / p95 / p99 |
|---|---|---|
| 10 | 56 / 59 / 60 (4 October, mains) | 31 / 34 / 48 |
| 50 | 84 / 133 / 163 (4 October, mains) | 32 / 58 / 75 |
| 100 | 2,380 / 3,964 / 4,134 | 64 / 115 / 209 |
| 150 | 3,696 / 7,211 / 7,828 | 79 / 139 / 188 |
| 200 | 4,976 / 9,654 / 10,445 | 114 / 224 / 290 |
| 250 | 3,771 / 6,375 / 6,885 | 313 / 1,054 / 1,371 |

The work itself is 4-5 ms a transfer; the rest is the wait between Kafka and
Python. One copy could not keep up at 100 a second; three hold the 300 ms target
to 200. Past that, 50 ms bundles do better (three copies at 250: p99 408), and four
copies contended for this laptop's cores.

## Run

`run.ps1 submit-job` (empty state) or `resume-job` (from the newest checkpoint)
ships these modules with `--pyFiles`; `run.ps1 serve-prep` first copies
`model.onnx`, `thresholds.json` and `second_look.json` from `ml/models`.
