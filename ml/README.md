# ml

Trains the model the Flink job uses. `dataset.py` runs the generated data through
the job's own feature code (`stream-processor/features.py`), so the model learns
from exactly the numbers it will see in the live system.

| File | What it does |
|---|---|
| `dataset.py` | runs the data through the job's features and rules: the training table |
| `train.py` | trains five LightGBM models on the earliest 64% of rows, sets the REVIEW cut-off where F1 is highest on the next 16%, and measures on the last 20% |
| `committee.py` | merges the five models into the one the job uses |
| `export_onnx.py` | saves that model as ONNX for Flink and checks that it gives the same answers |
| `manifest.py` | records what the served model was built from: data, features, files |
| `second_look.py` | chooses the second look's band and the examples TabPFN learns from |
| `seed_confirmed.py` | loads the confirmed fraud accounts from the history into Redis |
| `retrain.py` | trains a new model on the logged decisions and people's verdicts, compares it with the current one and reports drift; the retrainer service runs it every day |
| `tests/` | `python -m pytest ml -q` |

## Run

```bash
python train.py          # model.joblib, thresholds.json, feature_names.json, metrics.json
..\run.ps1 export-model  # export_onnx.py in a container: model.txt, model.onnx, manifest.json
python second_look.py --cache models_matrix.npz --tabpfn-model <checkpoint>
python seed_confirmed.py # with the stack up
..\run.ps1 retrain       # once, by hand; the retrainer service runs it every day
..\run.ps1 promote-model # switch the new model on
```

`export_onnx.py` runs in a container (`infra/ml/Dockerfile`, with the versions
pinned in `requirements.txt`), because Windows Smart App Control blocks a library
that onnx needs. `second_look.py` runs in `.venv-models`, where TabPFN is
installed. Then `run.ps1 resume-job` makes the job use the new model, cut-off and
band.

## Results on the held-out days

The main dataset (`data-generator`) has 500,000 transfers. The last 100,000 (the
last six days) are kept for testing; 176 of them are fraud. `models/metrics.json`
holds the exact figures, and `tools/boundary_audit.py` fails if this table differs
from it.

| metric | model | rules only |
|---|---|---|
| ROC-AUC | 0.998 | - |
| PR-AUC | 0.714 | - |
| precision at REVIEW | 0.721 | 0.011 |
| recall at REVIEW | 0.705 | 0.142 |
| F1 at REVIEW | 0.713 | - |

Recall by fraud type (at REVIEW): STRUCTURING 82.8%, APP 55.6%, ATO 87.8%, MULE 62.9%.
This run: cut at REVIEW = 0.1690.

The model reads 24 features (`stream-processor/capabilities.py`); three of them come
from the memory of confirmed fraud accounts. In this test every fraud's payee counts
as confirmed a day after the fraud. A real bank learns fewer, which costs about half
a point of F1. The data is generated, so these are design targets, not results from
a real bank.

## The second look

`second_look.py` takes the 100 highest risk scores just under the cut-off (0.0503 to
0.1690) as the band, and chooses TabPFN's own cut-off inside it (0.8989). TabPFN
learns from two parts of the training data. Measured once on the held-out days: the
band holds 110 transfers and 21 of the frauds the model misses; the second look holds
32 of them, 14 of them fraud. Lowering the cut-off to give the same 32 alerts would
catch 9 -
**recall 70.5% -> 78.4% for alerts 172 -> 204**.

Over twenty generated datasets, 3,497 test frauds, averages with 95% intervals:

| system | recall | precision | F1 | alerts | caught |
|---|---|---|---|---|---|
| the model alone | 61.0% ± 3.5 | 73.0% ± 4.3 | 66.0% ± 2.9 | 2,938 | 2,139 |
| a cut-off lowered to as many alerts | 67.8% ± 3.0 | 64.2% ± 4.2 | 65.4% ± 2.5 | 3,746 | 2,378 |
| **the second look** | **68.8% ± 3.0** | 65.1% ± 4.3 | **66.4% ± 2.6** | 3,746 | **2,416** |

The second look beats a lowered cut-off on 13 of 20 datasets, F1 +1.0 [+0.3, +1.6].
Against the model alone it gives about eight points of recall for eight points of
precision.

## Retraining on people's decisions

Every decision is stored with its feature values (`features` in
`fraud.transactions_scored`), and every verdict and client's report in
`fraud.cases`. Every day the retrainer service runs `retrain.py`
(`RETRAIN_EVERY_HOURS`, 24 by default; `run.ps1 retrain` runs it once):

1. It reports drift: for each feature, how far it has moved since training (the
   population stability index; over 0.25 means it has moved).
2. It trains the same five-model committee on the current model's training data plus
   the logged decisions, where fraud means a person confirmed it.
3. It sets the new model's cut-off so that it raises as many alerts as the current
   one.
4. It compares the two on the latest fifth of the logged decisions, which neither
   model learned from.

It saves the new model in `models/candidate/` and changes nothing else. A person
reads the comparison and switches the new model on with `run.ps1 promote-model`.
Each run's outcome goes to `models/retrain_status.json`, which the demo shows on the
Data & results tab.

Whether retraining helps was tested once, offline, on six generated datasets (the
main one and seeds 1-5). In each, the bank's history (the first 40%) has no examples
of one fraud scheme. On the next 40% the bank learns about a fraud a day after an
analyst confirms a held transfer, or a week later when a client reports a missed one
(half of the clients do). The last 20% is the test, every model at the same number
of alerts. Averages over the six; the retrained model beat the old one in all 30
runs:

| new scheme | known to the bank | PR-AUC kept | retrained | all known | caught, kept -> retrained | its frauds caught |
|---|---|---|---|---|---|---|
| none | 62% | 0.695 | 0.730 | 0.741 | 58.2% -> 60.7% | - |
| MULE | 56% | 0.616 | 0.702 | 0.735 | 51.7% -> 55.5% | 25 -> 47 of 238 |
| APP | 52% | 0.590 | 0.669 | 0.717 | 42.1% -> 44.9% | 28 -> 50 of 367 |
| ATO | 56% | 0.579 | 0.674 | 0.723 | 46.1% -> 51.7% | 70 -> 120 of 273 |
| STRUCTURING | 57% | 0.577 | 0.672 | 0.717 | 47.1% -> 53.8% | 64 -> 113 of 262 |

At the same workload the retrained model catches about 1.8 times as many frauds of a
new scheme: 330 against 187 of 1,140. Knowing every fraud would catch 512. The gap
is the labels a bank never gets.
