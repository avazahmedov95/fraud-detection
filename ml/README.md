# ml

Trains the model the Flink job uses. `dataset.py` runs the generated data through
the job's own feature code (`stream-processor/features.py`), so the model learns
from exactly the numbers it will see in the live system.

| File | What it does |
|---|---|
| `dataset.py` | runs the data through the job's features and rules: the training table |
| `train.py` | trains five LightGBM models on the earliest 64% of rows, sets the REVIEW cut-off where F1 is highest on the next 16%, and measures on the last 20%; `--served` measures a promoted model there |
| `committee.py` | merges the five models into the one the job uses |
| `export_onnx.py` | saves that model as ONNX for Flink and checks that it gives the same answers |
| `manifest.py` | records what the served model was built from: data, features, files |
| `second_look.py` | chooses the second look's band and the examples TabPFN learns from |
| `seed_confirmed.py` | loads the confirmed fraud accounts from the history into Redis |
| `retrain.py` | trains a new model on the logged decisions and people's verdicts, compares it with the current one and reports drift; the retrainer service runs it every week |
| `tests/` | `python -m pytest ml -q` |

## Run

```bash
python train.py          # model.joblib, thresholds.json, feature_names.json, metrics.json
..\run.ps1 export-model  # export_onnx.py in a container: model.txt, model.onnx, manifest.json
python second_look.py --cache models_matrix.npz --tabpfn-model <checkpoint>
python seed_confirmed.py # with the stack up
..\run.ps1 retrain       # once, by hand; the retrainer service runs it every week
..\run.ps1 promote-model # switch the new model on
```

`export_onnx.py` runs in a container (`infra/ml/Dockerfile`) because Windows Smart
App Control blocks a library that onnx needs. ONNX keeps split thresholds as
float32, so the export moves each one to the float32 at or below it; the features
are float32 too, so no decision changes. A failed check leaves the served model as
it was. `second_look.py` runs in `.venv-models`, where TabPFN is installed. Then
`run.ps1 resume-job` makes the job use the new model, cut-off and band.

## Results on the held-out days

The main dataset (`data-generator`) has 500,000 transfers. The last 100,000 (the
last six days) are kept for testing; 176 of them are fraud. `models/metrics.json`
holds the exact figures, and `tools/boundary_audit.py` fails if this table differs
from it.

| metric | model | rules only |
|---|---|---|
| ROC-AUC | 0.998 | - |
| PR-AUC | 0.733 | - |
| precision at REVIEW | 0.750 | 0.011 |
| recall at REVIEW | 0.699 | 0.142 |
| F1 at REVIEW | 0.724 | - |

Recall by fraud type (at REVIEW): STRUCTURING 86.2%, APP 55.6%, ATO 85.7%, MULE 60.0%.
This run: cut at REVIEW = 0.1803.

The model reads 28 features (`stream-processor/capabilities.py`): three come from
the memory of confirmed fraud accounts, four look at the last day. In this test
every fraud's payee counts as confirmed a day after the fraud; a real bank learns
fewer. The data is generated, so these are design targets, not results from a real
bank.

## The second look

`second_look.py` takes the 100 highest risk scores just under the cut-off (0.0504 to
0.1803) as the band, and chooses TabPFN's own cut-off inside it (0.9092). TabPFN
learns from two parts of the training data. On the held-out days the band holds 107
transfers and 26 of the frauds the model misses; the second look holds 21 of them,
14 of them fraud. Lowering the cut-off to give the same 21 alerts would catch 7 -
**recall 69.9% -> 77.8% for alerts 164 -> 185**.

On 20 fresh datasets (seeds 201-220, 3,901 test frauds), averages with 95% intervals:

| system | recall | precision | F1 | alerts | caught |
|---|---|---|---|---|---|
| the model alone | 64.5% ± 2.5 | 78.3% ± 3.6 | 70.3% ± 1.7 | 3,238 | 2,515 |
| a cut-off lowered to as many alerts | 70.7% ± 1.9 | 69.6% ± 3.5 | 69.8% ± 2.1 | 3,988 | 2,756 |
| **the second look** | **72.9% ± 1.8** | 71.8% ± 3.5 | **72.0% ± 2.1** | 3,988 | **2,842** |

The second look beats a lowered cut-off on 17 of 20 datasets and ties on 3, F1 +2.2
[+1.6, +2.8].

## Retraining on people's decisions

Every decision is stored with its feature values (`features` in
`fraud.transactions_scored`), and every verdict and client's report in
`fraud.cases`. Every week the retrainer service runs `retrain.py`
(`RETRAIN_EVERY_HOURS`, 168 by default; `run.ps1 retrain` runs it once):

1. It reports drift: for each feature, how far it has moved since training (the
   population stability index; over 0.25 means it has moved).
2. It trains the same five-model committee on the rows the current model was fitted
   on plus the logged decisions, where fraud means a person confirmed it. The rows
   that set the cut-off stay unseen: after a promotion the second look's band is
   chosen on them.
3. It sets the new model's cut-off so that it raises as many alerts as the current
   one.
4. It compares the two by F1 on decisions at least a week old, when clients have had
   time to report missed frauds: the latest fifth of those, which neither model
   learned from. On newer decisions the known frauds would be mostly the ones the
   current model held, and the comparison would favour it.

It saves the new model in `models/candidate/` and changes nothing else. Each run's
outcome goes to `models/retrain_status.json`, which the demo shows, and to
`fraud.retrain_runs`, which Grafana shows. A person reads it there and switches the
new model on with `run.ps1 promote-model`: it measures the model on the held-out
days (`train.py --served`; the figures above then change), exports it, chooses the
second look's band again and restarts the job. If a step fails, it stops and puts
the served model's files back.

On six fresh datasets (seeds 201-206) the bank's history (the first 40%) has no
examples of one fraud scheme. On the next 40% the bank learns of a fraud only from
an analyst's confirmation (a day later) or a client's report (half of them, a week
later). The last 20% is the test, both models at the same number of alerts. The
retrained model beat the old one in all 30 runs:

| new scheme | known to the bank | PR-AUC, old | retrained | all known | its frauds caught: old -> retrained (all known) |
|---|---|---|---|---|---|
| none | 67% | 0.678 | 0.732 | 0.746 | - |
| mules | 54% | 0.540 | 0.644 | 0.710 | 10 -> 24 of 337 (59) |
| APP | 55% | 0.620 | 0.682 | 0.727 | 46 -> 72 of 401 (94) |
| account takeover | 56% | 0.596 | 0.694 | 0.741 | 45 -> 91 of 234 (160) |
| structuring | 55% | 0.574 | 0.687 | 0.737 | 45 -> 123 of 273 (186) |

It catches 2.1 times as many frauds of a new scheme: 310 against 146 of 1,245.
Knowing every fraud would catch 499; the gap is the labels a bank never gets.
