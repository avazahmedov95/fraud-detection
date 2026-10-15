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

`export_onnx.py` runs in a container (`infra/ml/Dockerfile`, with the versions
pinned in `requirements.txt`), because Windows Smart App Control blocks a library
that onnx needs. It moves every split threshold to the float32 at or below it, as
ONNX keeps them: the features are float32 too, so no decision changes, and the ONNX
model agrees with the trained one. `second_look.py` runs in `.venv-models`, where TabPFN is
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
| PR-AUC | 0.733 | - |
| precision at REVIEW | 0.750 | 0.011 |
| recall at REVIEW | 0.699 | 0.142 |
| F1 at REVIEW | 0.724 | - |

Recall by fraud type (at REVIEW): STRUCTURING 86.2%, APP 55.6%, ATO 85.7%, MULE 60.0%.
This run: cut at REVIEW = 0.1803.

The model reads 28 features (`stream-processor/capabilities.py`); three of them come
from the memory of confirmed fraud accounts, and four look at the last day: money
into the payee, payers new to it, and the sender's first-time payees and the money
sent to them. On twenty generated datasets those four raised the model's F1 from 67.4% to
70.0%, on every one of them (`thesis/results.md`); on these held-out days, from
0.713 to 0.724. In this test every fraud's payee counts
as confirmed a day after the fraud. A real bank learns fewer, which costs about half
a point of F1. The data is generated, so these are design targets, not results from
a real bank.

## The second look

`second_look.py` takes the 100 highest risk scores just under the cut-off (0.0504 to
0.1803) as the band, and chooses TabPFN's own cut-off inside it (0.9092). TabPFN
learns from two parts of the training data. Measured once on the held-out days: the
band holds 107 transfers and 26 of the frauds the model misses; the second look holds
21 of them, 14 of them fraud. Lowering the cut-off to give the same 21 alerts would
catch 7 -
**recall 69.9% -> 77.8% for alerts 164 -> 185**.

Over twenty generated datasets, 3,480 test frauds, averages with 95% intervals:

| system | recall | precision | F1 | alerts | caught |
|---|---|---|---|---|---|
| the model alone | 66.5% ± 3.6 | 74.7% ± 3.3 | 70.0% ± 2.7 | 3,109 | 2,323 |
| a cut-off lowered to as many alerts | 72.3% ± 3.1 | 66.9% ± 2.7 | 69.2% ± 2.1 | 3,794 | 2,527 |
| **the second look** | **73.5% ± 3.2** | 68.1% ± 3.0 | **70.4% ± 2.2** | 3,794 | **2,574** |

The second look beats a lowered cut-off on 17 of 20 datasets, F1 +1.2 [+0.5, +1.9].
Against the model alone it gives about seven points of recall for seven points of
precision.

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
   time to report the frauds the system let go: the latest fifth of those, which
   neither model learned from. On newer decisions the known frauds would be mostly
   the ones the current model held, and the comparison would favour it.

It saves the new model in `models/candidate/` and changes nothing else. A person
reads the comparison and switches the new model on with `run.ps1 promote-model`: it
measures the model on the held-out days (`train.py --served`, the figures above then
change), exports it, chooses the second look's band again and restarts the job.
Each run's outcome goes to `models/retrain_status.json`, which the demo shows on the
About the project tab, and to the table `fraud.retrain_runs`, which Grafana shows in
its Retraining row: the person deciding reads it there.

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
