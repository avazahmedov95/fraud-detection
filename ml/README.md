# ml

Trains the model the Flink job serves. `dataset.py` replays the generated CSV
through the job's own feature code (`stream-processor/features.py`), so the model
trains on exactly what it will be served.

| File | What it does |
|---|---|
| `dataset.py` | the CSV through the deployed features and rules: the training matrix |
| `train.py` | five LightGBM fits on the earliest 64% of rows, the REVIEW cut-off at the F1 peak on the next 16%, metrics on the last 20% |
| `committee.py` | the five fits merged into the one booster the job serves |
| `export_onnx.py` | that booster to ONNX for Flink, checked against the native model |
| `manifest.py` | records what the served model was built from: dataset, features, artefacts |
| `second_look.py` | the second look's band and TabPFN context |
| `seed_confirmed.py` | the history's confirmed fraud accounts, into Redis |
| `tests/` | `python -m pytest ml -q` |

## Run

```bash
python train.py          # model.joblib, thresholds.json, feature_names.json, metrics.json
python export_onnx.py    # model.txt, model.onnx, manifest.json
python second_look.py --cache models_matrix.npz --tabpfn-model <checkpoint>
python seed_confirmed.py # with the stack up
```

`second_look.py` runs in `.venv-models`, where TabPFN is installed. Then
`run.ps1 resume-job` serves the model, its cut-off and the second look's band.

## Results on the held-out month

The dataset of record (`data-generator`): 500,000 transfers, the last 100,000 held
out, 176 of them fraud. `models/metrics.json` is the authoritative copy, and
`tools/boundary_audit.py` fails if this table disagrees with it.

| metric | model | rules only |
|---|---|---|
| ROC-AUC | 0.998 | - |
| PR-AUC | 0.714 | - |
| precision at REVIEW | 0.721 | 0.011 |
| recall at REVIEW | 0.705 | 0.142 |
| F1 at REVIEW | 0.713 | - |

Recall by fraud type (at REVIEW): STRUCTURING 82.8%, APP 55.6%, ATO 87.8%, MULE 62.9%.
This run: cut at REVIEW = 0.1690.

The model reads 24 features (`stream-processor/capabilities.py`); three of them are
the memory of confirmed fraud accounts. On this slice every fraud's payee counts as
confirmed a day after it, about half a point of F1 more than a bank would learn.
The data is synthetic: these are design targets, not production findings.

## The second look

On the cut-off rows `second_look.py` takes the 100 highest scores under the cut-off
as the band (0.0503 to 0.1690) and chooses TabPFN's own cut-off in it (0.8989), with
two pieces of the training slice as TabPFN's context. Read once on the held-out
month: the band holds 110 transfers and 21 of the frauds the model misses; the second
look holds 32 of them, 14 fraud, where a cut-off lowered to 32 alerts catches 9 -
**recall 70.5% -> 78.4% for alerts 172 -> 204**.

Over twenty generated datasets, 3,497 held-out frauds, means with 95% intervals:

| system | recall | precision | F1 | alerts | caught |
|---|---|---|---|---|---|
| the model alone | 61.0% ± 3.5 | 73.0% ± 4.3 | 66.0% ± 2.9 | 2,938 | 2,139 |
| a cut-off lowered to as many alerts | 67.8% ± 3.0 | 64.2% ± 4.2 | 65.4% ± 2.5 | 3,746 | 2,378 |
| **the second look** | **68.8% ± 3.0** | 65.1% ± 4.3 | **66.4% ± 2.6** | 3,746 | **2,416** |

The second look beats a lowered cut-off on 13 of 20 datasets, F1 +1.0 [+0.3, +1.6];
against the model alone it trades about eight points of precision for eight of recall.
