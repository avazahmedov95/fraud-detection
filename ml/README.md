# ml — phase 5 ✅

Offline training of the gradient-boosting fraud model, SHAP explainability, and
ONNX export for in-stream serving. The model trains on the **same feature
extractor the Flink job serves** (`stream-processor/features.py`), so there is no
train/serve skew.

```
the pipeline - run in order, produces what the Flink job serves
dataset.py        replay the CSV through stream-processor features+rules -> matrix
train.py          the committee: 64/16/20 by time, cutoffs from data, metrics
committee.py      five fits averaged into the one booster the job serves
export_onnx.py    LightGBM -> ONNX + parity check vs the native model
manifest.py       provenance: what the untracked artefacts were built from
explain.py        SHAP global (beeswarm + bar) and per-alert reason codes

experiments/      harnesses - each produces a NUMBER, not an artefact the
                  system uses, and none is imported by the pipeline above
  ablate_seeds.py what each capability is worth, across generator seeds, with
                  intervals; --only <capability> sweeps all of its modes
  layers.py       CEP-only vs ML-only vs fused on the held-out slice
  recall.py       per-type recall across seeds (budgeted; resumes)
                  One-off experiments are deleted once their decision is
                  written below: git log --diff-filter=D -- ml/experiments

models/           artifacts: model.joblib, model.onnx, thresholds.json, feature_names.json,
                  metrics.json, shap_summary.png, shap_importance.png
                  ablation/ holds every harness run that was kept
```

A harness has to meet a stricter bar than the pipeline: nothing downstream reads
its output except a sentence in the thesis, so it must be unable to be **wrong
quietly**. This directory has failed that way twice - see Capability ablation and
`docs/irp-framing.md` 7.7b.

Condensed on 2026-09-15 to the figures and what each supports; the full account
of every run is in git history (`git show 483891f:ml/README.md`).

## Why gradient boosting (not deep learning)

Small labelled fraud sets, SHAP reason codes required per alert (for the analyst
and the regulator), and CPU-speed inference inside Flink. Neural networks fit
later as **specialised feature providers** (e.g. behavioural biometrics) feeding
this model, not replacing it.

## Reproduce

```bash
pip install -r requirements.txt
python train.py          # -> model.joblib, thresholds.json, feature_names.json, metrics.json
python explain.py        # -> shap_summary.png, shap_importance.png
python export_onnx.py    # -> model.onnx (+ parity check)
```

`train.py` weights fraud by negatives over positives only above 0.5% fraud; below
it, every weight measured did worse than none, so it fits unweighted.
`CLASS_WEIGHTING=on` or `off` forces either. The tests are in `tests/`.

## Design targets on synthetic data (NOT validated production findings)

The held-out (later) time slice - the last 20% of the realistic profile, 100,000
transfers with 202 fraud - at the REVIEW cutoff chosen on the validation rows.
`models/metrics.json` is the authoritative copy, and `tools/boundary_audit.py`
fails if this table disagrees with it.

| metric                | ML model | CEP rules only |
|-----------------------|----------|----------------|
| ROC-AUC               | 0.961    | —              |
| PR-AUC                | 0.321    | —              |
| precision at REVIEW   | 0.538    | 0.013          |
| recall at REVIEW      | 0.351    | 0.149          |

Recall by fraud type (ML at REVIEW): STRUCTURING 42.4%, APP 31.0%, ATO 60.0%, MULE 29.0%.
The five fits alone scored 0.260-0.359 PR-AUC on the same slice; their committee
0.321 - inside that range, and no fit is known in advance to be the best.

Read plainly: the committee finds about a third of the fraud in the held-out
month, and about half its alerts are fraud - with the spread one retrain
moves (below). The rules alone reach 1.3% precision on data where
legitimate traffic also collects, splits and changes phones. The weak patterns
are APP and MULE.

### Since 2026-09-14: the realistic profile, a committee, cutoffs from data

Until 2026-09-14 the dataset of record was the baseline profile: fraud at 1.5%,
every legitimate transfer plainly legitimate, exact labels. A model trained there
scored 0.937 PR-AUC on it and 0.42 on data shaped like the realistic profile -
now the dataset of record (`docs/generator-spec.md` 10): 500,000 transfers, 0.18%
labelled fraud, legitimate look-alikes of every pattern, a tenth of fraud never
reported. Three changes, each measured before it was adopted:

- **No class weight at a realistic base rate.** Validation PR-AUC on the realistic
  profile was 0.515 unweighted, 0.344 at weight 10, 0.155 at 30, and on PaySim
  weighting collapsed every fit (0.032 against 0.267 unweighted).
- **A committee of five fits, served as one booster** (`committee.py`). The mean
  of several fits' log-odds beat a single fit on the realistic profile (0.488
  against 0.422). Row bagging was tested the same way and not adopted: +0.004 on
  the baseline profile.
- **Cutoffs chosen on data and shipped with the model.** An unweighted model's
  probabilities sit near the base rate, so a fixed 0.40 means nothing.
  `train.py` fits on the earliest 64% of rows, puts REVIEW where F1 peaks on the
  next 16%, and writes it to `thresholds.json`, which serve-prep ships beside
  `model.onnx` and the job reads (`stream-processor/config.py`). This run: cut at REVIEW = 0.0606, and
  **there is no BLOCK**: since 2026-09-19 the system never blocks on its own - the
  owner's decision - and every alert goes to a person. The CEP-only fallback keeps
  its fixed cutoff, since an additive rule score is not a probability.

*IBM AML, which the gates below use, was removed from the project on 2026-09-19:
its accounts include banks and companies, and this project is about transfers
between people. The gates stay as the record of the decisions they made.*

### Following the money a hop further: tried, not adopted, removed

Three features read the receiver-keyed store a day back instead of an hour, and
for the sender as well as the payee. The gate, fixed before each run, was the
owner's "adopt only if it does not get worse", on validation rows over five seeds:

- realistic profile: 0.515 -> 0.561, paired +0.046 [+0.016, +0.076]. Passed - but
  the generator builds its mules to collect and then pay on, so part of that is
  the features finding what the generator put there.
- IBM AML, which this project did not write: 0.0413 -> 0.0408, paired -0.0005
  [-0.0106, +0.0097] - below zero on the mean, so **not adopted** (the test rows,
  which decide nothing, improved: 0.0698 -> 0.0811).

The code was removed rather than kept switched off; commit `bfe556f` holds all of
it, with its harness and both gates.

### Multi-day link shapes: built as link_history, then removed

Seven columns over the 96 hours before each transfer (`experiments/shapes.py`):
fan-in and fan-out, the payers of the sender and the payees of the payee, money
going straight back, three-step circles, and a split gathered again. Computed
offline in replay order on the identity the job keys on; the rule - money_chains'
own - was committed before the run (`c1c58ea`).

| | validation PR-AUC, five seeds | paired difference | committee at its cutoff, test rows |
|---|---|---|---|
| realistic profile | 0.515 -> 0.583 | +0.067 [+0.035, +0.099] | precision 65.5% -> 68.4%, recall 45.0% -> 53.5% |
| IBM AML | 0.041 -> 0.097 | +0.056 [+0.028, +0.083] | precision 34.7% -> 53.9%, recall 30.1% -> 22.6% |

Both pass, by a wider margin than money_chains passed its first gate. Two things
the table does not say:

- **Each dataset is carried by different columns.** On the realistic profile the
  circle and split-and-gather columns are almost never nonzero - the generator
  draws no such shapes - and the gain is the multi-day counts. On IBM AML money
  goes straight back on 5.2% of laundering against 0.1% of legitimate transfers,
  and a split is gathered again on 7.2% against 0.7%: the typologies IBM injected.
  Part of each gain is the columns finding what that generator put there.
- **On IBM AML the validation-chosen cutoff trades recall for precision.** The
  ranking improved; the operating point did not improve on both axes.

The rule said to build them into the stream next. The cutoff menu below found the
IBM AML gain confined to the top of the list, and a third question, fixed before
its run, stopped the build; PaySim, asked last, restarted it for a strict cutoff.

### The alert cutoff: a menu, not a verdict

`experiments/thresholds.py`: the committee's test rows at cutoffs chosen on the
validation rows. Realistic profile, 100,000 test transfers, 202 fraud:

| cutoff | model | alerts | caught | real |
|---|---|---|---|---|
| F1 peak (the current rule) | without the shapes | 139 | 45.0% | 65.5% |
| top 0.2% of transfers | without | 198 | 56.4% | 57.6% |
| top 0.5% | without | 416 | 69.8% | 33.9% |
| F1 peak | with the shapes | 158 | 53.5% | 68.4% |
| F2 peak | with | 284 | 71.8% | 51.1% |
| top 1% | with | 862 | 91.1% | 21.3% |

IBM AML, 897,427 test transfers, 1,653 laundering:

| cutoff | model | alerts | caught | real |
|---|---|---|---|---|
| F1 peak | without | 1,432 | 30.1% | 34.7% |
| top 1% | without | 9,937 | 55.2% | 9.2% |
| F1 peak | with | 694 | 22.6% | 53.9% |
| the catch of "without" at its F1 peak | with | 3,326 | 30.7% | 15.2% |
| top 1% | with | 9,673 | 44.8% | 7.7% |

**On the realistic profile the shapes help at every cutoff. On IBM AML they help
only at the top of the list**: flagging under about 0.1% of transfers they catch
more, above it less, and matching the old model's catch costs 2.3 times the
alerts. Validation PR-AUC, which the gate read, weights the top of the list most -
which is how a gate can pass on a model that is worse where a bank with more
analysts would operate. That belongs in the decision to build the shapes into the
stream.

**Decision, 2026-09-16: the owner kept the F1-peak cutoff.** `train.py` is
unchanged, and the harness is deleted now that its question is answered; it is in
git history (`git show 5377e74:ml/experiments/thresholds.py`).

### Not companies: the same caveat on IBM AML's people-like transfers

`experiments/individuals.py` read the same models only on test transfers where both
accounts had at most 20 payees and 20 payers in the 96 hours before - 89.8% of the
transfers, 92.7% of the laundering. Share of the laundering in the top x% of
transfers by score:

| top | without the shapes | with |
|---|---|---|
| 0.1% | 23.0% | 27.4% |
| 0.2% | 33.4% | 30.5% |
| 0.5% | 40.9% | 35.5% |
| 1% | 54.9% | 46.1% |

The rule, committed before the run, needed the shapes at least level at every
budget; they lose at three of four, as they do on all rows. **Companies do not
explain the caveat, so the shapes are not built into the stream.** Its harness is
deleted (`git show f672c10:ml/experiments/individuals.py`);
`experiments/shapes.py` is back for one question more, on PaySim.

### PaySim, asked last: a narrow pass

The owner keeps a strict cutoff, and at the top of the list the shapes were ahead
on IBM AML too. That question is narrower than the first two and was chosen after
seeing IBM AML, so it was fixed before its run (`974ba0c`) and asked of data not
looked at yet: PaySim, the published split's first 24 days - fit on the earliest
80% of rows, validate on the rest - five seeds.

| | without the shapes | with |
|---|---|---|
| validation PR-AUC, per seed | 0.135 | 0.148, paired +0.013 [-0.034, +0.060] |
| averaged committee, validation / last seven days | 0.155 / 0.266 | 0.145 / 0.287 |
| fraud in the top 0.1% of validation transfers | 18.5% | 19.8% |

Both conditions hold, so by the rule the shapes are built into the stream for a
strict cutoff, with IBM AML's many-alert caveat. **The pass is narrow and says less
than it seems**: the interval spans zero, the averaged committee is lower on
validation, and one column does all the work - PaySim's senders almost never
repeat and it has no circles, so every column but the payee's distinct payers over
96 hours is zero. What each dataset supports is different: the multi-day counts on
the realistic profile, the payee's multi-day payers weakly on PaySim, and circles
and split-and-gather only at the top of the list on IBM AML.

### link_history as built: exact, switched on, then removed

Step 1 went into the stream as the `link_history` capability - five of the seven
columns, without the three-step circles and split-and-gather - and was measured
again (`experiments/links.py`, rule committed before the run in `77e6dad`):

| | parity with the offline columns | validation PR-AUC, paired | top 0.1% of validation transfers | verdict |
|---|---|---|---|---|
| realistic profile | 0 of 500,000 rows differ | +0.068 [+0.058, +0.077] | - | pass |
| PaySim | 0 of 6,362,620 rows differ | +0.031 [+0.002, +0.060] | 18.5% -> 20.2% | pass |
| IBM AML | stood in for by PaySim's | +0.032 [+0.021, +0.043] | 20.0% -> 16.9% | **fail** |

The deployed extractor reproduces the offline columns exactly, and switching it on
changes none of the existing twenty. The five columns pass on the realistic profile
and on PaySim - there now with an interval clear of zero - and lift IBM AML's
PR-AUC, but find less laundering in IBM AML's top 0.1%: the top-of-the-list gain
the seven columns had there came from the circles and split-and-gather, which step
1 does not build. By that rule it stayed off. On 2026-09-19 the owner switched it
on - IBM AML, whose accounts include banks and companies, no longer deciding - and
the served model was retrained with it: PR-AUC 0.472 -> 0.522, recall at REVIEW
0.505 -> 0.540, precision 0.637 -> 0.694, ROC-AUC 0.991 -> 0.973. **The same day
the owner removed it, to keep the project simple**: the gain was modest, confirmed
outside this project's own generator only weakly, and it doubled the receiver-side
state. The code is in git history (`git show 43962d5`, the last commit with it);
both harnesses are deleted
(`git show 77e6dad:ml/experiments/links.py`, `git show 77e6dad:ml/experiments/shapes.py`).

### Honest collections are not mistaken for mules

`experiments/collectors.py` (`git show 241ecac:ml/experiments/collectors.py`)
refitted the served committee with and without link_history - reproducing both
exactly, 160 alerts at 0.637 / 0.505 and 157 at 0.694 / 0.540 - and read the
held-out month by the payee's distinct payers over the 96 hours before each
transfer:

| payee's payers, 96 h | legitimate transfers | false alarms without | with | MULE transfers | caught without | with |
|---|---|---|---|---|---|---|
| none | 26,722 | 30 (0.11%) | 30 (0.11%) | 14 | 5 | 8 |
| 1-2 | 58,973 | 18 (0.03%) | 7 (0.01%) | 15 | 7 | 7 |
| 3-4 | 12,452 | 5 (0.04%) | 4 (0.03%) | 13 | 6 | 5 |
| 5 or more | 1,651 | 5 (0.30%) | 7 (0.42%) | 20 | 9 | 9 |

Transfers into an account collecting from five or more people - the realistic
profile's weddings, gifts and joint purchases - are flagged about ten times as
often as other legitimate transfers, and still rarely: 7 of 1,651, two more than
without link_history. False alarms fall overall, 58 to 48, mostly on payees with
one or two payers. What this cannot test is the seller paid by strangers all day:
the generator draws no merchant flows (`docs/generator-spec.md` 8), and such a
payee would sit far above the fifteen payers the model has seen. A deployment
needs a known-seller flag - self-employed status is something the bank holds -
before link_history reads a shop as a collection. link_history has since been
removed (above); the seller caveat stands for any count of a payee's payers.

### cross_network and device telemetry: removed, and what one retrain moves

On 2026-09-19 the owner removed two inputs to simplify the system: `cross_network`
(a transfer between UzCard and HUMO, which the generator draws independently of
fraud) and device telemetry (`device_is_new` and the `DEVICE_CHANGE` rule - the
bank's app now blocks a new device outright, CBU 3759). The vector became 18 columns.

Twenty paired fits of train.py's recipe on the realistic profile, with both and
without both (seeds 0-19): validation PR-AUC 0.489 -> 0.498, paired +0.009
[-0.033, +0.052], worse on 7 of 20 seeds - no measurable difference. The served
committee nevertheless moved: recall at REVIEW 0.505 -> 0.421, precision 0.637 ->
0.586, and a BLOCK cutoff appeared (auto-blocking was then prohibited, above).
That is not the two columns; it is what one
retrain does here. With 570 fraud to learn from, the five fits scatter (0.303-0.461
PR-AUC) and the F1-chosen cutoff lands on a different alert count each time:
removing either column alone moved the served recall to 0.421 or 0.436 with PR-AUC
unchanged (0.476, 0.469). **The headline figures carry about eight points of
retrain-to-retrain spread in recall, and are quoted with it.**

### The payee's account age: removed, and what it cost here

On 2026-09-19 the owner removed the payee's account age - `receiver_age`,
`receiver_is_fresh` and the `FRESH_RECEIVER` rule - with the Neo4j lookup that
supplied it and the BIN table that decided when it was visible. A sending bank
sees the account behind a card only when the payee is its own client, 6.85% of
transfers at the real card market (`docs/threat-model.md` §4), and the UzCard /
HUMO switch carries no such field. The vector is 16 columns.

Unlike the two columns above, this one cost more than a retrain moves. The served
committee went from PR-AUC 0.450 to 0.321, recall at REVIEW from 0.421 to 0.351 and
precision from 0.586 to 0.538; the rules alone from 0.317 recall to 0.149, as the
fallback-path figures below predicted. Five paired seeds on the baseline profile
had put the capability at −0.022 [−0.040, −0.004]. Part of that lift was the
generator's: it gives every payee an age and makes 70% of fraud payees younger
than 45 days (`docs/generator-spec.md` §2), which a bank that cannot see the
payee's account never observes. **The drop is the price of not assuming data the
deploying bank does not have, and the figures above are quoted without it.**

**Everything below is the baseline profile, the dataset of record until
2026-09-14, unless it says otherwise.**

### When the graph is down

Until 2026-09-19 the job looked the payee's age up in Neo4j, so an outage changed
the scores: with an unknown age encoded as -1 it raised ten times the false alarms,
and after the fix of 2026-09-13 - the age withheld on a tenth of the training rows -
it cost recall instead: on the realistic profile, alerts 145 to 111 and recall
0.421 to 0.292 (`docs/irp-framing.md` 7.7c). The age is gone (above), so the job
reads nothing from Neo4j: an outage now costs only the analysts' alert graph,
which the sink-writer discards with a running count until the graph returns.

**A disclaimer is not a refresh.** The table the one above replaced was measured
before the receiver-side aggregation capability and never updated: it showed MULE
recall at 54% against 85.7%, hiding the largest single gain in the project, under
a note saying `metrics.json` was authoritative. So the figures here are now
checked, not disclaimed: the audit reads the metrics table, the per-type line and
the calibration block out of this file on every run and compares them with
`metrics.json`; a figure that drifts or disappears fails.

**ROC-AUC near 1 is an artefact of synthetic data, not an achievement.** It is
reported for completeness; PR-AUC is the meaningful figure at a 0.1-1.5% positive
rate. PaySim has a documented equivalent (balance-column leakage).

**How much easier is this data? About 2x, and half of that is not the data.** A
public CatBoost baseline on PaySim (`ris3abh/aml-p2p-fraud-detection`, MIT)
reports **AUPRC 0.380**, reproduced here at 0.397 (`paysim_adapter.py
--baseline`), on a holdout at 1.142% fraud - matched to this project's held-out
slice at 1.23%. At their 2% alert budget this project catches 98.4% of fraud
against their 49.4%, and its top-decile lift is 10.0x against 7.0x. But about
half the gap is **feature availability**: stripped to amount and hour, the columns
a public dataset can carry, this model scores 0.653 (`validation/README.md`). So
0.937 -> 0.653 is what publishing costs, and 0.653 -> 0.380 is the generator being
separable. Their AUPRC *before* they removed the balance leakage was 0.988 (1.000
here): a PR-AUC in the high nineties is the range a known-broken model reaches on
public data. See `docs/related-work.md` §6.

## SHAP

**Global importance (mean |SHAP|)**, regenerated 2026-08-30 on the pinned
environment: `is_new_payee` (0.770), `log_amount` (0.694), `receiver_age` (0.603, since removed),
`rcv_inflow_1h` (0.516); the session signal `secs_login_z` sits sixth at 0.276.
SHAP is not bit-stable even when the model is - the explainer samples its
background set, about 0.002 per feature between runs - so quote three decimals at
most. Adding a feature redistributes attribution across all of them, so values
from before `rcv_inflow_1h` existed must not be read beside these.

**`is_new_payee` at the top is an upper bound, not a finding.** Fraud reaches a
stream-new payee 99.2% of the time on this data against 36.9% of legitimate
traffic, because the generator does not produce the evasion its own threat model
names - one small prior transfer establishes the payee. When it is produced
(`SEEDED_PAYEE_SHARE`, default off), APP detection on the affected episodes falls
from 56.0% to 31.8%, five seeds, delta −25.3 pp [−47.5, −3.1]
(`docs/threat-model.md` §4).

**`is_family` was the same artefact, and how it ended is the finding.** An earlier
revision reported it as the #1 feature (1.29) and the core research contribution:
the generator routed no fraud to relatives, so it separated the classes by
construction. It was not deleted - it is the `myid_kinship` capability, which
defaults to off, so it is absent from the deployed vector. Once the
generator modelled both directions (25% of legitimate transfers go to relatives,
and a realistic minority of fraud too), switching it on is worth nothing
measurable - the `myid_kinship=on` row below. A feature that dominates SHAP on
synthetic data is suspect until the generator models both sides of its behaviour
(`docs/irp-framing.md` §4).

The ONNX model reproduces native LightGBM probabilities to < 1e-6, so in-Flink
serving is faithful.

## Capability ablation

What each integration is worth, measured rather than assumed; the switches are in
`stream-processor/capabilities.py`. Every figure in this section is on the
baseline profile - the realistic profile has not been swept yet.

`experiments/ablate_seeds.py` sweeps the capabilities across generator seeds and
reports each delta paired within seed, with a 95% CI for the mean;
`--only payee_identity` sweeps one capability across **all** its declared modes. It
fingerprints the feature set and the generator sources and refuses to mix results
across versions, since either change moves the baseline and invalidates every
stored delta. Older runs are in the git history, not the tree (removed
2026-09-19). The receiver_age and device_telemetry rows below are history too:
both capabilities were removed on 2026-09-19 (above).

All rows measured on one feature set and one generator version, 5 seeds,
baseline PR-AUC **0.960 ± 0.018**:

| configuration | delta (95% CI) | sign | verdict |
|---|---|---|---|
| receiver_velocity=off | −0.036 [−0.068, −0.004] | 5/5 | **real** |
| session_telemetry=off | −0.034 [−0.051, −0.016] | 5/5 | **real** |
| receiver_age=off | −0.022 [−0.040, −0.004] | 4/5 | **real** |
| myid_kinship=on | +0.002 [−0.000, +0.004] | 4/5 | no effect |
| device_telemetry=off | +0.002 [−0.001, +0.005] | 4/5 | no effect |
| payee_identity=pinfl | +0.001 [−0.004, +0.007] | 2/5 | no effect |
| geo_telemetry=off | +0.000 [−0.002, +0.002] | 2/5 | no effect |

**`channel` is gone.** It measured −0.002 [−0.006, +0.002], no rule read its four
one-hot features, and no public dataset carries the field. It was removed on
07.09.2026 from the model, the wire, the ingress hash, ClickHouse, Grafana and the
case view; the contract was 20 columns, and is 16 since 2026-09-19. Dropping four columns of noise also
tightened every interval, which is how `receiver_age` moved from unresolved back
to **real**.

**Two rows used to read `+0.000 [+0.000, +0.000] | 5/5 | no effect`, and neither
was a measurement** - both arms trained the same model. The generator made 25
device changes in 50,000 rows, all fraud, below LightGBM's
`min_child_samples = 30`, so no split could form; and every person held one card,
so keying receiver state by PAN or by PINFL gave the same partition. Both were
fixed in the generator (`generator-spec.md` §2 and §4), and the real answers are
the small ones above. The harness now prints **NOT MEASURED** for an exactly-zero
delta on every seed, so the two can never again be confused.

**Read five seeds, not one.** On seed 42 alone `payee_identity` read +0.011, a
clear win; across five seeds it is noise, with two seeds carrying the whole
effect. The seed sweep exists to catch exactly that.

### Per-column value, and why the capability table could not give it

`experiments/ablate_seeds.py --features`

`core_history` is always on - it is the bank's own transaction stream, and no bank
runs without it - so eleven of the twenty columns had never been measured: no
toggle removed them. This drops one column at a time instead.

Paired within seed, 5 seeds, baseline 0.960 ± 0.018. Negative = the column
carried signal.

| column | Δ if dropped | verdict |
|---|---|---|
| `secs_login_z` | −0.0089 [−0.0141, −0.0037] | carries signal |
| `is_new_payee` | −0.0062 [−0.0112, −0.0012] | carries signal |
| the other eighteen | between −0.0131 and +0.0028, every interval crossing zero | not distinguishable alone |

**The eighteen are not a removal list.** Keeping only the two costs **−0.865** -
the model collapses. The columns are redundant with each other, not worthless:
drop `vel_1h` and `vel_10m` covers it. **"Contributes nothing on its own" and "can
be removed" are different claims, and only the second is a decision.**

Eight columns cleared the bar on the previous dataset and two do now, on the same
features. The 08.09.2026 generator fix (ATO episodes of 2-8 events instead of 2-4)
raised the baseline's spread from ±0.011 to ±0.018, and every interval widened
with it: `receiver_age`, at −0.0131 the largest effect in the table, spans
[−0.0303, +0.0040]. That is a statement about the measurement's power, not the
feature - the answer is more seeds, not fewer features.

### A rule's own precision is the wrong number

*`FRESH_RECEIVER` was removed with the payee's age on 2026-09-19 (above); the
lesson holds for every rule in an additive score.*

`FRESH_RECEIVER` looks like the most expensive control in the system: 326 hits on
fraud against **6,447 on legitimate traffic**, 4.8% precision.

- **On the deployed path it costs nothing.** The fused decision is the model's; the
  CEP score reaches it only through `MANDATORY_REVIEW_RULES`, and this rule is not
  in that set. Removing its 0.15 gives **byte-identical output** - its hits are
  reason codes on alerts raised on other grounds, not work arriving in a queue.
- **On the fallback path it is the most valuable rule.** With no model, removing
  it drops CEP-only recall from **49.7% to 28.9%**. It alone lifts 46 decisions to
  REVIEW, **31 of them fraud** - 67% precision on the decisions it causes. An
  additive score measures corroboration, and a rule's standalone precision
  describes a job it is never asked to do.

**Tightening its window makes it worse**, which is worth stating because it is the
obvious fix:

| receiver age | hits | fraud | precision | fraud recall |
|---|---|---|---|---|
| < 30d (current) | 1,354 | 67 | 4.9% | 45.0% |
| < 14d | 640 | 32 | 5.0% | 21.5% |
| < 7d | 281 | 10 | 3.6% | 6.7% |

Precision does not improve; recall collapses. `generator-spec.md` §2 ages 30% of
fraud accounts at 100-1,499 days deliberately, so "new account" is not a
fraud-exclusive signal.

### What this measurement does not cover

**PR-AUC here is the model's alone**, so a capability whose value sits in the CEP
rules is invisible to it. `geo_telemetry` reads "no effect" above while enabling
`IMPOSSIBLE_TRAVEL`, which flags 22 hijacked sessions with zero false positives
across 775 legitimate journeys. The ablation answers "what does this integration
give the model", not "what does it give the system"; rule-side value is read from
`stream-processor/experiments/replay.py` and the fused decision layer. Session
telemetry's value is mostly precision: without knowing the customer was on a call
and hesitating, the model flags more legitimate traffic to reach the same recall.

## Fan-in: a blind spot in a sender-keyed stream

MULE was persistently the weakest pattern - 57.0% recall [53.6%, 60.4%] pooled
over 20 seeds, against 89-99% for the others. Pooling mattered: a single test
slice holds ~35 mule events, whose interval is over 30 points wide. Splitting
held-out mule events by leg located it:

| leg | share of mule events | recall |
|---|---|---|
| fan-in (senders → mule) | 80% | 57.8% |
| fan-out (mule → destinations) | 20% | 93.8% |

The stream is partitioned **by sender**, so every feature described the sender's
own history. Fan-out is visible there - the mule is the sender. Fan-in is not: to
each contributing sender it is one ordinary transfer to a new payee, and nothing
in sender-keyed state can see that six other people sent to the same payee within
the hour. The fix is state keyed by **receiver**, which in a partitioned stream
means an external store - the `receiver_velocity` capability
(`rcv_distinct_senders_1h`, `rcv_inflow_1h`, and the `MULE_FAN_IN` rule). Pooled
over 8 seeds:

| fraud type | before | after |
|---|---|---|
| MULE | 55.7% | **83.4%** [78.1%, 87.6%] |
| APP | 86.9% | 85.7% |
| ATO | 93.1% | 91.6% |
| STRUCTURING | 98.2% | 97.2% |

The other types shift slightly down, within their intervals - the model
reallocates capacity rather than gaining everywhere.

This is a design finding rather than a bug fix: **a sender-partitioned streaming
topology cannot express receiver-side aggregation, and mule detection is exactly
that shape.** Any Flink/Kafka fraud design keyed on one party inherits the same
blind spot for patterns defined on the other.

Two methodological notes, both of which nearly produced false findings: the first
single-seed sweep put `receiver_age=off` at −0.043, a property of one dataset that
took 20 seeds to correct; and an earlier `ablate_seeds.py` judged the mean delta
against the standard deviation of the deltas instead of the standard error of the
mean, which called a clear effect unresolved. Effects are now judged on a t-based
confidence interval for the mean.

## Calibration

`metrics.json` carries a `calibration` block beside the AUCs, computed by
`train.py` on every run. It answers a different question: not *does the model rank
fraud above legitimate traffic* (ROC-AUC 0.961 / PR-AUC 0.321 on the realistic
profile) but *are its probabilities usable as magnitudes*.

```
brier               0.00166
n_alerts            132          (>= REVIEW on the held-out slice)
saturated_share     22.7%        rounding to 1.000
distinct_scores     89
median_alert_score  0.573876
scored_with         model.onnx
```

Read `saturated_share` and `distinct_scores` together. On the baseline profile
they read 66.9% and 33: the model separated the classes almost perfectly and
still could not **order** an alert queue, because the alerts piled up at the top
of the scale. On the realistic profile 132 alerts carry 89 distinct scores and
22.7% round to 1.000 - coarser than the 20-column model's 114 among 160, but a
queue that can still be ordered. AUC is blind to this by construction - it is a
rank statistic - and the finding surfaced only when a real work queue tried to
sort by score (`docs/irp-framing.md` §9.1). It is a property of near-separable
synthetic data, not of gradient boosting.
