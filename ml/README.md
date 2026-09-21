# ml

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
explain.py        global importance (beeswarm + bar) and per-alert reason codes

experiments/      harnesses - each produces a NUMBER, not an artefact the
                  system uses, and none is imported by the pipeline above
  ablate_seeds.py what each capability is worth, across generator seeds, with
                  intervals; --only <capability> sweeps all of its modes
  counters.py     the counterparty counters' gate, paired fits on two datasets
  collectors.py   honest collections vs mules, with and without the counters
  thresholds.py   the cutoff menu, read on the served model
  layers.py       the rules alone, the model alone and the deployed decision
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

Small labelled fraud sets, exact reason codes required per alert (for the analyst
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
transfers with 176 fraud - at the REVIEW cutoff chosen on the validation rows.
`models/metrics.json` is the authoritative copy, and `tools/boundary_audit.py`
fails if this table disagrees with it.

| metric                | ML model | CEP rules only |
|-----------------------|----------|----------------|
| ROC-AUC               | 0.997    | —              |
| PR-AUC                | 0.673    | —              |
| precision at REVIEW   | 0.634    | 0.011          |
| recall at REVIEW      | 0.670    | 0.142          |

Recall by fraud type (ML at REVIEW): STRUCTURING 82.8%, APP 52.4%, ATO 89.8%, MULE 48.6%.
The five fits alone scored 0.665-0.678 PR-AUC on the same slice and their committee
0.673, inside that range: with the L2 penalty (Feature importance, below) single
fits barely scatter, where without it they ranged 0.392-0.539 and averaging them
was what made the committee worth serving.

Read plainly: the committee finds two thirds of the fraud in the held-out month,
and nearly two alerts in three are fraud, on a dataset regenerated on 2026-09-20
(below). The rules alone reach 1.1% precision on data where legitimate traffic
also collects, splits and changes phones. The weak patterns are APP and MULE.

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
  `model.onnx` and the job reads (`stream-processor/config.py`). This run: cut at REVIEW = 0.1048, and
  **there is no BLOCK**: since 2026-09-19 the system never blocks on its own - the
  owner's decision - and every alert goes to a person. The CEP-only fallback keeps
  its fixed cutoff, since an additive rule score is not a probability.

*IBM AML, which the gates below use, was removed on 2026-09-19 - its accounts
include banks and companies, and this project is about transfers between people -
and returned on 2026-09-20 for information only (`validation/README.md` §4): it
decides no gate now. The gates below stay as the record of the decisions they made.*

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
unchanged. Both tables above are the 20-column model of that day, and the IBM AML
one is history - that dataset has only reported for information since 2026-09-20.

**Re-read on 2026-09-21, against the model actually served** - the committee
retrained that day with the L2 penalty (Feature importance, below).
`experiments/thresholds.py` loads `models/model.txt` instead of refitting, so the
menu describes the deployed system. Cutoffs chosen on the validation rows, read on
the 100,000 test transfers (176 fraud):

| cutoff | alerts per 100k | caught | real |
|---|---|---|---|
| top 0.05% of transfers | 56 | 29.0% | **91.1%** |
| top 0.1% | 87 | 42.6% | 86.2% |
| top 0.2% | 165 | 61.4% | 65.5% |
| **F1 peak (the current rule)** | **186** | **67.0%** | **63.4%** |
| F2 peak | 210 | 68.8% | 57.6% |
| top 0.5% | 446 | 80.1% | 31.6% |
| top 1% | 927 | 90.3% | 17.2% |

Against the unpenalised committee's menu of the same morning the penalty is ahead
at the short end and the long end - 42.6% caught at 86.2% real in 87 alerts, where
85 had held 38.6% at 80.0%, and 90.3% against 81.8% in the top 1% - and level at
the old committee's own operating point: in 131 alerts it catches 93 of 176 at
71.0% real, where the old one caught 96 at 73.3%, three transfers inside the noise
of 176 fraud. The F1 peak moved to a longer queue, 186 alerts against 131, because
it is chosen on the validation rows and the penalised model's peak sits further
out: more fraud caught (118 against 96) for more false alarms (68 against 35). The two ends worth
putting to an owner: the top 0.1% catches 42.6% of the fraud with 86% of alerts
real, the F2 peak 68.8% at 58%. Nothing is adopted here: the cutoff is a decision,
and it has not been changed.

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

### A rule for the counters: the gate, fixed before the run

`COLLECTOR` fires when six or more distinct people paid the payee within 24 hours
*and* the one-hour `MULE_FAN_IN` is silent, so one collection cannot score twice.
The count is the hour rule's own - no new number - and the weight is lower (0.25
against 0.35): a slow collection is weaker evidence than a burst, and on its own it
does not reach the 0.40 cutoff. The rule is not in `MANDATORY_REVIEW_RULES`, so it
cannot move a fused decision; its whole value is on the **CEP-only fallback**, which
caught 14.9% of the held-out month's fraud at 1.3% precision once `FRESH_RECEIVER`
went with the payee's age (14.2% at 1.1% on the file regenerated later that day).

The rule, fixed before the run:

1. CEP-only recall on the held-out slice rises by at least 5 points (0.149 -> 0.199
   or better).
2. Among the transfers the rule alone lifts to REVIEW, the fraud share is at least
   the CEP-only precision it is added to (1.3%).
3. Of the 2,973 legitimate transfers into accounts collecting from five or more
   other people in a week, it lifts at most 1% - 30 transfers.

All three hold: the rule stays on. Any fails: it comes out, and the numbers are
written here. `stream-processor/experiments/replay.py rule-value` runs it.

**Measured on 2026-09-20, and it comes out.** `replay.py rule-value` (in git
history, `git show 7d16c19`) replayed the 500,000 rows and read the held-out slice,
the arm without the rule being the same score minus its weight:

| | alerts | fraud | recall | precision |
|---|---|---|---|---|
| CEP-only, without | 2,309 | 30 | 0.149 | 0.013 |
| CEP-only, with | 2,351 | 34 | **0.168** | 0.014 |

Condition 2 held and held well: of the 42 decisions the rule alone lifts, 4 are
fraud - 9.5%, seven times the precision of the fallback it is added to. Conditions
1 and 3 failed: recall rose 1.9 points where the rule asked for 5, and the 38
legitimate transfers it lifted are all into accounts collecting from five or more
other people in a week, above the 30 the rule allowed. **So the rule is removed.**
Lowering the count to five, raising the weight or widening the window would each
have been chosen after seeing this table, which is the failure the gate exists to
prevent.

What the measurement says is not that the shape is wrong - the model reads the same
counts and they are worth +0.116 PR-AUC - but that a **fixed count over a day is
too blunt a cutoff** for a rule layer: at six payers it is mostly weddings, and the
mule collections it does catch are already corroborated by other rules. A
population-relative threshold, as `MULE_FAN_IN_MODE=relative` does for the hour
window, is the version worth gating next; it needs its own baseline in Redis.

### Rules on the counters: three attempts, none of them stays

The counters are worth +0.116 PR-AUC to the model (above). Turning the same shapes
into a CEP rule was tried three times, each with its conditions committed before its
run, and each removed by them. The fallback they were meant to strengthen caught
14.9% of the held-out month's fraud at 1.3% precision, where `FRESH_RECEIVER` left
it - 14.2% at 1.1% on the file regenerated later that day.

| attempt | rule | CEP-only recall | fires on legitimate rows | decisions it alone lifts | verdict |
|---|---|---|---|---|---|
| 1 | `COLLECTOR` - six payers in a day, where the hour rule is silent | 0.149 -> 0.168 | 2.3% | 42, 4 fraud (9.5%) | **out**: +1.9 points where 5 were asked, and 38 lifted collections against a cap of 30 |
| 2 | `PASS_THROUGH` - paid within **10 minutes**, 80% of it leaving again | 0.149 -> 0.153 | 0.195% (cap 0.1%) | 5, 1 fraud (20.0%) | **out**: +0.4 points, and twice the legitimate traffic allowed |
| 3 | the same rule, **hour** window | 0.149 -> 0.168 | **1.18%** (cap 0.1%) | 59, 4 fraud (6.8%) | **out**: every condition failed |

Attempt 2 fired on 196 transfers in the held-out month and one was fraud, because on
the realistic profile a mule pays on **5 to 60 minutes** after its last inbound
(`data-generator/config.py`, `mule_gap_minutes`) and ten minutes sees the short end.
**Attempt 3 was declared as fitted**: the owner asked for the hour after reading that
sentence, so the window came from the generator's own parameter rather than from the
shape. It was run with the same unsoftened conditions and a prediction written first
- more payouts caught, far more legitimate traffic touched, condition 3 the one to
fail. It caught four more fraud and touched twelve hundred legitimate transfers: the
prediction held, and conditions 1 and 2 failed with it.

**What three attempts say together.** The model reads these counts and gains from
them; a rule has to cut them at a number, and at every number tried - six payers a
day, ten minutes, an hour - the honest traffic of the same shape outnumbers the
fraud. This is the fan-in threshold's finding in two more shapes: *an additive rule
layer wants thresholds relative to what is observable, not constants*
(`docs/irp-framing.md` §6, third RQ3 result). It is also the cost of trying: three
gated attempts on one idea, all recorded, is the honest denominator for any rule
that does pass here later. The rules, the tests and the harness that measured them
are in git history: `git show 7d16c19` (attempt 1), `git show 8ba52f9` (attempt 2),
`git show 019fee8` (attempt 3).

### Honest collections are not mistaken for mules

`experiments/collectors.py` refits the served committee without the counterparty
counters and with them - reproducing the served one exactly, 186 alerts at
0.634 / 0.670, against 131 at 0.664 / 0.494 without - and reads the held-out month
by the payee's **other** payers over the 7 days before each transfer (re-run
2026-09-21 with the penalised recipe):

| payee's other payers, 7 d | legitimate transfers | false alarms without | with | MULE transfers | caught without | with |
|---|---|---|---|---|---|---|
| none | 22,807 | 19 (0.08%) | 45 (0.20%) | 9 | 3 | 6 |
| 1-2 | 56,587 | 15 (0.03%) | 10 (0.02%) | 10 | 2 | 3 |
| 3-4 | 17,617 | 4 (0.02%) | 5 (0.03%) | 8 | 3 | 5 |
| 5 or more | 2,813 | 6 (0.21%) | **8 (0.28%)** | 8 | 3 | 3 |

**The collection case is the one to watch, and the counters cost it a little.**
Transfers into an account collecting from five or more people - the realistic
profile's weddings, gifts and joint purchases - draw **8 false alarms of 2,813
with the counters against 6 without**, and the committee catches the same 3 of
that group's 8 mule transfers either way. Across the whole slice the counters
raise the catch from 87 to 118 and the false alarms from 44 to 68.

The same table has read three ways: two more alarms on collections on the
previous dataset (12 against 10), five fewer on the regenerated one with the
unpenalised recipe (2 against 7), two more again here. A handful of alarms on
2,813 transfers moves with every draw, so what can be said is that the counters
neither protect collections nor punish them measurably. What held every time is
where the alarms sit: on payees nobody else paid they rise (19 to 45 here),
because with no inbound history the only columns that can speak are the sender's
own - how many payees it paid this week, and how recently it was itself paid.

The same question was asked of `link_history` on 2026-09-19 with the same shape of
answer (5 to 7 false alarms of 1,651), before that capability was removed. What
none of this can test is the seller paid by strangers all day: the generator draws
no merchant flows (`docs/generator-spec.md` 8), and such a payee would sit far
above the payer counts the model has seen. A deployment needs a known-seller flag -
self-employed status is something the bank holds - before a payer count reads a
shop as a collection.

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
unchanged (0.476, 0.469). **The headline figures carried about eight points of
retrain-to-retrain spread in recall, and were quoted with it** - until the L2
penalty of 2026-09-21 took most of that spread away (Feature importance).

### The payee's account age: removed, and what it cost here

On 2026-09-19 the owner removed the payee's account age - `receiver_age`,
`receiver_is_fresh` and the `FRESH_RECEIVER` rule - with the Neo4j lookup that
supplied it and the BIN table that decided when it was visible. A sending bank
sees the account behind a card only when the payee is its own client, 6.85% of
transfers at the real card market (`docs/threat-model.md` §4), and the UzCard /
HUMO switch carries no such field. The vector became 16 columns, and 21 with the
counters below.

Unlike the two columns above, this one cost more than a retrain moves. The served
committee went from PR-AUC 0.450 to 0.321, recall at REVIEW from 0.421 to 0.351 and
precision from 0.586 to 0.538; the rules alone from 0.317 recall to 0.149, as the
fallback-path figures below predicted. Five paired seeds on the baseline profile
had put the capability at −0.022 [−0.040, −0.004]. Part of that lift was the
generator's: it gives every payee an age and makes 70% of fraud payees younger
than 45 days (`docs/generator-spec.md` §2), which a bank that cannot see the
payee's account never observes. **The drop is the price of not assuming data the
deploying bank does not have, and the figures above are quoted without it.**

### Counting counterparties over days: the rule, fixed before the run

`counterparty_history` (on by default since it passed its gates on 2026-09-20) adds
five columns: the payee's distinct
payers over 24 hours and over 7 days, the sender's distinct payees over the same
two windows, and the seconds since the sender's own account was last paid. The
windows are the regulator's - the CBU's internal-control rules define P2P activity
subject to control as counts of distinct counterparties over up to 30 days
(`docs/related-work.md` 6e) - shortened to a week, because both datasets here span
30 days and a month-long window on them would mean "everything since the file
began". The last column is the other half of the same shape: a mule collects over
days and passes the money on, so the interval between being paid and paying is
what a one-hour fan-in window cannot see.

The rule, fixed before any of it was measured:

1. **Parity.** The store-backed path and the in-process replay must produce the
   same columns for the same events (`stream-processor/tests/test_receiver_store.py`),
   and switching the capability on must leave the existing sixteen columns
   unchanged.
2. **This project's data.** Five paired fits of `train.py`'s recipe on the
   realistic profile, with and without the five columns: the mean validation
   PR-AUC difference at or above zero, its 95% interval's lower bound above
   -0.01, and the averaged committee's validation PR-AUC not lower.
3. **PaySim**, the published split's first 24 days (fit on the earliest 80%,
   validate on the rest), unweighted, five seeds: the mean validation PR-AUC
   difference not below zero, and the averaged committee finding at least as much
   fraud in the top 0.1% of the validation transfers.

All three pass: the capability is switched on and the served model retrained. Any
fails: it stays off, and the numbers are written here. `experiments/counters.py`
runs 2 and 3.

### What the counters measured

Parity held: 0 cells differ across the sixteen existing columns on 20,000 rows,
and the store-backed path matches the in-process replay row for row
(`stream-processor/tests/test_receiver_store.py`). Five paired fits, validation
PR-AUC:

| | without | with | paired difference |
|---|---|---|---|
| realistic profile | 0.426 | 0.542 | **+0.116 [+0.010, +0.222]**, better on 5 of 5 seeds |
| PaySim, published split | 0.135 | 0.165 | +0.030 [-0.013, +0.073], better on 4 of 5 |

**Re-measured on the regenerated dataset, 2026-09-20.** Rounding the amounts moved
the dataset of record (below), so the gate was run again on it. **Five seeds now
fail it**: +0.027 [-0.077, +0.132], better on 4 of 5 - the mean is positive and the
interval is nowhere near clear of zero. Twenty seeds, which is this project's own
remedy for a wide interval rather than a second look for a better answer, give
**+0.047 [+0.009, +0.085], better on 15 of 20**, with the averaged committee 0.421
-> 0.487 and its top 0.1% 33.9% -> 35.1%. By the conditions as written that is a
pass, and the capability stays on.

Read it as the correction it is: **the effect is real and about half the size the
first five seeds suggested.** The +0.116 above belongs to a file that no longer
exists, measured with too few seeds to pin an effect of this size; +0.047 on the
current data is the figure to quote. The decomposition below - counts against
transit - was measured on that older file and has not been re-run.

Both ran on the unpenalised recipe. Refitted with the L2 penalty, the counters
still raise the served committee's catch from 87 to 118 of 176 (collections,
below); the gate itself has not been re-run on that recipe.

On PaySim the averaged committee went 0.155 -> 0.196 and its top 0.1% of the
validation transfers held 18.5% -> 19.5% of the fraud, so condition 3 holds - but
the interval spans zero: *not worse, probably a little better*, not a proven gain.
Both conditions held, so the capability was switched on and the served model
retrained on 21 columns: PR-AUC 0.321 -> 0.480, recall at REVIEW 0.351 -> 0.436,
precision 0.538 -> 0.571. That is more than the payee's account age was worth
before it was removed (PR-AUC 0.450 then), on data a sending bank actually has.

**Neither half does it alone.** Measured apart on the realistic profile of the
time, after the gate: the four counts +0.067 [-0.072, +0.205], better on 4 of 5 seeds; the transit
interval alone **-0.042** [-0.140, +0.056], better on 1 of 5. Together they are
+0.116 on 5 of 5. The shape is the pair - this payee collects from many people,
*and* this sender was paid minutes ago - not either column. Dropping the transit
column now would be keeping the arm that won after seeing the result, so the five
stay together, as the rule said before the run.

**What this data makes easy.** The generator draws a mule's outflow minutes after
its last inflow (`data-generator/fraud_patterns.py`), so "paid, then paying" is
more regular here than in real traffic. The shape is the regulator's - the CBU
counts counterparties over a day and a month, and the Bank of Russia's equivalent
list treats money leaving within a minute of arriving as a sign - but its strength
on this data is partly the generator's.

### The dataset changed under these figures, 2026-09-20

The amounts were made round on both sides of the label (`docs/generator-spec.md` §8,
item 6), which meant regenerating the dataset of record. Two things moved with it,
and only the first is the fix:

- **roundness stopped marking the class**: 0.0% of ATO, MULE and STRUCTURING amounts
  used to be multiples of 1,000 against 39.6% of legitimate traffic; both sides now
  round about 90% of the time, within a point of each other in every size band;
- **the held-out month is a different draw**: 176 fraud against 202, mixed
  differently - **49 ATO against 10** - because the extra random numbers rounding
  consumes shift every episode's placement. ATO is the best-caught pattern here, so
  a good part of the jump below is that mix, not detection getting better.

So the figures across the two files are **not comparable point to point**. What can
be said is that the model is no longer able to separate the classes on an artefact
nobody intended, and that the data got easier in the process: the score distribution
is more saturated than it was (47.3% of alerts round to 1.000 against 23.4%).

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

## Feature importance (SHAP)

**Recomputed 2026-09-21 on the served committee**, over the 100,000 held-out
transfers, with LightGBM's own TreeSHAP (`pred_contrib`) - the algorithm the `shap`
package implements and the call the case view makes per alert
(`case-manager/explain.py`); the package itself cannot load on the owner's machine,
where an application-control policy blocks its numba extension. Mean |contribution|
to the log-odds:

| | feature | | | feature | |
|---|---|---|---|---|---|
| 1 | `rcv_inflow_1h` | 0.600 | 9 | `daily_sum_ratio` | 0.096 |
| 2 | `log_amount` | 0.497 | 10 | `amount_z` | 0.095 |
| 3 | `is_new_payee` | 0.359 | 11 | `secs_login_z` | 0.093 |
| 4 | `amount_to_mean` | 0.232 | 12 | `sender_payees_7d` | 0.067 |
| 5 | `payee_payers_7d` | 0.227 | 13 | `payee_payers_24h` | 0.061 |
| 6 | `hour` | 0.215 | 14 | `active_call` | 0.015 |
| 7 | `secs_since_last` | 0.212 | 15 | `rcv_distinct_senders_1h` | 0.014 |
| 8 | `secs_since_sender_inbound` | 0.111 | | | |

The caught fraud `explain.py` prints reads the way a case view should: +5.05 from
the money that reached the payee within the hour, +1.93 from the amount, +1.10 from
the payee's other payers this week, +0.82 from the payee being new to the sender.

**Money converging on the payee is the model's first reason** - the fan-in shape
the receiver-side store exists for (Fan-in, below) - and two counterparty counters
follow within the first eight. **`hour` sixth is the generator, not a finding**:
the diurnal pattern is drawn, not observed (`docs/irp-framing.md` 9.4).
**`is_new_payee` third is still an upper bound**: fraud reaches a stream-new payee
99.1% of the time on this data against 44.2% of legitimate traffic (keyed by card,
as the job keys it), because the generator does not produce the evasion its own
threat model names - one small prior transfer establishes the payee. When it is
produced (`SEEDED_PAYEE_SHARE`, default off), APP detection on the affected
episodes falls from 56.0% to 31.8%, five seeds, delta −25.3 pp [−47.5, −3.1]
(`docs/threat-model.md` §4).

**`active_call` near the bottom is not a useless column.** A call while confirming
marks **APP** (44.9% of those transfers against 10.0% of legitimate traffic) and
the *absence* of the others - 6.0% of MULE transfers and 7.7% of STRUCTURING carry
one, below the legitimate rate - so its contributions cancel across types in a
mean. It is a pattern discriminator, not a fraud flag.

### The L2 penalty on leaf values: found, measured, adopted 2026-09-21

The committee served until that day set no penalty, and on the regenerated file its
attribution ran to thousands: `daily_sum_ratio` at 1,895 mean |contribution|, raw
scores from −26,419 to +158 on the first 60,000 rows, leaves reaching ±18,111, and
55% of transfers beyond ±745, where even a 64-bit probability is exactly 0 or 1.
Most transfers were classified with certainty early, the later trees chased the
few hard cases where the second derivative had all but vanished, and a Newton step
over a vanishing second derivative is enormous. Neither its probabilities nor its
contributions could be read as evidence - the fraud above then carried +4,808 from
`daily_sum_ratio` against −1,110 from `log_amount` - and, measured, **its ranking
suffered too**. The served recipe with only `reg_lambda` changed, the same five
seeds and split, chosen on the validation rows:

| `reg_lambda` | validation PR-AUC | test PR-AUC | at the F1 cutoff: alerts, caught, real | largest leaf | largest mean contribution |
|---|---|---|---|---|---|
| 0 (served until 2026-09-21) | 0.434 | 0.573 | 131, 54.5%, 73.3% | 18,111 | 1,909 |
| 1 | 0.510 | 0.687 | 163, 62.5%, 67.5% | 1.28 | 0.78 |
| **10 (served since)** | **0.511** | 0.673 | 186, 67.0%, 63.4% | 1.28 | 0.61 |

The rule, committed before its confirmation (`1e6c2eb`): five committee seed sets,
0 against 10 on the same rows, adopted only if the paired validation difference is
above zero with a 95% interval clear of it. **It passed: +0.034 [+0.005, +0.063],
better on 5 of 5.** The penalty also took the lottery out of retraining:
unpenalised, the five sets scored 0.440-0.498 on validation - the served seeds 42-46
were the worst draw of them - and penalised 0.506-0.514. `subsample=0.8` left the
recipe at the same time: without a bagging frequency LightGBM ignores it, and
removing it changed no prediction (largest difference 0.0).

**`is_family` was the same artefact, and how it ended is the finding.** An earlier
revision reported it as the #1 feature (1.29) and the core research contribution:
the generator routed no fraud to relatives, so it separated the classes by
construction. It was not deleted - it is the `myid_kinship` capability, which
defaults to off, so it is absent from the deployed vector. Once the
generator modelled both directions (25% of legitimate transfers go to relatives,
and a realistic minority of fraud too), switching it on is worth nothing
measurable - the `myid_kinship=on` row below. A feature that dominates SHAP on
synthetic data is suspect until the generator models both sides of its behaviour
(`docs/irp-framing.md` §4). **The amounts are the next instance of this**, found by
the owner on 2026-09-20 and recorded in `docs/generator-spec.md` §2.

The ONNX model reproduces the native committee's probabilities to within 1.2e-7 on
the parity rows, and every figure with them: `experiments/layers.py` reads the
served ONNX and prints ROC-AUC 0.997 and PR-AUC 0.673, the native committee's, with
the same 118 frauds against 68 false alarms at the cutoff. Before the L2 penalty
they disagreed - 0.965 served against 0.988 native - because 32-bit inference tied
92.6% of held-out transfers at exactly 0.0; bounded leaves ended that.

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
case view; the contract was 20 columns, and is 21 since 2026-09-20. Dropping four columns of noise also
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
fraud above legitimate traffic* (ROC-AUC 0.997 / PR-AUC 0.673 on the realistic
profile) but *are its probabilities usable as magnitudes*.

```
brier               0.00093
n_alerts            186          (>= REVIEW on the held-out slice)
saturated_share     0.0%         rounding to 1.000
distinct_scores     161
median_alert_score  0.481618
```

Read `saturated_share` and `distinct_scores` together. On the baseline profile
they read 66.9% and 33: the model separated the classes almost perfectly and
still could not **order** an alert queue, because the alerts piled up at the top
of the scale. On the realistic profile the unpenalised committee's 131 alerts
carried 64 distinct scores and 47.3% rounded to 1.000; with the L2 penalty 186
alerts carry 161 distinct scores, none rounds to 1.000, and the median alert scores
0.48 - probabilities that can order a queue. AUC is blind to this by construction - it is a
rank statistic - and the finding surfaced only when a real work queue tried to
sort by score (`docs/irp-framing.md` §9.1). It was a property of near-separable
synthetic data met by a recipe with no penalty on leaf values (Feature
importance, above), not of gradient boosting as such.
