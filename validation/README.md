# External validation

Two datasets in use - one for decisions, one for information - and more examined
and rejected. The adapter owns the file's
shape; everything downstream of a translated event - the unit conversion, the
replay over the deployed rule engine, the report sections - is in
**`harness.py`**, so another dataset would be one measurement with it.

## The constraint that shapes everything here

18 of this project's 21 features are **relational** - computed over the history
of a named sender and a named receiver. Measured cost of losing them (seed 42,
held-out slice, baseline profile, on the 20-column vector of the time):

| available | PR-AUC | precision | recall |
|---|---|---|---|
| full system | 0.937 | 0.928 | 0.859 |
| no account identifiers | 0.761 | 0.563 | 0.745 |
| amount + hour only | 0.653 | 0.272 | 0.819 |

On the realistic profile the full system scores PR-AUC 0.573 (`ml/README.md`);
the stripped configurations have not been re-run there. Public transaction
datasets carry no account identifiers, because those are what cannot be
published:

> **Relational fraud detection cannot be validated end-to-end on public real
> data, because the identifiers that make it relational are the reason such data
> stays private.**

So the two halves are validated unequally: PaySim tests the sender-side
relational features, and the receiver side - fan-in and the counterparty
counters - is tested only on IBM AML, which reports for information because its
accounts include banks and companies (section 4).

---

## 1. PaySim - do the relational features work on foreign data?

`paysim_adapter.py`. An agent-based mobile-money generator (Lopez-Rojas et al.)
built for another market, with **identifiers on both sides**, so this project's
own extractor and CEP rules run on it unchanged - `rules.evaluate()` as deployed,
nothing retrained or tuned. Capabilities PaySim cannot support (geo,
session, kinship) are switched **off**, never defaulted, so no rule
fires on a fabricated zero.

It is not trained on, for three reasons: fitting to the data under test answers
a circular question; three of the features need data PaySim lacks, so a model on
what is left would not be the served one; and the rail and the units
differ. The one legitimate training use of foreign data is a throwaway model
fitted with and without a capability, to see whether an ablation delta
reproduces.

**Get it:** Kaggle, "Synthetic Financial Datasets For Fraud Detection" (~470 MB).

```bash
cd validation
python paysim_adapter.py --file PS_20174392719_1491204439457_log.csv --limit 500000
python paysim_adapter.py --file PS_20174392719_1491204439457_log.csv --baseline
python paysim_adapter.py --file PS_20174392719_1491204439457_log.csv --our-model
```

`TRANSFER` is the P2P analogue and the default. `--baseline` retrains the
published PaySim model that `docs/related-work.md` §6 calibrates against: its
AUPRC 0.380 reproduces at 0.397. PaySim's clock is hourly, so sub-hour patterns
are invisible - a **conservative** test.

### Result (500,000 TRANSFER rows, 2,520 fraud)

| rule | on fraud | on legit | lift |
|---|---|---|---|
| `NEW_PAYEE_HIGH_AMOUNT` | 5.95% | 1.49% | **4.0x** |
| `MULE_FAN_IN` | 0.00% | 0.07% | 0.0x |

**The relational feature transfers**: `NEW_PAYEE_HIGH_AMOUNT`, built entirely
from per-sender history, separates the classes 4:1 on data this project did not
produce. **`MULE_FAN_IN` finds nothing, and should not**: PaySim drains one
account straight to cash-out, with no collection stage. Fraud patterns are
market-specific.

With a fixed threshold the decision layer flagged nothing - the highest score any
fraud reached was 0.35 against 0.40. Capability-scaled thresholds
(`SCALE_THRESHOLDS_BY_CAPABILITY`) move REVIEW to 0.17:

| | fixed threshold | scaled threshold |
|---|---|---|
| fraud flagged | 0 / 2,520 (0.0%) | **150 / 2,520 (6.0%)** |
| legit flagged | 43 (0.01%) | 7,724 (1.55%) |
| decision lift | — | **3.9x** |

A weak detector, honestly: scaling turned a silent layer into a thin one; it did
not create signal.

### Result (`--our-model`): this project's model trained on PaySim

18 of the 21 features compute here - PaySim carries no call state, no region and
no session timing - on the published baseline's own split (24 days / 7 days).
**Re-run 2026-09-20** with the counterparty counters on; the figures the
counters replaced are in the paragraph below the table.

| configuration | feat | PR-AUC | ROC-AUC | rec@2% | lift |
|---|---|---|---|---|---|
| **fitted with class weighting** (scale_pos_weight ≈ 774) | | | | | |
| this project, all it can compute | 18 | 0.018 | 0.659 | 6.2% | 1.8× |
| — without receiver aggregation | 16 | 0.022 | 0.705 | 19.0% | 3.1× |
| — plus PaySim's own transaction type | 23 | 0.018 | 0.682 | 19.7% | 3.4× |
| **fitted unweighted**, as the published baseline was | | | | | |
| this project, all it can compute | 18 | **0.294** | 0.831 | 43.0% | 6.6× |
| — without receiver aggregation | 16 | **0.330** | 0.876 | 48.3% | 6.9× |
| — plus PaySim's own transaction type | 23 | **0.440** | 0.916 | 53.4% | 8.2× |
| published generic baseline (§6 of `related-work.md`) | ~24 | 0.380 | 0.908 | 49.4% | 7.0× |

1. **The recipe, not the features, produced the alarming number**: same features
   and split, **0.018 weighted against 0.294 unweighted** - heavy positive
   weighting flattens the top of the ranking AUPRC reads. `train.py` now fits
   unweighted below 0.5% fraud.
2. **Receiver aggregation reverses on PaySim**: removing the one-hour fan-in pair
   *improves* PR-AUC by 0.036, where on this project's data it costs the most.
   PaySim drains one account straight to cash-out with no collection stage, so no
   run on PaySim can validate that finding, and no dataset here does.
3. **With matching recipes the feature set now beats the published model**: 0.440
   against 0.380, 53.4% against 49.4% of the fraud inside a 2% alert budget, and
   8.2× against 7.0× lift - once PaySim's own transaction type is added to both.
   The counterparty counters are what moved it: before them the same three rows
   read 0.267, 0.315 and 0.373 on 14 columns (`git show 6ed678d`), level with the
   baseline rather than above it. A caveat the number does not carry: this is a
   model *fitted on PaySim*, not the served one, and the deployed rules still
   flag 6% of its fraud (§1 above).

The run also found an extractor defect: with no `receiver_card`, `payee_key`
returned "" and every payee shared one receiver state, fabricating fan-in.
`features.py` now warns, and the harness refuses such a stream
(`harness._require_a_payee_key`).

---

## 2. Zenodo 20030065 - examined and NOT used as claimed

DOI [10.5281/zenodo.20030065](https://doi.org/10.5281/zenodo.20030065),
published as *"A Production-Collected Online Banking Fraud Detection Dataset
from a Live Cloud-Based Deep Learning System"*. **It should not be cited as
production data.**

| Check | Result |
|---|---|
| rows | 57,394 in the file vs **56,962** claimed |
| fraud | 111 in the file vs **98** claimed |
| response latency (promised per record) | **absent** |
| `v7..v28` | max pairwise correlation 0.10, σ ≈ 1.17 → **PCA components** |
| `v1..v6` | σ ≈ 110,000, one pair correlated **0.9996** → balance-like |

Split on the shape of `transaction_id`, the counts resolve exactly: 56,962 rows
and 98 fraud are the described dataset - a 1/5 sample of the ULB credit-card set
to within rounding - and 432 rows of a live demo session (no `test_date`,
timestamps months later, a PaySim-shaped schema, what look like real IP
addresses) were appended after publication. The publisher did not miscount; the
release is contaminated. The PCA features rule it out anyway: features without
meaning carry no SHAP explanation, and CBU 3759 requires an explainable decision.

**What survives.** The fraud base rate does not depend on what the features mean,
and ~0.17–0.19% is a real figure from real card traffic - cited to the ULB dataset
directly.

---

## Rejected: IEEE-CIS

590k real transactions with pseudo card identifiers, but card-not-present
e-commerce: **there is no receiver as a party**, so the fan-in finding cannot be
tested at all. Also rejected: the Kaggle credit-card set,
PCA-anonymised into V1..V28, which makes the SHAP explanations CBU 3759 requires
meaningless.

## Rejected: Kaggle aryan208, "Financial Transactions Dataset for Fraud Detection"

5,000,000 transactions over 2023 naming a sender and a receiver account - the shape this
project needs - and a label that carries nothing. Screened on 2026-09-19:

| check | result |
|---|---|
| fraud | 3.59%, one type only: card_not_present |
| fraud share by transaction type | 3.56-3.63% for deposit, payment, transfer and withdrawal alike |
| history per account | median 5 transactions in the year |
| each supplied column alone against the label, AUC | amount, velocity, geo-anomaly, spending-deviation, time-since-last: 0.500-0.501 |
| time_since_last_transaction against the sender's real previous transaction | correlation 0.002 |

The label is independent of every column, including the ones named after fraud
signals, and the time-since-last column does not describe the file it is in: there is
nothing to detect. The Mendeley set of section 3 was downloaded again the same day and
is the one already rejected.

---

## Tests

```bash
python -m pytest validation -q
```

Fixtures in the shape of the real files, so the adapters - the mapping onto this
project's event contract, not the detection result - are known to work before
anything is downloaded.

---

## 4. IBM AML - removed 2026-09-19, returned 2026-09-20 for information

It left because its accounts include banks and companies and this project is about
transfers between people. It is back because it is the only dataset here with a
**collection stage on a minute clock**, and the counterparty counters adopted on
2026-09-20 (`ml/README.md`) are built for exactly that shape - PaySim drains one
account straight to cash-out and cannot test it at all.

**The terms, fixed before the run that follows:**

1. It reports **for information**. It does not decide whether a capability stays,
   goes or is adopted; that is what this project's own data and PaySim do.
2. Whatever it produces is written here, flattering or not. The dataset was removed
   once after failing a gate, so bringing it back on the expectation of a better
   answer is the exact way to fool oneself - naming that here is the guard.
3. It leaves again if the answer is **unreadable** rather than unflattering: hub
   accounts reaching 26,365 transactions a sender-day are what the retail rules
   choke on, and if they drown the collection signal, the file cannot answer the
   question it was brought back for.

Everything between the composition and **Re-run 2026-09-21** is **historical**,
measured before 2026-09-19 with the feature set of the time (14 computable columns,
no counterparty counters). The re-run on the current extractor follows it.

`ibm_aml_adapter.py`. AMLSim - removed with it, and not brought back - left two
readings of its inverted fan-in result: the
rule detects a rate, or the window cannot see a 363-day pattern. Separating them
needs a collection stage on a finer clock. *IBM Transactions for Anti-Money
Laundering* (Altman et al., NeurIPS 2023,
[arXiv:2306.16424](https://arxiv.org/abs/2306.16424); CDLA-Sharing-1.0),
`HI-Small_Trans.csv`:

| | PaySim | AMLSim | **IBM AML (HI-Small)** |
|---|---|---|---|
| clock resolution | 1 hour | 1 day | **1 minute** |
| rows | 6.36M | 198K | 5.08M |
| span | 30 days | ~2 years | **17 days** |
| positives | 0.129% | 0.339% | 0.102% |
| collection stage | absent | present, ~363-day span | **present, ~3.6-day span** |

The median laundering receiver collects over 86.8 hours against the one-hour
window (~87x, measured by section D on every run). Before any replay: 1.02% of
laundering receivers exceed six senders in an hour against 0.18% of the rest -
5.6x the right way, where AMLSim was 0.4x.

**Get it:** Kaggle
([ealtman2019/ibm-transactions-for-anti-money-laundering-aml](https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml)),
IBM Box, or a Hugging Face mirror of the transactions that needs no account.

```bash
python ibm_aml_adapter.py --file HI-Small_Trans.csv --limit 500000
python ibm_aml_adapter.py --file HI-Small_Trans.csv --patterns HI-Small_Patterns.txt
python ibm_aml_adapter.py --file HI-Small_Trans.csv --extract-only --cache ibm_features21.npz
python ibm_aml_adapter.py --file HI-Small_Trans.csv --our-model --cache ibm_features21.npz --seeds 10
python ibm_aml_adapter.py --file HI-Small_Trans.csv --patterns HI-Small_Patterns.txt --typology-recall --cache ibm_features21.npz --seeds 10
python ibm_aml_adapter.py --file HI-Small_Trans.csv --patterns HI-Small_Patterns.txt --budgets --individuals --cache ibm_features21.npz --seeds 5
```

`--patterns` names each laundering row from the sidecar, which needs a Kaggle
account. The full sidecar parses: **370 attempts, 3,209 edges, eight typologies**,
matching 61.9% of the laundering rows (below). Section B of the replay then
reports the RULES' recall per typology, at the replay's full cost;
`--typology-recall` reports the MODEL's from the cached matrix, in minutes.

Three translation decisions: self-transfers are dropped (12% of the file, mostly
`Reinvestment`); amounts are scaled per currency, each by its own median; and
payment format is not filtered on - laundering concentrates in ACH, so filtering
would select rows by the label.

### Result (4,487,133 rows, 5,166 laundering, full file)

| rule | on laundering | on legit | lift |
|---|---|---|---|
| `NEW_PAYEE_HIGH_AMOUNT` | 17.15% | 1.84% | **9.3x** |
| `AMOUNT_DEVIATION` | 6.08% | 0.88% | **6.9x** |
| `MULE_FAN_IN` | 3.17% | 1.12% | **2.8x** |
| `STRUCTURING` | 5.59% | 4.36% | 1.3x |
| `DISTINCT_PAYEE_BURST` | 11.61% | 9.48% | 1.2x |
| `VELOCITY` | 11.79% | 9.68% | 1.2x |
| `DAILY_LIMIT_BREACH` | 18.00% | 19.23% | 0.9x |

1. **`MULE_FAN_IN` separates the classes 2.8:1** on a clock three orders of
   magnitude finer than AMLSim's: the inversion there was AMLSim's timescale, not
   the rule. It is conservative - the rule sees a fraction of each 87-hour pattern
   - and the first external evidence for the pattern receiver-side aggregation
   rests on.
2. **The relational features transfer more strongly than on PaySim**:
   `NEW_PAYEE_HIGH_AMOUNT` 9.3x against 4.0x.
3. **The thresholds do not transfer.**
   The decision layer flags 39.1% of laundering - and **22.18% of legitimate
   traffic**. `VELOCITY`, `DISTINCT_PAYEE_BURST` and `DAILY_LIMIT_BREACH` count
   events against retail limits, and this file contains banks and corporates (a
   sender-day reaches 26,365 transactions).

The replay took 5.5 hours: the extractor makes five linear passes over each
sender's 24-hour history per event, free on retail traffic and quadratic on hubs
- a scalability boundary only foreign data could expose (`docs/irp-framing.md` 8,
twentieth). Hub accounts were not dropped to speed it up: they are almost all
legitimate, so dropping them would inflate every lift.

### Result (`--our-model`): this project's model on the published split

`train.py`'s hyperparameters on the features the deployed extractor computes,
scored as the IBM benchmark scores: the earliest 60% train, the next 20%
validate, the last 20% test (897,427 rows, 1,653 laundering), minority-class F1,
ten seeds.

| configuration | F1 %, threshold from validation | F1 % at 0.5 | PR-AUC | ROC-AUC |
|---|---|---|---|---|
| this project, 14 features | 17.5 +/- 3.9 | 6.5 | 0.065 | 0.927 |
| - without receiver aggregation | 15.4 +/- 3.5 | 4.7 | 0.057 | 0.917 |
| - plus the file's payment format and currency | **35.7 +/- 4.1** | 18.7 | 0.200 | 0.931 |
| *any of the three, class-weighted* | *1.5 - 2.2* | *1.5 - 2.1* | *0.009 at most* | |

Published on the same split (arXiv:2402.08593, Table 4): gradient boosting on the
file's own columns 21.3 +/- 0.3 (LightGBM) and 19.8 +/- 0.9 (XGBoost), GIN
28.7 +/- 1.1, GIN+EU 47.7 +/- 7.9, PNA 56.8 +/- 2.4, and gradient boosting with
graph features 62.9 +/- 0.3 and 64.8 +/- 0.5.

With the file's own columns this feature set sits between the tabular baselines
and the graph methods - though at a fixed 0.5 threshold it scores 18.7, and the
paper does not state its threshold rule. Class weighting collapses at this 0.1%
base rate, as on PaySim; `train.py` now fits unweighted below 0.5% fraud.

```bash
python ibm_aml_adapter.py --file HI-Small_Trans.csv --extract-only --cache ibm_features21.npz
python ibm_aml_adapter.py --file HI-Small_Trans.csv --our-model --cache ibm_features21.npz --seeds 10
```

### Receiver aggregation, asked so that seed noise cannot answer it (`--receiver-ablation`)

Ten seeds left the receiver-side delta inside the model's own spread (2.1 F1
points, CI [-1.5, +5.6]), so it was asked again with twenty seeds and a second
method - the seed-averaged model, with a paired bootstrap over the test rows -
under a rule fixed before the run: established only if both F1 intervals on this
project's own columns - fourteen of them at the time, eighteen now - exclude
zero.

| configuration | per seed, paired (20 seeds) | seed-averaged model, paired bootstrap |
|---|---|---|
| 14 features - F1 points | **+2.46 [+0.52, +4.40]**, 14 of 20 positive | **22.33 vs 20.00 = +2.33 [+1.05, +3.52]** |
| 14 features - PR-AUC | +0.012 [+0.005, +0.019] | 0.153 vs 0.099 = +0.053 [+0.042, +0.064] |
| plus format and currency - F1 points | -0.76 [-4.27, +2.75], 9 of 20 positive | 53.02 vs 49.77 = +3.25 [+1.73, +4.69] |
| plus format and currency - PR-AUC | +0.003 [-0.023, +0.028] | 0.463 vs 0.423 = +0.040 [+0.026, +0.055] |

**Established**, with caveats: it is a second look (the twenty seeds include the
ten), the two methods disagree once the file's own columns are added, and the
bootstrap measures only the test set's uncertainty. On PaySim, with no collection
stage, the same removal helped - the sign turns where the design says it should.
Averaging twenty fits also more than doubles PR-AUC (0.153 against 0.065).

```bash
python ibm_aml_adapter.py --file HI-Small_Trans.csv --receiver-ablation --cache ibm_features21.npz --seeds 20 --boots 1000
```

### Result (`--typology-recall`): which laundering patterns the model catches

The sidecar names 370 injected attempts, 3,209 edges, in eight typologies, and
matches **3,198 of the 5,166 laundering rows (61.9%)** - the 11 edges matching no
row are self-transfers, which the adapter drops. Recall per typology on the
published test split, the recipe fitted unweighted, three seeds:

| typology | in test | recall | across seeds |
|---|---|---|---|
| fan-in | 127 | **51.4%** | 47.2-54.3% |
| stack | 122 | 43.2% | 41.8-45.1% |
| random | 84 | 40.5% | 40.5-40.5% |
| gather-scatter | 378 | 39.5% | 36.8-43.4% |
| cycle | 99 | 37.0% | 36.4-38.4% |
| scatter-gather | 242 | 32.1% | 31.0-34.3% |
| bipartite | 67 | 30.8% | 26.9-34.3% |
| fan-out | 133 | 30.6% | 24.8-36.8% |
| **(unnamed)** | 401 | **5.8%** | 5.2-7.0% |
| all laundering | 1,653 | 30.3% | 29.2-31.5% |

1. **Fan-in is the best-detected typology**, at 51.4% against 30.3% overall, on a
   dataset this project did not write and with the patterns named by whoever
   injected them. It is external evidence for the shape receiver-side aggregation
   was built for (`ml/README.md`, "Fan-in"), from labels chosen independently of
   this project - where the ablation on IBM AML could only say the capability is
   worth something to the model, this says which pattern it is worth it on.
2. **The 38% of laundering rows the sidecar does not name are a different
   population, and nearly invisible at 5.8%.** 95.7% of them are the only
   laundering row at their receiver, against 59.4% of the named ones, and 99.4%
   have a receiver that appears in no named attempt at all. There is no relation
   to see, so a relational model sees nothing. The 30.3% overall is a mix of the
   two: quoting it alone understates the model on patterns and overstates it on
   isolated transfers.
3. The order is not a difficulty ranking for laundering in general - it is this
   generator's patterns, at this file's scale, under a one-hour receiver window
   that sees a fraction of each (median collection span by typology: 35.8 hours
   for fan-out, 86.1 for fan-in, 90.6 for gather-scatter).

The mode joins the sidecar's labels onto the matrix `--extract-only` cached, so
it costs minutes instead of the replay's hours, and it refuses to print if that
cache is not row-aligned with the file - a misaligned join would hang a typology
on the wrong transaction and still produce a table. Since 2026-09-21 it also
refuses a cache whose column names are not the ones the extractor computes now:
the 14-column and 18-column caches hold the same 4,487,133 rows, so alignment
alone cannot tell them apart. The fit is not bit-stable
(LightGBM's threaded histograms), so each row moves a few tenths of a point
between runs: one decimal is all these figures carry.

### Re-run 2026-09-21: the current extractor, 18 of its 21 columns

Running the whole file through the deployed extractor took 2 h 05; the fits are
ten seeds each, on the same 60/20/20 temporal split as above. Three features are
absent for the same reason as on PaySim - the file carries no call state, no
region and no session timing - and **all five counterparty counters compute**.

| configuration | feat | F1 %, cut from validation | F1 % at 0.5 | PR-AUC | ROC-AUC |
|---|---|---|---|---|---|
| this project, all it can compute | 18 | 22.8 +/- 4.2 | 19.2 | 0.099 | 0.884 |
| - without receiver aggregation | 16 | 19.3 +/- 7.1 | 17.2 | 0.083 | 0.892 |
| - without the counterparty counters | 13 | 18.5 +/- 4.3 | 6.8 | 0.073 | **0.938** |
| - plus the file's payment format and currency | 25 | **42.4 +/- 2.7** | 40.2 | **0.332** | 0.930 |
| *all four, class-weighted* | | *1.9 - 3.3* | *1.8 - 3.2* | *0.013 at most* | |

**1. The counters are the one thing this file separates from zero - narrowly.**
Paired by seed, the full set minus the set without:

| removed | F1 % | PR-AUC |
|---|---|---|
| the counterparty counters | **+4.30 +/- 4.00** | **+0.026 +/- 0.019** |
| receiver aggregation | +3.52 +/- 7.04, includes zero | +0.016 +/- 0.029, includes zero |

This is the first evidence for the counters from outside this project, and it
comes from the one file here with a collection stage on a minute clock - the shape
they were built for, and the reason the file was brought back. By term 1 it
decides nothing: the capability was adopted on this project's data and PaySim
before this run, and IBM AML only reports. What it adds is that the columns keep
working where the collection is somebody else's simulation rather than ours, on a
file whose accounts are not the ones this project serves. Both intervals clear
zero by about a tenth of their own width, so *positive and small* is the whole of
the claim.

**2. The feature set reads this file better than it did, and is now level with the
published tabular baselines rather than under them.** On its own columns the mean
moves 17.5 -> 22.8 F1 and PR-AUC 0.065 -> 0.099; with the file's own payment
format and currency added, 35.7 -> 42.4 and 0.200 -> 0.332. The published LightGBM
on this split scores 21.3 +/- 0.3 and XGBoost 19.8 +/- 0.9, so 22.8 +/- 4.2 is
**level with them, not above**: the spread here is fourteen times theirs, because
these columns are computed from a stream of events rather than read off the row.
With the file's own columns added, 42.4 sits between GIN (28.7) and GIN+EU
(47.7 +/- 7.9) and still far below the graph methods (PNA 56.8, GFP + gradient
boosting 62.9-64.8).

**3. ROC-AUC points the other way, and that is the useful part.** Dropping the
counters *raises* ROC-AUC to 0.938, the best figure in the table, while PR-AUC
falls to 0.073 and F1 at a fixed 0.5 cut collapses 19.2 -> 6.8. At a 0.115% base
rate ROC-AUC is decided by how the 99.9% of legitimate rows are ordered among
themselves, which no analyst ever sees; the top of the ranking, which is all a
queue holds, got worse. Same file, same fits, opposite verdicts - which is why
this project reads PR-AUC and recall at a fixed alert budget (section 1) and
quotes ROC-AUC only beside them.

**4. Class weighting still collapses** at this base rate, as on PaySim: 1.9-3.3 F1
against 18.5-42.4 unweighted. `train.py`'s unweighted rule below 0.5% fraud now
holds on a third dataset.

The model here is **fitted on IBM AML**, not the served one: it measures the
feature set, not the deployed system. What the deployed rules make of the file is
the section below.

**The rules replay reproduces the historical run to the digit.** Every per-rule
lift above is unchanged, and so is the decision layer: 39.1% of laundering flagged,
**22.18% of legitimate traffic**. Nine rules became seven since that run -
`FRESH_RECEIVER` and `DEVICE_CHANGE` left with receiver age and device telemetry -
and the three attempts to turn the counterparty counters into a rule all failed and
were removed (`ml/README.md`). On this file none of that could move a number: the
two departed rules need data IBM AML does not carry, so they never fired here, and
the counters added no rule to replace them. A capability adopted, two deleted and
three rule attempts abandoned, and the rule layer's reading of 4.5 million foreign
rows did not change by a digit.

It took **2 h 46**, against the 5.5 hours recorded above for the 14-column run, on
four more columns and with the model fits sharing the machine for the first hour.
Not a controlled comparison - too much moved - but the direction is the one step 4
of the mentor plan asked about, and the single-pass counterparty count (`f7a6a03`,
written because a hub account holds tens of thousands of them) is the only change
aimed at it.

**New: which patterns the RULES catch** - section B of the replay, the fifth of the
five next steps sent to the mentor on 2026-09-16, and the only one that needed a
full replay to answer:

| typology | rows | flagged | rules | *model, test slice* |
|---|---|---|---|---|
| fan-in | 318 | 125 | **39.3%** | ***58.6%*** |
| bipartite | 263 | 97 | 36.9% | *18.4%* |
| scatter-gather | 626 | 230 | 36.7% | *39.2%* |
| stack | 466 | 164 | 35.2% | *20.5%* |
| cycle | 287 | 95 | 33.1% | *20.5%* |
| random | 191 | 63 | 33.0% | *20.5%* |
| gather-scatter | 705 | 210 | 29.8% | *45.1%* |
| fan-out | 342 | 92 | 26.9% | *37.3%* |
| all 3,198 named rows | | 1,076 | 33.6% | |
| the 1,968 rows the sidecar does not name | | 946 | **48.1%** | ***1.9%*** |

The model column is the re-run below, on the last 20% of the file; the rules run on
all of it, so the two columns are not a race - see point 2.

**1. Fan-in is the best-caught typology for the rules too**, as it is for the model.
Two detectors built a year apart agreeing on which shape is easiest, on labels this
project did not write, is the strongest form the fan-in evidence takes here. Read it
with the spread in view: 26.9% to 39.3% against a 22.18% false-alarm rate is a lift
of 1.2x to 1.8x. **The rules rank the typologies roughly as the model does and
separate them far less.**

**2. The two layers fail differently, and the unnamed rows show it.** The 1,968
laundering rows the sidecar does not name are the model's worst population (1.9%)
and the rules' best (48.1%). That is each layer's own ordering, not a comparison:
the rules reach 48.1% by flagging 22.18% of everything, the model reaches 1.9%
while flagging 0.29%, and by lift the model is ahead on every row of this table.
What it says is *why* they fail differently. 95.7% of these rows are the only
laundering row at their receiver, so a relational model has nothing to look at,
while one large transfer to an unfamiliar payee needs no relation at all - which is
`NEW_PAYEE_HIGH_AMOUNT`, at 9.3x the best-separating rule on the file. This is the
clearest external support for keeping both layers rather than letting the model
subsume the rules (`docs/irp-framing.md`).

**3. The thresholds still do not transfer, for the reason already recorded.** 22.18%
of legitimate traffic is not a false-alarm rate this project would ship; it is what
retail limits do to a file where a sender-day reaches 26,365 transactions. That half
of the file stays unreadable, exactly as term 3 warned - and the half it was brought
back for reads clearly.

### Re-run 2026-09-21 (`--typology-recall`): the model, ten seeds, 18 columns

| typology | in test | recall | across seeds | *was, 14 columns / 3 seeds* |
|---|---|---|---|---|
| fan-in | 127 | **58.6%** | 40.9-70.9% | *51.4%* |
| gather-scatter | 378 | 45.1% | 29.9-56.9% | *39.5%* |
| scatter-gather | 242 | 39.2% | 24.4-47.1% | *32.1%* |
| fan-out | 133 | 37.3% | 27.8-50.4% | *30.6%* |
| cycle | 99 | 20.5% | 11.1-32.3% | *37.0%* |
| random | 84 | 20.5% | 11.9-25.0% | *40.5%* |
| stack | 122 | 20.5% | 10.7-27.9% | *43.2%* |
| bipartite | 67 | 18.4% | 10.4-23.9% | *30.8%* |
| **(unnamed)** | 401 | **1.9%** | 0.2-4.2% | *5.8%* |
| all laundering | 1,653 | 28.5% | 18.6-36.0% | *30.3%* |

**The total barely moved (30.3% -> 28.5%) and the composition moved decisively.**
The four typologies that rose - fan-in, gather-scatter, scatter-gather, fan-out -
are the four whose shape *is* a count of counterparties. The four that fell - cycle,
stack, random, bipartite - are chains and pairs, where that count stays small. That
is what the counters were built to do, and it matches the ablation above, but it is
not proof: three changes landed together, since `cross_network` left with the
network capability, the five counters arrived, and the seed count went from three to
ten.

**The wide ranges are the honest part of this table.** The F1 cut taken from
validation lands anywhere between 1,364 and 4,980 alerts depending on the seed, so
fan-in reads 40.9% on one fit and 70.9% on another. The three-seed ranges in the
historical table above (fan-in 47.2-54.3%) were narrow because three fits were
drawn, not because the estimate was steady. Only the ordering survives ten seeds;
the levels should be quoted with their ranges or not at all.

### Re-run 2026-09-21 (`--budgets`): recall at a fixed alert budget

The owner's target for this file was to catch more than half of its laundering, and
neither table above answers that: the F1 cut lands on a different queue every seed
(1,364 alerts to 4,980), and a catch rate without a queue length behind it is not a
number anyone can act on. This is the reading section 1 already uses for PaySim -
review the most suspicious x% of transfers, count the laundering inside - so the two
datasets can be put side by side. Five seeds, unweighted, this project's 18 columns.

| budget | alerts | of all laundering | across seeds | of the named patterns |
|---|---|---|---|---|
| 0.5% | 4,487 | 35.3% | 27.0-43.6% | 44.6% |
| 1% | 8,974 | 45.1% | 35.5-51.8% | **53.7%** |
| 2% | 17,949 | **57.3%** | 44.0-66.7% | 63.8% |
| 5% | 44,871 | 64.8% | 48.3-72.8% | 69.1% |

**1. The target is met, at a stated price.** 57.3% of all laundering inside a 2%
budget, and the named patterns cross half already at 1%. At the same 2% budget
PaySim reads 53.4% (section 1), so the feature set is not weaker on this file - it
was being read at the short end of the scale. The 28.5% above is the same model at
a ~0.3% queue.

**2. Fan-in crosses first and by the widest margin.** Recall per typology inside the
same budgets:

| typology | rows | at 1% | at 2% |
|---|---|---|---|
| fan-in | 127 | **69.0%** | **74.5%** |
| gather-scatter | 378 | 61.1% | 69.1% |
| fan-out | 133 | 52.8% | 63.5% |
| scatter-gather | 242 | 51.9% | 60.2% |
| stack | 122 | 48.0% | 60.8% |
| random | 84 | 44.0% | 61.2% |
| bipartite | 67 | 40.9% | 58.8% |
| cycle | 99 | 35.4% | 49.1% |

Every typology but `cycle` is over half at a 2% budget, and the collection shapes -
fan-in and gather-scatter - lead at every budget, which is the ordering both layers
already agreed on.

**3. Screening the hubs out changes almost nothing, which is the finding.** The file
names no account type, so "individuals" cannot be selected from it; what can be
applied is the screen `ml/README.md` used before - neither side over twenty distinct
counterparties, here over the week the counters cover. It keeps **804,725 of 897,427
rows and 1,531 of the 1,653 laundering rows** (89.7% and 92.6%, within a point of the
89.8%/92.7% that screen kept with the columns of the time). With the queue drawn from
those rows only, fan-in reads **68.8% at 1% and 74.6% at 2%** against 69.0% and 74.5%
on everything - a fifth of a point. Overall recall moves more (62.3% against 57.3% at
2%, 79.7% against 64.8% at 5%), because the hubs it removes are where the false
alarms sit, not where the missed laundering is. **The companies are not what holds
the number down** - the same conclusion `f672c10` reached from the other direction.

**4. The file's own columns are worth more than anything this project can add.**
With payment format and currency the same table reads 57.0% at a 0.5% budget and
fan-in 83.6% at 1%. A bank has those columns; this project's extractor does not, and
no feature built from a P2P stream closes that gap.

**Not an operating point.** 2% of this test slice is 17,949 alerts. The slice is the
last fifth of the file's rows, and IBM AML's traffic is front-loaded: 97% of those
rows fall inside two days, and a sparse tail of a few hundred runs on to ten. The
deployed system runs at 131 alerts per 100,000 transfers, which is 0.13% - fifteen
times fewer. These budgets exist to make two datasets comparable, not to propose a
queue.

### Verdict under the terms

**IBM AML stays, for information.** Term 3 asked whether hub accounts drown the
collection signal, and they do not: the counters' interval is the only one in the
model table that clears zero, fan-in comes first in both layers, and both
per-typology tables are legible. Term 2 asked that the answer be recorded whatever
it said; it happened to be favourable, and the unfavourable halves - level with the
tabular baselines rather than above them, intervals that clear zero only narrowly,
seed ranges thirty points wide, 22.18% of legitimate traffic flagged, the graph
methods still far ahead - are in the same tables. Term 1 stands: nothing here moved
a decision. Every capability in `capabilities.py` was settled on this project's own
data and PaySim before this run, and none was reopened after it.

---

## Removed: IBM AMLSim

IBM's open-source laundering simulator, run here from 2026-08-31 and **removed on
2026-09-19 at the owner's decision**, with IBM AML: this project is about card
transfers between people. What it showed stays where it was used: the fixed
six-sender `MULE_FAN_IN` fired on 3.12% of its legitimate traffic and caught none
of its fan-in typology, which is why the relative mode exists
(`docs/irp-framing.md` §6, third RQ3 result). The adapter, its Docker toolchain
and the full results are in git history (`git show 71c3cdc:validation/README.md`,
section 3).

The lesson it left holds for every dataset here - this generator, PaySim, Zenodo,
AMLSim: each placed its positive class where the method of placement is itself the
strongest predictor. **The first step in using a foreign dataset is a screen for
it, not the last.**

---

## 3. Mendeley ktbthg777x - examined and NOT used

*"Synthetic Banking Transaction Dataset with Multi-Pattern Fraud Labels for
Machine Learning Research"*, [doi 10.17632/ktbthg777x.1](https://data.mendeley.com/datasets/ktbthg777x/1),
CC BY 4.0: 1,000,000 rows, and the only public dataset found with `device_id` and
coordinates. It does not survive contact.

**1. The coordinates are random, and the release says so.** Labelled
`geo_anomaly` rows sit a median 9,880 km from the customer's previous transaction,
legitimate ones 9,995 km - the median distance between two random points on a
globe.

**2. The history is too sparse**: ten transactions per customer per year, so the
windows are empty and 14 of 20 features degenerate.

**3. The "laundering ring" has no collection stage**: 80 transfers to 80 distinct
receivers, each receiving once.

Nothing quotable survives; recorded so the next reader does not spend the same
day on it.
