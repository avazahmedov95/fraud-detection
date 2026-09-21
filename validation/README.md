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

On the realistic profile the full system scores PR-AUC 0.673 (`ml/README.md`);
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
**Re-run 2026-09-21** with the L2 penalty the served recipe adopted that day
(`ml/README.md`); the figures it replaced are in the paragraph below the table.

| configuration | feat | PR-AUC | ROC-AUC | rec@2% | lift |
|---|---|---|---|---|---|
| **fitted with class weighting** (scale_pos_weight ≈ 774) | | | | | |
| this project, all it can compute | 18 | 0.030 | 0.754 | 18.7% | 3.0× |
| — without receiver aggregation | 16 | 0.035 | 0.783 | 24.2% | 4.9× |
| — plus PaySim's own transaction type | 23 | 0.065 | 0.896 | 21.7% | 5.5× |
| **fitted unweighted**, as the published baseline was | | | | | |
| this project, all it can compute | 18 | **0.364** | 0.884 | 48.1% | 7.1× |
| — without receiver aggregation | 16 | **0.365** | 0.883 | 48.4% | 7.1× |
| — plus PaySim's own transaction type | 23 | **0.529** | 0.957 | 58.8% | 8.6× |
| published generic baseline (§6 of `related-work.md`) | ~24 | 0.380 | 0.908 | 49.4% | 7.0× |

1. **The recipe, not the features, produced the alarming number**: same features
   and split, **0.030 weighted against 0.364 unweighted** - heavy positive
   weighting flattens the top of the ranking AUPRC reads. `train.py` fits
   unweighted below 0.5% fraud.
2. **Receiver aggregation is neutral on PaySim**: removing the one-hour fan-in pair
   moves PR-AUC by 0.001, where on this project's data it costs the most (before
   the penalty the removal *improved* PR-AUC by 0.036). PaySim drains one account
   straight to cash-out with no collection stage, so no run on PaySim can
   validate that finding; IBM AML is where it is read (section 4).
3. **With matching recipes the feature set beats the published model**: 0.529
   against 0.380, 58.8% against 49.4% of the fraud inside a 2% alert budget, and
   8.6× against 7.0× lift - once PaySim's own transaction type is added to both.
   On this project's 18 columns alone it is level: 0.364 against 0.380, 48.1%
   against 49.4%. The penalty moved every unweighted row: on 2026-09-20 they read
   0.294, 0.330 and 0.440, and before the counterparty counters 0.267, 0.315 and
   0.373 on 14 columns (`git show 6ed678d`). A caveat the number does not carry:
   this is a model *fitted on PaySim*, not the served one, and the deployed rules
   still flag 6% of its fraud (§1 above).

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

Measured twice that day: in the morning on the recipe then served, and in the
evening on the L2-penalised recipe adopted on this project's own data that day
(`ml/README.md`, Feature importance). The evening figures are the ones below, with
the morning's beside them, because the difference is the largest effect this file
has shown.

| configuration | feat | F1 %, cut from validation | F1 % at 0.5 | PR-AUC | ROC-AUC | *morning: F1 %, PR-AUC* |
|---|---|---|---|---|---|---|
| this project, all it can compute | 18 | **56.7 +/- 0.4** | 38.5 | **0.544** | 0.978 | *22.8 +/- 4.2, 0.099* |
| - without receiver aggregation | 16 | 53.2 +/- 0.4 | 31.7 | 0.509 | 0.977 | *19.3 +/- 7.1, 0.083* |
| - without the counterparty counters | 13 | 36.6 +/- 0.6 | 6.4 | 0.308 | 0.973 | *18.5 +/- 4.3, 0.073* |
| - plus the file's payment format and currency | 25 | **67.0 +/- 0.3** | 53.3 | **0.669** | 0.988 | *42.4 +/- 2.7, 0.332* |
| *all four, class-weighted* | | *4.5 - 5.7* | *2.8 - 4.6* | *0.026 at most* | | |

**1. The penalty changed what this file says about the feature set.** On its own
18 columns the model moves 22.8 -> 56.7 F1 and PR-AUC 0.099 -> 0.544: from level
with the published tabular baselines (LightGBM 21.3, XGBoost 19.8) to level with
PNA (56.8), the stronger graph neural network, and above GIN+EU (47.7). With the
file's own payment format and currency, 67.0 +/- 0.3 is above both GFP + gradient
boosting results (62.9, 64.8) - **on the threshold chosen on validation, a rule the
paper does not state**; at a fixed 0.5 the same model scores 53.3, between GIN+EU
and PNA. So the claim is range, not rank: streaming counts of counterparties, fitted
by a regularised booster, reach the published graph methods on this benchmark. The
morning's unpenalised fits did not show it, because their leaves blew up on the
near-separable majority exactly as on this project's own data, and harder at a
0.115% base rate.

**2. Both relational capabilities clear zero now, the counters by a wide margin.**
Paired by seed, the full set minus the set without:

| removed | F1 % | PR-AUC | *morning* |
|---|---|---|---|
| the counterparty counters | **+20.1 +/- 0.5** | **+0.237 +/- 0.003** | *+4.3 +/- 4.0, +0.026 +/- 0.019* |
| receiver aggregation | **+3.5 +/- 0.5** | **+0.035 +/- 0.004** | *+3.5 +/- 7.0, includes zero* |

The counters are worth twenty F1 points on the one file here with a collection stage
on a minute clock - the shape they were built for, and the reason the file was
brought back - and receiver aggregation, which the morning could not separate from
noise, is established. By term 1 this decides nothing: both capabilities were
settled on this project's data and PaySim before the run. What it adds is that the
columns work where the collection is somebody else's simulation, on accounts this
project does not serve.

**3. The seed lottery is gone.** Ten seeds scatter by 0.3-0.6 F1 points against
2.7-7.1 in the morning, and the cut chosen on validation lands between 891 and 1,062
alerts instead of anywhere from 1,364 to 4,980. The morning's wide intervals were
the unpenalised model, not the file.

**4. ROC-AUC still disagrees with the top of the ranking.** Without the counters it
reads 0.973 against 0.978, nearly level, while PR-AUC falls from 0.544 to 0.308 and
F1 at a fixed 0.5 from 38.5 to 6.4. At a 0.115% base rate ROC-AUC is decided by how
the 99.9% of legitimate rows are ordered among themselves, which no analyst ever
sees - which is why this project reads PR-AUC and recall at a fixed alert budget
(section 1) and quotes ROC-AUC only beside them.

**5. Class weighting still collapses** at this base rate, as on PaySim: 4.5-5.7 F1
against 36.6-67.0 unweighted. `train.py`'s unweighted rule below 0.5% fraud holds on
a third dataset.

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
| fan-in | 318 | 125 | **39.3%** | ***78.7%*** |
| bipartite | 263 | 97 | 36.9% | *26.7%* |
| scatter-gather | 626 | 230 | 36.7% | *62.3%* |
| stack | 466 | 164 | 35.2% | *35.0%* |
| cycle | 287 | 95 | 33.1% | *37.8%* |
| random | 191 | 63 | 33.0% | *34.9%* |
| gather-scatter | 705 | 210 | 29.8% | *72.1%* |
| fan-out | 342 | 92 | 26.9% | *65.4%* |
| all 3,198 named rows | | 1,076 | 33.6% | |
| the 1,968 rows the sidecar does not name | | 946 | **48.1%** | ***1.7%*** |

The model column is the re-run below, on the last 20% of the file; the rules run on
all of it, so the two columns are not a race - see point 2.

**1. Fan-in is the best-caught typology for the rules too**, as it is for the model.
Two detectors built a year apart agreeing on which shape is easiest, on labels this
project did not write, is the strongest form the fan-in evidence takes here. Read it
with the spread in view: 26.9% to 39.3% against a 22.18% false-alarm rate is a lift
of 1.2x to 1.8x. **The rules rank the typologies roughly as the model does and
separate them far less.**

**2. The two layers fail differently, and the unnamed rows show it.** The 1,968
laundering rows the sidecar does not name are the model's worst population (1.7%)
and the rules' best (48.1%). That is each layer's own ordering, not a comparison:
the rules reach 48.1% by flagging 22.18% of everything, the model reaches 1.7%
while flagging 0.11%, and by lift the model is ahead on every row of this table.
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

At the F1 cut from validation - about 950 alerts, 0.11% of the slice, some 78% of
them laundering:

| typology | in test | recall | across seeds | *morning, unpenalised* |
|---|---|---|---|---|
| fan-in | 127 | **78.7%** | 77.2-80.3% | *58.6%* |
| gather-scatter | 378 | 72.1% | 69.3-74.6% | *45.1%* |
| fan-out | 133 | 65.4% | 62.4-67.7% | *37.3%* |
| scatter-gather | 242 | 62.3% | 58.7-65.3% | *39.2%* |
| cycle | 99 | 37.8% | 32.3-44.4% | *20.5%* |
| stack | 122 | 35.0% | 32.8-36.9% | *20.5%* |
| random | 84 | 34.9% | 32.1-38.1% | *20.5%* |
| bipartite | 67 | 26.7% | 25.4-28.4% | *18.4%* |
| **(unnamed)** | 401 | **1.7%** | 1.0-2.2% | *1.9%* |
| all laundering | 1,653 | 45.0% | 43.5-46.9% | *28.5%* |

**Every named typology rose, and the order held.** The four whose shape is a count
of counterparties - fan-in, gather-scatter, fan-out, scatter-gather - lead at
62-79%; the chains and pairs - cycle, stack, random, bipartite - trail at 27-38%,
and three of them still sit below the 14-column historical table above. The
laundering the sidecar does not name stays invisible (1.7%): with no relation to
see, a relational model sees nothing. The ranges are a few points wide, where the
morning's ran to thirty.

### Re-run 2026-09-21 (`--budgets`): recall at a fixed alert budget

The owner's target for this file was to catch more than half of its laundering. A
catch rate means nothing without the queue length that bought it, so this is the
reading section 1 already uses for PaySim - review the most suspicious x% of
transfers, count the laundering inside - and the two datasets can be put side by
side. Five seeds, unweighted, this project's 18 columns.

| budget | alerts | of all laundering | across seeds | of the named patterns | *morning* |
|---|---|---|---|---|---|
| 0.1% | 897 | 43.6% | 43.1-43.9% | 57.1% | *10.8%* |
| 0.5% | 4,487 | 63.1% | 62.8-63.5% | 78.2% | *35.3%* |
| 1% | 8,974 | 69.6% | 69.5-69.8% | **83.6%** | *45.1%* |
| 2% | 17,949 | **79.0%** | 78.8-79.2% | 90.1% | *57.3%* |
| 5% | 44,871 | 85.9% | 85.8-86.2% | 94.4% | *64.8%* |

**1. The target is met at a tenth of the queue it once needed.** Over half of all
laundering is inside a 0.2% budget (53.2%), and 79.0% inside 2%, where the morning's
model held 57.3%. At the same 2% budget PaySim reads 48.1% on this project's
columns and 58.8% with its own transaction type (section 1).

**2. Fan-in first, by the widest margin.** Recall per typology inside the same
budgets:

| typology | rows | at 1% | at 2% |
|---|---|---|---|
| fan-in | 127 | **95.0%** | **97.2%** |
| gather-scatter | 378 | 90.3% | 93.4% |
| fan-out | 133 | 87.8% | 94.0% |
| scatter-gather | 242 | 87.4% | 93.4% |
| cycle | 99 | 70.7% | 84.0% |
| stack | 122 | 70.0% | 76.4% |
| random | 84 | 68.8% | 83.3% |
| bipartite | 67 | 64.8% | 80.0% |

Every typology is over 60% at a 1% budget, and the collection shapes lead at every
budget - the ordering both layers already agreed on.

**3. Screening the hubs out changes little.** The file names no account type, so
"individuals" cannot be selected from it; what can be applied is the screen
`ml/README.md` used before - neither side over twenty distinct counterparties, here
over the week the counters cover. It keeps **804,725 of 897,427 rows and 1,531 of
the 1,653 laundering rows** (89.7% and 92.6%). With the queue drawn from those rows
only, fan-in reads 94.2% at 1% and 96.9% at 2% against 95.0% and 97.2% on
everything; overall recall rises more (84.0% against 79.0% at 2%, 95.5% against
85.9% at 5%), because the hubs the screen removes are where the false alarms sit,
not where the missed laundering is. **The companies are not what holds the number
down** - the same conclusion `f672c10` reached from the other direction.

**4. The file's own columns still add, and less than they did.** With payment format
and currency the same table reads 74.8% at a 0.5% budget and 80.6% at 1%, against
63.1% and 69.6% without them - a gap of eleven points at 1%, where the morning's was
twenty. A bank has those columns; this project's extractor does not.

**Not an operating point.** 2% of this test slice is 17,949 alerts. The slice is the
last fifth of the file's rows, and IBM AML's traffic is front-loaded: 97% of those
rows fall inside two days, and a sparse tail of a few hundred runs on to ten. The
deployed system runs at 186 alerts per 100,000 transfers, which is 0.19% - about
ten times fewer. These budgets exist to make two datasets comparable, not to propose a
queue.

### Verdict under the terms

**IBM AML stays, for information.** Term 3 asked whether hub accounts drown the
collection signal, and they do not: both relational capabilities clear zero, the
counters by twenty F1 points, fan-in comes first in both layers, and every table is
legible. Term 2 asked that the answer be recorded whatever it said, and the
unfavourable halves are in the same tables: the published F1 figures rest on a
threshold rule the paper does not state, so *within the range of the graph methods*
is the claim and not *above them*; the laundering the sidecar does not name stays
invisible to a relational model (1.7%); the retail rules flag 22.18% of legitimate
traffic. Term 1 stands: nothing here moved a decision. The L2 penalty was adopted on
this project's own data before this re-run (`ml/README.md`), and no capability was
reopened after it.

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
