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

So only one half is validated here: PaySim tests the sender-side relational
features. The receiver side (fan-in) has no usable public source - the ones
tried are recorded below.

---

## 1. PaySim - do the relational features work on foreign data?

`paysim_adapter.py`. An agent-based mobile-money generator (Lopez-Rojas et al.)
built for another market, with **identifiers on both sides**, so this project's
own extractor and CEP rules run on it unchanged - `rules.evaluate()` as deployed,
nothing retrained or tuned. Capabilities PaySim cannot support (geo,
session, kinship) are switched **off**, never defaulted, so no rule
fires on a fabricated zero.

It is not trained on, for three reasons: fitting to the data under test answers
a circular question; five of the remaining features need data PaySim lacks, so a
model on what is left would not be the served one; and the rail and the units
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

## Rejected: IBM AML (HI-Small)

*IBM Transactions for Anti-Money Laundering* (Altman et al., NeurIPS 2023): 5.08M
synthetic transfers on a one-minute clock, with a collection stage. Used from
2026-09-08 and **removed on 2026-09-19 at the owner's decision**: its accounts
include banks and companies, and this project is about card transfers between
people. The decisions made with it stay recorded where they were made
(`ml/README.md`); the adapter and its results are in git history
(`git show 8f46624:validation/README.md`, section 4, and
`git show 8f46624:validation/ibm_aml_adapter.py`).

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

Everything below the composition is **historical**, measured before 2026-09-19 with
the feature set of the time (14 computable columns, no counterparty counters). The
re-run on the current 21-column extractor is reported under it when it finishes.

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
python ibm_aml_adapter.py --file HI-Small_Trans.csv --patterns HI-Small_Patterns.txt --typology-recall
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
python ibm_aml_adapter.py --file HI-Small_Trans.csv --extract-only --cache ibm_features.npz
python ibm_aml_adapter.py --file HI-Small_Trans.csv --our-model --cache ibm_features.npz --seeds 10
```

### Receiver aggregation, asked so that seed noise cannot answer it (`--receiver-ablation`)

Ten seeds left the receiver-side delta inside the model's own spread (2.1 F1
points, CI [-1.5, +5.6]), so it was asked again with twenty seeds and a second
method - the seed-averaged model, with a paired bootstrap over the test rows -
under a rule fixed before the run: established only if both F1 intervals on the
fourteen features exclude zero.

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
python ibm_aml_adapter.py --file HI-Small_Trans.csv --receiver-ablation --cache ibm_features.npz --seeds 20 --boots 1000
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
on the wrong transaction and still produce a table. The fit is not bit-stable
(LightGBM's threaded histograms), so each row moves a few tenths of a point
between runs: one decimal is all these figures carry.

---

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
