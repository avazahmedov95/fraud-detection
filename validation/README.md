# External validation

One dataset used, and more examined and rejected. The adapter owns the file's
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
