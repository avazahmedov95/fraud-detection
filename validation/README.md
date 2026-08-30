# External validation

Two datasets answering two different questions. Neither answers both, and the
distinction is the point.

## The constraint that shapes everything here

14 of this project's 24 features are **relational** — computed over the history
of a named sender and a named receiver: velocity, new-payee, amount deviation
against a personal baseline, fan-in concentration.

Measured cost of losing them (seed 42, held-out slice):

| available | PR-AUC | precision | recall |
|---|---|---|---|
| full system | 0.959 | 0.860 | 0.902 |
| no account identifiers | 0.812 | 0.624 | 0.797 |
| amount + hour only | 0.678 | 0.218 | 0.886 |

Public transaction datasets do not carry account identifiers, because those are
precisely what cannot be published. So:

> **Relational fraud detection cannot be validated end-to-end on public real
> data, because the identifiers that make it relational are the reason such data
> stays private.**

That is a structural result about the field, not a shortcoming of this project,
and it is worth stating in the thesis as one. The response is to validate the
two halves separately against the best available source for each.

---

## 1. PaySim — do the relational features work on foreign data?

`paysim_adapter.py`

**The risk it addresses.** Every result so far comes from a generator written by
the same person as the detector. The obvious objection is that the features
detect fraud because the generator put it there in a detectable form.

**Why PaySim.** Agent-based mobile-money generator by Lopez-Rojas et al., built
for a different market with no knowledge of this system, and the only public
dataset carrying **identifiers on both sides** — so this project's actual feature
extractor and CEP rules can run on it unchanged.

**What is run.** `rules.evaluate()` exactly as deployed. No retraining, no
threshold tuning. Capabilities PaySim cannot support (device, geo, session,
channel, receiver age, kinship) are switched **off** rather than defaulted, so no
rule can fire on a fabricated zero — enforced by a test.

**Get it.** Kaggle, "Synthetic Financial Datasets For Fraud Detection" (~470 MB).

```bash
cd validation
python paysim_adapter.py --file PS_20174392719_1491204439457_log.csv --limit 500000
```

`TRANSFER` is the P2P analogue and the default. Start with `--limit` — the full
file is 6.3M rows.

### Result (500,000 TRANSFER rows, 2,520 fraud)

**Per-rule lift — threshold-free, and the measure that answers the question:**

| rule | on fraud | on legit | lift |
|---|---|---|---|
| `NEW_PAYEE_HIGH_AMOUNT` | 5.95% | 1.49% | **4.0x** |
| `MULE_FAN_IN` | 0.00% | 0.07% | 0.0x |

**The relational feature transfers.** `NEW_PAYEE_HIGH_AMOUNT` is computed
entirely from per-sender history — the machinery that cannot exist without
account identifiers — and it separates the classes 4:1 on a dataset this project
did not produce. That is the non-circular evidence the exercise was for.

**`MULE_FAN_IN` finds nothing, and should not.** PaySim models fraud as draining
one account straight to cash-out; there is no collection stage where many
senders converge on a drop account. The rule has nothing to detect. This is a
difference between fraud *phenomena*, not a rule failure — and it is itself
worth reporting: **fraud patterns are market-specific**, which is the premise of
building an Uzbekistan-specific system rather than importing a generic one.

### The threshold finding, and its fix

Before capability-scaled thresholds, this run flagged **0 of 2,520 fraud**. The
highest score any fraud reached was 0.35, against a review cutoff of 0.40 — while
the rules separated the classes 4:1. The score is additive, so a fixed cutoff
encodes *how many rules must agree*; with most capabilities absent they never do.

With scaling (`SCALE_THRESHOLDS_BY_CAPABILITY`, on by default), the profile's
weakest reachable pattern is 0.30 rather than 0.70, so REVIEW moves 0.40 → 0.17:

| | fixed threshold | scaled threshold |
|---|---|---|
| fraud flagged | 0 / 2,520 (0.0%) | **150 / 2,520 (6.0%)** |
| legit flagged | 43 (0.01%) | 7,724 (1.55%) |
| decision lift | — | **3.9x** |

**Read this honestly: 6% recall is a weak detector.** The decision-layer lift
(3.9x) simply reproduces the single available rule's lift (4.0x), because one
rule is all that fires. Scaling converted a *silent* layer into a *working but
thin* one — it restored sensitivity, it did not manufacture signal. A profile
that cannot observe a pattern still cannot detect it.

**Limitation, stated up front.** PaySim timestamps are hourly (`step`), so every
transaction in an hour shares a clock reading. The 10-minute velocity window and
the 1-hour window see nearly the same set, and `secs_since_last` is 0 within a
step. Sub-hour patterns are invisible here. That is a property of PaySim, not of
the rules, and it makes this a **conservative** test.

---

## 2. Zenodo 20030065 — examined and NOT used as claimed

`zenodo_calibration.py`, `zenodo_provenance.py`

DOI [10.5281/zenodo.20030065](https://doi.org/10.5281/zenodo.20030065),
published as *"A Production-Collected Online Banking Fraud Detection Dataset
from a Live Cloud-Based Deep Learning System"*.

**It should not be cited as production data.** Checked, because a citation that
collapses under a reviewer's question is worse than no citation.

| Check | Result |
|---|---|
| rows | 57,394 in the file vs **56,962** claimed |
| fraud | 111 in the file vs **98** claimed |
| response latency (promised per record) | **absent** |
| `v7..v28` | max pairwise correlation 0.10, medians 0.0003, σ ≈ 1.17 → **PCA components** |
| `v1..v6` | σ ≈ 110,000, one pair correlated **0.9996** → balance-like, not PCA |
| `v2` vs `amount` | identical maximum (659,035.26), correlation 0.93 |

The arithmetic also matches a 1/5 sample of the ULB credit-card dataset to
within rounding: 284,807/5 = 56,961.4 against 56,962 claimed; 492/5 = 98.4
against 98 claimed; ULB's fraud rate 0.1727% against 0.172% claimed.

**Three consequences.**

1. `v7..v28` being PCA components imports the exact objection that ruled out the
   ULB/Kaggle set — features with no meaning cannot carry SHAP explanations, and
   CBU 3759 requires an explainable decision.
2. `v1..v6` look like balance columns, one pair correlated 0.9996 — the shape of
   *before/after balance* pairs, which is **PaySim's documented leakage
   problem**. A model trained here would score near-perfectly for no reason.
3. It cannot be described as independent real-world corroboration if it is
   assembled from a dataset already in wide circulation.

**What survives.** The fraud base rate does not depend on what the features
mean, and ~0.17–0.19% is a real figure from real card traffic. So the finding is
kept and **cited to the ULB dataset directly** rather than through this record.

```bash
python zenodo_provenance.py --file fraud_tests_export_20260501_080333.csv
```

Kept in the repository because the investigation is itself a result — see
`docs/irp-framing.md`.

---

## Rejected: IEEE-CIS

590k real transactions with pseudo card identifiers, so some relational features
would be computable. Rejected because it is card-not-present e-commerce: **there
is no receiver as a party**, so the strongest finding in this project — fan-in
concentration at a payee, worth −0.032 PR-AUC — cannot be tested at all. Also
rejected: the Kaggle credit-card set, PCA-anonymised into V1..V28, which makes
the SHAP explanations required under CBU 3759 meaningless.

---

## Tests

```bash
cd validation && python -m pytest -q
```

Fixtures in the shape of both real files, so the harnesses are known to work
before anything is downloaded. They test the **adapters** — the mapping from a
foreign schema onto this project's event contract — not the detection outcome,
which is what the real run is for.
