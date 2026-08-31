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
for a different market with no knowledge of this system, carrying **identifiers
on both sides** — so this project's actual feature extractor and CEP rules can
run on it unchanged. Its transactions are consumer-to-consumer money transfers,
which is the closest available analogue to card P2P.

*Corrected 2026-08-31:* an earlier revision called PaySim "the only public
dataset carrying identifiers on both sides". That is wrong. IBM's **AMLSim** and
**AML-Data** carry originator and beneficiary accounts with labels, and BankSim
carries customer→merchant pairs. What is true is narrower and still the reason
PaySim was chosen: it is the closest public analogue *in transaction type*. The
correction matters because one of those alternatives can test the rule PaySim
cannot — see below.

**What is run.** `rules.evaluate()` exactly as deployed. No retraining, no
threshold tuning.

**Why not train on it — the question a reviewer asks first.** Three reasons, in
order of weight. (i) **Training here destroys the thing this directory is for.**
The evidence being sought is that the features detect fraud in data this project
did not produce; fit the model to that data and the test answers a different,
circular question. (ii) **The feature spaces do not align.** 14 of 24 features
are relational and six more need device, geo, session, channel, receiver age or
kinship, none of which PaySim carries. Training on what remains would produce a
model over ~5 features while the Flink job computes 24 — train/serve skew, which
is precisely what the single ordered `FEATURE_NAMES` built from `capabilities.py`
exists to make impossible. (iii) **Different rail, different units.** PaySim is
mobile money at ~1/1000 of UZS amounts; pooling it with generated card P2P
produces a model for neither. The amount rescaling below is a unit conversion
applied so absolute thresholds can fire at all, not a step toward training.

The one legitimate training use of foreign data is not training *this* system:
it is fitting a throwaway model twice on a foreign dataset, with and without a
capability, to see whether an **ablation delta reproduces** off this project's
own generator. That is the shape the AMLSim run below should take. Capabilities PaySim cannot support (device, geo, session,
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

### The gap this leaves, and what closes it

Receiver-side aggregation is this project's **largest measured effect**
(−0.032 PR-AUC) and its one structural design finding. It is also the one thing
here with **no external validation whatsoever**, because the only foreign dataset
run so far cannot express the pattern. Saying "the rule has nothing to detect" is
true and is also the most convenient possible outcome, which is a reason to
distrust it.

**IBM AMLSim** (open source, agent-based, run locally) generates **fan-in** as an
explicit typology — "multiple accounts send substantial funds to a single main
account" — alongside fan-out, scatter-gather and gather-scatter, with originator
and beneficiary accounts and alert labels. It is the dataset that could falsify
the claim rather than confirm one already made, and it is the next external run
this project owes.

Scope it honestly when run: AMLSim's fan-in is an **interbank AML typology**, not
consumer card-to-card, so amounts, cadence and account population all differ.
Like the PaySim exercise, it tests whether the rule's *shape* transfers, not its
thresholds — which is the only kind of transfer test a threshold-carrying rule
can pass on foreign data anyway.

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
| ...but partitioned by `transaction_id` shape | the counts resolve **exactly** — see below |
| response latency (promised per record) | **absent** |
| `v7..v28` | max pairwise correlation 0.10, medians 0.0003, σ ≈ 1.17 → **PCA components** |
| `v1..v6` | σ ≈ 110,000, one pair correlated **0.9996** → balance-like, not PCA |
| `v2` vs `amount` | identical maximum (659,035.26), correlation 0.93 |

The arithmetic also matches a 1/5 sample of the ULB credit-card dataset to
within rounding: 284,807/5 = 56,961.4 against 56,962 claimed; 492/5 = 98.4
against 98 claimed; ULB's fraud rate 0.1727% against 0.172% claimed.

### The count mismatch is contamination, not miscounting

`transaction_id` has three incompatible shapes in one file, and splitting on
them resolves both discrepancies exactly:

| `transaction_id` shape | rows | fraud | `test_date` | timestamps |
|---|---|---|---|---|
| `TXN`+base32 | **56,962** | **98** | set on all | 1–31 Jan 2026 |
| `txn_<epoch>_<n>` | 423 | 4 | none | 10–13 Apr 2026 |
| 8 hex chars | 9 | 9 | none | 9 Apr – 1 May 2026 |

The first block is the described dataset, to the row and to the label:
56,962 and 98, exactly as claimed. 432 further rows carrying 13 of the flagged
frauds were appended **after** publication — no `test_date`, timestamps months
past the dataset's own window, and a different feature schema: in the 423
`txn_` rows 81.6% of the `v*` cells are exactly zero and `v2 == amount` in
100% of them, which is a PaySim-shaped record (amount, old/new balances) poured
into slots named for someone else's PCA components. The 9 hex-id rows are one
canned test record replayed, `amount` 149.99 every time.

Those 432 rows are somebody clicking through a demo UI, captured into a
published research dataset. Two further marks of the same thing: the
`fraud_probability` column carries **two scales at once** — fractions for the
dataset block, percentages for most of the appended rows (51.03 alongside
0.51) — while `risk_level` is banded on the fraction scale, so a percent-scale
row lands in a band by accident. And `ip_address` holds what look like the
testers' real addresses, one of them repeated 425 times.

**This correction matters for how the finding is stated.** Reporting "57,394
against 56,962 claimed" as a bare mismatch implies the publisher miscounted.
They did not. The dataset is exactly what its description says; what fails is
the release, which shipped live-testing rows and third-party IPs alongside it.
The PCA finding below is unaffected and remains the reason the file is not used.

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

## 3. AMLSim — the adapter, and how to run it

`amlsim_adapter.py`. Same contract as the PaySim adapter: the deployed
`rules.evaluate()`, nothing retrained, no threshold tuned.

**What it supports that PaySim did not.** AMLSim's `accounts.csv` carries
`open_dt`, so `receiver_age` is computed from the data instead of being switched
off — one more of the six capabilities PaySim forced off, and the first foreign
run in which `FRESH_RECEIVER` is exercised at all. And `alert_transactions.csv`
labels `fan_in` and `fan_out` **separately**, so the leg asymmetry in
`ml/README.md` can be checked rather than asserted.

```powershell
# once, outside this repo
git clone https://github.com/IBM/AMLSim.git

# the toolchain, in a container - see amlsim.Dockerfile for why
docker build -f amlsim.Dockerfile -t amlsim:1.0 .
docker run --rm -e MAVEN_OPTS="-Xms1g -Xmx4g" `
  -v "C:/path/to/AMLSim:/amlsim" -w /amlsim amlsim:1.0 bash -lc `
  "bash scripts/build_AMLSim.sh && \
   python scripts/transaction_graph_generator.py conf.json && \
   bash scripts/run_AMLSim.sh conf.json && \
   python scripts/convert_logs.py conf.json"

# then, here
python amlsim_adapter.py --dir C:/path/to/AMLSim/outputs/<simulation_name>
```

**Two upstream traps, both handled by that container rather than by editing
AMLSim.** `requirements.txt` pins `networkx==1.11`, and the generator calls
`nx.set_edge_attributes(g, 'active', False)` twice — the 1.x argument order,
reversed in 2.x, so on a modern networkx it is wrong rather than loud. And
`pygraphviz` and `matplotlib` are in that file but imported by none of the
scripts used here; they are the painful Windows dependencies and are skipped on
purpose. Pinning the interpreter instead of patching two lines is what preserves
the reason AMLSim was chosen at all — that a reader reproduces the data by
running the published code unmodified.

**`convert_logs.py` is not optional.** `run_AMLSim.sh` leaves only the raw
simulator log (`tx_log.csv`) plus a counter file; the `schema.json`-shaped
outputs this adapter reads — `accounts.csv`, `transactions.csv`,
`alert_transactions.csv` — are produced by the conversion step, and the shipped
run script does not call it. A run that stops after the simulator looks
successful and yields a directory the adapter cannot read.

**Which profile.** `conf.json` → `"input": {"directory": "paramFiles/100K"}`.
The shipped profiles carry 3 / 30 / 300 / 3000 `fan_in` alerts at 1K / 10K /
100K / 1M, and each alert spans 5–10 accounts. 100K therefore yields roughly
1,500–3,000 fan-in transactions, the same order as the 2,520 fraud rows in the
PaySim run. 10K would give 30 alerts, and this project has already been burned
once by an interval wider than the effect it was measuring — a single test slice
of ~35 mule events had a 30-point interval (`ml/README.md`, "Fan-in").

Raising the alert **count** for statistical power is legitimate. Changing
`min_period`/`max_period` is not: that is the pattern's duration, and altering it
to fit `RECEIVER_WINDOW_S` would be tuning the dataset to the rule. Leave those
columns alone.

`python -m pytest test_adapters.py -q` exercises the adapter against fixtures in
the shape of all three output files, so the harness is known to work before the
simulator is built.

### The window problem, stated before the run rather than after it

AMLSim's clock advances **one day per step**, and its `fan_in` typology spreads
over `min_period..max_period` steps — 5 to 20 in the shipped parameter files.
`RECEIVER_WINDOW_S` is 3600 s. So the deployed window covers at most one
simulated day, while each collection pattern is spread across five to twenty of
them: **the window sees a fraction of every fan-in by construction.**

On the fixture, `MULE_FAN_IN` does not appear in the rule-lift table at all.
That is the predicted behaviour, not a defect, and it sets up the one reading
this run must not fall into: a null fan-in result here is **ambiguous** between
*the rule does not transfer* and *the window is shorter than the pattern*, and
the two carry opposite conclusions. Section D of the report says so on every run.

Widening the window to fit is tuning on the validation set, which is what this
directory exists to avoid. A longer-window run is a legitimate **separate**
experiment and must be reported as one. Note the direction of travel against
PaySim: there, hourly timestamps made the test conservative; here a daily clock
against an hour-long window makes it conservative again, for a different reason.
Neither dataset can flatter the rule by accident.

### Result, 10K profile, 2026-08-31

197,905 transactions, 12,043 accounts, 671 SAR-labelled (0.339%), typologies
`cycle` 291 / `fan_in` 199 / `fan_out` 181.

| rule | on SAR | on legit | lift |
|---|---|---|---|
| `AMOUNT_DEVIATION` | 0.15% | 0.02% | 7.7x |
| `MULE_FAN_IN` | 0.15% | **3.12%** | **0.0x** |

| typology | n | flagged | recall |
|---|---|---|---|
| cycle | 291 | 1 | 0.3% |
| fan_in | **199** | **0** | **0.0%** |
| fan_out | 181 | 1 | 0.6% |

Decision layer: 2 of 671 SAR flagged against 6,185 of 197,234 legitimate —
**lift 0.1x, worse than chance.** Essentially every alert is `MULE_FAN_IN`
firing on legitimate traffic.

**This is not a null result. The rule is anti-correlated with the label**, and
the reason is measurable rather than speculative:

| | all receivers | `fan_in` targets |
|---|---|---|
| distinct senders per receiver **per day**, median | 1 | 1 |
| share above the rule's threshold (>5) | **2.69%** | **1.16%** |
| distinct senders **over the whole run**, median | 1 | 16 |
| p95 | 26 | 154 |

and one figure settles the window question: **the median `fan_in` alert spans
363 days** (min 72, max 616). The 5-20 in `alertPatterns.csv` is the interval
between individual transfers, not the length of the pattern — a misreading
recorded here because it changed the expected answer. `RECEIVER_WINDOW_S` is one
hour. The mismatch is not a factor of a few, it is about 10^4. SAR amounts are
also no larger than legitimate ones (median 575 against 547), so
`AMOUNT_DEVIATION` has nothing to work with either.

**Three findings, and the first is the one worth the chapter.**

1. **`MULE_FAN_IN` detects a *rate*, not a *topology*.** It encodes the
   assumption that collection into a drop account is fast relative to
   background. In an AML typology collection is deliberately *slower* than
   background, so `fan_in` targets cross the threshold **less** often than
   ordinary accounts (1.16% against 2.69%) and the lift inverts. The rule did
   not miss; it reversed.
2. **The threshold of six senders is not portable on its own.** In a scale-free
   transaction graph 2.69% of receiver-days exceed it as ordinary hub
   behaviour. Six was calibrated against a population where that is rare.
3. **Widening the window would not rescue it.** `fan_in` targets have a median
   of 16 distinct senders over the run against 1 for the population - but the
   population's p95 is 26, above the targets' median. An unnormalised count
   separates the classes poorly at *any* window.

**What this does and does not say about the -0.032.** It does not falsify it:
that figure is a *feature* ablation on a model free to learn its own decision
surface, and this is a fixed-threshold rule. It does establish something the
own-generator evaluation could not - **the idea of receiver-side aggregation
transfers; this rule does not.** Portable: the feature `rcv_distinct_senders`.
Not portable: the window and the absolute threshold, both of which encode a
local baseline that was never written down as an assumption.

That is the same distinction the PaySim run reached for the decision threshold
(`SCALE_THRESHOLDS_BY_CAPABILITY`), extended from *which capabilities exist* to
*what the local traffic looks like*. Two foreign datasets, two different
constants, the same lesson: this system's rules carry unstated assumptions
about their deployment population, and each one is invisible until a population
that violates it turns up.

**Operational consequence, stated plainly.** Deployed unchanged against a
population with this tempo, the CEP layer would alert on 3.14% of legitimate
traffic and catch 0.3% of the fraud. The capability-scaled thresholds handle a
missing data source; nothing in the system currently handles a different
baseline, and that gap is now measured rather than suspected.

### The ablation was run, and AMLSim cannot answer the question

`amlsim_ablation.py`, 10K profile. The conclusion is negative about the
**dataset**, not about the hypothesis, and it took three independent findings to
establish - any one of which alone would have compromised the measurement.

| run | PR-AUC with receiver block | without | delta |
|---|---|---|---|
| first, no bagging | 0.9507 | 0.8816 | **+0.069**, CI of *zero width* |
| bagging on | 0.0153 | 0.2263 | −0.211 [−0.573, +0.151] |
| bagging on, `is_new_payee` dropped | 0.0273 | 0.3629 | −0.336 [−0.643, −0.028] |

**A sixty-fold swing in PR-AUC from a bagging parameter.** No healthy
measurement does that, and chasing the sign through three configurations would
have produced whichever answer was looked for. What the three runs actually
found:

1. **The zero-width interval was not precision, it was absence of
   measurement.** Without `subsample`/`colsample_bytree` LightGBM is
   deterministic given the data, so five seeds returned five identical models.
   A degenerate interval reads as a very tight one. Now fixed, and recorded in
   the code rather than quietly corrected.
2. **`is_new_payee` separates the classes at rank-AUC 0.906** - 97% of SAR rows
   go to a never-before-paid payee against 16% of legitimate ones. AMLSim plants
   each alert as fresh graph edges while ordinary traffic reuses established
   ones, so the feature encodes *how the positives were injected*, not how they
   behave. This is `is_family` again (`ml/README.md`), in someone else's
   generator, in a different column.
3. **The receiver features are strongly non-stationary.** `rcv_txcount_90d`
   runs 67 → 199 → 102 across time deciles while the SAR rate runs 0.54% →
   0.28%. Under a time-ordered split - the correct protocol - the model trains
   in one regime and is scored in another, so the delta measures the
   simulation's shape as much as the feature's value.

**Bottom line: AMLSim cannot validate the −0.032, and the reason is structural.**
Its class membership is dominated by injection artefacts and its feature
distributions are non-stationary by construction. This is a property of the
dataset, discovered by measurement, and it is not repairable by adjusting the
experiment. Continuing to adjust until a positive delta appeared would be
precisely the failure this directory exists to prevent.

**What survives, and it is worth more than the delta would have been.** Four
datasets have now been examined for this project - its own generator, PaySim,
Zenodo 20030065, AMLSim - and **every one placed its positive class where the
method of placement is itself the strongest predictor**: `is_family` here,
balance columns in PaySim and Zenodo, `is_new_payee` in AMLSim. That is not four
coincidences. It is a property of how synthetic fraud data gets made, and it
means **the first step in using a foreign dataset is a screen for it, not the
last.** `amlsim_ablation.py` now carries two such screens - `leakage_screen()`
for near-deterministic single features and `drift_screen()` for
non-stationarity - and both were written after being caught by what they now
detect.

**Still owed for the receiver-side claim.** It has no external corroboration and
this run did not provide one. What would: a dataset whose positive class is
observed rather than injected. None of the four qualifies, and §8 of
`docs/related-work.md` records why none is likely to.

### The stronger half: reproduce the ablation, do not just run the rules

Running the rules answers "does the shape transfer". It cannot answer whether
receiver-side aggregation is *worth* anything, because the CEP layer has no
counterfactual. The measurement that would corroborate the −0.032 is a throwaway
model fitted twice on AMLSim's own features — once with `rcv_distinct_senders_1h`
and `rcv_inflow_1h`, once without — and the delta read **in sign**, not in
magnitude. Feature sets and baselines differ, so the number will not be −0.032
and must not be quoted as though it could be.

This is the one legitimate use of foreign data for training here, and it is
training a disposable model to test a claim, never the deployed one. See "Why
not train on it" above.


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
