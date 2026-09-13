# External validation

Three datasets answering three different questions, and two more examined and
rejected. No single one answers everything, and the distinction is the point.

Each has its own adapter, because each file has its own shape. Everything
downstream of a translated event — the unit conversion, the replay over the
deployed rule engine, the report sections — is in **`harness.py`**, shared by all
three: three copies of the measurement would make the three results
incomparable the first time one of them was changed.

## The constraint that shapes everything here

14 of this project's 20 features are **relational** — computed over the history
of a named sender and a named receiver: velocity, new-payee, amount deviation
against a personal baseline, fan-in concentration.

Measured cost of losing them (seed 42, held-out slice, 2026-09-07 dataset; the
full-system row re-measured 2026-09-13, when training began withholding the payee
age on a tenth of its rows - `ml/README.md`):

| available | PR-AUC | precision | recall |
|---|---|---|---|
| full system | 0.937 | 0.928 | 0.859 |
| no account identifiers | 0.761 | 0.563 | 0.745 |
| amount + hour only | 0.653 | 0.272 | 0.819 |

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
circular question. (ii) **The feature spaces do not align.** 14 of 20 features
are relational and five more need device, geo, session, receiver age or
kinship, none of which PaySim carries. Training on what remains would produce a
model over ~5 features while the Flink job computes 20 — train/serve skew, which
is precisely what the single ordered `FEATURE_NAMES` built from `capabilities.py`
exists to make impossible. (iii) **Different rail, different units.** PaySim is
mobile money at ~1/1000 of UZS amounts; pooling it with generated card P2P
produces a model for neither. The amount rescaling below is a unit conversion
applied so absolute thresholds can fire at all, not a step toward training.

The one legitimate training use of foreign data is not training *this* system:
it is fitting a throwaway model twice on a foreign dataset, with and without a
capability, to see whether an **ablation delta reproduces** off this project's
own generator. That is the shape the AMLSim run below should take. Capabilities PaySim cannot support (device, geo, session,
receiver age, kinship) are switched **off** rather than defaulted, so no
rule can fire on a fabricated zero — enforced by a test.

**Get it.** Kaggle, "Synthetic Financial Datasets For Fraud Detection" (~470 MB).

```bash
cd validation
python paysim_adapter.py --file PS_20174392719_1491204439457_log.csv --limit 500000
```

`TRANSFER` is the P2P analogue and the default. Start with `--limit` — the full
file is 6.3M rows.

**A second mode, answering a different question.** `--baseline` ignores the rules
and retrains the published PaySim model instead, on the full file and on the
split its authors used:

```bash
python paysim_adapter.py --file PS_20174392719_1491204439457_log.csv --baseline
```

The run above asks *do this project's rules carry signal on foreign data*. This
one asks *is the number this project calibrates itself against real* — the AUPRC
0.380 that `docs/related-work.md` §6 quotes from `ris3abh/aml-p2p-fraud-detection`.
It is: reproduced at 0.397, with their other three figures landing too. It also
found the two things §6 had wrong about that number, the prevalence it was
measured at being the one that mattered. Takes a few minutes on 6.36M rows.

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

### Result (`--our-model`): this project's model trained on PaySim

The rules replay above asks whether the rules fire on foreign data. This asks the
blunter question — *how does this system actually score on it* — and the answer
is worth having in full, including the part that goes against the project.

14 of the 20 features are computed here, and 13 of them carry information.
PaySim carries identifiers on both sides, so per-sender history and receiver-side
aggregation both work; but it names no bank on either side, so `cross_network` -
which compares the issuers behind the two cards - is a constant zero. It belongs to
the core contract and cannot be switched off the way a capability can. A tree never
splits on a constant, so the figures below do not move, but the count is 13, and
the same gap was found on IBM AML before its extraction ran (section 4). What
PaySim cannot supply at all is device, geo, session, receiver age and kinship, and
those capabilities are switched off rather than defaulted. Trained on the published
baseline's own split (24 days / 7 days, a cut at step 576).

| configuration | feat | PR-AUC | ROC-AUC | rec@2% | lift |
|---|---|---|---|---|---|
| **fitted as `train.py` fits it** (scale_pos_weight ≈ 974) | | | | | |
| this project, all it can compute | 14 | 0.032 | 0.726 | 19.7% | 3.9× |
| — without receiver aggregation | 12 | 0.021 | 0.696 | 17.3% | 2.9× |
| — plus PaySim's own transaction type | 19 | 0.040 | 0.809 | 17.6% | 4.7× |
| **fitted unweighted**, as the published baseline was | | | | | |
| this project, all it can compute | 14 | **0.267** | 0.853 | 39.8% | 6.8× |
| — without receiver aggregation | 12 | **0.315** | 0.867 | 44.6% | 6.9× |
| — plus PaySim's own transaction type | 19 | **0.373** | 0.889 | 46.4% | 7.2× |
| published generic baseline (§6 of `related-work.md`) | ~24 | 0.380 | 0.908 | 49.4% | 7.0× |

**Three things follow, and the second is the uncomfortable one.**

**1. The training recipe, not the feature set, produced the alarming number.**
`train.py` sets `scale_pos_weight` from the class ratio. At this project's 1.5%
fraud that is ~65 and harmless; at PaySim's 0.129% it is ~974, and heavy positive
weighting flattens the top of the ranking, which is precisely what AUPRC reads.
Same features, same split, **0.032 weighted against 0.267 unweighted**. Reporting
only the first beside a baseline fitted unweighted would have compared this
project's *recipe* against their *features* and called the difference a result.
It is a genuine caveat for deployment: the recipe is calibrated for a 1.5% base
rate and should not be carried unchanged to a rarer one. Since 2026-09-13 it
cannot be: below 0.5% fraud `train.py` stops unless told how to weight
(`CLASS_WEIGHTING`, `ml/README.md`).

**2. Receiver aggregation does not merely fail to transfer — it reverses.**
Removing those two features **improves** PaySim by +0.049 (0.267 → 0.315), where
on this project's own data the same removal costs −0.040, the largest effect in
the ablation. The sign flips.

That is not a refutation, and the reason was written down before the run: PaySim
drains one account straight to cash-out, with no collection stage where many
senders converge on a drop account. There is no fan-in to detect, so features
that look for it contribute noise, and a model does better without them. The
prediction was made qualitatively above and is now quantitative.

But it must be said plainly: **this run does not validate the project's largest
finding, and no run on PaySim can.** The finding stays externally unvalidated
until it meets a dataset that models a collection stage — AMLSim, below.

**3. Once the recipe matches, the feature set is competitive.** 0.373 against the
baseline's 0.380, with PaySim's own transaction type added to make the inputs
comparable — this project's contract has no column for transaction type because
its rail has one. That is a fair result on a dataset this project did not produce,
and it is the strongest positive statement this exercise supports.

```bash
python paysim_adapter.py --file PS_20174392719_1491204439457_log.csv --our-model
```

Two minutes to extract 6.36M rows, then six model fits.

> **A defect this run found in the extractor.** `features.payee_key` returned an
> empty string when an event carried no `receiver_card`, silently. PaySim names
> accounts and issues no PANs, so *every* payee collapsed into one shared
> receiver state: fan-in was computed over the entire stream, which both
> fabricates the pattern `MULE_FAN_IN` looks for and makes the replay quadratic —
> that is how it surfaced, as a run that never finished. The `pinfl` branch had
> warned about its own missing identity since it was written; the `card` branch
> had not. It warns now, and `test_payee_identity.py` pins both.

### The gap this leaves, and what closes it

Receiver-side aggregation is this project's **largest measured effect**
(−0.032 PR-AUC) and its one structural design finding. It is also the one thing
here with **no external validation whatsoever**, because the only foreign dataset
run so far cannot express the pattern. Saying "the rule has nothing to detect" is
true and is also the most convenient possible outcome, which is a reason to
distrust it.

> **Since then (section 4).** IBM AML has a collection stage, and it gives
> rule-level evidence - `MULE_FAN_IN` separates the classes 2.8:1 - and, at
> twenty seeds, model-level evidence that meets a rule fixed before the run:
> removing the two receiver-side features costs 2.5 F1 points, 95% CI
> [+0.5, +4.4], and a seed-averaged model agrees (+2.3, [+1.1, +3.5]). It took a
> second look to get there, and section 4 says what that costs.

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

`zenodo.py` - `--only provenance` and `--only profile`, or both

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
python zenodo.py --file fraud_tests_export_20260501_080333.csv
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

`python -m pytest tests/test_adapters.py -q` exercises the adapter against fixtures in
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

Fixtures in the shape of all three real files, so the harnesses are known to work
before anything is downloaded. They test the **adapters** — the mapping from a
foreign schema onto this project's event contract — not the detection outcome,
which is what the real run is for.

---

## 4. IBM AML — the collection stage on a clock that can see it

`ibm_aml_adapter.py`

**The risk it addresses.** Section 3 above reports a rule that did not merely fail
to transfer, it **inverted**: `MULE_FAN_IN` fired *less* often on AMLSim's fan-in
targets than on ordinary accounts. Two incompatible readings survive that run.
Either the rule encodes a rate rather than a topology and does not generalise, or
the deployed one-hour window cannot see a pattern whose median span is 363 days.
Both were stated; neither could be eliminated, because AMLSim cannot separate
them.

Separating them needs a dataset that models a collection stage on a **finer
clock**. This is that dataset.

**What it is.** *IBM Transactions for Anti-Money Laundering* — Altman et al.,
"Realistic Synthetic Financial Transactions for Anti-Money Laundering Models",
NeurIPS 2023 Datasets and Benchmarks
([arXiv:2306.16424](https://arxiv.org/abs/2306.16424)). A multi-agent simulation
of banks, businesses and individuals, released under CDLA-Sharing-1.0 as six
graphs from 5M to 180M transactions. `HI-Small_Trans.csv` is used here.

| | PaySim | AMLSim | **IBM AML (HI-Small)** |
|---|---|---|---|
| clock resolution | 1 hour | 1 day | **1 minute** |
| rows | 6.36M | 198K | 5.08M |
| span | 30 days | ~2 years | **17 days** |
| positives | 0.129% | 0.339% | 0.102% |
| collection stage | absent | present, ~363-day span | **present, ~3.6-day span** |

**Why the clock is the whole point.** `RECEIVER_WINDOW_S` is one hour. Against
AMLSim's 363-day patterns the mismatch is about 10⁴ and swamps every other
effect. Here the median laundering receiver collects over **86.8 hours**, so the
mismatch is ~87× — still real, still reported, but no longer large enough to
explain any result on its own. Section D of the report measures it from the data
on every run rather than restating it as a caveat.

**The measurement that made this dataset worth fetching**, taken before any rule
was replayed — distinct senders per receiver per hour, against the rule's
threshold of more than five:

| | above threshold |
|---|---|
| receivers of laundering funds | **1.02%** |
| all other receivers | 0.18% |

**5.6× the right way.** On AMLSim the same quantity was 1.16% against 2.69% —
0.4×, inverted. The sign flips back on an independent dataset with a finer clock,
which is the first evidence that section 3's inversion is a property of that
dataset's timescale rather than of the rule. It is not yet a positive result for
the rule: the deployed replay is what tests that, and 87× is still 87×.

**Get it.** Kaggle
([ealtman2019/ibm-transactions-for-anti-money-laundering-aml](https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml)),
IBM Box (linked from [IBM/AML-Data](https://github.com/IBM/AML-Data)), or a
Hugging Face mirror of the HI-Small transactions that needs no Kaggle account.

```bash
python ibm_aml_adapter.py --file HI-Small_Trans.csv --limit 500000
```

`--patterns HI-Small_Patterns.txt` adds the per-typology breakdown; the released
transaction CSV carries only `Is Laundering`, and which typology a row belongs to
lives in that sidecar. Without it section B is empty and the aggregate question is
still answered.

The sidecar is not on the Hugging Face mirror and needs a Kaggle account, so
`read_patterns` is written from the dataset's description and has **never been run
against a real one**. Its column indices are confirmed — IBM's own Multi-GNN
loader reads the accounts from positions 2 and 4 — but the `BEGIN LAUNDERING
ATTEMPT` marker lines are not. Whoever runs it first must check that section B
comes back non-empty: an empty table means the parse failed, not that the system
missed everything, and reading one as the other would be the worst available
outcome.

**Three translation decisions, none of them free.**

1. **Self-transfers are dropped** — 591,212 rows, 12% of the file, almost all
   `Reinvestment`. An account paying itself is not a transfer between two parties;
   left in, every one becomes a fan-in edge from an account to itself. The count
   is printed on every run.
2. **Amounts are scaled per currency**, each by its own median. The file carries
   15 currencies and this project's thresholds are written in UZS, so one global
   factor would put a yen and a dollar amount on opposite sides of the structuring
   threshold for no reason but their denomination.
3. **No filter on payment format**, deliberately. Laundering here concentrates in
   ACH (0.75% against 0.02% elsewhere), so defaulting to ACH would be selecting
   rows by the label. The per-format rates are printed instead, and `--formats`
   makes a narrower run an explicit choice.

### Result (4,487,133 rows, 5,166 laundering, full file)

**Per-rule lift — threshold-free, and the section that answers the question:**

| rule | on laundering | on legit | lift |
|---|---|---|---|
| `NEW_PAYEE_HIGH_AMOUNT` | 17.15% | 1.84% | **9.3x** |
| `AMOUNT_DEVIATION` | 6.08% | 0.88% | **6.9x** |
| `MULE_FAN_IN` | 3.17% | 1.12% | **2.8x** |
| `STRUCTURING` | 5.59% | 4.36% | 1.3x |
| `DISTINCT_PAYEE_BURST` | 11.61% | 9.48% | 1.2x |
| `VELOCITY` | 11.79% | 9.68% | 1.2x |
| `DAILY_LIMIT_BREACH` | 18.00% | 19.23% | 0.9x |

**1. The question this dataset was fetched for is answered.** `MULE_FAN_IN`
separates the classes 2.8:1. On AMLSim the same rule ran the wrong way — it
fired on 1.16% of fan-in targets against 2.69% of ordinary receivers — and two
readings survived that run: the rule detects a rate rather than a topology, or
the window is shorter than the pattern. The sign flips back here, on an
independent dataset with a clock three orders of magnitude finer. **The
inversion was a property of AMLSim's timescale, not of the rule.**

It is a conservative 2.8x, which is what makes it worth having. The median
laundering receiver here still collects over 86.8 hours against a one-hour
window — 87x — so the rule sees a fraction of each pattern and separates the
classes anyway. Section D prints that ratio from the data on every run.

This is the first external evidence for the receiver-side aggregation this
project's largest measured effect (−0.032 PR-AUC) rests on. It does not
reproduce that number, and cannot: the effect is a model ablation and this is a
rule replay. What it establishes is that the pattern the rule looks for exists
in data this project did not produce, and that the rule finds it.

**2. The relational features transfer more strongly than on PaySim.**
`NEW_PAYEE_HIGH_AMOUNT` reaches 9.3x here against 4.0x on PaySim — computed
entirely from per-sender history, the machinery that cannot exist without
account identifiers. `AMOUNT_DEVIATION` at 6.9x had no counterpart on PaySim,
whose hourly clock left personal baselines nearly flat.

**3. The thresholds do not transfer, and this run says so more sharply than any
other.** The decision layer flags 39.1% of laundering — and **22.18% of
legitimate traffic**, 994,140 alerts. Lift 1.8x; the best cutoff available on
this data (0.35) reaches only 2.5x.

The reason is measurable rather than mysterious, and it is the same one behind
the runtime below. Three rules — `VELOCITY`, `DISTINCT_PAYEE_BURST`,
`DAILY_LIMIT_BREACH` — count events against limits calibrated for a retail
sender. The median sender-day here is 2 transactions, but the maximum is
**26,365**: the simulation contains banks and corporates. Those accounts breach
a personal daily limit continuously, which is why `DAILY_LIMIT_BREACH` lands at
0.9x — *below* one, anti-correlated with the label, because breaching it is
ordinary corporate behaviour and laundering deliberately is not.

None of that contradicts the design. It is the clearest available demonstration
of what "a result here is about the rules' shape, never about their thresholds"
actually costs when the shape is right and the population is wrong.

### What the run cost, and the defect it exposed

**5.5 hours for 4.5M rows**, and the rate collapsed as it went: 22,102 rows/s
over the first 100,000, 226 rows/s cumulative by the end. Profiled rather than
guessed — one generator expression in `features.extract` was entered 157 million
times for 400,000 events.

`extract` makes **five separate linear passes** over the sender's 24-hour
history per event: `vel_10m`, `vel_1h`, the structuring band, the distinct-payee
set and `daily_sum`, each recomputed from scratch. Per-event cost is therefore
O(sender's recent history), which on retail P2P is free — the median sender-day
is 2 events — and quadratic on a stream containing hub accounts, where one
sender-day reaches 26,365.

**This is a property of the deployed extractor, not of the harness.** The
latency figures this project reports are measured on retail-shaped traffic,
where the history is short by construction, so nothing had ever exercised the
term that grows. It is a scalability boundary found by running on foreign data,
which is what this directory is for, and it is worth stating as one rather than
optimising away quietly.

Hub accounts were **not** excluded to make the run finish. They are almost
entirely legitimate traffic, so dropping them would raise the base rate and
inflate every lift in the table above — selecting on a quantity correlated with
the label, which is the one thing this directory exists to avoid.

```bash
python ibm_aml_adapter.py --file HI-Small_Trans.csv
```

Progress is printed to stderr, so a redirected report stays clean and a five-hour
run can be told apart from a hung one.

### Result (`--our-model`): this project's model on the published split

The rule replay above says whether the rules fire on this data. This says how the
model scores - fitted with `train.py`'s hyperparameters on the features the
deployed extractor computes, and scored the way the IBM benchmark scores: the
earliest 60% of transactions train, the next 20% validate, the last 20% test
(897,427 rows, 1,653 laundering), minority-class F1. Ten seeds per
configuration; the +/- is the spread across them.

Fourteen features compute here, all informative: device, geo, session, receiver
age and kinship are off as for the rule replay, and `cross_network` reads the bank
on each side - it was a constant zero until the smoke run before this extraction
caught it (98% of transfers are interbank).

| configuration | F1 %, threshold from validation | F1 % at 0.5 | PR-AUC | ROC-AUC |
|---|---|---|---|---|
| this project, 14 features | 17.5 +/- 3.9 | 6.5 | 0.065 | 0.927 |
| - without receiver aggregation | 15.4 +/- 3.5 | 4.7 | 0.057 | 0.917 |
| - plus the file's payment format and currency | **35.7 +/- 4.1** | 18.7 | 0.200 | 0.931 |
| *any of the three, with `train.py`'s class weighting* | *1.5 - 2.2* | *1.5 - 2.1* | *0.009 at most* | |

Published on the same split (arXiv:2402.08593, Table 4): gradient boosting on the
file's own columns 21.3 +/- 0.3 (LightGBM) and 19.8 +/- 0.9 (XGBoost), GIN
28.7 +/- 1.1, GIN+EU 47.7 +/- 7.9, PNA 56.8 +/- 2.4, and gradient boosting with
graph features 62.9 +/- 0.3 and 64.8 +/- 0.5.

**1. With the file's own columns, this feature set sits between the tabular
baselines and the graph methods.** 35.7 against about 20 for gradient boosting on
the raw columns and 28.7 for GIN; well short of the 57-65 reached by methods that
see cycles and multi-hop scatter-gather, which nothing in this contract describes.
The fourteen features alone reach 17.5 - below the raw-column baselines, which
have the payment format this contract has no column for, and which carries most
of the signal here: ACH holds 87% of the laundering.

**2. The comparison hinges on a threshold the paper does not state.** At a
threshold chosen on validation - the rule used here - 35.7 is clearly above the
tabular baselines; at a fixed 0.5 the same model scores 18.7, slightly below them.
The paper does not say which rule its F1 used, so the honest reading lies between
"clearly better" and "slightly worse", and this section does not pick the kinder
one.

**3. Receiver aggregation points the right way; ten seeds could not tell it from zero.**
Paired by seed, removing the two receiver-side features costs 2.1 F1 points, 95%
CI [-1.5, +5.6], and 0.009 PR-AUC, CI [-0.006, +0.023]; seven seeds of ten go the
way the design predicts. On PaySim, which has no collection stage, the same
removal *improved* PR-AUC by 0.049, so the sign has turned as predicted - but at
this spread ten seeds do not establish it. Twenty seeds and a second method do,
with caveats - the next subsection.

**4. The training recipe does not survive a 0.1% base rate - now on two
datasets.** `scale_pos_weight` from the class ratio is about 870 here, and every
weighted fit collapses to an F1 of 1.5 - 2.2%. PaySim showed the same, less
severely. It is a deployment caveat, not a feature result - and since 2026-09-13
a guarded one: `train.py` stops below 0.5% fraud unless told how to weight.

**5. The spread is wide, and it misled once already.** +/-4 F1 points across
seeds, against the published +/-0.3 for LightGBM: F1 at a tuned threshold on 1,653
positives moves with every tree the column sampling changes. The first attempt,
at three seeds, put the fourteen features at 21.3 - level with the published
baseline. Ten put them at 17.5.

The extraction took 1 h 47 min over 4,487,133 rows - against 5.5 hours for the
rule replay over the same rows, a difference not diagnosed further - and is
cached, so every fit reads it rather than repeating it:

```bash
python ibm_aml_adapter.py --file HI-Small_Trans.csv --extract-only --cache ibm_features.npz
python ibm_aml_adapter.py --file HI-Small_Trans.csv --our-model --cache ibm_features.npz --seeds 10
```

### Receiver aggregation, asked so that seed noise cannot answer it (`--receiver-ablation`)

Ten seeds left the receiver-side delta inside the model's own spread (point 3
above). This asks again with twenty seeds, and a second way: the mean of all
twenty fits' probabilities - a seed-averaged model, which removes most of the
fit-to-fit noise - with a paired bootstrap over the test rows (1,000 resamples)
for the uncertainty that remains. The rule was fixed before the run: established
only if both F1 intervals on the fourteen features exclude zero.

| configuration | per seed, paired (20 seeds) | seed-averaged model, paired bootstrap |
|---|---|---|
| 14 features - F1 points | **+2.46 [+0.52, +4.40]**, 14 of 20 positive | **22.33 vs 20.00 = +2.33 [+1.05, +3.52]** |
| 14 features - PR-AUC | +0.012 [+0.005, +0.019] | 0.153 vs 0.099 = +0.053 [+0.042, +0.064] |
| plus format and currency - F1 points | -0.76 [-4.27, +2.75], 9 of 20 positive | 53.02 vs 49.77 = +3.25 [+1.73, +4.69] |
| plus format and currency - PR-AUC | +0.003 [-0.023, +0.028] | 0.463 vs 0.423 = +0.040 [+0.026, +0.055] |

**Verdict: established.** On the fourteen features both intervals exclude zero,
for PR-AUC as well as F1. It is the first model-level external evidence for the
fan-in design: on data with a collection stage, removing the two receiver-side
features costs about 2.4 F1 points and about a third of the averaged model's
PR-AUC. On PaySim, which has no collection stage, the same removal helped (point
3) - the sign turns where the design says it should.

Three things temper it, and belong next to it:

- **It is a second look.** The twenty seeds include the ten above, and they were
  run because ten were inconclusive. A result sought until it appears is weaker
  than one found the first time; the rule was fixed before this run but not
  before the first, so the per-seed interval is somewhat optimistic. The
  averaged model is a different method and agrees, which is the main reason to
  believe it.
- **With the file's own columns the two methods disagree.** Per seed the delta
  drowns in a spread of +/-4 F1 points; on the averaged model it is +3.25, clear
  of zero. The rule did not cover this configuration; the disagreement is
  recorded, not resolved.
- **The bootstrap measures the test set's uncertainty only.** It holds the
  twenty-fit average fixed; a different twenty seeds would move it somewhat.

**A side result, not what the run was for.** Averaging twenty fits more than
doubles PR-AUC against the single fits in the table above - 0.153 against 0.065
on the fourteen features, 0.463 against 0.200 with the file's columns - and
lifts F1 to 22.3 and 53.0. 53.0 sits near PNA's 56.8, but the paper's table
compares single models and this is an average of twenty; it is recorded as a
lead, not as a place in that table.

Forty minutes on this laptop, most of it the eighty fits:

```bash
python ibm_aml_adapter.py --file HI-Small_Trans.csv --receiver-ablation --cache ibm_features.npz --seeds 20 --boots 1000
```

---

## 5. Mendeley ktbthg777x — examined and NOT used

*"Synthetic Banking Transaction Dataset with Multi-Pattern Fraud Labels for
Machine Learning Research"*, [doi 10.17632/ktbthg777x.1](https://data.mendeley.com/datasets/ktbthg777x/1),
CC BY 4.0. 1,000,000 rows, 100,000 customers, 20,000 merchants, four fraud
patterns: card testing, account takeover, money laundering rings, geographic
anomalies.

It was the strongest candidate on paper, and the only public dataset found that
carries **`device_id` and coordinates** — the two capabilities no external run has
ever exercised. It does not survive contact.

**1. The coordinates are random, and the release says so.** Its own limitations
note reads: coordinates "are randomized within realistic ranges; they do not
represent actual merchant locations". Every country in the file spans latitude
−60…70 and longitude −180…180; `merchant_country` and the coordinates are
independent draws. One of the four fraud patterns is nonetheless *geographic
anomaly*.

Measured on the 400 rows carrying that label, against the same customers' own
legitimate rows:

| | median distance from that customer's previous transaction |
|---|---|
| labelled `geo_anomaly` | 9,880 km |
| legitimate, same customers | 9,995 km |

The labelled anomalies are, if anything, **less** anomalous. 10,000 km is simply
the median distance between two uniform points on a globe. A fraud class defined
by geography, in a file whose geography carries no information.

**2. The history is too sparse for any relational feature.** Ten transactions per
customer over a year; a median of 19 days between two events on the same card. The
10-minute and 1-hour windows are empty on essentially every row, and
`secs_since_last` is measured in weeks. 14 of 20 features degenerate.

**3. The "laundering ring" has no collection stage.** 80 transfers, 80 distinct
senders, 80 distinct receivers — each receiver receives exactly one. The
`merchant_id` on those rows is the literal string `TRANSFER_TO_<customer>`. There
is no convergence to detect, which is the one thing this project most needs a
dataset to contain.

**What survives.** Nothing quotable. Recorded here so the next reader does not
spend the same day on it, and because it is the second published dataset in this
directory whose headline claim does not survive being checked — see section 2, and
`docs/irp-framing.md`.
