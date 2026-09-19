# Related work and external datasets

Working note, not thesis text. Each entry says what the source is, what it
supports here, and - the part that matters - what it does **not** support.
Sources assessed 2026-08-31; condensed on 2026-09-15 (the full version is in git
history). **§9 answers "how did the literature affect the system"**: five sources
changed code, the rest changed only how results are reported.

---

## 1. The term "P2P" is ambiguous, and the ambiguity is expensive

| Sense | Domain | Relevance here |
|---|---|---|
| P2P **payments** | card-to-card / instant transfer between individuals | **this project** |
| P2P **lending** | marketplace credit, borrower↔lender matching | different fraud taxonomy; methodology transfers, findings do not |
| P2P **networking** | peer-to-peer protocols, botnet C&C | none |

Two of the first six documents assessed turned out to be about lending and one
about networking - worth a sentence in the methodology chapter.

---

## 2. Machado et al. (2026) - systematic review of P2P **lending** fraud

*Anatomy of peer-to-peer (P2P) lending fraud: A review with managerial
implications.* International Journal of Information Management Data Insights 6,
100425. 53 studies.

Its consolidated constraints read as an independent checklist: scarce labels
(generated here, their construction specified in `docs/generator-spec.md` §5),
severe imbalance (PR-AUC reported, not ROC-AUC), limited cross-platform
transferability (**not addressed** - one generator, one market), evolving fraud
(drift against evasion, `docs/threat-model.md` §5), relational modelling
(receiver-side aggregation) and explainability (SHAP reason codes). Its
evaluation-practice table is the citable part: study after study records
**"Accuracy reported"** and *NR* everywhere else.

**Not supported:** anything about payments - its taxonomy (identity abuse, loan
stacking, collusion) has no APP, ATO or muling. Cite it for evaluation practice and
data constraints only.

---

## 3. Wang (2018) - P2P lending fraud at HC Financial

*Detection of fraudulent users in P2P financial market.* MATEC Web of Conferences
189, 06004. Random forest and GBDT, ~35 features, one Chinese lending platform.
Useful as **contrast**: AUC 0.780 with no precision or recall, justified as
"insensitive to class balance" at a >10% fraud rate - the reasoning `ml/README.md`
argues against at 0.1-1.5%. Its identity graph is the shape of the MyID kinship
integration measured here as worthless (+0.002 PR-AUC, CI straddling zero).

---

## 4. Hemel, Hallaji & Razavi-Far (2026) - TSAI-MetaFraud

*A Benchmark Dataset for Financial Fraud Transaction and Behavioral Risk Detection
in Metaverse Ecosystems.* arXiv:2607.09528v1. A multimodal OpenSimulator benchmark
with a strict inductive split. Their Table VII, transaction fraud detection:

| model | class | P | R | F1 |
|---|---|---|---|---|
| XGBoost (tabular) | Real (benign) | 0.97 | 0.99 | **0.98** |
| XGBoost (tabular) | Behavioral fraud | 0.81 | 0.69 | 0.75 |
| XGBoost (tabular) | Financial fraud | 0.00 | 0.00 | **0.00** |
| XGBoost (tabular) | Both (hybrid) | 0.00 | 0.00 | **0.00** |
| GraphSAGE (graph) | Real (benign) | 0.93 | 0.88 | 0.90 |
| GraphSAGE (graph) | Behavioral fraud | 0.34 | 0.49 | 0.40 |
| GraphSAGE (graph) | Financial fraud | 0.37 | 0.41 | **0.39** |
| GraphSAGE (graph) | Both (hybrid) | 0.55 | 0.62 | **0.58** |

Gradient boosting on tabular features - this project's model family - scores
**exactly zero** on both financial-fraud classes; only the graph model finds them.
It is the same structural claim as this project's fan-in result (`ml/README.md`,
"Fan-in"), reached independently: a per-record view cannot express a pattern
defined over the relation between records. **Not supported:** its absolute numbers
- virtual currency, no regulator, no card networks.

---

## 5. Saad et al. (2011) - P2P botnets

*Detecting P2P Botnets through Network Behavior Analysis and Machine Learning.*
PST 2011. **Not relevant** - peer-to-peer networking, no payments. Recorded so the
search is not repeated.

---

## 6. `ris3abh/aml-p2p-fraud-detection` (MIT) - the calibration point

PaySim, 6.36M mobile-money transactions, CatBoost with `scale_pos_weight=974`.
Reported by the repository, and reproduced here with
`paysim_adapter.py --baseline`:

| metric | reported | reproduced | measured on |
|---|---|---|---|
| AUPRC (= PR-AUC) | **0.380** | **0.397** | the 7-day holdout, **1.142% fraud** |
| AUC-ROC | 0.908 | 0.878 | " |
| recall | 49.4% (916 of 1,854) | 46.8% | at a **2% alert budget**, threshold 0.987 |
| top-decile lift | 7.0× | 7.25× | " |
| AUPRC *before* removing balance leakage | 0.988 | **1.000** | same split |

Their split is 24 days train / 7 days test, a cut at step 576 that reproduces
their stated rates. The AUPRC belongs to a holdout at **1.142%** fraud, not
PaySim's overall 0.129% - an earlier revision had the prevalence wrong
(`irp-framing.md` 8, eighteenth). Against this project's held-out slice at 1.23%
the prevalences match, so the comparison is like for like:

| | this project | ris3abh / PaySim |
|---|---|---|
| test prevalence | 1.23% | 1.142% |
| recall @ 2% alert budget | **98.4%** | 49.4% |
| lift @ top decile | 10.0× (capped) | 7.0× |
| PR-AUC | 0.937 (0.960 ± 0.018 across seeds) | 0.380 |

Those rows are the baseline profile. On the realistic profile - fraud at 0.18%,
legitimate look-alikes, a tenth of fraud unreported -
the committee scores 0.321 PR-AUC, and the gap has closed (at a far lower fraud
rate, so not directly comparable): much of the
old distance was a generator whose classes separated by construction.

**How much of the gap is features, and how much is data** (`validation/README.md`):

| features available | PR-AUC on THIS data |
|---|---|
| all 20 | 0.937 |
| amount + hour only | 0.653 |
| — PaySim, leak-free, 6 raw columns | 0.380 |

Roughly half the gap is features public data cannot publish, and half a generator
whose classes separated by construction. And PaySim with its balance columns
scores 1.000: **a PR-AUC in the high nineties is the range a known-broken model
reaches on public data**, which is why `generator-spec.md` §7 calls this
project's 0.960 a design target, not a result.

Two notes: `scale_pos_weight=974` cost almost everything in reproduction (0.397 →
0.022), so the weighting probably did not apply to their scored model; and
PaySim's last 296 rows are 100% fraud, so a temporal split reaching the tail
inherits a region with no negatives. **Not supported:** anything beyond model
difficulty on a dataset - batch notebooks, mobile money.

---

## 6a. Afriyie et al. (2023) - the evaluation failure, in a peer-reviewed journal

*A supervised machine learning algorithm for detecting and predicting fraud in
credit card transactions.* Decision Analytics Journal 6, 100163. Their abstract
recommends random forest at "a maximum accuracy of 96%". Recomputed from their
own confusion matrices (96,961 test rows, 0.44% fraud):

| | random forest | decision tree |
|---|---|---|
| accuracy | 0.958 *(their headline)* | 0.916 |
| recall / sensitivity | 0.958 *(reported)* | 0.930 *(reported)* |
| **precision** | **0.092** *(not reported)* | **0.047** *(not reported)* |
| **F1** | **0.167** *(not reported)* | **0.089** *(not reported)* |
| alerts per true catch | **10.9** | 21.4 |

**A classifier that predicts "never fraud" scores 0.996 accuracy on this test
set.** The authors name the imbalance objection themselves and headline accuracy
anyway - the concrete instance of §2's "Accuracy reported / NR", and the strongest
argument for this project's reporting: precision and recall together, PR-AUC,
per-type recall with intervals.

---

## 6b. Tritscher et al. (2022) - the second precedent for generating data

*Open ERP System Data For Occupational Fraud Detection.* arXiv:2206.04460v2. Data
generated by participants playing fraud scenarios on a real ERP interface - the
same reasoning as `generator-spec.md` §0: real data is unavailable, so generate.
Two of their criticisms land here. Generators without published parameters and
code are unverifiable - `generator-spec.md` publishes both, with hashes. And
**post-hoc fraud injection** makes fraud diverge from normal data - this project
injects fraud into generated traffic (§5), and ROC-AUC 0.999 on the baseline
profile is that divergence observed.

---

## 6c. Wang, Liu, He & Du (2020) - graph attention on real P2P lending data

*A Graph Attentive Network Model for P2P Lending Fraud Detection.* KSEM 2020.

| model | precision | recall | F1 | accuracy | AUC |
|---|---|---|---|---|---|
| GCN-a | 0.727 | 0.856 | 0.786 | 0.770 | 0.823 |
| GAT-a | 0.734 | 0.885 | 0.802 | 0.784 | 0.825 |
| EGNN(A)-a | 0.736 | 0.916 | 0.816 | 0.796 | 0.829 |
| **FDNE-f** | 0.740 | 0.919 | 0.820 | 0.800 | 0.830 |

Relational structure beats demographic attributes on *real* data, with a
per-decision explanation - the pair of claims this project makes. **Caveat:**
their label is delinquency ("overdue users as fraud users"), which is not fraud.

---

## 6d. Cybersecurity Centre of Uzbekistan, 2025 annual report - the only national source

State Institution "Cybersecurity Centre" (csec.uz), annual digest for 2025, in
Uzbek - **the only source here with real Uzbek numbers**, used in
`threat-model.md` §3a and `irp-framing.md` §7.5:

| finding | figure |
|---|---|
| mobile applications examined | 40 in 2025 vs 18 in 2024, **+122%** |
| high-severity mobile findings that are transport security | **54 of 157** (interception 33, transport security disabled 13, unencrypted transmission 8) |
| databases leaked to darknet | 37 organisations, **21M rows**, plus 1,697 login/password pairs |
| personal records whose leak was prevented | **23M+**, "close to two-thirds of the population" |
| banking/finance share of detected vulnerabilities | **22.84%**, second behind public administration (25.84%) |
| web-application attacks | 67M+ malicious requests, **+430%** year on year |

**The trap:** of 247 recorded incidents only three are phishing - but the register
counts incidents against *state web resources*, and consumer APP fraud is outside
its view. Quote the number with its scope, or it argues against the thesis's own
premise.

---

## 6e. Uzbek primary sources: scale, and a regulator that moved

### The size of the thing (Central Bank of Uzbekistan)

*Review of international migration and currency operations of individuals*,
Central Bank of Uzbekistan, March 2026:

| | 2024 | 2025 | change |
|---|---|---|---|
| remittances received, total | $14,851mn | **$18,948mn** | +28% |
| — traditional MTO systems | $8,161mn | $9,903mn (52%) | +21% |
| — **P2P direct to bank cards** | $5,916mn | **$8,648mn (46%)** | **×1.4** |
| — conventional bank transfers | $774mn | $397mn (2%) | **−49%** |

P2P to cards rose from 40% to 46% of inbound remittances in a year. On that
cross-border share receiver-side aggregation is the **only** signal, because the
sender is not the bank's customer (`threat-model.md` §1).

### The regulator moved, and the economics changed (from 16.11.2026)

Amended Central Bank requirements for P2P transfers: transfers through websites
prohibited; a new device deactivates the linked cards; organisations set their own
limit for transfers needing no extra confirmation - and **carry the liability for
fraud within it**; counterparty names are masked. The liability clause moves the
loss onto the institution and makes detection quality set a revenue-bearing limit
(`threat-model.md` §2).

### P2P monitoring is also a tax programme, and that is a confound

kun.uz, 13 May 2026: from April 2026 the **Tax Committee** monitors individuals'
P2P activity, with a 30 trillion soum revenue target and undisclosed selection
criteria. `STRUCTURING` detects transfers kept under a threshold - also the
signature of undeclared trading income. A fraud alert protects the account
holder; a tax referral is used against them. The thesis should say which one this
system is for, and treat tax enforcement as a confound for any future validation
on real Uzbek traffic.

---

## 7. Datasets considered and not adopted

Extends the table in `generator-spec.md` §0; the datasets actually run are in
`validation/README.md`.

- **IBM Synthetic Data Sets (SynDS)** - P2P payment data with full labels, but a
  commercial product (the public repository holds schemas only), US consumer-app
  semantics and no session signals. Citable as evidence that full-label synthetic
  P2P data is an accepted industry instrument.
- **Kaggle UPI datasets** (e.g. `skullagos5246/upi-transactions-2024`) - themselves
  synthetic, with no generating specification.
- **AMLSim** (IBM, open source) - run, then removed with IBM AML-Data on
  2026-09-19 (`validation/README.md`, Removed).
- **The AI4FCF catalogue** (`sites.google.com/view/ai4fcf/open-datasets`): BankSim
  (customer→merchant, so receiver concentration is normal), IBM AML-Data (run, then
  removed: it includes banks and companies - `validation/README.md`, Rejected), Amaretto (capital markets), the Czech financial
  dataset (no fraud labels), the Libra Bank graph, Paradise/Panama Papers.
- **`CiferAI/Cifer-Fraud-Detection-Dataset-AF`** - 21M rows of PaySim's
  phenomenology: no collection stage, balance-column leakage, and `isFlaggedFraud`
  is a system decision, not ground truth.
- **`ealaxi/banksim1`** - customer→merchant, the same disqualifier as IEEE-CIS.
- **`mlg-ulb/creditcardfraud`** / OpenML 42175 - the ULB set, PCA-anonymised, and
  the source the Zenodo file was sampled from.
- **`amazon-science/fraud-dataset-benchmark`** - loaders for nine datasets and
  **no P2P payment dataset at all**: evidence for the gap in §8.
- **Kaggle `sriharshaeedala/financial-fraud-detection-dataset`** (a PaySim
  re-upload) and **`kartik2112/fraud-detection`** (the Sparkov set behind §6a,
  customer→merchant).
- **Vendor material** (fraud.net, North Haven Analytics) - unverifiable claims, no
  denominator or baseline. fraud.net's "under 50 ms" is *scoring* latency,
  comparable to this project's few milliseconds of scoring work
  (`irp-framing.md` §7), not to its 300 ms end-to-end budget.

---

## 8. What none of these sources supplies

- **No source of real Uzbek P2P card data.**
- **No cross-platform validation** - one generator, one market; declared, not
  closable with these sources.
- **No external benchmark this system's numbers are directly comparable to** - §6
  anchors the difficulty of the *data*, not a comparison of *systems*.
- **No external evidence for receiver-side aggregation.** IBM AML supplied the only
  one and was removed as not P2P (`validation/README.md`, Rejected); PaySim has no
  collection stage, and AMLSim, removed too, ran on a daily clock that cannot see
  one.

---

## 9. Where each source is actually used

### Sources that changed the code

| Source | What was taken | Where it lands |
|---|---|---|
| **PaySim** (§6) | The rule layer went **mute** on foreign data: the highest score any fraud reached was 0.35 against a 0.40 cutoff, while the rules separated the classes 4:1. | `capabilities.scaled_threshold`, reached through `fusion.cutoffs`; adapter `validation/paysim_adapter.py`. |
| **PaySim, via `ris3abh`** (§6) | PR-AUC **0.380** on the public benchmark, reproduced at 0.397; the leaky 0.988 reproduced at 1.000. | The decomposition of the gap into features and separability - `generator-spec.md` §0, §7. |
| **IBM AMLSim** (§7; removed 2026-09-19) | `MULE_FAN_IN` at six senders fired on **3.12%** of legitimate traffic and caught **0.0%** of the fan-in typology. | `rules.PopulationBaseline`, `MULE_FAN_IN_MODE=relative`, +6.9 pp - `irp-framing.md` §6, third RQ3 result. |
| **CBU Regulation No. 3759** | The BRV-denominated threshold, and the fact that the project's earlier citation of it was wrong. | `data-generator/config.STRUCTURING_THRESHOLD`, mirrored in `stream-processor/config.py`. |
| **Cybersecurity Centre of Uzbekistan, 2025** (§6d) | **54 of 157** high-severity mobile findings are transport security. | Why transport overhead was measured at all - `irp-framing.md` §7.5, `threat-model.md` §3a. |
| **Tritscher et al. 2022** (§6b) | The published criterion a generator must meet, and the criticism of post-hoc fraud injection. | `generator-spec.md` §0 and §5. |

### Sources that changed how results are reported, and nothing else

| Source | What was taken | Where it lands |
|---|---|---|
| **Machado et al. 2026** (§2) | The "Accuracy reported / NR" evaluation table. | PR-AUC with intervals and per-type recall - `ml/README.md`, `irp-framing.md` §10. |
| **Afriyie et al. 2023** (§6a) | Precision **0.092** and F1 **0.167**, recomputed from their own tables. | The strongest single argument for this project's reporting discipline. |
| **Hemel et al. 2026** (§4) | Tabular XGBoost at **0.00** on financial fraud; only the graph model finds it. | An independent arrival at the fan-in argument. |
| **Wang, Liu, He & Du 2020** (§6c) | Relational structure beats demographics on real data, with explanations; delinquency as a label. | Receiver-side aggregation and SHAP as a pair; the label caveat as contrast. |
| **Wang 2018** (§3) | AUC 0.780 at >10% fraud, "insensitive to class balance". | Cited as contrast. |

**Saad et al. 2011** (§5) and **Zenodo 20030065** supply nothing; recorded so the
search is not repeated.

**Five sources reached the code; the rest shaped how results are reported** -
Machado, Afriyie, Hemel and both Wang papers appear nowhere in this repository
outside this file. Reporting discipline is a contribution of its own, and
claiming that all of them shaped the design would be false.
