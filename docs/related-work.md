# Related work and external datasets

Working note, not thesis text. Each entry says what the source is, what it
supports in this project, and — the part that matters — what it does **not**
support. Sources assessed 2026-08-31.

The organising question is not "is this about fraud" but "does this constrain a
claim this project makes". Several sources below are about something else
entirely and are recorded so the same search is not run twice.

---

## 1. The term "P2P" is ambiguous, and the ambiguity is expensive

Three unrelated literatures share the abbreviation:

| Sense | Domain | Relevance here |
|---|---|---|
| P2P **payments** | card-to-card / instant transfer between individuals | **this project** |
| P2P **lending** | marketplace credit, borrower↔lender matching | different fraud taxonomy; methodology transfers, findings do not |
| P2P **networking** | peer-to-peer protocols, botnet C&C | none |

This is worth one sentence in the methodology chapter, because it changes what a
literature search returns. Two of the six documents assessed here turned out to
be the second sense and one the third.

---

## 2. Machado et al. (2026) — systematic review of P2P **lending** fraud

*Anatomy of peer-to-peer (P2P) lending fraud: A review with managerial
implications.* International Journal of Information Management Data Insights
6, 100425. Twente / Oradea / Bern / Hamburg / Cambridge CCAF / Utrecht.
53 studies, predefined eligibility criteria.

**The most useful single source in this batch.** Its consolidated constraints
read as an independent checklist against this system's design decisions:

| Review finding | This project |
|---|---|
| "scarce and heterogeneous fraud labels" | labels are generated, and the generator spec states their construction (`docs/generator-spec.md` §5) |
| "severe class imbalance" | 1.5% positive rate; PR-AUC reported rather than ROC-AUC (`ml/README.md`) |
| "limited cross-platform transferability" | **not addressed** — one generator, one market. An honest gap, see §7 below |
| "temporally evolving fraud behaviour" | drift-vs-evasion test in `docs/threat-model.md` §5 |
| relational modelling "that captures coordinated activity (graph-based features and network representations)" | receiver-side aggregation, `MULE_FAN_IN`, Neo4j |
| explainability "to support auditability and user trust" | SHAP reason codes per alert |
| "strong within-platform performance often degrades under temporal splits or platform shifts, underscoring the need for drift-aware validation" | time-ordered split in `train.py`; no platform-shift test exists |

Its evaluation-practice table is the citable part: study after study is recorded
as **"Accuracy reported"** with *NR* (not reported) in every other column. In a
literature where accuracy on a 0.1–10% positive rate is the headline number,
reporting PR-AUC with intervals and per-type recall is not a courtesy — it is the
contribution being made about method.

**What it does not support.** P2P *lending*, not payments. Its fraud taxonomy —
identity abuse, misrepresentation, loan stacking, collusion, predatory lending,
platform misconduct — does not contain APP, ATO or muling as this project defines
them. Cite it for evaluation practice, data constraints and modelling trends.
Do not cite it for fraud rates, feature choices, or anything about instant
transfers.

---

## 3. Wang (2018) — P2P lending fraud at HC Financial

*Detection of fraudulent users in P2P financial market.* MATEC Web of
Conferences 189, 06004 (MEAMT 2018). Random forest and GBDT, ~35 features,
one Chinese lending platform.

Value is as **contrast**, not support.

- Reported **AUC 0.780 (test) / 0.797 (validation)** with tanh feature scaling.
  No precision, no recall, no confusion matrix, no interval.
- Its stated rationale — AUC "because it is insensitive to class balance ratio" —
  is precisely the reasoning `ml/README.md` argues against. At their >10% fraud
  rate the choice is defensible; carried into a 0.1–1.5% setting it is not, and
  the paper offers no such caveat.
- Their fraud rate ">10%, sometimes a lot higher" versus 1.5% here is the
  cleanest single illustration that lending and payments are different problems.
  They write that the high rate "saves the day for algorithm engineers"; a
  payments system does not get that.

Also note the knowledge graph they describe — phone logs, ID cards, addresses,
kinship edges — is the same shape as the MyID kinship integration this project
**measured and found worthless** (+0.004 PR-AUC). A source that asserts the value
of graph identity data, next to a measurement that it adds nothing here, is a
better citation than one that agrees.

---

## 4. Hemel, Hallaji & Razavi-Far (2026) — TSAI-MetaFraud

*A Benchmark Dataset for Financial Fraud Transaction and Behavioral Risk
Detection in Metaverse Ecosystems.* arXiv:2607.09528v1, 10 July 2026. UNB.
Dataset at `github.com/tsai-unb/MetaFraud`.

Multimodal benchmark built on OpenSimulator: avatar behaviour, transactions,
and interaction graph, with four tasks (transaction fraud detection, cross-modal
node classification, temporal link prediction, weakly supervised detection).
Strict **inductive** split — test avatars unseen in training.

**The result worth taking is not about the metaverse.** Their Table VII, on
transaction fraud detection:

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

A gradient-boosting model on tabular features — the same family as this
project's LightGBM — scores 0.98 on the majority class and **exactly zero** on
both financial-fraud classes. The graph model is worse on benign traffic and is
the only one that finds financial fraud at all.

That is structurally the same finding as this project's fan-in result
(`ml/README.md`, "Fan-in: a blind spot in a sender-keyed stream"): a per-record
tabular view cannot express a pattern defined over the relation between records,
and no amount of tuning recovers it. Reached independently, in a different
domain, with different models, in the same month. **Two independent arrivals at
the same structural claim is worth more than either alone**, and it is the
strongest external support in this batch for the receiver-side aggregation
argument.

**What it does not support.** Virtual currency in a simulated world; no
regulator, no settlement finality, no card networks, no session-level banking
telemetry. Cite the tabular-vs-graph contrast. Do not cite its absolute numbers
as comparable to anything here.

---

## 5. Saad et al. (2011) — P2P botnets

*Detecting P2P Botnets through Network Behavior Analysis and Machine Learning.*
PST 2011.

**Not relevant.** "P2P" here is peer-to-peer *networking*: botnet command and
control, detected from network flow behaviour. No financial transactions, no
fraud in the payments sense. Recorded so the search is not repeated. Its only
use is as the example in §1 above.

---

## 6. `ris3abh/aml-p2p-fraud-detection` (MIT) — the calibration point

PaySim, 6.36M mobile-money transactions, 0.129% fraud, CatBoost with
`scale_pos_weight=974`, isotonic calibration. Self-reported by the repository:

| metric | value |
|---|---|
| AUPRC (= PR-AUC) | **0.380** |
| AUC-ROC | 0.908 |
| recall | 49.4% (916 of 1,854) |
| top-decile lift | 7.0× |
| Brier (calibrated) | 0.0086 |

These are the author's own figures, not independently reproduced here.

**This is the most uncomfortable and therefore the most valuable item in the
batch.** PR-AUC 0.380 on a public dataset against **0.966 ± 0.008** here is a
factor of 2.5, and the commission will ask about it. The answer is already the
project's position — the generator produces a *design fixture* whose classes are
separable by construction (`generator-spec.md` §0, §7) — but the position is
currently argued qualitatively. This supplies the number that makes it
quantitative: not "our data is probably easier", but "our data is 2.5× easier
than the standard public benchmark, measured".

Two further points from the same repository, both independently confirming
things this project already asserts:

- They found and removed **balance-derived leakage** in PaySim. `generator-spec.md`
  §7 already cites PaySim's balance leakage as the cautionary precedent; here is
  a 2025-era project rediscovering it in practice.
- They report an **11× increase in fraud rate from train to test** and treat it
  as temporal drift. That is an empirical instance of the drift the review in §2
  says is under-tested.

**What it does not support.** Batch, notebook-based, no streaming; mobile money,
not card P2P. It is a comparison of *model difficulty on a dataset*, nothing else.

---

## 7. Datasets considered and not adopted

Extends the table in `generator-spec.md` §0.

**IBM Synthetic Data Sets (SynDS).** As of October 2025 includes P2P payment
data modelled on Venmo/Zelle-style platforms, with — per IBM's description —
labelling of 100% of criminal activity, perpetrator identities, transaction
purposes and money-laundering pathways. The closest thing to an off-the-shelf
substitute for this project's generator.

Rejected for three reasons, in order of weight: it is a **commercial product**,
so a thesis built on it is not reproducible by a reader who does not buy it (the
Apache-2.0 GitHub repository `IBM/IBM-Synthetic-Data-Sets` publishes **schemas
and DDL only**, not the data); its transfer semantics are US consumer apps, not
UzCard/HUMO card-to-card; and it carries no session-level signals of the kind
CBU 3759 makes relevant. Worth **citing** as evidence that full-label synthetic
P2P payment data is an accepted industry instrument — which is exactly the
argument `generator-spec.md` §0 makes.

The accompanying IBM community post reports a bank that "went from only scoring
20% of their transactions to scoring 100%" after moving the model on-platform.
Usable as motivation for why scoring must be cheap enough to run on everything —
a coverage argument, adjacent to this project's latency budget. A figure of
"4.5 µs decision latency" appears in the **comments** on that post, not in IBM's
text; do not cite it as an IBM claim.

**Kaggle UPI transaction datasets** (several, e.g. `skullagos5246/upi-transactions-2024`,
`kalpitlabs/upi-fraud-detection-dataset-india-synthetic`). India's UPI is the
closest real-world analogue to the setting here: a national instant-payment rail
with per-transaction fraud pressure. All the ones found are **themselves
synthetic**, uploaded without a generating specification. A synthetic dataset
whose assumptions are undocumented is strictly worse than one whose assumptions
are written down, which is the whole argument of `generator-spec.md` §7. Column
lists and licences could not be confirmed from outside Kaggle and would need
checking before any use.

**fraud.net P2P AI fraud automation** (vendor page). Claims: scoring "in under
50 milliseconds", "97% Fewer False Positives", "88% Fraud Reduction", "600+
fraud schemes", behavioural biometrics and device signals.

None of it is verifiable — no denominator, no baseline, no dataset, no
definition of a false positive. Its use in the thesis is as an **object lesson
about evaluation**, alongside the review in §2: the commercial claims and much of
the academic literature share the same defect, which is reporting a number
without the conditions under which it was obtained. That is the gap the
measurement discipline in `docs/irp-framing.md` is built against.

The one figure worth engaging is the 50 ms. It is *scoring* latency, not
end-to-end: the comparable figure here is 4.3–6.4 ms of scoring work
(`irp-framing.md` §7.5a), inside a decision path of 62–87 ms whose remainder is
framework buffering. Comparing their 50 ms to this project's 300 ms budget would
be comparing different quantities.

---

## 8. What none of these sources supplies

Stated so the gap is not mistaken for an oversight:

- **No source of real Uzbek P2P card data.** Unchanged.
- **No cross-platform validation.** The review in §2 names transferability as a
  central constraint of the field, and this project has one generator and one
  market. It cannot be closed with the sources above; it can only be declared.
- **No external benchmark this system's numbers are directly comparable to.**
  §6 gives a difficulty anchor for the *data*, not a comparison of *systems* —
  different dataset, different task, batch versus streaming.
