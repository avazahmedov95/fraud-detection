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

## 6a. Afriyie et al. (2023) — the evaluation failure, in a peer-reviewed journal

*A supervised machine learning algorithm for detecting and predicting fraud in
credit card transactions.* Decision Analytics Journal 6, 100163. Elsevier.
Logistic regression, decision tree and random forest on the Sparkov-generated
Kaggle set (`kartik2112/fraud-detection`).

Their abstract: random forest "produces a maximum accuracy of 96% (with an area
under the curve value of 98.9%)", and they "recommend random forest as the most
appropriate machine learning algorithm".

Their own published confusion matrices say something else. Recomputed from
Table 7 (random forest) and Table 5 (decision tree) — 96,961 test rows, 427 of
them fraud, a prevalence of 0.44%:

| | random forest | decision tree |
|---|---|---|
| accuracy | 0.958 *(their headline)* | 0.916 |
| recall / sensitivity | 0.958 *(reported)* | 0.930 *(reported)* |
| specificity | 0.958 *(reported)* | 0.916 *(reported)* |
| **precision** | **0.092** *(not reported)* | **0.047** *(not reported)* |
| **F1** | **0.167** *(not reported)* | **0.089** *(not reported)* |
| alerts per true catch | **10.9** | 21.4 |

**A classifier that predicts "never fraud" scores 0.996 accuracy on this test
set.** The recommended model scores 0.958. Judged by the metric the paper puts
in its abstract, the recommended system is worse than doing nothing at all, and
in operation it would raise eleven alerts per fraud caught.

What makes it worth citing rather than merely wrong is that the authors write,
in their own methods section: *"Our dataset is quite unbalanced, thus comparing
the model's using accuracy as the only performance metric may not be appropriate
in this context."* They state the objection, define precision and F1 in the text,
publish the confusion matrices from which both are computable — and then headline
accuracy anyway and never print either number.

This is the concrete instance of the pattern the review in §2 reports across the
corpus as "Accuracy reported / NR". It is the strongest available argument for
this project's reporting discipline: PR-AUC as the primary figure at a 1.5%
positive rate, precision and recall always together, per-type recall with
intervals, and no headline metric whose trivial baseline beats the model.

Note also which metrics *do* get reported. Accuracy, sensitivity and specificity
are all majority-class-friendly at low prevalence; a 0.958 specificity sounds
strong and is 4,052 false alerts. Precision is the one that exposes it, and it
is the one absent.

---

## 6b. Tritscher et al. (2022) — the second precedent for generating data

*Open ERP System Data For Occupational Fraud Detection.* arXiv:2206.04460v2.
University of Würzburg. Data generated by having human participants play a
"serious game" on a real ERP system interface, with fraud scenarios modelled in
cooperation with auditing experts.

Different domain, different generation method, identical reasoning to
`generator-spec.md` §0: the real data is unavailable — there, trade secrets and
privacy; here, bank confidentiality — so the field either generates or stops.
Currently that section rests on PaySim alone. This is a second, independent,
peer-reviewed instance, and a stronger one for the argument about *openness*,
because their stated motivation is that earlier ERP generators "did not provide
data to the public, limiting open and reproducible research".

Two of their criticisms of prior work land on this project and should be
answered rather than ignored:

- **On unverifiable generators.** They reject 3LSPG because "with no data, code,
  and chosen simulation parameters available, modeling realistic ERP system data
  through this approach is challenging". This is exactly the standard
  `generator-spec.md` is written to meet — a full parameter specification, the
  code, dataset SHA-256 hashes, and a determinism proof — and it is worth saying
  so explicitly, because meeting a published criterion is a stronger claim than
  meeting one's own.
- **On post-hoc fraud injection.** They criticise the white-collar-hacking-contest
  approach because frauds are "modeled into an existing database in post,
  potentially causing unwanted divergence between normal and fraudulent data
  characteristics". **This project injects fraud into generated legitimate
  traffic (§5), so the criticism applies directly** — and the ROC-AUC of 0.999
  is precisely that divergence, observed. The honest move is to cite Tritscher
  for the mechanism rather than present the separability finding as an
  unexplained artefact of synthetic data.

---

## 6c. Wang, Liu, He & Du (2020) — graph attention on real P2P lending data

*A Graph Attentive Network Model for P2P Lending Fraud Detection.* KSEM 2020,
LNAI. Renmin University of China and Tsinghua. Model "FDNE": graph attention
with a novel edge-feature attention and global normalisation, over user
relationships plus loan-description text.

| model | precision | recall | F1 | accuracy | AUC |
|---|---|---|---|---|---|
| GCN-a | 0.727 | 0.856 | 0.786 | 0.770 | 0.823 |
| GAT-a | 0.734 | 0.885 | 0.802 | 0.784 | 0.825 |
| EGNN(A)-a | 0.736 | 0.916 | 0.816 | 0.796 | 0.829 |
| **FDNE-f** | 0.740 | 0.919 | 0.820 | 0.800 | 0.830 |

Two things to take. First, relational structure beats demographic attributes on
*real* data, and attention supplies a per-decision explanation — the same pair of
claims this project makes for receiver-side aggregation plus SHAP. Second, they
report precision and recall together, which by the standard of §6a is worth
noting.

**The label definition is the caveat, and it is a large one.** They write that
"P2P companies often regard overdue users as fraud users", and that is the label
they train on. Delinquency is not fraud; a borrower who cannot pay and a borrower
who never intended to are different people. This is a concrete instance of the
"scarce and heterogeneous fraud labels" the review in §2 identifies, and it is
worth contrasting with this project, where the label is what the generator
injected and its definition is written down.

---

## 6d. Cybersecurity Centre of Uzbekistan, 2025 annual report — the only national source

State Institution "Cybersecurity Centre", Republic of Uzbekistan (csec.uz),
annual analytical digest for 2025, in Uzbek. **The only source in this whole
assessment that reports real Uzbek numbers**, which for a thesis about the Uzbek
market makes it disproportionately valuable — for motivation and threat model,
not for data.

Load-bearing figures, used in `threat-model.md` §3a and `irp-framing.md` §7.5:

| finding | figure |
|---|---|
| mobile applications examined | 40 in 2025 vs 18 in 2024, **+122%** |
| high-severity mobile findings that are transport security | **54 of 157** (interception 33, transport security disabled 13, unencrypted transmission 8) |
| databases leaked to darknet | 37 organisations, **21M rows**, plus 1,697 login/password pairs |
| personal records whose leak was prevented | **23M+**, "close to two-thirds of the population" |
| banking/finance share of detected vulnerabilities | **22.84%**, second behind public administration (25.84%) |
| "session retained" as a high-severity finding | 82 in information systems, 8 in mobile apps |
| web-application attacks | 67M+ malicious requests, **+430%** year on year |
| cyberthreat records collected | 2,002,904, 30% critical |

Its own forward-looking conclusion states that the 122% rise in mobile
examinations confirms attacker attention is moving to smartphones **and the
financial applications on them**. That is a national authority asserting, in
2025, the premise this project is built on.

**The trap in it, and it is a real one.** Of 247 recorded incidents in 2025,
**three** are phishing; the top entry is website defacement at 219. A reader who
finds that number will conclude social-engineering fraud is marginal in
Uzbekistan. The register counts incidents **against state web resources** —
consumer APP fraud is outside both its remit and its visibility. The thesis
should quote the number *and* the scope together, because quoting it without the
scope argues against the thesis's own premise.

---

## 6e. Uzbek primary sources: scale, and a regulator that moved

Three sources that are not literature at all — they are the subject. Assessed
2026-08-31.

### The size of the thing (Central Bank of Uzbekistan)

*Xalqaro migratsiya va jismoniy shaxslarning valyuta operatsiyalari sharhi*
(Review of international migration and currency operations of individuals),
Central Bank of Uzbekistan, March 2026.

| | 2024 | 2025 | change |
|---|---|---|---|
| remittances received, total | $14,851mn | **$18,948mn** | +28% |
| — traditional MTO systems | $8,161mn | $9,903mn (52%) | +21% |
| — **P2P direct to bank cards** | $5,916mn | **$8,648mn (46%)** | **×1.4** |
| — conventional bank transfers | $774mn | $397mn (2%) | **−49%** |

P2P card-to-card went from 40% to 46% of inbound remittances in one year, while
the bank-transfer channel it is replacing halved. This is an official national
figure for the scale of the channel this project defends, and the introduction
currently has no scale figure at all.

The scoping consequence is worked out in `threat-model.md` §1: excluding
cross-border traffic excludes ~46% of a $18.9bn flow arriving on the same cards,
and on that portion receiver-side aggregation is not the best signal but the
**only** one, because the sender is not the bank's customer.

### The regulator moved, and the economics changed (from 16.11.2026)

Amended Central Bank requirements for P2P transfers, reported by uzdaily.uz and
fintech-retail.com:

- P2P transfers through **websites are prohibited** for credit and payment
  organisations.
- Logging in from a **new device deactivates the linked cards**; biometric
  identification is required to use an account from a different device, and all
  affiliated cards go inactive after a password reset.
- Organisations may **set their own limit** below which a transfer needs no
  additional confirmation.
- **Liability for fraudulent transactions performed without additional
  confirmation, within that self-set limit, falls on the credit or payment
  organisation.**
- Counterparty names must be shown **partially masked** in mobile applications.

The liability clause is the important one and is treated in `threat-model.md` §2:
it moves the loss onto the institution's own balance sheet and makes detection
quality the variable that sets a revenue-bearing limit. The device clauses are a
second instance of the prevention/detection substitution, and a cleaner one than
the `active_call` case, because they mandate as prevention exactly the capability
this project's ablation measured at **+0.000**.

The masking clause deserves one line of its own: it reduces what a victim can
verify about a destination account at confirmation time, which is a mild argument
*against* the APP victim's own defences and therefore *for* automated detection.

### P2P monitoring is also a tax programme, and that is a confound

kun.uz, 13 May 2026. From April 2026 the **Tax Committee** — not the Central
Bank — began monitoring individuals' P2P activity, with notices copied to the
Department for Combating Economic Crimes. Reported details: a budget target of
**30 trillion soum** in additional revenue for the year; cases built on
individuals with ~2,500 P2P transactions over three years, or annual card
turnover above 500mn soum; a **20% penalty** on the understated tax base; ten
days to file corrected returns. The selection criteria are **not disclosed**, and
neither is how data covered by banking secrecy was obtained.

**Why this belongs in the thesis and not in a footnote.** This project's
`STRUCTURING` rule detects transfers arranged to sit under a reporting threshold.
That behaviour is *also* the signature of undeclared trading income, and in
Uzbekistan in 2026 it is the tax authority, not the fraud function, that is
acting on it. The same detector serves two purposes with different due-process
requirements: a fraud alert protects the account holder, a tax referral is used
against them. Chapter 9 should say plainly which one this system is for, and that
the outputs are not interchangeable — an antifraud model whose alerts flow to
revenue enforcement is a different system with a different consent basis, however
identical the code.

It is also a live confound for any future validation on real Uzbek traffic:
between 2026 and whenever such data becomes available, the observable behaviour
of P2P users is being changed by tax enforcement, not only by fraud.

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

**AMLSim (IBM, open source) — the one worth actually running.** Agent-based
generator of interbank transaction graphs with eight money-laundering
typologies, and the list includes **fan-in** ("multiple accounts send substantial
funds to a single main account"), **fan-out**, **scatter-gather** and
**gather-scatter**. Records carry originator and beneficiary accounts and alert
labels; the simulator is run locally, so the data is reproducible by a reader
rather than downloaded on trust.

This matters because of a specific gap. `MULE_FAN_IN` and the receiver-side
aggregation behind it are this project's **largest measured effect** (−0.032
PR-AUC) and its only structural design finding — and they have **no external
validation at all**, because PaySim models fraud as a straight drain to cash-out
and contains no collection stage for the rule to see (`validation/README.md` §1).
AMLSim generates exactly that collection stage. It is the obvious next external
run, and the first one that could falsify the project's headline claim rather
than confirm a claim already made.

Caveat before running it: AMLSim's fan-in is an interbank AML typology, not
consumer card-to-card, so the amounts, cadence and account population differ.
That makes it a test of the **rule's shape**, not of its thresholds — which is
what the PaySim exercise established is the useful kind of transfer test anyway.

**Other datasets from the AI4FCF catalogue** (`sites.google.com/view/ai4fcf/open-datasets`),
recorded so the search is not repeated: **BankSim** (594,643 synthetic payments,
7,200 fraud, customer→*merchant* identifiers — retail, so fan-in at a receiver is
normal behaviour rather than signal); **IBM AML-Data / AMLworld** (multi-agent
generated bank transfers with laundering labels); **Amaretto** (29.7M capital-markets
transactions, 5 patterns); the **Czech financial dataset** (~1M real anonymised
transactions, 4,500 accounts, **no fraud labels**); **Libra Bank transaction graph**
(real, anonymised, with alerts); Paradise/Panama Papers (offshore records, no
transaction labels).

**`CiferAI/Cifer-Fraud-Detection-Dataset-AF`** (Hugging Face, Apache-2.0). 21M
transactions across 14 CSVs, 1.84 GB, synthetic, with `nameOrig`/`nameDest`
identifiers and two labels. Larger than PaySim and better licensed, and it is
still **PaySim's phenomenology**: the same column set (type, amount, old/new
balances both sides), the same lineage, and therefore the same missing collection
stage that makes `MULE_FAN_IN` untestable. Three times the rows does not add a
pattern that was never modelled. Two specific cautions if it is ever used: it
carries the **balance columns**, which is PaySim's documented leakage surface;
and `isFlaggedFraud` is a **system decision, not ground truth** — training or
scoring against it would repeat exactly the defect that made the Zenodo file
unusable (`validation/README.md` §2).

**`ealaxi/banksim1`** (BankSim, Lopez-Rojas & Axelsson). 594,643 synthetic
payments, 7,200 fraud, with identifiers — but customer→**merchant**. Concentration
at a receiver is ordinary merchant behaviour there, not signal, so the
receiver-side work has no meaning on it. Same disqualifier as IEEE-CIS.

**`mlg-ulb/creditcardfraud` and OpenML id 42175** are the same ULB dataset from
two hosts, already ruled out in `generator-spec.md` §0 for PCA anonymisation. Worth
knowing that this is the set the Zenodo file in §2 of `validation/README.md` was
assembled from.

**`amazon-science/fraud-dataset-benchmark`** (MIT-0). Loaders, not data, for nine
datasets: IEEE-CIS, ULB credit card, an e-commerce set, Sparkov, Twitter bots,
malicious URLs, fake job postings, vehicle-loan default, IP blocklist. **Its
value here is a negative result.** The most prominent attempt to standardise
fraud-detection benchmarking spans bot detection, URL classification and content
moderation, and contains **no P2P payment dataset at all**. That is direct
evidence for the gap claim in §8 — not an oversight by its authors, but a
consequence of the same confidentiality that keeps P2P transfer data private.

**`northhavenanalytics.com/fraud-detection-guide`** is a vendor guide from a
synthetic-data consultancy: no original data, no benchmarks, built around the ULB
dataset to argue for buying synthetic data. Same category as the fraud.net page
below — usable as an illustration of how claims are made in the commercial
literature, not as a source.

**Kaggle `sriharshaeedala/financial-fraud-detection-dataset`** is a re-upload of
PaySim, already held locally in `validation/`. **Kaggle `kartik2112/fraud-detection`**
is the Sparkov-generated card set used by Afriyie et al. (§6a); it carries a
customer identifier and a *merchant*, not a P2P counterparty, so it has the same
disqualifying shape as IEEE-CIS for the receiver-side work. Its value here is
that it is the dataset behind §6a.

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
- **The receiver-side finding is still unvalidated externally** — but that is now
  a task rather than a limitation, since AMLSim (§7) generates the fan-in stage
  PaySim lacks. Until it is run, the strongest claim in this project rests on one
  generator, and that generator is this project's own.
