# Related work

What each source is, and what we take from it.

## "P2P" means three different things

P2P **payments** between people (this project), P2P **lending** (online loans
between people) and P2P **networking** (computer networks). Several of the first
papers we found turned out to be about lending or networks, so searches have to
say "payments".

## How fraud detection should be measured

- **Machado et al. (2026)**, *Anatomy of peer-to-peer (P2P) lending fraud: A review
  with managerial implications*, International Journal of Information Management
  Data Insights 6, 100425. A review of 53 studies of lending fraud. Most of them
  report only accuracy. We use its list of data problems (few labels, very few
  frauds, fraud that changes, links between accounts, explanations) as a checklist.
  It is about lending, not payments.
- **Afriyie et al. (2023)**, *A supervised machine learning algorithm for detecting
  and predicting fraud in credit card transactions*, Decision Analytics Journal 6,
  100163. Reports 96% accuracy. From their own tables, precision is 9.2% and F1
  16.7%: about 11 alerts for every fraud caught. A model that never says "fraud"
  would score 99.6% accuracy on their test set. This is why we report recall,
  precision, F1 and PR-AUC, not accuracy.
- **Wang (2018)**, *Detection of fraudulent users in P2P financial market*, MATEC
  Web of Conferences 189, 06004. Reports only AUC 0.780 on a lending platform with
  over 10% fraud. Used as a contrast.

## Links between accounts matter

- **Hemel, Hallaji & Razavi-Far (2026)**, *A Benchmark Dataset for Financial Fraud
  Transaction and Behavioral Risk Detection in Metaverse Ecosystems*,
  arXiv:2607.09528. On financial fraud a table model (XGBoost) scores 0.00; only a
  graph model finds it. The same idea as our mule finding: a view of one transfer
  at a time cannot see a pattern made of several transfers.
- **Wang, Liu, He & Du (2020)**, *A Graph Attentive Network Model for P2P Lending
  Fraud Detection*, KSEM 2020. On real lending data, links between accounts beat
  personal attributes, and each decision is explained. But their label is late
  payment, not fraud.

## Public data and how to read scores on it

- **PaySim**, and the open repository `ris3abh/aml-p2p-fraud-detection` (MIT
  licence). It reports PR-AUC 0.380 on PaySim's last 7 days (1.142% fraud); we
  reproduced 0.397. With PaySim's balance columns the score jumps to 0.988-1.000,
  because those columns give the answer away. So a very high score on public data
  can mean a broken model.
- **Tritscher et al. (2022)**, *Open ERP System Data For Occupational Fraud
  Detection*, arXiv:2206.04460. They also generate fraud data because real data is
  not available. They ask that a generator publish its code and parameters, which
  ours does, and warn that fraud added on top of normal data is too easy to find.
  Ours adds fraud this way too, which is part of why our scores are so high.

## Sources from Uzbekistan

- **Cybersecurity Centre of Uzbekistan, annual report 2025** (csec.uz):
  - 40 mobile apps checked, against 18 in 2024;
  - 54 of 157 serious app flaws were about protecting data in transit;
  - 21 million leaked database rows and 1,697 login and password pairs found on the
    darknet;
  - banking and finance has the second largest share of flaws (22.84%).

  Only 3 of its 247 incidents were phishing, but it records attacks on state
  websites, not fraud against customers.
- **Central Bank of Uzbekistan, *Review of international migration and currency
  operations of individuals*** (March 2026). $18.9 billion received from abroad in
  2025; $8.6 billion of it (46%) as P2P transfers to cards, 1.4 times more than a
  year before.
- **Central Bank requirements for P2P transfers, from 16 November 2026:**
  - no transfers through websites;
  - a new device switches the linked cards off;
  - each bank sets its own limit for transfers without extra confirmation, and pays
    for fraud within it;
  - payee names are masked.
- **Anti-money-laundering rules for banks, No. 343-В-12** (3 March 2023, registered
  2886-10). Suspicious P2P activity is defined by counts over up to 30 days
  (`threats.md`). This led to our day-and-week counterparty features.
- **CBU resolution 3759** (21 January 2026). The bank app must block use during
  calls. The source of our reporting threshold in BRV.
- **kun.uz, 13 May 2026.** From April 2026 the Tax Committee watches people's P2P
  activity. Our structuring rule would also catch undeclared trading income. This
  system is meant to protect account holders; tax checks would confuse any test on
  real Uzbek data.

## Model families

- **TabPFN** (Prior Labs, checkpoint 3.5). A ready-trained model for tables. In our
  tests it ranked fraud a little better than our model (PR-AUC 0.968 against 0.954).
  But it answered about 3,000 times slower, so we use it only as the second look.
- **arXiv:2407.00956**, *A Closer Look at Deep Learning Methods on Tabular
  Datasets*. Tree models stay very strong on tables. This agrees with our
  comparison: the tree models were ahead of SVM and logistic regression.
- **arXiv:2410.17758**, *A Neural Network Alternative to Tree-based Models*. Built
  for gene data with thousands of columns; our data has 24. Read, not tried.
- **ACM Computing Surveys (2026)**, *A Survey on Tabular Data: From Tree-based
  Methods to Tabular Deep Learning*, doi 10.1145/3807777. Background only.

## Sources that changed the system

| Source | What changed |
|---|---|
| PaySim | rule limits now follow the data that is available |
| AML rules No. 343-В-12 | counting a card's counterparties over a day and a week |
| CBU resolution 3759 | the reporting threshold |
| Cybersecurity Centre report | we measured the cost of encryption and mutual TLS |
| Tritscher et al. | the generator's rules and code are published |
| TabPFN | the second look |

The other papers changed only how we report results.

## Datasets we looked at and did not use

- **Kaggle credit card set (ULB)**: the columns are anonymised, so no reason can be
  given for a decision. Its 0.17% fraud rate is the real card rate our data is held
  against.
- **IBM Synthetic Data Sets**: P2P payments with labels, but a paid product.
- **Kaggle UPI sets**: themselves generated, with no description of how.
- **BankSim, Sparkov**: people paying shops, not people.
- **AMLSim**: tried and removed; its daily clock cannot see a collection stage.

## What no source gives us

- real Uzbek P2P transfer data;
- a system we could compare with directly;
- public P2P evidence for the payee side. The closest is IBM AML, transfers between
  bank accounts, where fan-in is the best-caught scheme.
