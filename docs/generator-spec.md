# Synthetic data generator: mathematical specification

Working note addressing reviewer point 2. Not thesis text.

Every distribution, parameter and dependency used to produce the dataset, stated
formally enough to reimplement from this document alone. Constants are quoted
from `data-generator/config.py`; where the code and this document disagree, the
code is authoritative and this document is a bug.

---

## 0. Why generate data at all

No public dataset of Uzbek P2P card transactions exists — confirmed by search,
and unsurprising: the data is bank-confidential and the market is small enough
that anonymisation would not protect participants.

The alternatives were considered and rejected:

| Option | Why not |
|---|---|
| IEEE-CIS (590k txns) | Real, but e-commerce card-not-present, not P2P. Different fraud shapes, no counterparty, no session signals. |
| CCF / Kaggle credit card | PCA-anonymised into V1..V28. Features have no meaning, so SHAP explanations — a hard requirement under CBU 3759 — are meaningless. |
| Zenodo 20030065 (57k txns) | Closest match: 30 days of production online banking, includes response latency. Retained as a **validation** set (§7), not as a source for the design. |
| IBM Synthetic Data Sets (SynDS) | Since Oct 2025 includes P2P payment data modelled on Venmo/Zelle, fully labelled. Closest off-the-shelf substitute, and rejected mainly because it is a **commercial product**: a thesis built on it is not reproducible by a reader who has not bought it. The Apache-2.0 repo publishes schemas and DDL only. US consumer-app semantics, no session signals. |
| Kaggle UPI sets (several) | India's UPI is the closest real instant-payment rail. But every set found is **itself synthetic and published without a generating specification** — strictly worse than a documented generator, by the argument in §7. |

Assessed in full, with what each does and does not support, in
`docs/related-work.md`.

The V1..V28 objection above is not hypothetical, and the Zenodo file in the row
above demonstrates it. 432 of its rows carry a completely different feature
schema in the same `v1..v28` slots — a PaySim-shaped record with 81.6% of the
cells zero and `v2 == amount` throughout — and nothing in the file marks the
change, because a column named `v14` asserts nothing that could be violated.
Anonymised features do not only cost explainability, which is the objection
above; they remove the reader's ability to notice that the wrong quantity is in
the column. `validation/zenodo_provenance.py` makes the split visible.

So the generator produces a **design fixture**: a dataset whose statistical
structure is stated up front, used to develop and instrument the pipeline. Every
metric derived from it is a design target, not a finding.

---

## 1. Notation

- $\mathcal{U}(a,b)$ — continuous uniform; $\mathcal{U}\{a,\dots,b\}$ — discrete uniform on integers, upper bound inclusive
- $\mathcal{N}(\mu,\sigma^2)$ — normal
- $\text{LogN}(\mu,\sigma)$ — lognormal, i.e. $\exp(X)$ with $X \sim \mathcal{N}(\mu,\sigma^2)$; $\mu,\sigma$ are parameters of the **underlying normal**
- $\text{Bern}(p)$ — Bernoulli
- $\text{Cat}(\mathbf{p})$ — categorical with probability vector $\mathbf{p}$
- $\text{clip}(x,a,b) = \min(\max(x,a),b)$

All randomness comes from one `numpy.random.default_rng(seed)` stream (PCG64),
default seed 42. The dataset is a deterministic function of the seed.

---

## 2. Population

$N_p = 5000$ persons, generated in household clusters until the count is reached.

**Household size.** $S \sim \mathcal{U}\{1,\dots,6\}$. Members of a household
share a region and are treated as MyID-verified relatives (§5.2).

**Region.** Per household, $R \sim \text{Cat}(\mathbf{w}_R)$ over the 14
administrative divisions, $\mathbf{w}_R$ approximating population share
(Tashkent City 0.18, Fergana 0.11, Samarkand 0.11, …, Karakalpakstan 0.01).

**Account age**, days, a two-component mixture that prevents "new account" from
being a fraud-exclusive signal:

$$
A \sim \begin{cases}
\mathcal{U}\{1,\dots,29\} & \text{w.p. } 0.12 \\
\mathcal{U}\{30,\dots,3649\} & \text{w.p. } 0.88
\end{cases}
$$

**Spend baseline.** $T \sim \text{LogN}(11.8, 0.6)$ UZS, median
$e^{11.8} \approx 133{,}000$ UZS. Lognormal because income and spend are
multiplicative processes; the parameters put the bulk in the 50k–350k range with
a right tail into the millions.

**Active hours.** $H_{\text{start}} \sim \mathcal{U}\{6,\dots,10\}$,
$H_{\text{end}} \sim \mathcal{U}\{18,\dots,23\}$, per person.

**Personal confirmation-time median.**
$m_i = 40 \cdot \exp(\mathcal{N}(0, 0.35^2))$ seconds. This is what makes
`secs_login_z` a *personal* baseline: a deliberate pensioner and a fast
20-year-old are both normal relative to themselves.

**Card and issuer.** Issuer sampled $\propto$ cards in circulation (CBU figures
as at 1 April 2026, via Kursiv; `banks.csv`), *not* uniformly — uniform
assignment would make the on-us rate an artefact of the number of banks in the
list. PAN = 6-digit BIN + 9 uniform digits + Luhn check digit. PINFL = 14 uniform
digits. Both synthetic.

**Fraud accounts.** $N_f = \max(50, N_p/25) = 200$, each its own household (so
mules are not "relatives" of each other). Age:

$$
A_f \sim \begin{cases}
\mathcal{U}\{100,\dots,1499\} & \text{w.p. } 0.30 \quad \text{(aged/farmed)} \\
\mathcal{U}\{1,\dots,44\} & \text{w.p. } 0.70
\end{cases}
$$

The aged 30% is what stops `receiver_age` from being a perfect separator.

---

## 3. Payee graph

Each person is assigned $K \sim \mathcal{U}\{3,\dots,8\}$ frequent payees. Each
draw is a relative with probability $\phi = 0.35$ (if the household has any),
otherwise uniform over the population.

$\phi > 0$ is load-bearing: an earlier revision had **no** fraud routed to
relatives, which made `is_family` separate the classes by construction and rank
first in SHAP. See §5.2 and `docs/threat-model.md`.

---

## 4. Legitimate traffic

$n_{\text{legit}} = (1 - 0.015) \cdot 50{,}000 = 49{,}250$ events.

**Sender.** Heavy-tailed activity: draw $u_i \sim \mathcal{U}(0,1)$ per person,
set activity weight $\propto u_i^3$, sample senders from
$\text{Cat}(\mathbf{u}^3 / \sum \mathbf{u}^3)$. The cube is a crude power-law
stand-in: a few very active senders, most rarely transacting.

**Payee.** Frequent payee w.p. 0.95; a fresh uniform draw w.p. 0.05.

**Hard negatives** (share $\eta = 0.03$): legitimate transfers deliberately
shaped like APP fraud — new payee, and w.p. 0.5 a large one-off
$\text{LogN}(15.0, 0.5)$ (median ≈ 3.3M UZS). Without these the classes separate
trivially on `is_new_payee` × amount.

**Amount.** $X \sim \text{clip}(\text{LogN}(\log T_i, 0.5), 1000, 5\times10^7)$ —
centred on the sender's own baseline, so deviation is meaningful per person.

**Timestamp.** Uniform over the 30-day span, then the hour is replaced by
$\mathcal{U}\{H_{\text{start}}, H_{\text{end}}\}$ and minute/second uniform.

> **Known limitation.** This makes the hour marginal per-person plausible but
> destroys within-day autocorrelation and any weekday/weekend or payday
> structure. Real P2P traffic has strong periodicity. A model could not learn
> temporal seasonality from this data, and `hour` should be read as a weak
> feature rather than a meaningful one.

**Channel.** $\text{Cat}(0.70, 0.12, 0.13, 0.05)$ over
MOBILE_APP / USSD / WEB / ATM.

**Region.** The sender's home region, unless travelling (§6).

**Balance before.** $X \cdot \mathcal{U}(1.2, 8.0)$ — mechanically consistent
rather than modelled.

---

## 5. Fraud injection

$n_{\text{fraud}} = 0.015 \cdot 50{,}000 = 750$ events, allocated by **transaction**
budget: APP 0.35, MULE 0.25, ATO 0.20, STRUCTURING 0.20. Because episodes emit
differing numbers of events, realised counts differ from a naive per-episode
split (observed: APP 251, MULE 196, STRUCTURING 152, ATO 151).

### 5.1 Session signals (all patterns)

Confirmation latency, seconds:

$$
L = \max\left(3.0,\; m_i \cdot \kappa \cdot \exp(\mathcal{N}(0, 0.55^2))\right)
$$

$$
\kappa \sim \begin{cases}
\mathcal{U}(2.5, 5.0) & \text{APP — victim listening to instructions} \\
\mathcal{U}(0.4, 0.7) & \text{ATO — attacker in a hurry} \\
1 & \text{otherwise}
\end{cases}
$$

Active call: $\text{Bern}(0.70)$ for APP, $\text{Bern}(0.03)$ otherwise.

The $\kappa$ mechanism is why `secs_login_z` is **non-monotonic**: APP pushes
right, ATO pushes left, and only a personal z-score in log space sees both.

### 5.2 APP — authorised push payment

One event per episode. Victim uniform from the population. Payee is a relative
w.p. $\psi = 0.10$ (complicit family, a documented variant), else a fraud
account.

Balance $B \sim \text{LogN}(15.5, 0.6)$ (median ≈ 5.4M UZS). Amount:

$$
X \sim \begin{cases}
\text{clip}(\text{LogN}(14.0, 0.5), \cdot) & \text{w.p. } 0.40 \quad \text{(moderate — overlaps legitimate)} \\
\text{clip}(B \cdot \mathcal{U}(0.5, 0.95), \cdot) & \text{w.p. } 0.60 \quad \text{(drain)}
\end{cases}
$$

The 40% moderate branch exists so APP is not trivially "the largest transfers".

### 5.3 ATO — account takeover

$\mathcal{U}\{2,3,4\}$ events per episode, spaced $i \cdot \mathcal{U}(1,4)$
minutes. Stealth w.p. 0.40: victim's own device and region (on-device malware),
leaving only behavioural signals.

Non-stealth (0.60) anchors to the victim's real history: pick a random prior
legitimate event $(t_j, r_j)$, set the session $\Delta \sim \mathcal{U}(3,45)$
minutes later, from a region $r'$ satisfying

$$
d(r_j, r') \ge 100 \text{ km} \quad\wedge\quad \frac{d(r_j, r')}{\Delta/60} > 900 \text{ km/h}
$$

If no such region exists, the session stays at $r_j$ rather than inventing a
journey.

> **This makes the injected impossible-travel pattern detectable by
> construction** — the generator and detector share the reachability threshold.
> The detection rate on it is therefore **not a result**. The reportable result
> is the false-positive rate on the independently-generated legitimate journeys
> of §6: **0 of 775**.

Amount $\text{clip}(\text{LogN}(14.5, 0.5), \cdot)$ per event.

### 5.4 STRUCTURING

$\mathcal{U}\{5,\dots,11\}$ events, spaced $i \cdot \mathcal{U}(3,15)$ minutes,
each $X = 10^7 \cdot \mathcal{U}(0.85, 0.99)$ — deliberately just under the
reporting threshold.

### 5.5 MULE — fan-in then fan-out

Mule is an ordinary recruited person w.p. $\rho = 0.30$, else a purpose-made
fraud account. Only recruited mules have relatives, hence family fan-in.

Fan-in: $n_{\text{in}} \sim \mathcal{U}\{4,\dots,8\}$ senders, each a relative of
the mule w.p. $\psi = 0.10$ else uniform, amount
$\text{clip}(\text{LogN}(13.5, 0.6), \cdot)$, spaced $i \cdot \mathcal{U}(1,6)$
minutes.

Fan-out: $n_{\text{out}} \sim \mathcal{U}\{1,2\}$ transfers of
$\frac{1}{n_{\text{out}}} \sum X_{\text{in}} \cdot \mathcal{U}(0.80, 0.98)$.

> Counting inbound transfers per payee in the output gives values outside
> $[4,8]$: low counts are fan-**out** destinations (which receive 1–2 transfers,
> not 4–8), and counts above 8 are accounts that served as the mule in more than
> one episode, since the pool of 200 fraud accounts is sampled with replacement.

The concentration is the pattern, and §RQ3 of the framing note shows it is
invisible to sender-keyed state — 80% of mule events are fan-in legs, and recall
on them was 57.8% until receiver-keyed state was added.

---

## 6. Travel (the negative control)

Each person is selected for travel w.p. $\tau = 0.18$ and then attempts
$\mathcal{U}\{1,2\}$ journeys. A journey to region $r'$ departs at a uniform
time, takes $d(r, r')/70$ hours at road speed, stays $\mathcal{U}(12, 96)$ hours,
and returns symmetrically. Transactions during the stay carry $r'$; transactions
falling *in transit* are re-timed to $\mathcal{U}(5,180)$ minutes after arrival.

> **Realised share is 16.7%, not 18%.** A journey whose randomly drawn
> destination equals the person's home region is skipped, so a person selected
> for one journey who draws their own region ends up with no plan. $\tau$ is the
> selection probability, not the observed traveller share — a distinction worth
> keeping because the observed figure is what a reader can check.

That re-timing is essential: an event at the origin followed minutes later by one
at the destination would manufacture impossible travel inside legitimate traffic.

Distances use the detector's own coordinate table
(`stream-processor/geo.py`, administrative centres, haversine) so simulation and
detector cannot disagree about geography. Ground speed 70 km/h is deliberately
conservative — slower travel means longer journeys, which makes the negative
control *harder*, not easier.

---

## 7. Defence of the approach

**Why parametric sampling rather than copulas or a GAN.**

Both alternatives estimate a joint distribution **from data**. There is no Uzbek
P2P data to fit — that is the premise. Fitting a GAN to IEEE-CIS or the Kaggle
credit-card set would produce synthetic *e-commerce card-not-present* data
wearing Uzbek field names: the covariance structure learned would be that of a
different payment system, and the resulting realism would be spurious. Copulas
have the same problem one level down — a copula needs an empirical dependence
structure to reproduce.

The precedent is PaySim (Lopez-Rojas et al.), which is parametric and agent-based
for the same reason: mobile-money transaction data was unavailable, so the
generator encodes stated assumptions instead of fitted ones. That trade is the
point — **a parametric generator's assumptions are legible and falsifiable,
whereas a fitted generator's are implicit in weights.** Every number in §§2–6 can
be argued with; a GAN's cannot.

PaySim also supplies the cautionary precedent: its balance columns leak the label,
and models trained on it report near-perfect scores that mean nothing. This
project's equivalents are recorded rather than hidden — see §8.

**On the ROC-AUC ≈ 0.999 this data produces.** It is an artefact of a generator
whose classes are separable by construction along several axes at once. PR-AUC
(0.966 ± 0.008 across seeds) is the figure to read at a 1.5% positive rate, and
even that is a design target.

---

## 8. What this generator does not model

Stated because a specification that only lists what is included is not a
specification.

1. **Temporal structure.** No weekday/weekend effect, no payday spikes, no
   within-day autocorrelation (§4). `hour` is consequently a weak feature.
2. **Merchant or biller flows.** P2P only; real card traffic is mostly neither.
3. **Adaptive adversaries.** Fraud parameters are fixed. An attacker who observes
   the detector and adapts is out of scope for the data and treated analytically
   in `docs/threat-model.md`.
4. **Network effects between fraud episodes.** Episodes are independent; real
   mule networks share infrastructure, devices and timing.
5. **Legitimate account takeover-like behaviour.** A user genuinely switching
   phone and city simultaneously is rare here and would be a false positive.
6. **Amount rounding.** Real transfers cluster on round numbers (100k, 500k);
   these are continuous lognormal draws. A model could not learn round-number
   effects, which are a real signal.
7. **Label noise.** Ground truth is exact. Real fraud labels arrive late, are
   incomplete, and include disputed chargebacks that were not fraud.

Items 1, 6 and 7 are the ones most likely to make measured performance optimistic
relative to production.

---

## 9. Reproduction

```bash
cd data-generator && python generator.py --out ./out          # seed 42, defaults
python generator.py --seed 7 --persons 5000 --transactions 50000
```

Output is a deterministic function of `(seed, n_persons, n_transactions,
fraud_rate, days, start_date)` plus `banks.csv` - but only since the payee
ordering was fixed on 2026-08-30; see "Determinism is not free" below. The
multi-seed ablation
(`ml/ablation_seeds.py`) regenerates across 20 seeds; between-seed variation in
baseline PR-AUC is ±0.008–0.035 depending on configuration, which is why no
single-dataset figure is quoted anywhere in this project.

### Determinism is not free, and the seed does not buy it

Until 2026-08-30 the claim above was false, and the failure is worth recording
because none of this project's guards could see it.

`_assign_payees` collected each sender's frequent payees in a `set` and stored
`list(chosen)`. Iterating a set of strings orders them by hash, and CPython
randomises string hashing per process unless `PYTHONHASHSEED` is fixed. The
payee list therefore came out in a different order in every interpreter, and the
receiver is drawn from that list by index.

Measured on two runs differing in nothing but `PYTHONHASHSEED` - same seed, same
source, same pinned versions:

| file | result |
|---|---|
| `persons.csv` | byte-identical |
| `transactions.csv` | 36,072 of 50,000 rows differ |

Every differing row carried the same sender, timestamp, amount, channel, device,
region and session signals, and a different receiver. With
`payees[p.pinfl] = sorted(chosen)` the output is byte-identical across
`PYTHONHASHSEED` 1, 2 and 99.

Two consequences outlast the one-line fix:

- **Pinned dependencies are necessary and not sufficient.** Every declared
  version matched across those runs; what differed was an undeclared property of
  the interpreter process. `data-generator/requirements.txt` argues that pinning
  numpy is what makes `seed = 42` mean the same dataset next year. That argument
  is correct and incomplete.
- **The ablation's version guard cannot catch this.** `ml/ablation_seeds.py`
  fingerprints the feature set and the generator sources and refuses to mix
  results across versions. The guard assumes identical sources imply identical
  data. Two runs with the same fingerprint could stand on different datasets.

`transaction_id` remains a bare `uuid.uuid4()`, drawn outside the seeded stream,
so that column alone is still not reproducible. It carries no analytical content
and every comparison above was taken with it removed.

### The dataset of record

`data-generator/out/` is gitignored, so the files every reported figure was
computed on are pinned here instead. Generated 2026-07-19 on seed 42 with
defaults.

```
transactions.csv   50,000 rows   16,162,896 bytes
  sha256  b767f38489ab65628028b91638ca6cbfa7e0377128c0f86e844dffb35e0db596
persons.csv         5,200 rows      546,044 bytes
  sha256  010cddd6a60f30ee322dfd8c57643db87636f0ba1c3358e02d66711fcd9e463f
```

Both were written with LF line endings. `pandas.to_csv` takes its terminator
from `os.linesep`, so they were not produced on the Windows host, and a
regeneration there differs in hash for that reason alone before any content
difference is considered.

**The fix does not regenerate these files.** Sorting changes which payee is
drawn and therefore the entire downstream RNG stream. The dataset above stays
frozen as the one the reported figures were measured on; determinism applies
from this commit forward.

`out/relationships.csv` is *not* part of this dataset. It is a leftover from the
design in which kinship edges were loaded into the graph - removed because the
`is_family` signal it carried was an artefact (see `infra/neo4j/import.cypher`).
Nothing in the current pipeline reads it.

### Specification against the produced dataset

Checked on seed 42, 50,000 events. A specification nobody verified against the
output is a wish list, so these are the numbers a reader can reproduce.

| Quantity | Specified | Observed |
|---|---|---|
| transactions | 50,000 | 50,000 |
| fraud rate | 1.5% | 1.500% |
| fresh legitimate accounts | 12% | 12.5% |
| aged fraud accounts | 30% | 27.0% |
| channel mix (MOBILE_APP / USSD / WEB / ATM) | 0.70 / 0.12 / 0.13 / 0.05 | 0.700 / 0.118 / 0.130 / 0.051 |
| median legitimate amount | ≈133k (baseline median) | 138,740 UZS |
| `active_call` legitimate | 0.03 | 0.029 |
| `active_call` APP | 0.70 | 0.669 |
| STRUCTURING as fraction of threshold | 0.85–0.99 | 0.851–0.990 |
| ATO events per episode | 2–4 | {2, 3, 4} |
| travellers | $\tau$ = 0.18 selection | 16.7% realised (see §6) |

`verify_spec.py` regenerates these comparisons.
