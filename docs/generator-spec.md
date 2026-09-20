# Synthetic data generator: mathematical specification

Working note addressing reviewer point 2. Not thesis text.

Every distribution, parameter and dependency used to produce the dataset, stated
formally enough to reimplement from this document alone. Constants are quoted
from `data-generator/config.py`; where the code and this document disagree, the
code is authoritative and this document is a bug. Condensed on 2026-09-15; the
history of each change is in git history.

---

## 0. Why generate data at all

No public dataset of Uzbek P2P card transactions exists: the data is
bank-confidential, and the market is small enough that anonymisation would not
protect participants. The alternatives, assessed in full in `docs/related-work.md`:

| Option | Why not |
|---|---|
| IEEE-CIS (590k txns) | Real, but e-commerce card-not-present: no counterparty, no session signals. |
| CCF / Kaggle credit card | PCA-anonymised into V1..V28, so SHAP explanations - a hard requirement under CBU 3759 - are meaningless. |
| Zenodo 20030065 (57k txns) | Published as production data; examined and rejected (`validation/README.md` §2). |
| IBM Synthetic Data Sets (SynDS) | Labelled P2P payment data, but a **commercial product**, not reproducible by a reader who has not bought it; US consumer-app semantics, no session signals. |
| Kaggle UPI sets (several) | **Themselves synthetic and published without a generating specification** - strictly worse than a documented generator (§7). |

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
default seed 42. The dataset is a deterministic function of the seed. Sizes and
rates below are the baseline profile's; §10 lists what the realistic profile
changes.

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
$e^{11.8} \approx 133{,}000$ UZS - lognormal, because income and spend are
multiplicative processes.

**Active hours.** $H_{\text{start}} \sim \mathcal{U}\{6,\dots,10\}$,
$H_{\text{end}} \sim \mathcal{U}\{18,\dots,23\}$, per person.

**Personal confirmation-time median.**
$m_i = 40 \cdot \exp(\mathcal{N}(0, 0.35^2))$ seconds, which makes `secs_login_z`
a *personal* baseline: a deliberate pensioner and a fast 20-year-old are both
normal relative to themselves.

**Card and issuer.** Issuer sampled $\propto$ cards in circulation (CBU figures
as at 1 April 2026, `banks.csv`), not uniformly, so the on-us rate is not an
artefact of the length of the bank list. PAN = 6-digit BIN + 9 uniform digits +
Luhn check digit; PINFL = 14 uniform digits. Both synthetic.

**Second card.** A person holds a card at a *second* bank w.p. $\kappa = 0.20$,
and transfers to them arrive on it w.p. $0.40$; the second issuer is redrawn until
it differs from the first. This is what makes `payee_identity` measurable - with
one card each, PAN and PINFL keys partition the stream identically. Receiving
only: a sender drawing from two cards would fragment the per-sender history.

**Fraud accounts.** $N_f = \max(50, N_p/25) = 200$, each its own household (so
mules are not "relatives" of each other). Age:

$$
A_f \sim \begin{cases}
\mathcal{U}\{100,\dots,1499\} & \text{w.p. } 0.30 \quad \text{(aged/farmed)} \\
\mathcal{U}\{1,\dots,44\} & \text{w.p. } 0.70
\end{cases}
$$

The aged 30% is what stopped `receiver_age` from being a perfect separator. The
detector no longer reads the age (since 2026-09-19); the column stays in the
dataset, whose hash is pinned.

**Amounts are not round, and real ones are.** The owner's reading of the market on
2026-09-20: a person almost always sends a round sum - 350,000 or 6,000,000 - and
rarely 435,345. This generator draws amounts from continuous distributions, so
only 39.6% of legitimate transfers are multiples of 1,000 and 5.1% of 100,000. The
deviation is not symmetric, which is the part that matters: **ATO, MULE and
STRUCTURING amounts are 0.0% multiples of 1,000** against 39.6% of legitimate
traffic and 40.0% of APP, because those patterns compute their amounts from
balances, collected sums and threshold bands. Roundness is therefore a near-perfect
class separator in this data **by construction**, exactly as `is_family` once was
(`ml/README.md`). No feature reads it today and none should be added while this
holds: it would separate the classes without detecting anything. Fixing it means
rounding amounts on both sides - the legitimate ones and the fraudulent ones whose
mechanism would still produce round numbers - and regenerating the dataset of
record, which re-opens every figure pinned to its hash.

---

## 3. Payee graph

Each person is assigned $K \sim \mathcal{U}\{3,\dots,8\}$ frequent payees. Each
draw is a relative with probability $\phi = 0.35$ (if the household has any),
otherwise uniform over the population. $\phi > 0$ is load-bearing: with no fraud
routed to relatives, `is_family` separated the classes by construction.

---

## 4. Legitimate traffic

$n_{\text{legit}} = (1 - 0.015) \cdot 50{,}000 = 49{,}250$ events.

**Sender.** Heavy-tailed activity: draw $u_i \sim \mathcal{U}(0,1)$ per person,
set activity weight $\propto u_i^3$, sample senders from
$\text{Cat}(\mathbf{u}^3 / \sum \mathbf{u}^3)$ - a crude power law: a few very
active senders, most rarely transacting.

**Payee.** Frequent payee w.p. 0.95; a fresh uniform draw w.p. 0.05.

**Hard negatives** (share $\eta = 0.03$): legitimate transfers deliberately
shaped like APP fraud — new payee, and w.p. 0.5 a large one-off
$\text{LogN}(15.0, 0.5)$ (median ≈ 3.3M UZS). Without these the classes separate
trivially on `is_new_payee` × amount.

**Amount.** $X \sim \text{clip}(\text{LogN}(\log T_i, 0.5), 1000, 5\times10^7)$ —
centred on the sender's own baseline, so deviation is meaningful per person.

**Timestamp.** Uniform over the 30-day span, then the hour is replaced by
$\mathcal{U}\{H_{\text{start}}, H_{\text{end}}\}$ and minute/second uniform.

> **Known limitation.** No weekday, payday or within-day structure, so `hour` is a
> weak feature rather than a meaningful one.

**Device.** A person owns a second device w.p. $\sigma = 0.25$, drawn once in
`persons.py`, and each of their events comes from it w.p. $\rho = 0.15$; everyone
else transacts from one device. With a single device each, `device_is_new` fired
only on fraud - the label under another name; now it fires on both classes, and
`verify_spec.py` checks that it does, on enough rows to split on.

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

$\mathcal{U}\{2,\dots,8\}$ events per episode, spaced $i \cdot \mathcal{U}(1,4)$
minutes - enough to trip `VELOCITY`, since `threat-model.md` §4 has the takeover
operator unable to slow down; the thresholds were not touched. Stealth w.p. 0.40:
victim's own device and region (on-device malware), leaving only behavioural
signals.

Non-stealth (0.60) anchors to the victim's real history: pick a random prior
legitimate event $(t_j, r_j)$, set the session $\Delta \sim \mathcal{U}(3,45)$
minutes later, from a region $r'$ satisfying

$$
d(r_j, r') \ge 100 \text{ km} \quad\wedge\quad \frac{d(r_j, r')}{\Delta/60} > 900 \text{ km/h}
$$

If no such region exists, the session stays at $r_j$ rather than inventing a
journey.

> The generator and detector share the reachability threshold, so the detection
> rate on these hijacks is **not a result**. The false-positive rate on the
> independently generated legitimate journeys of §6 is: **0 of 775**.

Amount $\text{clip}(\text{LogN}(14.5, 0.5), \cdot)$ per event.

### 5.4 STRUCTURING

$\mathcal{U}\{5,\dots,11\}$ events, spaced $i \cdot \mathcal{U}(3,15)$ minutes,
each $X = 10^7 \cdot \mathcal{U}(0.85, 0.99)$ — deliberately just under the
reporting threshold. The slow spacing is deliberate: a smurfing run that paces
itself is the modelled behaviour.

### 5.5 MULE — fan-in then fan-out

Mule is an ordinary recruited person w.p. $\rho = 0.30$, else a purpose-made
fraud account. Only recruited mules have relatives, hence family fan-in.

Fan-in: $n_{\text{in}} \sim \mathcal{U}\{4,\dots,8\}$ senders, each a relative of
the mule w.p. $\psi = 0.10$ else uniform, amount
$\text{clip}(\text{LogN}(13.5, 0.6), \cdot)$, spaced $i \cdot \mathcal{U}(1,6)$
minutes.

Fan-out: $n_{\text{out}} \sim \mathcal{U}\{1,2\}$ transfers of
$\frac{1}{n_{\text{out}}} \sum X_{\text{in}} \cdot \mathcal{U}(0.80, 0.98)$.

Inbound counts per payee outside $[4,8]$ are fan-out destinations (1–2 transfers)
or accounts that served as the mule in more than one episode - the 200 fraud
accounts are sampled with replacement. 80% of mule events are fan-in legs,
invisible to sender-keyed state (`irp-framing.md` §4).

---

## 6. Travel (the negative control)

Each person is selected for travel w.p. $\tau = 0.18$ and then attempts
$\mathcal{U}\{1,2\}$ journeys. A journey to region $r'$ departs at a uniform
time, takes $d(r, r')/70$ hours at road speed, stays $\mathcal{U}(12, 96)$ hours,
and returns symmetrically. Transactions during the stay carry $r'$; transactions
falling *in transit* are re-timed to $\mathcal{U}(5,180)$ minutes after arrival -
otherwise an event at the origin followed minutes later by one at the destination
would manufacture impossible travel inside legitimate traffic.

$\tau$ is the selection probability, not the observed share: a journey that draws
the person's own region is skipped, so the realised share lands near $\tau$ on
either side (18.04% and 16.98% on two regenerations).

Distances use the detector's own coordinate table (`stream-processor/geo.py`,
administrative centres, haversine), so simulation and detector cannot disagree
about geography. 70 km/h is deliberately slow: longer journeys make the negative
control *harder*.

---

## 7. Defence of the approach

**Why parametric sampling rather than copulas or a GAN.** Both estimate a joint
distribution **from data**, and there is no Uzbek P2P data to fit: a GAN trained
on IEEE-CIS would produce e-commerce covariance under Uzbek field names. The
precedent is PaySim, parametric and agent-based for the same reason, and the trade
is the point: **a parametric generator's assumptions are legible and falsifiable,
a fitted generator's are implicit in weights.** PaySim is also the cautionary
precedent - its balance columns leak the label - and this project's equivalents
are recorded in §8.

**A second precedent, and a criterion this spec meets.** Tritscher et al. (2022)
generate public ERP fraud data for the same reason, and reject generators whose
data, code and parameters are unpublished - the criterion this document, the code
and the pinned hashes (§9) meet. They also criticise injecting fraud into existing
data "in post": §5 does exactly that, and the divergence it causes is measurable.

**On the ROC-AUC ≈ 0.999 this data produces.** It is that divergence: classes
separable by construction along several axes at once. On the baseline profile,
PR-AUC (0.960 ± 0.018 across seeds) is the figure to read, and even that is a
design target, measured on a held-out slice at 1.23% fraud.

---

## 8. What this generator does not model

1. **Temporal structure.** No weekday/weekend effect, no payday spikes, no
   within-day autocorrelation (§4). `hour` is consequently a weak feature.
2. **Merchant or biller flows.** P2P only; real card traffic is mostly neither.
3. **Adaptive adversaries.** Fraud parameters are fixed; an attacker who adapts
   is treated analytically in `docs/threat-model.md`.
4. **Network effects between fraud episodes.** Episodes are independent; real
   mule networks share infrastructure, devices and timing.
5. **Legitimate account takeover-like behaviour.** A user genuinely switching
   phone and city at once is rare here and would be a false positive.
6. **Amount rounding.** Real transfers cluster on round numbers; the baseline
   profile's amounts are continuous (the realistic profile rounds 40%, §10).
7. **Label noise.** The baseline profile's ground truth is exact; real labels
   arrive late and incomplete (the realistic profile leaves 10% unreported, §10).

Items 1, 6 and 7 are the ones most likely to make measured performance optimistic
relative to production.

---

## 9. Reproduction

```bash
cd data-generator
python generator.py --profile realistic --out ./out   # the dataset of record, seed 42
python generator.py --profile baseline --out ./out    # the baseline profile
```

Output is a deterministic function of the seed, the sizes, the profile and
`banks.csv`. `ml/experiments/ablate_seeds.py` regenerates across seeds; baseline
PR-AUC varies by ±0.008–0.035 between seeds, which is why no single-dataset figure
is quoted anywhere in this project.

### Determinism is not free, and the seed does not buy it

Until 2026-08-30 each sender's payees were collected in a `set`, and CPython
randomises string hashing per process: the same seed and the same pinned versions
gave a different receiver on 36,072 of 50,000 rows. `sorted(chosen)` fixed it.
Two lessons: pinned dependencies are necessary and not sufficient, and a
fingerprint over the sources cannot catch a difference in the interpreter.
`transaction_id` is still a bare `uuid4()` outside the seeded stream; it carries
no analytical content and is excluded from every comparison.

### The dataset of record

`data-generator/out/` is gitignored, so the files every reported figure was
computed on are pinned here. **Since 2026-09-14 it is the realistic profile**
(§10), seed 42, `generator.py --profile realistic`:

```
transactions.csv  500,000 rows  157,241,920 bytes
  sha256  0338db95c10a264c5154605811a890be9eeb55f212665344642a0477c2c7380f
persons.csv        52,000 rows    5,782,987 bytes
  sha256  c80f44e2e7403d803f51275c1e854f1cd7ae5cb20c12ad60e713873c457414bc
```

Every figure dated before 2026-09-14 was measured on the baseline dataset,
regenerated 2026-09-07 and kept at `data-generator/out_frozen_2026-09-07/`:

```
transactions.csv   50,000 rows   15,730,392 bytes
  sha256  ca8a3dcd48bf586be144e6c7b96c78f27d0a7edfe57bfe1282994d6ab53f7899
persons.csv         5,200 rows      578,549 bytes
  sha256  dbe01edd7626def3c8ce50a343915d80fe309c97895342476d9c22a01be37cb6
```

These hashes hold on any host: `generator.py` writes LF line endings explicitly,
where `pandas.to_csv` would take them from the operating system. Figures dated
before 2026-09-07 come from the 2026-07-19 dataset (`b767f38489ab…` /
`010cddd6a60f…`, kept at `data-generator/out_frozen_2026-07-19/`), which the payee
fix makes impossible to regenerate. `out/relationships.csv` is a leftover of the
removed kinship graph, and nothing reads it.

### Specification against the produced dataset

Checked on seed 42, baseline profile, 50,000 events:

| Quantity | Specified | Observed |
|---|---|---|
| transactions | 50,000 | 50,000 |
| fraud rate | 1.5% | 1.500% |
| fresh legitimate accounts | 12% | 12.2% |
| aged fraud accounts | 30% | 28.0% |
| median legitimate amount | ≈133k (baseline median) | 137,430 UZS |
| `active_call` legitimate | 0.03 | 0.030 |
| `active_call` APP | 0.70 | 0.649 |
| STRUCTURING as fraction of threshold | 0.85–0.99 | 0.851–0.990 |
| ATO events per episode | 2–8 | {2, 3, 4, 5, 6, 7, 8} |
| travellers | $\tau$ = 0.18 selection | 17.0% realised (see §6) |
| a first-seen device | both classes, ≥ 30 rows (§4) | 570 legit / 11 fraud, 1.9% precision |
| receivers on >1 card | $\kappa$ = 0.20 hold one (§2) | 892 of 5,078, 9,826 rows |

`verify_spec.py` regenerates these comparisons.

---

## 10. The realistic profile

`generator.py --profile realistic` - the dataset of record since 2026-09-14.

The baseline profile separates too easily: fraud at 1.5% of traffic, about ten
times what real card traffic carries (0.17-0.19%, `validation/README.md` 2); every
legitimate transfer looks legitimate; and the labels are exact. The model it
trained scored 0.937 PR-AUC on it and 0.42 on data shaped like the profile below.
This profile keeps every mechanism and moves each parameter toward overlap:

| knob | baseline | realistic | why |
|---|---|---|---|
| transactions / persons | 50,000 / 5,000 | 500,000 / 50,000 | enough fraud to measure at a real rate |
| fraud rate | 1.5% | 0.2% | real card traffic and PaySim both sit near 0.1-0.2% |
| unreported fraud | 0 | 10% of episodes | real labels are incomplete (§8, item 7): the behaviour stays, the label does not |
| legitimate hard negatives | 3% | 8% | a new payee and a large one-off - rent, a car, a deposit |
| legitimate active call | 3% | 10% | people send money while talking to the payee |
| APP active call | 70% | 45% | much coaching happens in messengers, or before the transfer |
| APP confirmation stretch | 2.5-5x | 1.0-3.5x | a coached victim is not always slower |
| confirmation-time spread | 0.55 | 0.75 | personal habits vary more |
| APP moderate amounts | 40% | 60% | fewer drains, more amounts in the ordinary range |
| aged fraud accounts | 30% | 50% | bought and rented accounts, not only fresh ones |
| ATO stealth | 40% | 60% | more takeovers from the victim's own device |
| MULE recruited people | 30% | 50% | ordinary histories, not purpose-made accounts |
| MULE senders, spacing | 4-8, 1-6 min | 3-10, 5-60 min | collection spread over hours, not minutes |
| STRUCTURING events, spacing, band | 5-11, 3-15 min, 85-99% | 3-8, 10-90 min, 70-99% | pieces spread through the day |
| legitimate collections | 0 | 1% of persons | 5-15 senders within hours: a wedding, a gift, a joint purchase |
| legitimate split payments | 0 | 0.3% of persons | 3-5 large parts to one payee |
| phone changes | 0 | 4% of persons | a new phone, kept from then on |
| round sums | 0 | 40% | people send round amounts (§8, item 6) |

A harder benchmark on the same machinery, not a calibration: no row comes from
Uzbek data; each moves a parameter the baseline set at its easiest, in the
direction the public datasets and §8 point. Every added knob is off at 0 and
tested before any random draw, so `--profile baseline` still reproduces the
2026-09-07 dataset draw for draw. `verify_spec.py` checks the realistic profile's
own values by default (15/15).

Realised on seed 42: 500,000 transactions, 878 labelled fraud (0.176%) - APP 315,
MULE 216, STRUCTURING 181, ATO 166; 39.6% of legitimate amounts are round sums,
and 9,712 legitimate transfers come from a changed phone.
