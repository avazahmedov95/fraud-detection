# Adversarial threat model

Working note addressing reviewer point 1, and supplying the operational basis
for point 5 (organic drift vs adversarial evasion). Not thesis text. Condensed on
2026-09-15; the full version is in git history
(`git show 483891f:docs/threat-model.md`).

The generator's four fraud patterns describe *what the traffic looks like*, not
*what an adversary can do*. This note states the adversary's capabilities, what
each control assumes they cannot do, and what it would cost them to be wrong.

---

## 1. Scope

**In scope.** Fraud against instant P2P card-to-card transfers on the UzCard and
HUMO networks, detected between authorisation and settlement by a single issuing
bank's antifraud system. Everything below assumes the attacker operates **through
the payment system as a user of it**, not against the system.

**Out of scope**, deliberately: compromise of the detection infrastructure (Kafka,
Flink, ClickHouse) - a platform problem, handled by the transport and integrity
controls of reviewer points 3 and 4; insider threat at the bank; card-present,
e-commerce and cross-border flows; attacks on the payment switch or the card
networks.

**The cross-border exclusion is not small.** The Central Bank's *Review of
international migration and currency operations of individuals* (March 2026)
reports that of **$18.9bn** of remittances received in 2025, **$8.6bn, 46%,
arrived as P2P transfers sent directly to individuals' bank cards** - a channel
that grew 1.4x in a year while conventional bank transfers fell 49% to $397mn.

The exclusion is still correct, for a structural reason: for an inbound
cross-border transfer the **sender is not a customer of this bank**, so the 14
relational features computed over sender state do not exist. What survives is
the receiver side - account age, inflow concentration, distinct senders per hour.
On domestic traffic receiver-side aggregation is the most valuable capability
(§4); on the 46% of inflow from abroad it is **the only one available**. The
evaluation cannot show that, because the generator models domestic traffic only.

---

## 2. Assets and security goals

| Asset | Goal | Failure |
|---|---|---|
| Customer funds in transit | A fraudulent transfer is stopped before settlement | Irreversible loss; instant transfers do not unwind |
| Customer trust in instant payments | Legitimate transfers are not blocked | False positives push users to cash |
| Regulatory standing (CBU 3759) | Reportable patterns are detected and recorded | Sanction; loss of licence conditions |
| Audit trail | Every decision is reconstructable | Cannot defend a decision in dispute or inspection |

The first two goals are in direct tension; the threat model says where the
trade-off should sit rather than eliminating it.

**From 16 November 2026 the first row stops being the customer's loss.** The
Central Bank's amended P2P requirements make the credit or payment organisation
**liable for fraudulent transactions carried out without additional verification
within the limit that organisation itself set**. The loss moves onto the bank's
own balance sheet, and the bank now chooses its exposure: the amount below which
no confirmation is required is also the amount it will pay for, so a higher no-OTP
limit is affordable exactly to the extent that detection is good. That belongs in
the introduction, not only here: **regulation has converted fraud detection
quality from a cost centre into the variable that sets a revenue-bearing limit.**

---

## 3. Adversary model

Three adversaries, distinguished by what they control rather than by pattern
name: a control is only as strong as the attacker's inability to influence its
inputs.

**A1 — social engineer (APP fraud).** *Controls* what the victim is told to do:
amount, timing, destination account and the story around it - including ending a
phone call before confirming, using a particular channel, or splitting a transfer.
*Does not control* the victim's device, location, transaction history or account
age: every baseline the system holds was built before the attacker arrived.
*Constraint:* each episode needs a live human interaction, which bounds volume and
makes the attacker's time the scarce resource.

**A2 — account takeover operator.** *Controls* the session - device, IP and hence
apparent region, timing, amount, destination - with valid credentials. *Does not
control* the victim's historical baseline, cannot make the victim's other activity
consistent with the takeover, and cannot be in two places at once - the one
physical constraint the system can rely on. *Constraint:* the window is short;
credentials get revoked and the victim notices.

**A3 — mule network operator.** *Controls* a population of accounts, their age
(accounts can be farmed and aged before use), the number of senders feeding each,
and the timing of fan-in and fan-out; recruits through personal networks,
families included. *Does not control* the fact that money must converge
somewhere: concentration is not incidental to the pattern, it *is* the pattern.
*Constraint:* capital and coordination - aged accounts cost money to farm, and
spreading fan-in across time and accounts cuts throughput.

---

## 3a. The adversary model against national data

One national source lets parts of §3 be checked rather than asserted: the **State
Institution "Cybersecurity Centre" of the Republic of Uzbekistan, 2025 annual
report** (csec.uz). It is a cybersecurity register, not a fraud register, so what
it can support has to be stated carefully.

**What it supports.**

- *A2's premise - that credentials are obtainable.* In 2025 the Centre found
  databases of 37 organisations, **21 million rows in total**, with **1,697 user
  login/password pairs**, leaked to darknet networks; separately, rapid analysis
  of 10 systems prevented the leak of over **23 million personal records**, close
  to two-thirds of the population.
- *The attack surface is moving toward this system's setting.* Mobile application
  security expertise rose from 18 apps in 2024 to **40 in 2025 (+122%)**, which
  the report reads as attacker attention shifting to smartphones **and to the
  financial applications on them**. Banking and finance is second by share of
  detected vulnerabilities (**22.84%**, behind public administration at 25.84%).
- *Session persistence is a real defect.* "Session retained" is the second most
  common high-severity finding in information systems (82 instances) and appears
  in mobile apps (8). `COACHED_SESSION` and `DEVICE_CHANGE` assume a session
  boundary means something; where sessions do not expire, that assumption is
  weaker than §4 implies.

**What it does not support.** Of 247 recorded incidents in 2025, **only 3 are
phishing** - which does not say that social-engineering fraud is negligible or A1
over-modelled. The register counts incidents **against state web resources** (219
are website defacements); consumer APP fraud is neither its remit nor within its
visibility. The absence is a scope artefact, and worth stating in the thesis
before a reader quotes the number the other way.

**Prevention is arriving from a second direction.** Alongside the CBU 3759
requirements (§5), the Centre ships citizen-facing tools - a "CyberQalqon"
Telegram bot for checking suspicious files, "Xavfsizlink" for checking links
against phishing, and a permissions monitor. That reinforces §5's substitution
argument: controls that remove the observable, whoever deploys them, reduce what
a detection system can claim credit for.

---

## 4. What each control assumes

A control is a bet that the adversary cannot cheaply influence its input. Stating
the bet makes it falsifiable — and shows which controls are load-bearing.

| Control | Assumes the attacker cannot... | A1 | A2 | A3 | Cost to evade |
|---|---|---|---|---|---|
| `AMOUNT_DEVIATION`, `amount_z` | make the amount look normal *for this victim* | no | no | n/a | **low** — cap the ask below the victim's baseline; costs yield per episode |
| `NEW_PAYEE_HIGH_AMOUNT` | avoid being a new payee | no | no | no | **low** — a prior small transfer establishes the payee |
| `VELOCITY`, `DISTINCT_PAYEE_BURST` | slow down | no | **yes** | no | **low for A1/A3, high for A2** — A2's window is short by nature |
| `STRUCTURING` | keep amounts away from the threshold band | n/a | n/a | no | **low** — but structuring *is* the evasion; the rule catches the evasive form |
| `DAILY_LIMIT_BREACH` | stay under the daily limit | no | no | no | **low** — regulatory floor, not a detection claim |
| `COACHED_SESSION` (`active_call`, `secs_login_z`) | be on a call, or hesitate, while confirming | **no** | n/a | n/a | **very low once known** — "hang up before you confirm" |
| `DEVICE_CHANGE` | present a known device | yes | **no** | yes | **medium** — malware on the victim's own device defeats it |
| `GEO_ANOMALY` | appear from the victim's usual region | yes | **no** | yes | **low** — a proxy in the right city |
| `IMPOSSIBLE_TRAVEL` | be in two places at once | yes | **yes** | yes | **high** — requires a proxy geographically consistent with the victim's *recent* activity, which the attacker cannot observe |
| `FRESH_RECEIVER`, `receiver_age` | use an aged destination account | no | no | **no** | **medium** — account farming; real cost, real lead time |
| `MULE_FAN_IN` (`rcv_distinct_senders_1h`) | avoid concentration at the payee | n/a | n/a | **no** | **high** — spreading fan-in across accounts and hours cuts the network's throughput, which is its purpose |

"yes" means the assumption holds against that adversary; "no" means they can
break it.

### Two of these controls assume data the deploying bank mostly does not have

A card-to-card transfer reaches the sending bank as a **destination PAN**.
Resolving it to the person behind it is a core-banking lookup available only for
the bank's own clients - **6.85% of transfers** at the measured card-market
concentration (69.0 million cards, 34 banks, largest share 16.3%; the generated
stream realises 6.73%). So `FRESH_RECEIVER` and `receiver_age`, rated "medium"
above, are **unavailable on 93% of traffic**, and `MULE_FAN_IN` aggregates over
the card, not the person. Two consequences:

- **A3's cost to evade is overstated at bank level.** A mule holding several cards
  is already split across as many fan-in buckets when the key is the PAN, at no
  cost to the operator. The "high" rating holds at switch or platform level, where
  the payee resolves to one person.
- **A2 and A3 both benefit from the same gap**, and it is a
  position-in-the-topology gap, not a modelling one - the strongest argument in
  this document for resolution at the national-platform level, and a measured one.

`docs/irp-framing.md` §6, "RQ3, fourth result" carries the measurement, including
why resolving the payee per-transfer where the bank *can* makes detection worse
(−17.4% of `MULE_FAN_IN`'s true positives).

### One of those "cost to evade" ratings is now measured rather than reasoned

Every rating above was an argument. `NEW_PAYEE_HIGH_AMOUNT` is the first with a
price attached, obtainable only after noticing that **the generator never
produced the evasion this table names**: on the frozen dataset 99.20% of fraud
went to a stream-new payee against 36.93% of legitimate traffic, so the control's
value could not be measured, only its ceiling.

`SEEDED_PAYEE_SHARE` (`data-generator/config.py`) produces it: a share of APP
episodes are preceded by one small transfer to the same payee, one to three weeks
earlier, labelled **not fraud** because no loss occurs on it. Default 0.0, so
nothing quoted elsewhere moves. Five generator seeds at share 0.5, APP episodes
late enough in the window for a seed to land
(`stream-processor/experiments/replay.py payee-seeding`):

| payee at the time of the fraud | episodes | detected |
|---|---|---|
| new to the stream | 159 | **56.0%** |
| established by one prior transfer | 201 | **31.8%** |

Per-seed delta **−25.3 pp, 95% CI [−47.5, −3.1], 4 of 5 negative.** The interval
excludes zero, so the evasion works; it is also 44 points wide, mostly from 30-50
episodes per group per seed rather than disagreement between datasets - trust the
direction more than the magnitude.

- **The rating "low" was right, and now it has a number.** One small prior
  transfer - no infrastructure, one sentence added to the script the fraudster is
  already reading to the victim - removes roughly half the detection on that
  episode.
- **`is_new_payee`'s measured importance is an upper bound.** It is the model's
  top SHAP feature at 0.770, on data where fraud reaches a stream-new payee 99.2%
  of the time - a property of a generator that does not model the evasion, to be
  reported as such, exactly as the `is_family` artefact was.

The measurement is deliberately a **lower** bound on full evasion: only APP is
seeded. ATO is excluded because §3 gives A2 a short window, and seeding weeks
ahead would put in the data an evasion this threat model says that adversary
cannot perform.

### The uncomfortable finding

Measured detection value (the capability ablation in `ml/README.md`, baseline
profile, five seeds) against cost to evade:

| Capability | Δ PR-AUC | Cost to evade |
|---|---|---|
| receiver-side aggregation | −0.036 | **high** |
| session telemetry | −0.034 | **very low** |
| receiver account age | −0.022 | medium |
| geo telemetry | ~0 in the model, but enables IMPOSSIBLE_TRAVEL | high for that rule, low for GEO_ANOMALY |

**The second most valuable signal is the most fragile.** `COACHED_SESSION` works
today because APP victims are on the phone with the fraudster while they confirm.
The adaptation is one sentence of script - *hang up, then confirm* - at no cost.
Evading receiver-side aggregation instead means spreading collection across more
accounts and more hours, which cuts the throughput the mule network exists to
provide: a control whose evasion is *expensive by construction*.

Hence a reporting discipline for the thesis: **detection value and robustness are
different axes, and a system evaluated only on the first will over-credit its most
brittle signals.** The ablation measures the first; this table is the second.

---

## 5. Drift versus evasion (reviewer point 5)

Both appear as degrading detection quality. They are distinguishable, and the
capability registry makes the test concrete.

| | Organic drift | Adversarial evasion |
|---|---|---|
| Which class moves | both — P(x) shifts | only the fraud class — P(x \| fraud) shifts, P(x \| legit) stable |
| Which features move | any, following behaviour change | **only attacker-controllable ones** (§4) |
| Speed | slow, continuous | fast, stepwise — a script change propagates in days |
| Direction | undirected | toward the decision boundary |

**Operational test.** Monitor per-feature distributions split by outcome:

1. A shift in `P(feature | legit)` alongside `P(feature | fraud)` is drift - more
   people banking at night, the population adopting a new channel.
2. A shift in `P(feature | fraud)` alone, on a feature the adversary controls, is
   evasion.
3. A shift on a feature the adversary *cannot* control (receiver-side
   concentration, physical travel consistency) is more likely a data-quality or
   integration fault than either - worth alerting on separately, because it
   usually means an upstream feed broke.

The third row turns a monitoring ambiguity into three distinguishable causes with
different responses.

**Concrete prediction, falsifiable.** If `COACHED_SESSION` is deployed and
announced, `P(active_call = 1 | APP fraud)` should fall toward the population
base rate (~3%) within weeks, while `P(active_call = 1 | legitimate)` stays
unchanged - the signature of evasion, measurable on the audit log the system
already writes.

**The prediction has already been answered, and not by the adversary.** CBU
Board resolution 3759 of 21 January 2026 requires the mobile application to
restrict user access during audio and video calls, messenger calls included, and
during remote-control sessions. In a compliant deployment the coached session
never reaches confirmation, so `active_call = 1` should be absent from the stream
rather than merely rarer.

- `COACHED_SESSION` detects a state a compliant app must prevent. Its measured
  contribution - session telemetry at −0.034 PR-AUC, the second most valuable
  capability in the ablation - is measured on a capability regulation is
  removing.
- The predicted evasion is performed by the regulator rather than the attacker,
  for a better reason, on a fixed date and for everyone at once.
- **Detection value and prevention are substitutes, and a detection system must
  not claim credit for what prevention removes.** The ablation ranks data sources
  by what their absence costs the model; it has no axis for whether a control
  elsewhere in the stack is meant to make the source unobservable. On all three
  axes session telemetry reads the same way: high value, lowest robustness, and
  now legislated away.

A blanket block carries a cost the rule did not: the bank's own support line
walking a customer through a transfer is blocked along with the fraudster.
Whitelisting the institution's support number is the obvious refinement; whether
3759 admits such an exception has to be read from the clause itself. This rests
on the published summary of 3759, not the clause text - quote the wording
directly before the thesis leans on it.

**A second instance, on a capability the ablation measured as worthless.** The
same November 2026 package requires that logging in from a new device deactivates
the linked cards, and that biometric identification be performed before an
account is used from a different device or after a password reset.
`DEVICE_CHANGE` measures exactly that event as a *signal*; the regulation converts
it into a *hard control*. And `device_telemetry=off` measured **+0.002 [−0.001,
+0.005]** over five seeds - no effect. The capability worth nothing to the
detector is the one regulation found worth mandating as prevention.

That is the substitution stated from the other end: a signal contributes nothing
to a *detector* precisely when the event it marks is rare or already handled, and
such an event is the cheapest to *prevent* outright. **A capability's detection
value and its prevention value can be inversely related, and an ablation measures
only the first** - so ranking data sources by ablation delta alone ranks
prevention candidates last.

---

## 6. Attacks on the detection system itself

Out of scope for detection logic, in scope for the platform controls the
reviewer asks about in points 3 and 4.

| Attack | Control | Status |
|---|---|---|
| Forged or replayed events on `transactions.raw` | mTLS between switch and ingest; payload authentication | scoped, unmeasured (point 3) |
| Tampering with a decision after the fact | Ingress hash binds decision to event; audit hash chain makes any edit/delete/reorder evident (`verify_audit.py`); WORM grants make the log append-only | **implemented** (point 4) |
| Model theft or inversion via probing | Rate limits on decision feedback; the customer sees only allow/block | not analysed |
| Poisoning the model through crafted training data | Retraining is offline on labelled data with human review | acceptable while retraining is manual; becomes a real risk under automated retraining |

The last row is worth flagging: this system is safe from poisoning **because it
does not retrain automatically** - a property of the current operating model, not
of the design, and it changes the moment retraining is automated.

---

## 7. Residual risk

Stated plainly, because a threat model that concludes "we are covered" is not
one.

1. **A1 with a disciplined script** — a moderate amount, an established payee, no
   call during confirmation — defeats every low-cost control simultaneously.
   What remains is the receiver-side signal: the destination account still has to
   receive, and mule accounts still concentrate.
2. **A2 with malware on the victim's own device** presents the right device and
   the right region. Only velocity and amount deviation remain, and both are
   evadable by patience.
3. **A3 with sufficient capital** — aged accounts, fan-in spread over days —
   defeats detection at the cost of throughput. This is the intended outcome:
   the control does not stop the network, it makes it smaller.

In each case the surviving control is one whose evasion costs the attacker
something real. That, rather than coverage, is the design goal worth stating.
