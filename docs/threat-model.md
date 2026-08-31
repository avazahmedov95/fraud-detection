# Adversarial threat model

Working note addressing reviewer point 1, and supplying the operational basis
for point 5 (organic drift vs adversarial evasion). Not thesis text.

The four fraud patterns in the generator are raw material, not a threat model:
they describe *what the traffic looks like*, not *what an adversary can do*. This
note states the adversary's capabilities, what each control assumes they cannot
do, and what it would cost them to be wrong.

---

## 1. Scope

**In scope.** Fraud against instant P2P card-to-card transfers on the UzCard and
HUMO networks, detected between authorisation and settlement by a single
issuing bank's antifraud system.

**Out of scope**, and deliberately so:

- Compromise of the detection infrastructure itself (Kafka, Flink, ClickHouse).
  That is a platform security problem, addressed by the transport and integrity
  controls in reviewer points 3 and 4, not by detection logic.
- Insider threat at the bank.
- Card-present, e-commerce and cross-border flows.
- Attacks on the payment switch or the card networks.

The boundary matters: everything below assumes the attacker operates **through
the payment system as a user of it**, not against the system.

**The cross-border exclusion needs its number, because it is not small.** The
Central Bank's *Review of international migration and currency operations of
individuals* (March 2026) reports that of **$18.9bn** of remittances received in
Uzbekistan in 2025 — up 28% on 2024 — **$8.6bn, 46%, arrived as P2P transfers
sent directly to individuals' bank cards**, a channel that grew 1.4x in a year
while conventional bank transfers fell 49% to $397mn. So "cross-border is out of
scope" excludes roughly half of a flow that lands on exactly the cards this
system watches.

The exclusion is still correct, and the reason is structural rather than
convenient: for an inbound cross-border transfer the **sender is not a customer
of this bank and has no history here**, so the 14 relational features computed
over sender state do not exist. What survives is the receiver side — account age,
inflow concentration, distinct senders per hour.

That inverts the usual reading of §4 for this market. On domestic traffic,
receiver-side aggregation is the most valuable capability (−0.032 PR-AUC). On the
46% of inflow that arrives from abroad it is not the most valuable, it is **the
only one available**. The finding this project treats as its strongest is, in the
Uzbek remittance context, more load-bearing than its own evaluation shows —
and that evaluation cannot demonstrate it, because the generator models domestic
traffic only.

---

## 2. Assets and security goals

| Asset | Goal | Failure |
|---|---|---|
| Customer funds in transit | A fraudulent transfer is stopped before settlement | Irreversible loss; instant transfers do not unwind |
| Customer trust in instant payments | Legitimate transfers are not blocked | False positives push users to cash |
| Regulatory standing (CBU 3759) | Reportable patterns are detected and recorded | Sanction; loss of licence conditions |
| Audit trail | Every decision is reconstructable | Cannot defend a decision in dispute or inspection |

The first two goals are in direct tension, and the threat model exists to say
where the trade-off should sit rather than to eliminate it.

**From 16 November 2026 the first row stops being the customer's loss.** The
Central Bank's amended P2P requirements make the credit or payment organisation
**liable for fraudulent transactions carried out without additional verification
within the limit that organisation itself set**. Three consequences, and the
third is the one for the thesis.

- The asset table above is written from the customer's side. After that date the
  loss lands on the bank's own balance sheet, so detection stops being a
  prudential or reputational matter and becomes a priced one.
- **The bank now chooses its own exposure.** Institutions may set the transfer
  amount below which no additional confirmation is required — and that same
  threshold defines what they will pay for. A detection system's value becomes
  directly expressible: a higher no-OTP limit is affordable exactly to the extent
  that detection is good, so the model's precision and recall convert into a
  number the institution can set.
- This is the strongest available motivation for the whole system, and it did not
  exist when the project started. It should be stated in the introduction rather
  than left in a threat model: **regulation has converted fraud detection quality
  from a cost centre into the variable that sets a revenue-bearing limit.**

---

## 3. Adversary model

Three adversaries, distinguished by what they control rather than by pattern
name. This is the distinction that matters for evasion: a control is only as
strong as the attacker's inability to influence its inputs.

### A1 — Social engineer (APP fraud)

**Controls:** what the victim is told to do — amount, timing, destination
account, and the story around it. Can instruct the victim to end a phone call
before confirming, to use a particular channel, or to split a transfer.

**Does not control:** the victim's device, location, transaction history, or
account age. Every baseline the system holds about the victim was built before
the attacker arrived.

**Constraint:** each episode requires a live human interaction, which bounds
volume and makes the attacker's time the scarce resource.

### A2 — Account takeover operator

**Controls:** the session — device, IP and hence apparent region, timing, amount,
destination. Holds valid credentials.

**Does not control:** the victim's historical baseline, and cannot make the
victim's *other* activity consistent with the takeover session. Cannot be in two
places at once — the one physical constraint the system can rely on.

**Constraint:** the window is short. Credentials get revoked; the victim
notices.

### A3 — Mule network operator

**Controls:** a population of accounts, their age (accounts can be farmed and
aged before use), the number of senders feeding each, and the timing of fan-in
and fan-out. Recruits through personal networks, including families.

**Does not control:** the fact that money must converge somewhere. Concentration
is not incidental to the pattern; it *is* the pattern.

**Constraint:** capital and coordination. Aged accounts cost money to farm;
spreading fan-in across time and accounts reduces throughput.

---

## 3a. The adversary model against national data

The three adversaries above are constructed from the fraud literature and from
how the payment system works. One national source lets parts of them be checked
rather than asserted: the **State Institution "Cybersecurity Centre" of the
Republic of Uzbekistan, 2025 annual report** (csec.uz). It is a cybersecurity
register, not a fraud register, so what it can and cannot support has to be
stated carefully.

**What it supports.**

*A2's premise — that credentials are obtainable — is quantified.* During 2025
the Centre found databases belonging to 37 organisations, **21 million rows in
total**, together with **1,697 user login/password pairs**, leaked to darknet
networks; separately, rapid analysis of 10 systems prevented the leak of over
**23 million personal records**, which the report notes is close to two-thirds
of the country's population. A2 is modelled as "holds valid credentials", and
that assumption does not need to be argued.

*The attack surface is moving toward exactly this system's setting.* Mobile
application security expertise rose from 18 apps in 2024 to **40 in 2025
(+122%)**, and the report's own forward-looking conclusion reads the rise as
confirmation that attacker attention is shifting to smartphones **and to the
financial applications on them**. Banking and finance is the second-largest
sector by share of detected vulnerabilities (**22.84%**, behind public
administration at 25.84%).

*Session persistence is a real defect, not a theoretical one.* "Session retained"
is the second most common high-severity finding in information systems (82
instances) and appears again in mobile apps (8). The `COACHED_SESSION` and
`DEVICE_CHANGE` controls assume a session boundary means something; in a
deployment where sessions do not expire, that assumption is weaker than §4
implies.

**What it does not support, and the misreading to head off.** Of 247 recorded
cybersecurity incidents in 2025, **only 3 are phishing**. Read carelessly, that
says social-engineering fraud is negligible in Uzbekistan and A1 is
over-modelled. It says nothing of the kind. The Centre's incident register
counts incidents **against state web resources** — the most common entry is
website defacement, at 219 — and consumer-facing APP fraud is neither its remit
nor within its visibility. The absence is a scope artefact. This is worth
stating explicitly in the thesis, because it is the kind of number a reader will
find, quote, and draw the opposite conclusion from.

**Prevention is arriving from a second direction.** Alongside the CBU 3759
requirements discussed in §5, the Centre now ships citizen-facing preventive
tools — a "CyberQalqon" Telegram bot for checking suspicious files, "Xavfsizlink"
for checking links against phishing, and a permissions monitor. That reinforces
the substitution argument in §5 rather than softening it: controls that remove
the observable, whoever deploys them, reduce what a detection system can claim
credit for.

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

### The uncomfortable finding

Ranking capabilities by measured detection value (from the ablation) against cost
to evade:

| Capability | Δ PR-AUC | Cost to evade |
|---|---|---|
| receiver-side aggregation | −0.032 | **high** |
| session telemetry | −0.019 | **very low** |
| receiver account age | −0.011 | medium |
| geo telemetry | ~0 in the model, but enables IMPOSSIBLE_TRAVEL | high for that rule, low for GEO_ANOMALY |

**The second most valuable signal is the most fragile.** `COACHED_SESSION` works
today because APP victims are on the phone with the fraudster while they confirm.
It survives exactly as long as attackers do not adapt, and the adaptation is one
sentence of script: *hang up, then confirm*. No infrastructure, no cost.

Receiver-side aggregation is the opposite: evading it requires the mule network
to spread collection across more accounts and more hours, which directly reduces
the throughput the network exists to provide. That is a control whose evasion is
*expensive by construction*.

This suggests a reporting discipline for the thesis: **detection value and
robustness are different axes, and a system evaluated only on the first will
over-credit its most brittle signals.** The ablation measures the first. This
table is the second.

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

1. A shift in `P(feature | legit)` alongside `P(feature | fraud)` is drift —
   more people banking at night, the population adopting a new channel.
2. A shift in `P(feature | fraud)` alone, on a feature the adversary controls,
   is evasion.
3. A shift on a feature the adversary *cannot* control (receiver-side
   concentration, physical travel consistency) is more likely a data-quality or
   integration fault than either — worth alerting on separately, because it
   usually means an upstream feed broke.

The third row is why this framing is worth having: it turns a monitoring
ambiguity into three distinguishable causes with different responses.

**Concrete prediction, falsifiable.** If `COACHED_SESSION` is deployed and
announced, `P(active_call = 1 | APP fraud)` should fall toward the population
base rate (~3%) within weeks, while `P(active_call = 1 | legitimate)` stays
unchanged. That is the signature of evasion, and it is measurable on the audit
log the system already writes.

**The prediction has already been answered, and not by the adversary.** CBU
Board resolution 3759 of 21 January 2026 requires the mobile application to
restrict user access during audio and video calls, messenger calls included,
and during remote-control sessions. In a compliant deployment the coached
session never reaches confirmation, so `active_call = 1` should be absent from
the stream rather than merely rarer.

Three consequences, and the third belongs in the main argument.

- `COACHED_SESSION` detects a state a compliant app must prevent. Its measured
  contribution - session telemetry at -0.019 PR-AUC, the second most valuable
  capability in the ablation - is measured on a capability that regulation is
  removing.
- The evasion predicted above is performed by the regulator rather than the
  attacker, for a better reason, on a fixed date and for everyone at once. An
  adversary would have adapted eventually; compliance adapts on their behalf.
- **Detection value and prevention are substitutes, and a detection system must
  not claim credit for what prevention removes.** The capability ablation ranks
  data sources by what their absence costs the model. This is the third axis it
  does not have: whether a control elsewhere in the stack is supposed to make
  the source unobservable in the first place. On all three, session telemetry
  reads the same way - high value, lowest robustness, and now legislated away.

**A second instance, and this one lands on a capability the ablation already
measured as worthless.** The same November 2026 package requires that logging in
from a new device deactivates the linked cards, and that biometric identification
be performed before an account is used from a different device or after a
password reset. `DEVICE_CHANGE` measures exactly that event as a *signal*; the
regulation converts it into a *hard control*.

Note what makes this cleaner than the `active_call` case. Session telemetry was
the second most valuable capability in the ablation, so its removal by regulation
is a loss. `device_telemetry=off` measured **+0.000 [+0.000, +0.000]** — no
effect at all, on five seeds. The capability whose detection value this project
could not measure is the one regulation found worth mandating as prevention.

That is not a contradiction, it is the substitution stated from the other end: a
signal contributes nothing to a *detector* precisely when the event it marks is
rare or already handled, and it is exactly such an event that is cheapest to
*prevent* outright. **A capability's detection value and its prevention value can
be inversely related, and an ablation measures only the first.** Any argument
that ranks data sources by ablation delta alone will therefore rank prevention
candidates last.

A blanket block carries a cost the rule did not: the bank's own support line
walking a customer through a transfer is blocked along with the fraudster.
Whitelisting the institution's support number is the obvious refinement, and
whether 3759 admits such an exception has to be read from the clause itself.

This paragraph rests on the published summary of 3759, not on the clause text.
Quote the wording directly before the thesis leans on it.

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
does not retrain automatically**. That is a property of the current operating
model, not of the design, and it changes the moment retraining is automated.

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
