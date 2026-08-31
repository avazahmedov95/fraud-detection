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
