# Threats: who attacks, and what the system can see

## Scope

**In scope:** fraud on instant card-to-card transfers between people (UzCard and
HUMO), caught by the sending bank between the payer's confirmation and the payment.
The attacker uses the payment system the way a customer does.

**Out of scope:**
- attacks on the system itself (Kafka, Flink, ClickHouse); the security measures
  are in `system.md`;
- insiders at the bank;
- card payments in shops and online;
- cross-border transfers. For these the sender is not our bank's client, so only
  the payee side could work. They are large: in 2025, 46% of the money sent to
  Uzbek cards from abroad came as P2P transfers (Central Bank, March 2026).

## What we protect

| What | Goal | If it fails |
|---|---|---|
| money in transit | stop fraud before the money leaves | the loss cannot be undone |
| trust in instant payments | do not stop honest transfers | people go back to cash |
| the regulator's rules (CBU 3759) | record the patterns that must be reported | sanctions |
| the record of decisions | every decision can be checked later | a decision cannot be defended |

The first two pull against each other. From 16 November 2026 the bank pays for
fraud within its own no-confirmation limit, so the balance between them becomes a
business decision.

## Three attackers

1. **The social engineer (APP).** Controls what the victim is told: the amount, the
   time, the payee, even "hang up before you confirm". Cannot change the victim's
   phone, place or history. Each attack needs a live conversation, which limits how
   many they can run.
2. **The account takeover.** Controls the session: device, place, timing, amount,
   with real login details. Such details do leak: in 2025 Uzbekistan's
   Cybersecurity Centre found 1,697 login and password pairs on the darknet. Cannot
   fake the victim's past, and cannot be in two places at once. Has little time
   before the victim notices.
3. **The mule network.** Controls many accounts, how old they are, how many people
   pay each one, and when. Cannot avoid the money meeting in one place: that is the
   scheme itself. Spreading it out costs time and money.

## How easy each check is to avoid

| Check | To avoid it, the attacker must | Cost |
|---|---|---|
| amount unusual for this person | ask for less | low |
| new payee with a large amount | send a small amount to the payee first | low |
| many transfers quickly | slow down | low, but high for a takeover, which has no time |
| structuring | stay away from the band under the threshold | low, but that is the scheme itself |
| call during confirmation | tell the victim to hang up before confirming | very low |
| unusual region | use a connection in the right city | low |
| impossible travel | know where the victim really was recently | high |
| many senders to one payee (fan-in) | spread the money over more accounts and hours | high: it slows the network down |

We tested one of these. If an APP fraudster first sends one small transfer to the
payee, the share of such frauds we catch falls from 56% to 32%.

**The most useful signals are not the hardest to avoid.** The app's session signals
are the second most useful data source, and the cheapest to avoid. How much a
signal helps and how easily it can be avoided are different things, and a report
should give both.

## The payee gap

A sending bank sees only the number of the receiving card. It knows the person
behind it only for its own clients: 6.85% of transfers. So a mule with several
cards looks like several payees to us, at no cost to the mule. A national platform
that knows the person behind every card would close this gap.

## The regulator counts the same things

The Central Bank's anti-money-laundering rules (No. 343-В-12 of 3 March 2023) call
P2P activity suspicious by counts over up to 30 days:

- 500 BRV or more received on one card from one or several cards;
- five or more cards paying one foreign wallet;
- 20 or more cards on one account.

Our payee-side features count the same things over an hour, a day and a week. These
rules are written for the bank that holds the collecting account; our system sits
at the sending bank.

## Drift or evasion?

Both make detection worse. They can be told apart by watching each feature
separately for fraud and for normal transfers:

- normal and fraud transfers change together: **drift**, people's habits changed;
- only fraud changes, on a feature the attacker controls: **evasion**;
- a feature nobody can control changes (fan-in, travel): probably a **broken data
  feed**.

The retrainer reports every day which features have moved since training.

Prevention can remove a signal before an attacker does. CBU resolution 3759 (21
January 2026) requires the bank app to block use during calls, so "a call during
confirmation" should disappear from the data. The rules from 16 November 2026 also
switch cards off when someone logs in from a new device. A detection system should
not take credit for what prevention removes.

## Attacks on the system itself

| Attack | What protects against it |
|---|---|
| forged messages on `transactions.raw` | certificates on both sides (mutual TLS) and message encryption, available but off by default |
| changing a decision afterwards | the audit chain shows any change, deletion or reordering |
| probing the model | the customer sees only "allowed" or "held" |
| poisoning the training data | retraining runs every day by itself, and clients' reports become labels, so false reports could teach the model wrong things; a person must switch on each new model after reading its comparison |

## What remains

1. A careful social engineer (a moderate amount, a known payee, no call) avoids
   every cheap check. Only the payee side remains: the money still has to arrive
   somewhere.
2. A takeover with malware on the victim's phone has the right device and place.
   Only the speed and amount checks remain, and patience avoids them.
3. A mule network with money and time avoids detection, but becomes slower and
   smaller.

In each case what still works is a check that costs the attacker something real.
That, more than catching everything, is the goal.
