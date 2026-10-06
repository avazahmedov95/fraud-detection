# Research

## The problem

In Uzbekistan people send money from card to card in seconds, on the UzCard and
HUMO networks. Fraudsters use this in four main ways:

- they talk people into sending money (APP fraud);
- they take over accounts;
- they collect money through mule accounts;
- they split large sums to stay under the reporting threshold (structuring).

An instant transfer cannot be taken back, so the bank has to decide before the
money leaves. Our target is a decision in under 300 milliseconds.

The rules are changing too. From 16 November 2026 the Central Bank makes the bank
pay for fraud within the limit it sets for transfers without extra confirmation.
A bank that detects fraud better can afford a higher limit, so detection quality
becomes a business number, not only a cost.

## The question

How much does real-time fraud detection depend on the data a bank can see, rather
than on the detection method?

We look at it in three parts:

1. What does each kind of data add: the payee's side, the app's session signals,
   the region, the payee's account age, family links, the device?
2. Which of these can one bank actually get in Uzbekistan today, and what does it
   lose without the rest?
3. Does the way the data stream is organised limit what can be detected, whatever
   the model?

We do not compare streaming engines, such as Flink against Spark. That is a
different question, and a fair comparison would need both engines tuned by someone
neutral. The whole system runs on Apache Flink.

## How we answer it

- We built a working system (`system.md`). It decides on every transfer in real
  time with rules and a model, holds suspicious transfers for an analyst, and
  learns from the analysts' decisions.
- No public data of Uzbek card transfers exists, so we generate our own
  (`data.md`). We also test the model on two public datasets.
- We measure what each part adds on data the model has never seen
  (`results.md`).

## What we found

1. **What the bank can see matters more than the method.** Measured in September
   on an earlier, easier version of our data, five datasets each time. Without the
   payee's side (who else is paying this card) detection fell the most: PR-AUC
   -0.036. Without the app's session signals it fell almost as much: -0.034.
   Family links, the device and the region added almost nothing to the model. The
   region still matters for one rule: it catches a session from a place the person
   could not have reached in time, with no false alarms on 775 real journeys.
2. **Money flowing into a mule is invisible from the sender's side.** The stream is
   split by sender, so each sender sees one ordinary transfer. 80% of mule
   transfers are of this kind. Keeping the payee's side in Redis raised the share
   of mule fraud caught from 55.7% to 83.4%.
3. **The sending bank usually does not know who owns the receiving card.** Only
   6.85% of transfers go to a client of the same bank. So the payee's account age,
   a popular fraud signal, is unknown for 93% of transfers, and we removed it. This
   is an argument for checking payees at the national level.
4. **Data from people helps most now.** A memory of the accounts in confirmed frauds,
   built from analysts' decisions, raised the share of fraud caught from 67.0% to
   70.5% with fewer false alarms. Retraining on people's decisions catches 1.8
   times as many frauds of a new scheme (`results.md`).
5. **Time is lost in waiting, not in the model.** The model takes 0.4 ms and all
   the work about 4-5 ms. The rest of each decision is the transfer waiting between
   Kafka and Python. Shortening that wait cut the average decision time at 50
   transfers a second from 180 ms to 32 ms.
6. **The most dangerous failures are silent.** We found 22 failures where the
   system kept running and every check looked fine, while part of the job had
   stopped. For example, the model file was not found and the job quietly scored
   with rules only; and the text "False" was read as true on every live transfer.
   The system now records what actually ran, not what was configured.
7. **Public data cannot test the payee side.** Public datasets hide the links
   between accounts that this kind of detection needs. On PaySim, a rule built only
   from the sender's own history still separates fraud from normal transfers four
   times better than chance. But the mule rule finds nothing there, because PaySim
   has no collection stage.
8. **Rule limits must follow the data that is available.** On PaySim our rules first
   flagged none of 2,520 frauds: fewer signals were available, so the rule score
   never reached the fixed limit. Scaling the limit to what can still be reached
   flagged 150 of them.

## Limits

- The data is generated. Every figure is a design target, not a result from a real
  bank.
- Everything runs on one laptop, so the speed figures depend on power and load.
- About a tenth of fraud is never reported, as in a real bank, and the demo has few
  analysts' decisions so far.
- The generated data has no weekly or monthly rhythm, so the hour of day is a weak
  signal.
- Cross-border transfers are not modelled, although in 2025 46% of the money sent
  to Uzbek cards from abroad came as P2P transfers (Central Bank, March 2026).
