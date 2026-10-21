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

1. **What the bank can see matters more than the method.** On five fresh datasets,
   without the payee's side (who pays this card, the counterparties over a day and
   a week, the memory of confirmed accounts) PR-AUC fell from 0.748 to 0.555 and F1
   from 71.7% to 55.6%. The app's session signals add less (PR-AUC 0.019); the
   region and a new device add nothing to the model, a family link about one point
   of F1. The region still serves one rule: it catches a session from a place the
   person could not have reached in time, and held 7 of 38,966 legitimate moves
   between regions (`results.md`).
2. **Money flowing into a mule is invisible from the sender's side.** The stream is
   split by sender, so each sender sees one ordinary transfer; 81% of mule transfers
   are of this kind. So the payee's side is kept in Redis. Counting, over the last
   day, the payee's payers who are new to it and the sender's first-time payees
   raised F1 by 2.6 points on all of 20 generated datasets (`results.md`).
3. **The sending bank usually does not know who owns the receiving card.** Only
   7% of transfers go to a client of the same bank. So the payee's account age,
   a popular fraud signal, is unknown for 93% of transfers, and we removed it. This
   is an argument for checking payees at the national level.
4. **Data from people helps most now.** A memory of the accounts in confirmed
   frauds, built from analysts' decisions, raised F1 on IBM AML in all five runs, by
   3.7 to 6.3 points. In a month through the running system, false holds fell from
   249 in the first ten days to 98 in the last eleven as the history and the memory
   grew. Retraining on people's decisions catches 2.1 times as many frauds of a new
   scheme (`results.md`).
5. **Time is lost in waiting, not in the model.** The model takes 0.4 ms and the
   job's own work about 3 ms. The rest of each decision is the transfer waiting
   between Kafka and Python. Three copies of the job, a 20 ms hand-off and state
   saves that do not wait for all copies keep 99 of 100 decisions under 300 ms up to
   200 transfers a second.
6. **The most dangerous failures are silent.** We found 22 failures where the
   system kept running and every check looked fine, while part of the job had
   stopped. For example, the model file was not found and the job quietly scored
   with rules only; and the text "False" was read as true on every live transfer.
   The system now records what actually ran, not what was configured.
7. **Public data cannot test the payee side.** Public datasets hide the links
   between accounts that this kind of detection needs. PaySim has no collection
   stage, so the mule rule finds nothing there; only the sender's own history works.
8. **Rule limits must follow the data that is available.** On PaySim our rules first
   flagged nothing: with fewer signals the rule score never reached a limit set on
   full data. The limit is now scaled to what the available data can still reach
   (`stream-processor/capabilities.py`).

## Limits

- The data is generated. Every figure is a design target, not a result from a real
  bank.
- Everything runs on one laptop, so the speed figures depend on power and load.
- About a tenth of fraud is never reported, as in a real bank.
- The generated data has no weekly or monthly rhythm, so the hour of day is a weak
  signal.
- Cross-border transfers are not modelled, although in 2025 46% of the money sent
  to Uzbek cards from abroad came as P2P transfers (Central Bank, March 2026).
