# How the system works

## One transfer, from start to decision

1. **The payment switch** creates the transfer. Our data generator plays this part
   (`data-generator/kafka_producer.py`). It sends each transfer to Kafka, topic
   `transactions.raw`, keyed by the sender's card so that one sender's transfers
   stay in order.
2. **The Flink job** (`stream-processor/`) reads it. Three copies of the job run
   side by side, each with its own senders.
   - It reads the sender's history from Flink's own memory.
   - It reads the payee's side from Redis: who paid this card in the last hour, day
     and week, which cards it dealt with, and whether any of them belong to
     confirmed frauds.
   - It computes 24 features (numbers that describe the transfer and its context)
     and runs 10 hard rules.
   - The model gives a risk score from 0 to 1. It is five LightGBM models averaged
     into one, run inside Flink as an ONNX file.
   - The decision:
     - **hold** (REVIEW) when the risk is at or above the cut-off, or when a rule
       the regulator requires fired (STRUCTURING, DAILY_LIMIT_BREACH);
     - **second look** when the risk is just under the cut-off;
     - otherwise **allow**.
   - Every decision goes to the topic `transactions.scored`, with its 24 feature
     values and the time each step took.
3. **The second look** (`second-look/`). Transfers just under the cut-off, about 1
   in 1,000, go to a second model, TabPFN. It answers in about 0.3 seconds. If it
   sees fraud, the transfer is held; otherwise it goes. If it does not answer within
   5 seconds, or is down, the transfer is held for the analyst.
4. **The sink writer** (`sink-writer/`) writes every decision to ClickHouse and adds
   it to the audit chain. For every hold it opens a case with the reasons in words:
   which features pushed the risk up, taken from the model's own trees.
5. **The analyst** (`demo/`: the demo page or the command line) blocks a held
   transfer as fraud or releases it as a false alarm. A verdict can be corrected,
   and a client's report of a fraud the system let go can be recorded. A confirmed
   fraud puts the payee's card on the list of confirmed fraud accounts in Redis, and
   the model sees it on the next transfers from or to that card.
6. **Retraining** (`ml/retrain.py`, the retrainer service). Every day it trains a new
   model on the logged decisions, with the labels the analysts and clients gave. It
   compares the new model with the current one at the same number of alerts, and
   reports which features have changed since training (drift). A person decides
   whether to switch the new model on.

The system never blocks a transfer by itself. A person does.

## The parts

| Part | What it does | Built with |
|---|---|---|
| data generator | people, transfers and fraud; plays the payment switch | Python |
| Kafka | the queues between the parts | Apache Kafka |
| Flink job | features, rules, model, decision | PyFlink, ONNX Runtime |
| Redis | the payee's side and the confirmed fraud accounts | Redis |
| second look | a second model for transfers just under the cut-off | Python, TabPFN |
| sink writer | stores decisions, the audit chain and the cases | Python, ClickHouse |
| ClickHouse | the warehouse: decisions, audit log, cases | ClickHouse |
| demo pages | the live system, the analyst's queue, the results; and a page for an audience that tells one fraud case step by step | Python, HTML |
| retrainer | a new model every day, switched on by a person | Python, LightGBM |
| Grafana | dashboards over ClickHouse | Grafana |

## The 24 features

- **The sender's own history (11):** the amount; the amount against the sender's
  usual amount and spread; whether the payee is new; transfers in the last 10
  minutes and hour; different payees in 10 minutes; transfers just under the
  reporting threshold in an hour; time since the last transfer; share of the daily
  limit used; the hour of day.
- **The payee's side (2):** how many different people paid this card in the last
  hour, and how much money came in.
- **Region (1):** whether the sender is away from their usual region.
- **App session (2):** whether there was a call during confirmation, and how long the
  confirmation took against the person's own usual time.
- **Counterparties over a day and a week (5):** how many people paid the payee, how
  many people the sender paid, and how long ago the sender's own account was last
  paid (money in, then money out is the mule pattern).
- **Confirmed fraud accounts (3):** whether the payee or the sender was in a
  confirmed fraud, and how many of the payee's contacts of the last week were.

## The 10 rules

| Rule | Fires when |
|---|---|
| NEW_PAYEE_HIGH_AMOUNT | a new payee gets more than 3 times the sender's usual amount, and over 2,000,000 UZS |
| VELOCITY | more than 5 transfers in 10 minutes |
| DISTINCT_PAYEE_BURST | more than 5 different payees in 10 minutes |
| STRUCTURING | 3 or more transfers in an hour just under the 10,000,000 UZS threshold |
| DAILY_LIMIT_BREACH | more than 100,000,000 UZS in a day |
| AMOUNT_DEVIATION | an amount far above the sender's usual (more than 4 standard deviations) |
| GEO_ANOMALY | a transfer from a region other than the sender's usual one |
| IMPOSSIBLE_TRAVEL | the sender would have to travel faster than 900 km/h between regions |
| COACHED_SESSION | a call during confirmation, and a much slower confirmation than usual |
| MULE_FAN_IN | 6 or more different people pay one card within an hour |

The rules also name the type of an alert, for example "account takeover".

## When something breaks

- **Redis is down:** the job keeps deciding. The payee-side features read as
  "nothing seen", and the job tries Redis again every 5 seconds.
- **ClickHouse is down:** decisions still go out. The sink writer counts what it
  could not store.
- **The second look is down:** the job holds the transfers just under the cut-off
  itself.
- **A part crashes:** Flink saves its state every 2 seconds and restarts from there.
  A few transfers may be decided twice; none is lost.
- **A broken message:** it is dropped and counted, so one bad record cannot stop the
  stream.

## Records and security

- **The audit chain.** Each stored decision carries a hash of its own content and of
  the record before it. Changing, deleting or reordering any record breaks the
  chain, and `run.ps1 verify-audit` finds the break. A hash of the raw transfer is
  taken when it enters, so a decision cannot be matched to a different transfer.
- **Encryption.** Messages can be encrypted (AES-256-GCM), and Kafka can require
  certificates on both sides (mutual TLS). Measured in September, encryption adds no
  noticeable delay; it makes messages about half as big again.
- **Access.** Every service listens only on this computer.
