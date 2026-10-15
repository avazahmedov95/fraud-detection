# Results: before and after the latest changes

**Before** is the system on 30 September 2026. **After** adds what was built from 1
to 5 October:

- a suspicious transfer is held, not only reported;
- the second look with TabPFN;
- the memory of confirmed fraud accounts in Redis (3 new features, 24 instead of
  21);
- retraining on people's decisions;
- a faster stream.

On 13 October four day-level features followed, 28 instead of 24; on 14-15 October
a generated month went through the running system; on 15 October every measurement
was repeated on fresh data. The newest comes first.

All data comes from our generator (`data.md`), so these are design targets, not
results from a real bank.

## 15 October: every measurement again, on fresh data

The earlier figures came from data we also looked at while building the system. On
15 October we repeated the measurements on datasets made with new seeds (201 and
up), and wrote down before each run what would count as confirmed.

**Quality on 20 fresh datasets** (seeds 201-220, 3,901 test frauds). Confirmed if the
model's F1 falls in 67.3-72.7%, the 13 October interval, and the second look beats a
lowered cut-off on most datasets:

| | recall | precision | F1 | alerts | fraud caught |
|---|---|---|---|---|---|
| model | 64.5% ± 2.5 | 78.3% ± 3.6 | 70.3% ± 1.7 | 3,238 | 2,515 |
| cut-off lowered to as many alerts | 70.7% ± 1.9 | 69.6% ± 3.5 | 69.8% ± 2.1 | 3,988 | 2,756 |
| model + second look | 72.9% ± 1.8 | 71.8% ± 3.5 | 72.0% ± 2.1 | 3,988 | 2,842 |

Confirmed. The second look beat the lowered cut-off on 17 datasets and tied on 3:
F1 +2.2 [+1.6, +2.8], more than the +1.2 of 13 October.

**Retraining when fraud changes, on 6 fresh datasets** (seeds 201-206, the 28
features, the retrainer as it is now). Confirmed if the retrained model's PR-AUC
beats the old one's in most of the 30 runs and it catches more frauds of the new
schemes:

| new scheme | known to the bank | PR-AUC, old | retrained | all known | its frauds caught: old -> retrained (all known) |
|---|---|---|---|---|---|
| none | 67% | 0.678 | 0.732 | 0.746 | - |
| mules | 54% | 0.540 | 0.644 | 0.710 | 10 -> 24 of 337 (59) |
| APP | 55% | 0.620 | 0.682 | 0.727 | 46 -> 72 of 401 (94) |
| account takeover | 56% | 0.596 | 0.694 | 0.741 | 45 -> 91 of 234 (160) |
| structuring | 55% | 0.574 | 0.687 | 0.737 | 45 -> 123 of 273 (186) |

Confirmed: better in all 30 runs. It caught 310 frauds of a new scheme against 146
(2.1 times; 1.8 on 5 October), and 499 if every fraud were known.

**The memory on IBM AML** (22 of the 28 features plus the memory, seeds 10-14).
Confirmed if the memory, as a bank learns, raises F1 in most runs:

| | recall | precision | F1 | alerts |
|---|---|---|---|---|
| the model alone | 44.7% | 82.6% | 58.0% | 894 |
| with the memory, as a bank learns | 54.8% | 70.9% | 61.8% | 1,278 |
| with the memory, every laundering known | 58.2% | 71.0% | 63.9% | 1,356 |

Confirmed in all five runs, by 3.4 to 4.2 points. At the same 1,278 alerts the model
alone catches 836 laundering, the memory 905.

**Public datasets:** PaySim and IBM AML gave exactly the 13 October figures (F1 38.7%
and 58.3%; at a 2% alert budget 891 of 1,840 and 1,367 of 1,653). Each model there
is trained on the dataset itself, and the recipe has not changed.

**Speed, three repeats** on mains power, in milliseconds. Confirmed if at 150 a
second 99 of 100 decisions stay under 300 ms in all three:

| transfers a second | mean | p99 |
|---|---|---|
| 10 | 26-31 | 46-50 |
| 50 | 27-34 | 58-71 |
| 100 | 42-43 | 86-90 |
| 150 | 51-80 | 112, 141, 425 |
| 200 | 62-79 | 143-255 |

Not confirmed: one repeat at 150 a second reached 425 ms. For 16 seconds it stayed
near 48 ms; then for 4 seconds decisions took 0.2-0.65 s, while two saves of the
job's state took 200 and 370 ms instead of about 20. The job saves all its state
(40 MB now) every 2 seconds, and a busy disk holds the stream up. At 200 a second all
three repeats stayed under 300 ms, so the capacity is there; the risk is the save.

## 14-15 October: a month through the running system

We generated a new month with another seed: 500,000 transfers, 929 of them fraud,
from people and cards the system had never seen. It went through the running system
at 100 transfers a second (83 minutes). A script played the analyst: a day after
each hold it gave the true verdict. Half of the clients reported a fraud the system
let go, a week after it. Both went through the demo's own code, so a confirmed
fraud's payee joined the memory at once.

| days | frauds caught | holds | holds that were fraud |
|---|---|---|---|
| 1-10 | 211 of 320 (66%) | 460 | 46% |
| 11-20 | 248 of 327 (76%) | 371 | 67% |
| 21-31 | 202 of 282 (72%) | 300 | 67% |
| the month | 661 of 929 (71%) | 1,131 | 58% |

At first every sender and payee is new. As their history and the memory of confirmed
fraud accounts grow, the false holds fall from 249 in the first ten days to 98 in the
last eleven. By type: structuring 93%, account takeover 85%, mules 62%, APP 56%. The
second look made 232 of the holds, 63 of them fraud. The bank learned of 721 of the
929 frauds (78%): the analyst confirmed 652, clients reported 69.

The retrainer then trained a new model on these labels. This showed two faults, both
fixed:

- It compared the models on the newest decisions. There the bank knows mostly the
  frauds the current model held itself, so the comparison favours it: the retrainer
  said "not better" while on the true answers the new model caught more (139 against
  134 of 199 at the same 175 alerts). It now compares on decisions a week old or
  more, when clients' reports are in.
- The new model also learned from the rows where the current model's cut-off was set.
  After a switch the second look chooses its band on those rows; read on rows the
  model had learned, the band held all 100 of its transfers on the test days. The new
  model now learns from the current model's fitting rows and the logged decisions.

With both fixes, on the 76,793 decisions a week old or more:

| | model of 13 October | retrained |
|---|---|---|
| F1 on what the bank knows (108 frauds) | 78.3% | 77.8% |
| F1 on the true answers (130 frauds) | 71.4% | 72.4% |
| alerts | 122 | 113 |
| frauds caught at 122 alerts (true answers) | 90 | 90 |
| the main dataset's last six days: F1, caught | 72.4%, 123 of 176 | 73.8%, 124 of 176 |

The retrainer says "not better", and the other readings agree the two are about the
same: a month of the fraud types the model already knows adds little. The model of
13 October stays. The large gain came on 5 October, with a scheme the bank had not
seen (1.8 times as many caught, below).

One more fault came out while switching: ONNX keeps split thresholds as float32, so
the served model would have given 3 transfers in 2,000 a different risk (by up to
0.0075). The export now moves every threshold to the float32 at or below it. The
features are float32 too, so no decision changes, and the ONNX model and the trained
one now agree within 0.0000002.

## 13 October: four day-level features

The model now reads 28 features instead of 24. The four new ones look at the last
day: how much money came into the payee, how many of its payers had not paid it for
a month, how many payees the sender paid for the first time, and how much went to
them. A mule collects from new people within a day; a taken-over card is drained to
new payees.

They were built after a check fixed before the run passed on all 20 generated
datasets. Here they are measured the way the system runs: five LightGBM models, the
cut-off where F1 is highest, and the second look. The same 20 datasets, 3,480 test
frauds:

| | recall | precision | F1 | alerts | fraud caught |
|---|---|---|---|---|---|
| model, 24 features | 62.6% | 74.0% | 67.4% | 2,963 | 2,188 |
| model, 28 features | 66.5% | 74.7% | 70.0% | 3,109 | 2,323 |
| model + second look, 24 features | 70.8% | 65.8% | 67.9% | 3,784 | 2,479 |
| model + second look, 28 features | 73.5% | 68.1% | 70.4% | 3,794 | 2,574 |

The model's F1 rose on all 20 datasets, by 2.6 points on average (95% interval 2.0
to 3.2). With the second look the system catches 95 more frauds for the same number
of alerts.

On the main dataset's last six days (176 frauds):

| | recall | precision | F1 | alerts | fraud caught |
|---|---|---|---|---|---|
| model, 24 features | 70.5% | 72.1% | 71.3% | 172 | 124 |
| model, 28 features | 69.9% | 75.0% | 72.4% | 164 | 123 |
| model + second look, 24 features | 78.4% | 67.6% | 72.6% | 204 | 138 |
| model + second look, 28 features | 77.8% | 74.1% | 75.9% | 185 | 137 |

Here the system catches about the same fraud with 19 fewer alerts. One test of 176
frauds moves by a case or two either way (mules 62.9% -> 60.0%, structuring 82.8% ->
86.2%), so the 20 datasets above are the fairer picture.

On the public datasets, with the 22 of the 28 features they can supply:

- IBM AML (for information): F1 56.8% -> 58.3%; at a 2% alert budget 1,349 -> 1,367
  of the 1,653 laundering.
- PaySim: no change. Each sender appears there once, so a sender's day is always
  empty: transfers F1 38.5% -> 38.7%, all transactions at a 2% budget 890 -> 891 of
  1,840.

Speed, measured on 14 October on mains power the same way as on 5 October: 99 of
100 decisions stay under 300 ms up to 150 transfers a second (p99 297 ms), against
200 a second before; at 50 a second the mean is 50 ms. The work per transfer is
still about 4 ms, as the payee's side is now read from Redis in one round trip
instead of three, so the difference is in the wait between Kafka and Python.

## Quality on our main dataset

Tested on the last six days of the data: 100,000 transfers, 176 of them fraud,
which the model never saw while learning.

| | recall | precision | F1 | alerts | fraud caught |
|---|---|---|---|---|---|
| before: model, 21 features | 67.0% | 63.4% | 65.2% | 186 | 118 |
| after: model, 24 features | 70.5% | 72.1% | 71.3% | 172 | 124 |
| after: model + second look | 78.4% | 67.6% | 72.6% | 204 | 138 |

The model now catches more fraud with fewer alerts. The second look catches 14
more frauds for 32 more holds; lowering the cut-off to the same number of alerts
would catch 9.

Share of each fraud type caught by the model, before and after: mules 48.6% ->
62.9%, APP 52.4% -> 55.6%, account takeover 89.8% -> 87.8% (one case),
structuring 82.8% -> 82.8%.

## Over 20 generated datasets

One test has only 176 frauds, so we repeated it on 20 datasets made with different
seeds (3,497 test frauds in all). This is the fairer picture.

| | recall | precision | F1 |
|---|---|---|---|
| before: model | 54.4% | 70.5% | 60.8% |
| after: model | 61.0% | 73.0% | 66.0% |
| after: model + second look | 68.8% | 65.1% | 66.4% |

The second look gives about eight points of recall for eight points of precision.
It beats simply lowering the cut-off to the same number of alerts on 13 of the 20
datasets.

## Speed

Time from the moment a transfer arrives to the decision, in milliseconds. The
target is 300 ms. Both days were measured on the running system in the same way,
with the sender inside Docker, on battery power.

| transfers a second | | mean | p95 | p99 |
|---|---|---|---|---|
| 10 | before (30 September) | 97 | 162 | 219 |
| 10 | after (5 October) | 31 | 34 | 48 |
| 50 | before (30 September) | 180 | 290 | 359 |
| 50 | after (5 October) | 32 | 58 | 75 |

Where the time goes, at 10 transfers a second: the system's own work is about 4-5
ms and the model 0.4 ms of it. The rest is the transfer waiting between Kafka and
Python: 91.5 ms before, 25 ms after. Two changes cut the wait: three copies of the
job run side by side instead of one, and each hands transfers to Python every 20
ms instead of every 50.

Under heavier load (5 October, 3,000 transfers per row, milliseconds):

| transfers a second | one copy: mean | one copy: p99 | three copies: mean | three copies: p99 | under 300 ms? |
|---|---|---|---|---|---|
| 100 | 2,380 | 4,134 | 64 | 209 | yes |
| 150 | 3,696 | 7,828 | 79 | 188 | yes |
| 200 | 4,976 | 10,445 | 114 | 290 | yes |
| 250 | 3,771 | 6,885 | 313 | 1,371 | no |

With one copy the system fell behind already at 100 transfers a second. With three,
99 of 100 decisions stay under 300 ms up to 200 a second.

Checked again on 6 October, after the code was simplified. At 50 transfers a second
two runs gave a mean of 35 and 37 ms, and p99 of 95 and 68 ms. That is the same,
within the normal spread between runs.

## Retraining when fraud changes

Tested on six generated datasets. In each, the bank's history (the first 40% of the
data) has no examples of one fraud scheme: a scheme the bank has not seen yet. On
the next 40% the bank learns about a fraud in only two ways. An analyst confirms a
held transfer (the bank knows a day later), or a client reports a missed one (half
of the clients do, a week later). On the last 20% we compare the old model with the
retrained one at the same number of alerts.

| new scheme | PR-AUC, old model | PR-AUC, retrained | if all fraud were known | frauds of the new scheme caught |
|---|---|---|---|---|
| none (control) | 0.695 | 0.730 | 0.741 | - |
| mules | 0.616 | 0.702 | 0.735 | 25 -> 47 of 238 |
| APP | 0.590 | 0.669 | 0.717 | 28 -> 50 of 367 |
| account takeover | 0.579 | 0.674 | 0.723 | 70 -> 120 of 273 |
| structuring | 0.577 | 0.672 | 0.717 | 64 -> 113 of 262 |

The retrained model was better in all 30 runs (6 datasets, 5 cases each). At the
same workload it caught about 1.8 times as many frauds of the new scheme (330
against 187). The gap to "all fraud known" is the cost of labels a bank never gets:
only about 55% of frauds became known in time.

## Public datasets, 5 October

| dataset | recall | precision | F1 |
|---|---|---|---|
| PaySim | 27.6% | 63.8% | 38.5% |
| IBM AML (for information) | 44.3% | 79.2% | 56.8% |

These did not change: they have no analysts' decisions, so the memory of confirmed
accounts is off there. PaySim is scored on its transfers between people.

### The memory on IBM AML, as a bank learns

On 7 October we played the analysts' decisions out from IBM AML's labels, the same
way as on our data. Before the test part every laundering is known a day later: the
bank's history. In the test part a laundering becomes known only if the model alone
held it (an analyst confirms it a day later), or, if it was missed, for half of them
a week later (the client reports it). Five runs with different seeds, the averages:

| | recall | precision | F1 | alerts |
|---|---|---|---|---|
| the model alone | 44.9% | 77.2% | 56.8% | 963 |
| with the memory, as a bank learns | 53.3% | 69.4% | 60.3% | 1,272 |
| with the memory, every laundering known (the ceiling) | 56.7% | 69.7% | 62.5% | 1,348 |

The memory raised F1 in all five runs, by 2.5 to 4.9 points. It is not only a longer
queue: at the same 1,272 alerts the model alone catches 809 laundering, the memory
881. The clients' reports come too late for the test part, so the gain comes from
the history and from the analysts' confirmations.

## At a 2% alert budget

Another way to read the same models: an analyst checks the 2% riskiest transfers of
the test part, and we count how much of the fraud is among them. The published
PaySim model reports its result this way, so here PaySim is scored on all its
transactions, as that model was. This reading needs no alert level, so on the
public datasets the model learns on the whole training part, as that model did (on
PaySim all 24 days). On our data it is the model the system runs. All three are read
with the 28 features of 13 October.

| data | alerts | fraud caught | alerts that are fraud |
|---|---|---|---|
| our data (the last six days) | 2,000 | 98.9% (174 of 176) | 8.7% |
| PaySim, all transactions | 3,229 | 48.4% (891 of 1,840) | 27.6% |
| the published PaySim model | - | 49.4% (916 of 1,854) | - |
| IBM AML (for information) | 17,949 | 82.7% (1,367 of 1,653) | 7.6% |

On our data 2% is far more alerts than there are frauds (2,000 against 176), so
almost all fraud is inside. On PaySim our features come close to the published
model, which also uses PaySim's own transaction type.

## Honest notes

- The "after" figures assume every fraud becomes known a day later. A bank that
  learns only from analysts and clients knows fewer (148 of the 176 in the test
  days), which costs about half a point of F1.
- Over 20 datasets the second look raises recall but lowers precision.
- The analyst of the month test never errs and always answers within a day; a real
  one is slower and sometimes wrong.
- The retraining test (5 October) and the memory on IBM AML (7 October) were
  measured with 24 features; on 15 October both were repeated with the 28.
- The job keeps each sender's history with no time limit and saves all of it every
  2 seconds. For a bank's millions of customers it would have to save only what
  changed and forget senders who stopped.
- Speed was measured on battery; on mains power the system is faster, so the
  figures are a safe lower bound.

## The data behind these figures

- `raw/graph_confirmation.json`: before and after for each of the 20 datasets
  (`model` is before; `every` is after; `bank` is after with only what a bank would
  learn).
- `raw/second_look_seeds.json`: the model alone, a lowered cut-off and the second
  look, for each of the 20 datasets.
- `raw/ibm_memory.json`: the memory on IBM AML, each of the five runs.
- `raw/day_features_seeds.json`: the 24 and the 28 features on the same 20
  datasets.
- `raw/fresh_seeds.json`, `raw/retrain_fresh.json`, `raw/ibm_memory_28.json`,
  `raw/speed_repeats.json`: the repeat on fresh data of 15 October, each run.
- The decisions of the speed runs are kept in `_warehouse_backup_2026-10-06/`
  (not in git). The last hash of its audit chain is recorded in commit `b2062f2`.
- The month test of 14-15 October was deleted from ClickHouse and Redis after it was
  read; its figures are kept only here.
