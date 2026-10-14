# Results: before and after the latest changes

**Before** is the system on 30 September 2026. **After** adds what was built from 1
to 5 October:

- a suspicious transfer is held, not only reported;
- the second look with TabPFN;
- the memory of confirmed fraud accounts in Redis (3 new features, 24 instead of
  21);
- retraining on people's decisions;
- a faster stream.

On 13 October four day-level features followed, 28 instead of 24; their before and
after comes first, in the next section.

All data comes from our generator (`data.md`), so these are design targets, not
results from a real bank.

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
- The retraining test (5 October) and the memory on IBM AML (7 October) were
  measured with 24 features.
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
- The decisions of the speed runs are kept in `_warehouse_backup_2026-10-06/`
  (not in git). The last hash of its audit chain is recorded in commit `b2062f2`.
