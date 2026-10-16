# Results

The latest measurement of each part of the system. All data comes from our
generator (`data.md`), so these are design targets, not results from a real bank.
Older measurements are in git (`README.md`).

## Quality on the main dataset

The last six days of the main dataset: 100,000 transfers, 176 of them fraud, which
the model never saw while learning.

| | recall | precision | F1 | alerts | fraud caught |
|---|---|---|---|---|---|
| model | 69.9% | 75.0% | 72.4% | 164 | 123 |
| model + second look | 77.8% | 74.1% | 75.9% | 185 | 137 |

The second look holds 21 more transfers, 14 of them fraud. Lowering the cut-off to
the same 21 alerts would catch 7. By type, the model catches structuring 86%,
account takeover 86%, mules 60% and APP 56%.

## Quality on 20 fresh datasets

One test of 176 frauds moves by a case or two either way, so on 15 October we
repeated it on 20 datasets with new seeds (201-220, 3,901 test frauds), never used
while building the system. Averages with 95% intervals:

| | recall | precision | F1 | alerts | fraud caught |
|---|---|---|---|---|---|
| model | 64.5% ± 2.5 | 78.3% ± 3.6 | 70.3% ± 1.7 | 3,238 | 2,515 |
| cut-off lowered to as many alerts | 70.7% ± 1.9 | 69.6% ± 3.5 | 69.8% ± 2.1 | 3,988 | 2,756 |
| model + second look | 72.9% ± 1.8 | 71.8% ± 3.5 | 72.0% ± 2.1 | 3,988 | 2,842 |

The second look beats a lowered cut-off on 17 datasets and ties on 3: F1 +2.2
[+1.6, +2.8]. Against the model alone it gives eight points of recall for six and a
half of precision.

The four day-level features (money into the payee, payers new to it, the sender's
first-time payees and the money sent to them) were added on 13 October. On 20
earlier datasets they raised the model's F1 on every one, by 2.6 points [2.0, 3.2].

## A month through the running system

On 14-15 October a new month went through the running system: 500,000 transfers, 929
of them fraud, from people and cards the system had never seen, at 100 a second. A
script played the analyst: a day after each hold it gave the true verdict. Half of
the clients reported a missed fraud a week later.

| days | frauds caught | holds | holds that were fraud |
|---|---|---|---|
| 1-10 | 211 of 320 (66%) | 460 | 46% |
| 11-20 | 248 of 327 (76%) | 371 | 67% |
| 21-31 | 202 of 282 (72%) | 300 | 67% |
| the month | 661 of 929 (71%) | 1,131 | 58% |

At first every sender and payee is new. As their history and the memory of
confirmed fraud accounts grow, false holds fall from 249 in the first ten days to 98
in the last eleven. The bank learned of 721 of the 929 frauds (78%): the analyst
confirmed 652, clients reported 69.

A model retrained on these labels was about as good as the served one (F1 72.4%
against 71.4% on the true answers, the same 90 frauds caught at 122 alerts), so the
served model stays: a month of fraud types the model already knows adds little.
The test also showed two faults in the retrainer, both fixed: it compared the models
on decisions too new for clients' reports, which favours the served model; and it
learned from the rows where the served model's cut-off was set, which broke the
second look's band after a switch.

## Retraining when fraud changes

Six fresh datasets (seeds 201-206). In each, the bank's history (the first 40%) has
no examples of one fraud scheme. On the next 40% the bank learns of a fraud only
when an analyst confirms a hold (a day later) or a client reports a missed one (half
of them, a week later). On the last 20% the old and the retrained model are compared
at the same number of alerts. Averages, 15 October:

| new scheme | known to the bank | PR-AUC, old | retrained | all known | its frauds caught: old -> retrained (all known) |
|---|---|---|---|---|---|
| none | 67% | 0.678 | 0.732 | 0.746 | - |
| mules | 54% | 0.540 | 0.644 | 0.710 | 10 -> 24 of 337 (59) |
| APP | 55% | 0.620 | 0.682 | 0.727 | 46 -> 72 of 401 (94) |
| account takeover | 56% | 0.596 | 0.694 | 0.741 | 45 -> 91 of 234 (160) |
| structuring | 55% | 0.574 | 0.687 | 0.737 | 45 -> 123 of 273 (186) |

The retrained model was better in all 30 runs. It caught 310 frauds of a new scheme
against 146, 2.1 times as many; 499 if every fraud were known. The gap is the labels
a bank never gets.

## The memory on IBM AML

IBM AML with the analysts' verdicts played out from its labels the way a bank learns
them: 22 of the 28 features plus the memory of confirmed accounts, five seeds, 15
October:

| | recall | precision | F1 | alerts |
|---|---|---|---|---|
| the model alone | 44.7% | 82.6% | 58.0% | 894 |
| with the memory, as a bank learns | 54.8% | 70.9% | 61.8% | 1,278 |
| with the memory, every laundering known | 58.2% | 71.0% | 63.9% | 1,356 |

The memory raised F1 in all five runs, by 3.4 to 4.2 points. It is not only a longer
queue: at the same 1,278 alerts the model alone catches 836 laundering, the memory
905.

## Public datasets

The model trained on each dataset itself, with the 22 of the 28 features it can
supply (`validation/README.md`), 13 October, repeated with the same result on 15
October: PaySim F1 38.7%, IBM AML 58.3% (for information only: its accounts include
banks and companies).

At a 2% alert budget an analyst checks the 2% riskiest transfers of the test part.
The published PaySim model reports its result this way, so PaySim is scored here on
all its transactions, as that model was:

| data | alerts | fraud caught | alerts that are fraud |
|---|---|---|---|
| our data (the last six days) | 2,000 | 98.9% (174 of 176) | 8.7% |
| PaySim, all transactions | 3,229 | 48.4% (891 of 1,840) | 27.6% |
| the published PaySim model | - | 49.4% (916 of 1,854) | - |
| IBM AML | 17,949 | 82.7% (1,367 of 1,653) | 7.6% |

On our data 2% is far more alerts than there are frauds, so almost all fraud is
inside. On PaySim our features come close to the published model, which also uses
PaySim's own transaction type.

## Speed

From a transfer's arrival to the decision, in milliseconds, on mains power, 15
October. The target is 300 ms. Three repeats at each rate:

| transfers a second | mean | p99 |
|---|---|---|
| 10 | 26-31 | 46-50 |
| 50 | 27-34 | 58-71 |
| 100 | 42-43 | 86-90 |
| 150 | 51-80 | 112, 141, 425 |
| 200 | 62-79 | 143-255 |

The job's own work is about 3 ms a transfer and the model 0.4 ms of it; the rest is
the wait between Kafka and Python. One repeat at 150 a second reached 425 ms: for 4
seconds two saves of the job's state took 200 and 370 ms instead of about 20. To
find the cause we ran the job in four versions, three repeats each:

| version of the job | p99, 150 a second | p99, 200 a second | saves over 150 ms | rules and memory, ms |
|---|---|---|---|---|
| keeps every sender; saves wait for all three copies | 107-147 | 157-654 | 12 | 0.5-0.8 |
| forgets through Flink's state TTL; saves wait | 144-348 | 191-1044 | 12 | 2.3-4.4 |
| state TTL; saves do not wait (unaligned) | 132-242 | 183-337 | 1 | 2.3-4.0 |
| forgets through a daily timer; saves do not wait | 128-173 | 155-268 | 3 | 0.6-0.7 |

The state's size was not the cause (11 MB in all four). Saves waited until all three
copies of the job reached them, and a busy copy delays that; unaligned saves do not
wait. Flink's state TTL cost 2-3 ms a transfer; a timer per sender, moved at most
once a day, costs nothing measurable. The job runs the last version: 99 of 100
decisions under 300 ms at 150 and at 200 transfers a second in all three repeats.
The first table was taken with the first version; the two changes matter only under
load.

## Notes

- On the main dataset every fraud's payee counts as confirmed a day after the fraud.
  A real bank learns fewer (78% in the month test), so the memory helps less there.
- The second look raises recall and lowers precision.
- The analyst of the month test never errs and always answers within a day; a real
  one is slower and sometimes wrong.
- The job forgets a sender silent for 90 days, longer than any window a feature
  reads. It saves all its state every 2 seconds; for a bank's millions of customers
  it would have to save only what changed.
- Everything runs on one laptop, so speed depends on power and on other programs.

## The data behind these figures

- `raw/fresh_seeds.json`: the 20 fresh datasets, each run.
- `raw/day_features_seeds.json`: the 24 and the 28 features on 20 earlier datasets.
- `raw/retrain_fresh.json`: retraining when fraud changes, each run.
- `raw/ibm_memory_28.json`: the memory on IBM AML, each run.
- `raw/speed_repeats.json`: every speed repeat and the four versions of the job.
- The month test was deleted from ClickHouse and Redis after it was read; its
  figures are kept only here.
