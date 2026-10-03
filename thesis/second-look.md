# second-look

TabPFN's second look at the transfers just under the review cut-off.

The served model decides every transfer inside the Flink job in a fraction of a
millisecond. A transfer whose score falls in the band just under the cut-off -
from 0.0508 up to 0.1690 with today's model - is not let go: the job decides
`SECOND_LOOK`, and the transfer waits while this service asks TabPFN, which ranks
such transfers better (`ml/README.md`, TabPFN as a second opinion). At or above
TabPFN's own cut-off the transfer is held for the analyst like any REVIEW; under it,
it goes.

```
fraud.second_look --> TabPFN, two pieces of context --> transactions.scored  every decision
                                                     --> fraud.alerts        a REVIEW: the case that holds it
```

## Why this band, and these numbers

On the held-out slice the band holds 109 transfers (0.11%) and 20 of the frauds
the served model misses. The second look holds 32 of them, **14 fraud**, where a
cut-off lowered far enough to raise 32 alerts catches 9: recall on the slice goes
from 70.5% to 78.4%. That is the model served since 2026-10-02, with the confirmed
cases; under the one before it the band held 18 such frauds and the second look 11,
67.0% to 73.3%.

Everything was chosen on the cutoff rows first, by `ml/second_look.py`: the band
(the 100 highest-scored rows under the cut-off), TabPFN's cut-off in it (its F1
peak, 1e-4 under it), and two pieces of context rather than five (`ml/README.md`).
`second_look.json` records the review cut-off the band was chosen under, and the
job reads the band only while it serves that cut-off: a band chosen for one model
means nothing under another.

## The record it writes

The job's own record of the transfer, re-issued with the final decision. Two
fields change: `final_score` becomes TabPFN's - the score the decision was taken on
- and `model_version` becomes `second-look:<checkpoint>`. `ml_score` stays the
served model's, so case-manager still checks an explanation against the model it
explains, and the job's `stage_ms` stays behind. The warehouse therefore holds two
rows for such a transfer, `SECOND_LOOK` and then the final one, and a case the
second look opens carries its `model_version` and both scores: the queue says who
held it, and the demo shows the served model's risk beside TabPFN's.

## When it cannot answer

No transfer waits longer than five seconds (`SECOND_LOOK_DEADLINE_S`) for an answer
that is not coming; it is held for the analyst instead, unscored. The band is
suspicious by construction: letting it go unread would be the silent failure.

- **No feature vector, or TabPFN fails:** the transfer is held, `:unscored`.
- **It waited past the deadline** - a backlog after a restart, a slow answer: held
  at once, `:unscored`, without asking TabPFN, so the queue drains instead of
  growing.
- **The service is down, starting or stuck:** it renews `second-look:alive` in Redis
  between transfers and once a second while idle, and the key lapses five seconds
  after the last renewal. Without it the job does not send the transfer here: it
  holds it itself, as `second-look:unscored` (`fraud_job.py`). Only transfers already
  in `fraud.second_look` when the service stopped wait for it to return - and are
  then held at once, as late. Without the checkpoint the service says so and stops;
  without `second_look.json` beside the job the band is off, and band transfers are
  allowed as before.

## What it costs

One answer takes about 0.3 s on an idle laptop (320 ms median, 463 ms at the
longest, over the 218 band rows of both slices), after one to two minutes at start
to cache the context. It needs about 2.6 GB: each piece keeps its own copy of
TabPFN's weights, which members that cache their context cannot share - five pieces
took 5.2 GB. The cached answers agree with the uncached ones the cut-off was chosen
on to within 1e-5, and make the same decisions on both slices.

**Beside the running stream** an answer took 0.4 to 1.0 s, with two threads that
sleep while they wait (`OMP_NUM_THREADS`, `OMP_WAIT_POLICY` in docker-compose). With
torch's default - a thread per core, spinning - it took 6 to 20 s, because the
spinning threads fought the job's processes for the cores, and the waiting
transfers queued. Live, 2026-10-01, with the demo's stream at 200x: 122 transfers
sent for a second look in 15 minutes, every one decided - 95 let go, 27 held, each
with its case. That was 1.4% of the traffic, not 0.12%, because the job had just
restarted with empty sender state and every payee looked new.

**The wait it adds, end to end** (2026-10-02, on mains power, the demo's stream for
8 minutes at 37 transfers a second): 60 of 17,845 transfers (0.34%) went for a
second look, every one was decided, 8 held. From the job's decision to the second
look's, read off Kafka's own timestamps: **median 1.13 s, p90 2.0 s, longest 3.4 s**.
TabPFN took 0.58 s of it at the median and 0.88 s at the longest (the service's
log); the rest is the hops, out of the job to `fraud.second_look` and back. No other
transfer waits for it, and it does not slow the job: through the 100/s arm of the
throughput sweep (`docs/irp-framing.md` 7.6) the service sat at 0.5% CPU.

One instance decides one transfer at a time, so it keeps up while band transfers
arrive slower than about one in 0.6 s: at 0.12% of the traffic that is some 1,400
transfers a second, at the 1.4% after a restart about 120. Past that they queue, and
the deadline above holds the ones that have waited too long.

TabPFN reads the model's version from the checkpoint's file **name** - "v3.5" in it -
and without one silently takes the oldest version, with another configuration and
other answers. The container mounts the file under a name of its own, so `look.py`
links it back to the name in `second_look.json`, and refuses a file whose hash is
not the one the cut-off was chosen with.

TabPFN's weights come under the vendor's licence, accepted by the owner for this
research. A deployment would need its terms checked.

## Files

| File | What it does |
|---|---|
| `consumer.py` | the service: read a waiting transfer, score it, publish, then commit |
| `decide.py` | the decision on one transfer. No I/O |
| `look.py` | TabPFN over its pieces, as `ml/experiments/models.py` reads them |
| `config.py` | topics and mounts, from the environment |
| `tests/test_decide.py` | 7 tests: the decision, where it goes, holding what it cannot score or what waited past the deadline |

## Use

```powershell
cd ml; python second_look.py --cache models_matrix.npz --tabpfn-model <checkpoint>   # per model
.\run.ps1 serve-prep          # the band beside the job
.\run.ps1 up                  # builds and starts second-look with the stack
```

`TABPFN_CHECKPOINT` in `.env` names the weights, fetched by hand from the vendor's
page.
