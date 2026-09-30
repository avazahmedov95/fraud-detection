# demo

One page for showing the system at work: the transfers streaming through, a fraud
case replayed on demand with the decision and the reasons for it, the analyst queue
those decisions fill, the Grafana dashboard, where the decision time goes, and what
the model scored when it was trained. Russian and English, switched on the page.

```powershell
.\run.ps1 up            # the stack, the demo container with it
.\run.ps1 submit-job    # the Flink job; the page opens once it is running
```

Then http://localhost:8090, from this machine only: the page can send transfers and
close cases, so the port is published on the loopback address alone.

## It shows the running system or nothing

The page is the stack's `demo` container (`infra/demo/Dockerfile`), not a host
process, for two reasons. It exists only while the stack does. And the transfers it
sends are stamped on the containers' clock, the one the job and Kafka stamp theirs
on; the host's clock drifts from it by hundreds of milliseconds, which is why the
latency runs moved their producer into the Docker network.

Until Kafka, ClickHouse, the Flink job and the generator's dataset are all there,
the page shows one screen: which of the four is missing and the command that
starts it. The API refuses everything but its status in the meantime. Nothing on
the page is sample data.

## Four tabs, and where each comes from

It holds no detection logic. Everything on the page is the running system's own
output, read where the rest of the project reads it:

| view | what it shows | reads | writes |
|---|---|---|---|
| Desk: stream | every decision since the container started, twenty at a time and more on request, filtered by either card, amount, time and decision - amount, route, risk, decision - with the counts and the median time from arrival to decision | `transactions.scored` | the background replay: `data-generator/kafka_producer.py`, unchanged, at 100-500x the dataset's own pacing, from a random row of the held-out part, into `transactions.raw` |
| Desk: fraud cases | one real episode sent through the job - the sender's usual transfers, then the fraud - with what was caught and what was falsely flagged; its last alert opens by itself | `transactions.scored` | the episode's rows, into `transactions.raw` |
| Desk: selected transfer | the model's risk against the alert level, its reasons, and the hard rules that fired, with what each one did: decide by itself (the two the regulator requires), name the alert, or neither | case-manager's `Explainer`; `ml/models/thresholds.json` | - |
| Desk: analyst queue | the cases opened since the container started, in case-manager's own order, twenty at a time, filtered by either card, amount, time and alert type | `fraud.cases`, through case-manager's `CaseStore` | a verdict, through the same store (`resolved_by = demo`) |
| About the project | the components, each opening its details; the path one real decision took, step by step with its own stage times; the model; its 21 features and the 10 rules - with the numbers each checks and what it does - each list behind a button | `transactions.scored` (the decision and its `stage_ms`); `ml/models/metrics.json`; the warehouse's stage averages | - |
| Grafana | the provisioned overview dashboard, in a frame | ClickHouse, through Grafana | - |
| Data & results, live | each stage's average time over the latest 1,000 decisions, adding up to the time from arrival to decision; the analyst's verdicts | `transactions_scored.stage_*_ms`; `fraud.cases` | - |
| Data & results, at training | one table dated by the model's export: each dataset, what it holds, our result on it and the published one | `ml/models/metrics.json` and `manifest.json` as `ml/` wrote them; `results.json` | - |

The analyst does not release or block a payment. The system has already decided,
and the prototype records the decision without enforcing it
(`case-manager/README.md`), so the analyst's two buttons are a verdict - the only
real label the system gets.

The training figures are not live, and the page says so: `ml/train.py` measured them
once, on the part of the data the model never saw. The public-dataset figures are
not computed here either. `results.json` summarises `validation/README.md` and
carries each figure with the line it came from; the audit fails when that line is
gone.

## The fraud cases are real episodes, not scripts

Each case is picked from the held-out 20% of the dataset - the rows after
`ml/train.py`'s cut, which the model never trained on - and replayed:

- **the sender gets a new card** with the same six-digit BIN, so the issuing bank
  is unchanged, and the job's state for that sender
  starts empty: it meets them through the episode's own history, up to eight of
  their earlier transfers, before the fraud arrives;
- **receivers keep their cards**, so a mule paid inside the episode is the same
  card when it pays out;
- **times move, gaps stay**: the episode is shifted to end now, with every gap
  between its rows kept, since the job's windows run on event time.

Phone scam (APP), account takeover, money mule, structuring, and an ordinary
transfer as the control. Each run is scored against the rows' own labels:
fraudulent transfers caught, and false alarms on the ordinary ones. A miss shows
as a miss.

## What it does not show

- **Anything before the container started**, on the desk. The stream and the queue
  begin when `server.py` does; the warehouse holds every earlier run, and the
  Data & results tab reads it.
- **The stage times of a producer outside Docker.** The first stage runs from the
  producer's `ingested_at` to Kafka's append time, two clocks that agree only
  inside the Docker network.
- **A clean warehouse.** The background replay sends dataset rows under their
  original ids, and ClickHouse keeps every copy. The fraud cases use new ids.

`server.py` is the whole server - standard-library HTTP, kafka-python and
case-manager's own modules; `index.html` is the page and `about.js` its About
tab; `tests/` covers which rows make an episode and how they are replayed, when
the page counts the system as running, what the filter lets through, and that the
stages, the features, the rules, their numbers and the alert names it shows are
the job's.
