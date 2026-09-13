# demo

One page for showing the system at work: the transfers streaming through, a
fraud episode replayed on demand with the decision and the reasons for it, and
the analyst queue those decisions fill. Russian and English, switched on the page.

```powershell
.\run.ps1 up                                  # the stack
.\run.ps1 submit-job                          # the Flink job, if it is not running
.venv311\Scripts\python.exe demo\server.py    # then open http://localhost:8090
```

## Three views, and where each comes from

It holds no detection logic. Everything on the page is the running system's own
output, read where the rest of the project reads it:

| view | what it shows | reads | writes |
|---|---|---|---|
| Stream | every decision since the server started - amount, route, decision, risk, rules fired - with counts and the median decision time | `transactions.scored` | the background replay: `data-generator/kafka_producer.py`, unchanged, at 100-500x the dataset's own pacing, into `transactions.raw` |
| Scenarios | one episode row by row - the sender's usual transfers, then the fraud - with what was caught, what was falsely flagged, and why | `transactions.scored`; case-manager's `Explainer` for the reasons | the episode's rows, into `transactions.raw` |
| Analyst | the case queue in case-manager's own order, with the reasons; by default only the cases opened since the server started | `fraud.cases`, through case-manager's `CaseStore` | a verdict, through the same store (`resolved_by = demo`) |

## The scenarios are real episodes, not scripts

Each scenario is picked from the held-out 20% of the dataset - the rows after
`ml/train.py`'s cut, which the model never trained on - and replayed:

- **the sender gets a new card** with the same six-digit BIN, so the issuer, the
  network and the on-us test are unchanged, and the job's state for that sender
  starts empty: it meets them through the episode's own history, up to eight of
  their earlier transfers, before the fraud arrives;
- **receivers keep their cards**, because the payee's account age is looked up in
  Neo4j by card and a new card would read as unknown. For the same reason a mule,
  paid inside the episode, keeps its card when it pays out;
- **times move, gaps stay**: the episode is shifted to end now, with every gap
  between its rows kept, since the job's windows run on event time.

Ordinary transfer, phone scam (APP), account takeover, money mule, structuring.
Each run is scored against the rows' own labels: fraudulent transfers caught, and
false alarms on the ordinary ones.

## What it does not show

- **Anything before the server started.** The stream begins when `server.py` does.
- **End-to-end latency.** The decision time is the job's own `scoring_ms`, on one
  clock. The host's and the containers' clocks drift apart by hundreds of
  milliseconds (Makefile, `produce-stream-docker`), so an end-to-end figure needs
  the in-network producer, not this page.
- **A clean warehouse.** The background replay sends dataset rows under their
  original ids, and ClickHouse keeps every copy. The scenarios use new ids.

`server.py` is the whole server - standard-library HTTP, kafka-python and
case-manager's own modules; `index.html` is the page; `tests/` covers which rows
make an episode and how they are replayed.
