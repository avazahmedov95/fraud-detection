# second-look

TabPFN's second look at the transfers just under the review cut-off. The job sends
such a transfer to `fraud.second_look` instead of letting it go; this service scores
it with TabPFN and publishes the decision to `transactions.scored`: at or above
TabPFN's own cut-off the transfer is held for the analyst like any REVIEW, and the
sink writer opens its case; under it it goes.

| File | What it does |
|---|---|
| `consumer.py` | the service: read, score, publish, and renew `second-look:alive` in Redis |
| `look.py` | TabPFN over the context pieces in `ml/models/second_look.npz`, cached once at start |
| `decide.py` | the decision record, and the five-second deadline |
| `config.py` | topics, the deadline, Redis |
| `tests/` | `python -m pytest second-look -q` |

The band, TabPFN's cut-off and its context are chosen by `ml/second_look.py`; the
job sends a transfer here only while `second_look.json` was chosen under the cut-off
it serves. A transfer that waited five seconds or more is held unscored rather than
let go, and while the service is not renewing its Redis flag the job holds such
transfers itself. Each piece of context keeps its own copy of TabPFN's weights, a
gigabyte apiece; an answer takes about a third of a second.

The checkpoint is a gated download, fetched by hand and named by
`TABPFN_CHECKPOINT` in `.env`; without it the service says so and stops.
