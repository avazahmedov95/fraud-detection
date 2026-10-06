# second-look

A second model, TabPFN, looks at the transfers just under the review cut-off. The
job sends such a transfer to the topic `fraud.second_look` instead of letting it go.
This service scores it with TabPFN and publishes the decision to
`transactions.scored`. At or above TabPFN's own cut-off the transfer is held for the
analyst, like any REVIEW, and the sink writer opens a case for it; below the cut-off
it goes.

| File | What it does |
|---|---|
| `consumer.py` | the service: read, score, publish, and keep the `second-look:alive` flag in Redis fresh |
| `look.py` | TabPFN over its examples in `ml/models/second_look.npz`, loaded once at start |
| `decide.py` | the decision record, and the five-second deadline |
| `config.py` | topics, the deadline, Redis |
| `tests/` | `python -m pytest second-look -q` |

`ml/second_look.py` chooses the band, TabPFN's cut-off and the examples TabPFN
learns from. The job uses the band only if it was chosen for the cut-off the job is
using. A transfer that has waited 5 seconds or more is held without a score rather
than let go, and while this service is not refreshing its flag in Redis, the job
holds such transfers itself. Each part of TabPFN's examples keeps its own copy of
the model's weights, a gigabyte each; an answer takes about a third of a second.

The weights are a gated download, fetched by hand and named by `TABPFN_CHECKPOINT`
in `.env`. Without them the service says so and stops.
