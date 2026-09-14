"""Builds the training matrix by replaying the CSV through the SAME feature
extractor the Flink job uses, so the model trains on what it will be served.

The row-to-event mapping is `features.event_from`, not a local copy. It was a
local copy - a fourth one, beside three in stream-processor - and a mapping
written out four times is four places to forget a column, where forgetting one
reads as absent rather than failing.
"""

import os
import sys
from collections import defaultdict

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "stream-processor"))
import features as F          # noqa: E402
import rules as R             # noqa: E402

FEATURE_NAMES = F.FEATURE_NAMES


def build_matrix(csv_path: str, age_unknown=None, nrows=None) -> pd.DataFrame:
    """`age_unknown`, if given, is called with the row count and returns a boolean
    array over the rows in time order. A True row is replayed with no payee age,
    as the live job replays every event while Neo4j cannot be read - withheld
    before the rules run, not blanked after, so FRESH_RECEIVER cannot fire and
    cep_score agrees with the missing age as it does live.
    """
    # nrows: the first rows only - the file is written in time order - for a caller
    # that needs valid feature vectors rather than the whole replay.
    df = pd.read_csv(csv_path, nrows=nrows).sort_values("event_time").reset_index(drop=True)
    unknown = None if age_unknown is None else age_unknown(len(df))
    states = defaultdict(R.SenderState)
    # Keyed by payee, mirroring the shared store the live job reads.
    receiver_states = defaultdict(R.ReceiverState)
    # cep_score is a model feature MULE_FAN_IN feeds: a population-relative threshold live
    # against a constant in training would make it mean two different things either side
    # of deployment - train/serve skew, which the single ordered FEATURE_NAMES makes
    # impossible. Offline the replay is one process, so this baseline sees exactly what
    # PopulationStore reads out of Redis in the job.
    population = R.PopulationBaseline()
    rows = []
    for i, rec in enumerate(df.itertuples(index=False)):
        d = rec._asdict()
        event = F.event_from(d)
        now = pd.Timestamp(d["event_time"]).timestamp()
        withheld = unknown is not None and unknown[i]
        res = R.evaluate(event,
                         None if withheld else
                         F.age_or_none(d.get("receiver_account_age_days")),
                         states[d["sender_card"]], now,
                         receiver_states[F.payee_key(event)],
                         population=population,
                         sender_inbound=receiver_states[F.payer_key(event)])
        row = dict(zip(FEATURE_NAMES, res["features"]))
        row["cep_score"] = res["cep_score"]
        row["label"] = int(d["label_is_fraud"])
        row["fraud_type"] = d.get("label_fraud_type", "NONE")
        row["event_time"] = d["event_time"]
        rows.append(row)
    return pd.DataFrame(rows)


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "../data-generator/out/transactions.csv"
    m = build_matrix(path)
    print(m[FEATURE_NAMES + ["cep_score", "label"]].describe().T.to_string())
    print(f"\nrows: {len(m):,}  positives: {int(m.label.sum())}  ({m.label.mean():.2%})")
