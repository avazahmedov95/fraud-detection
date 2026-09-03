"""Builds the training matrix by replaying the CSV through the SAME feature
extractor the Flink job uses, so the model trains on what it will be served.
"""

import os
import sys
from collections import defaultdict

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "stream-processor"))
import features as F          # noqa: E402
import rules as R             # noqa: E402

FEATURE_NAMES = F.FEATURE_NAMES
_EVENT_KEYS = ("amount_uzs", "sender_pinfl", "receiver_pinfl", "device_id", "sender_region",
               "channel", "sender_network", "receiver_network",
               # behavioural session signals (backlog #7) - must be forwarded, otherwise
               # active_call / secs_login_z train as constant zeros.
               "active_call", "secs_login_to_confirm",
               # features.py derives the issuer from the BIN: same path as the live job.
               "sender_card", "receiver_card", "is_family_transfer")


def _as_bool(v):
    return str(v).strip().lower() in ("true", "1")


def _as_age(v):
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def build_matrix(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path).sort_values("event_time").reset_index(drop=True)
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
    for rec in df.itertuples(index=False):
        d = rec._asdict()
        event = {k: d[k] for k in _EVENT_KEYS}
        now = pd.Timestamp(d["event_time"]).timestamp()
        res = R.evaluate(event,
                         _as_age(d.get("receiver_account_age_days")),
                         states[d["sender_card"]], now,
                         receiver_states[F.payee_key(event)],
                         population=population)
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
