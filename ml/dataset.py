"""The training matrix: the CSV replayed through the job's own features and rules,
so the model trains on what it will be served."""

import hashlib
import os
import sys
from collections import defaultdict

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "stream-processor"))
import config as C            # noqa: E402
import features as F          # noqa: E402
import rules as R             # noqa: E402

FEATURE_NAMES = F.FEATURE_NAMES


def confirmations(df):
    """When each fraud's payee joins the confirmed accounts: a day later, as the
    analysts' verdicts add them live."""
    frauds = df[df["label_is_fraud"] == 1].to_dict("records")
    return sorted((pd.Timestamp(r["event_time"]).timestamp() + C.CONFIRMATION_DELAY_S,
                   F.payee_key(F.event_from(r))) for r in frauds)


def build_matrix(csv_path: str, nrows=None) -> pd.DataFrame:
    """The generated CSV replayed in time order; `nrows` replays only the first rows."""
    df = pd.read_csv(csv_path, nrows=nrows).sort_values("event_time").reset_index(drop=True)
    states = defaultdict(R.SenderState)
    # Keyed by payee, mirroring the shared store the live job reads.
    receiver_states = defaultdict(R.ReceiverState)
    marks, confirmed, j = confirmations(df), set(), 0
    rows = []
    for rec in df.itertuples(index=False):
        d = rec._asdict()
        event = F.event_from(d)
        now = pd.Timestamp(d["event_time"]).timestamp()
        while j < len(marks) and marks[j][0] <= now:
            confirmed.add(marks[j][1])
            j += 1
        # .get, not [...]: the sender's own inbound state exists only once someone
        # has paid them, and a defaultdict lookup would invent an empty one.
        paid_sender = receiver_states.get(F.sender_key(event))
        res = R.evaluate(event,
                         states[d["sender_card"]], now,
                         receiver_states[F.payee_key(event)],
                         sender_inbound_ts=(paid_sender.last_inbound_ts
                                            if paid_sender else None),
                         confirmed=confirmed)
        F.add_contact(receiver_states[F.sender_key(event)], F.payee_key(event), now)
        row = dict(zip(FEATURE_NAMES, res["features"]))
        row["cep_score"] = res["cep_score"]
        row["label"] = int(d["label_is_fraud"])
        row["fraud_type"] = d.get("label_fraud_type", "NONE")
        row["event_time"] = d["event_time"]
        rows.append(row)
    return pd.DataFrame(rows)


def _source(csv_path):
    """The CSV's SHA-256: a cache built from other data is not read."""
    h = hashlib.sha256()
    with open(csv_path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def cached_matrix(csv_path, cache=None):
    """build_matrix's features and labels, read from `cache` when it was built from
    this CSV with these columns, and built into it otherwise: the replay takes minutes."""
    source = _source(csv_path)
    if cache and os.path.exists(cache):
        with np.load(cache, allow_pickle=False) as z:
            if ([str(n) for n in z["names"]] == list(FEATURE_NAMES)
                    and "source" in z.files and str(z["source"]) == source):
                return z["X"], z["y"]
        print(f"{cache} was built from other data or columns: building it again")
    df = build_matrix(csv_path)
    X = df[list(FEATURE_NAMES)].astype("float32").values
    y = df["label"].values.astype("int8")
    if cache:
        np.savez_compressed(cache, X=X, y=y, names=np.array(list(FEATURE_NAMES)),
                            source=np.array(source))
    return X, y


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "../data-generator/out/transactions.csv"
    m = build_matrix(path)
    print(m[FEATURE_NAMES + ["cep_score", "label"]].describe().T.to_string())
    print(f"\nrows: {len(m):,}  positives: {int(m.label.sum())}  ({m.label.mean():.2%})")
