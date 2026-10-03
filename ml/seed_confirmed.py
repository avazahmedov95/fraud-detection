"""Seeds the accounts in confirmed frauds the job reads (config.CONFIRMED_KEY) with
the labelled history: the payees of the frauds confirmed by the first held-out
transfer, the set the model's test rows start from. The analysts' verdicts add to
it from there (case-manager/store.py).

    python seed_confirmed.py [--host localhost] [--port 6379]
"""

import argparse

import pandas as pd
import redis

import dataset as D
import train as T


def history(csv_path):
    df = pd.read_csv(csv_path).sort_values("event_time").reset_index(drop=True)
    start = pd.Timestamp(df["event_time"].iloc[T.cut_index(len(df))]).timestamp()
    return {card for t, card in D.confirmations(df) if t <= start}


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=6379)
    a = ap.parse_args()
    cards = sorted(history(T.CSV))
    added = redis.Redis(host=a.host, port=a.port).sadd(D.C.CONFIRMED_KEY, *cards)
    print(f"{D.C.CONFIRMED_KEY}: {len(cards):,} accounts from the history, {added:,} new")
