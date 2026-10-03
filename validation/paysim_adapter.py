"""This project's model on PaySim: what the file holds, its customer-to-customer
transfers (type TRANSFER, the P2P part of the file) replayed through the deployed
feature extractor, and the model fitted on the published split (24 days to train,
7 to test) - recall, precision and F1 on the test week.

    python paysim_adapter.py --file PS_20174392719_1491204439457_log.csv
"""

import argparse
import os

import pandas as pd

import harness as RP

#: The published split: `step` is the hour, and the first 24 days train.
CUT_STEP = 576


def to_events(df, scale):
    """PaySim rows -> events with only the fields PaySim has. Same-step events share
    a timestamp: sub-hour patterns are invisible, a property of PaySim."""
    for r in df.itertuples(index=False):
        yield RP.Event(ev={"amount_uzs": float(r.amount) * scale,
                           "sender_pinfl": r.nameOrig, "receiver_pinfl": r.nameDest},
                       ts=int(r.step) * 3600, label=int(r.isFraud))


def describe(df, transfers):
    fraud = df[df.isFraud == 1]
    print(f"PaySim: {len(df):,} simulated mobile-money transactions over "
          f"{df.step.max() // 24 + 1} days, hourly steps; fraud {len(fraud):,} "
          f"({len(fraud) / len(df):.2%}), all of it {' and '.join(sorted(fraud.type.unique()))}")
    print(f"  columns: {', '.join(df.columns)}")
    print(f"  scored here, the TRANSFER rows: {len(transfers):,}, fraud "
          f"{int(transfers.isFraud.sum()):,} ({transfers.isFraud.mean():.2%}); "
          f"{transfers.nameOrig.nunique():,} senders, {transfers.nameDest.nunique():,} payees")


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--file", required=True, help="the PaySim CSV")
    ap.add_argument("--cache", default="paysim_features.npz")
    args = ap.parse_args()
    if not os.path.exists(args.file):
        raise SystemExit(f"{args.file} not found")

    df = pd.read_csv(args.file)
    transfers = (df[df.type == "TRANSFER"].sort_values("step", kind="stable")
                 .reset_index(drop=True))
    describe(df, transfers)
    df = transfers
    # PaySim carries no call state, no region and no session timing.
    RP.capability_profile("geo_telemetry", "session_telemetry")
    X, y = RP.cached_matrix(args.cache, lambda: RP.extract_features(
        to_events(df, RP.scale_factor(df.amount)), total=len(df))[:2])
    train = int((df.step <= CUT_STEP).sum())
    RP.print_scores(RP.fit_and_score(X, y, fit=int(train * 0.8), cut=train))


if __name__ == "__main__":
    main()
