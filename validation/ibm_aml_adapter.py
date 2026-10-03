"""This project's model on IBM AML (Altman et al., NeurIPS 2023, HI-Small): what
the file holds, its transfers replayed through the deployed feature extractor, and
the model fitted on the published split (60/20/20 by time) - recall, precision and
F1 on the last fifth. For information only: its accounts include banks and
companies.

    python ibm_aml_adapter.py --file HI-Small_Trans.csv
"""

import argparse
import os

import pandas as pd

import harness as RP

#: The headers contain spaces; pandas splits the two `Account` columns into
#: Account / Account.1.
COLUMNS = {"Account": "sender", "Account.1": "receiver", "Amount Paid": "amount",
           "Payment Currency": "currency", "Payment Format": "fmt",
           "Is Laundering": "label"}


def load(path):
    """The file in time order, without self-transfers (12% of it, mostly
    `Reinvestment`): they are not transfers between two parties."""
    d = pd.read_csv(path, dtype={"Account": str, "Account.1": str})
    d.columns = [c.strip() for c in d.columns]
    missing = [c for c in COLUMNS if c not in d.columns]
    if missing:
        raise SystemExit(f"{path} is missing {missing}; expected the released IBM AML schema")
    d = d.rename(columns=COLUMNS)
    d["ts"] = pd.to_datetime(d["Timestamp"], format="%Y/%m/%d %H:%M")
    n_self = int((d.sender == d.receiver).sum())
    d = d[d.sender != d.receiver].sort_values("ts").reset_index(drop=True)
    return d, n_self


def to_events(d):
    """One scale per currency, from its median, so "just under a limit" means the
    same in yen and in dollars."""
    scales = {cur: RP.scale_factor(g.amount) for cur, g in d.groupby("currency")}
    for r in d.itertuples(index=False):
        yield RP.Event(ev={"amount_uzs": float(r.amount) * scales[r.currency],
                           "sender_pinfl": r.sender, "receiver_pinfl": r.receiver},
                       ts=int(r.ts.timestamp()), label=int(r.label))


def describe(d, n_self):
    print(f"IBM AML HI-Small: {len(d):,} transfers between bank accounts over "
          f"{(d.ts.max() - d.ts.min()).days} days, minute timestamps "
          f"({n_self:,} self-transfers dropped)")
    print(f"  laundering: {int(d.label.sum()):,} ({d.label.mean():.2%})")
    print(f"  accounts: {pd.concat([d.sender, d.receiver]).nunique():,}; "
          f"{d.currency.nunique()} currencies; payment formats: "
          f"{', '.join(sorted(d.fmt.unique()))}")


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--file", required=True, help="HI-Small_Trans.csv")
    ap.add_argument("--cache", default="ibm_features.npz")
    args = ap.parse_args()
    if not os.path.exists(args.file):
        raise SystemExit(f"{args.file} not found")

    d, n_self = load(args.file)
    describe(d, n_self)
    # Accounts, amounts and a clock: no call state, no region, no session timing.
    RP.capability_profile("geo_telemetry", "session_telemetry")
    X, y = RP.cached_matrix(args.cache, lambda: RP.extract_features(
        to_events(d), total=len(d))[:2])
    n = len(y)
    RP.print_scores(RP.fit_and_score(X, y, fit=int(n * 0.6), cut=int(n * 0.8)))


if __name__ == "__main__":
    main()
