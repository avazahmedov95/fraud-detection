"""Does the served model mistake honest collections for mule accounts?

A mule collects from many payers; so does an honest person collecting for a wedding,
a gift or a joint purchase - the realistic profile gives 1% of persons such
collections, 5-15 senders within hours (docs/generator-spec.md 10). link_history
counts payers per payee, so it could flag the one as readily as the other. This
reads the held-out month by the payee's distinct payers over the 96 hours before
each transfer, for the committee without link_history's five columns and with them
- both fitted exactly as train.py fits the served one - and reports, per group, the
false alarms on legitimate transfers and the catches on MULE. Descriptive: nothing
is adopted or removed on it. Sellers paid by strangers all day are not in the
generator (docs/generator-spec.md 8), so it cannot speak for them.

    python experiments/collectors.py [--cache matrix.npz]
"""
import argparse
import os
import sys

import numpy as np
from scipy.special import expit

_PKG = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _PKG)
import dataset as D  # noqa: E402
import train as T    # noqa: E402

LINKS = ("payee_payers_96h", "sender_payers_96h", "sender_payees_96h",
         "payee_payees_96h", "money_back_96h")
GROUPS = ((0, 0, "none"), (1, 2, "1-2"), (3, 4, "3-4"), (5, 10 ** 9, "5 or more"))


def _flags(X, y, cols, fit, cut):
    """train.py's committee and REVIEW cutoff, refitted on `cols`; test-row alerts."""
    spw = T.class_weight(int(y[:fit].sum()), int((y[:fit] == 0).sum()))
    va, te = [], []
    for seed in T.COMMITTEE_SEEDS:
        m = T.make_model(spw, random_state=seed)
        m.fit(X[:fit][:, cols], y[:fit])
        va.append(m.predict(X[fit:cut][:, cols], raw_score=True))
        te.append(m.predict(X[cut:][:, cols], raw_score=True))
    review, _ = T.choose_cutoffs(y[fit:cut], expit(np.mean(va, axis=0)))
    return expit(np.mean(te, axis=0)) >= review


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--cache", help="npz of the deployed realistic matrix (X, y, types, names)")
    args = ap.parse_args()
    if args.cache and os.path.exists(args.cache):
        z = np.load(args.cache, allow_pickle=False)
        X, y, types, names = z["X"], z["y"], z["types"], [str(n) for n in z["names"]]
    else:
        df = D.build_matrix(T.CSV, age_unknown=T.age_unknown_rows)
        names = list(D.FEATURE_NAMES)
        X, y = df[names].astype("float32").values, df.label.values.astype("int8")
        types = df.fraud_type.astype(str).values.astype("U")
        if args.cache:
            np.savez(args.cache, X=X, y=y, types=types, names=np.array(names))
    n = len(y)
    cut = T.cut_index(n)
    fit = int(cut * T.FIT_SHARE)
    base = [i for i, c in enumerate(names) if c not in LINKS]
    flags = {"without": _flags(X, y, base, fit, cut),
             "with": _flags(X, y, list(range(len(names))), fit, cut)}
    yte, tte = y[cut:], types[cut:]
    payers = X[cut:, names.index("payee_payers_96h")]

    for label, f in flags.items():
        tp = int((f & (yte == 1)).sum())
        print(f"{label:<8} {int(f.sum())} alerts, {tp} fraud: precision {tp / max(int(f.sum()), 1):.3f}, "
              f"recall {tp / int(yte.sum()):.3f}")
    print()
    print(f"{'payee payers, 96h':<18}{'legit':>8}{'false alarms without':>22}{'with':>12}"
          f"{'MULE':>7}{'caught without':>16}{'with':>6}")
    for lo, hi, name in GROUPS:
        g = (payers >= lo) & (payers <= hi)
        legit, mule = g & (yte == 0), g & (tte == "MULE")
        fa = {k: int((f & legit).sum()) for k, f in flags.items()}
        cm = {k: int((f & mule).sum()) for k, f in flags.items()}
        nl = max(int(legit.sum()), 1)
        print(f"{name:<18}{int(legit.sum()):>8,}{fa['without']:>12} ({fa['without'] / nl:.2%})"
              f"{fa['with']:>6} ({fa['with'] / nl:.2%}){int(mule.sum()):>7}"
              f"{cm['without']:>16}{cm['with']:>6}")


if __name__ == "__main__":
    main()
