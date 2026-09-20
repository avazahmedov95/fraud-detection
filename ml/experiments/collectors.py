"""Does the served model mistake honest collections for mule accounts?

A mule collects from many payers; so does an honest person collecting for a wedding,
a gift or a joint purchase - the realistic profile gives 1% of persons such
collections, 5-15 senders within hours (docs/generator-spec.md 10). The
counterparty counters count payers per payee over a day and a week, so they could
flag the one as readily as the other. This reads the held-out month by the payee's
other payers over the 7 days before each transfer, for the committee without the
five counter columns and with them - both fitted exactly as train.py fits the
served one - and reports, per group, the false alarms on legitimate transfers and
the catches on MULE. Descriptive: nothing is adopted or removed on it. Sellers paid
by strangers all day are not in the generator (docs/generator-spec.md 8), so it
cannot speak for them.

    cd ml
    python experiments/collectors.py --cache collectors_matrix.npz
"""
import argparse
import os
import sys

import numpy as np
from scipy.special import expit

_PKG = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _PKG)
sys.path.insert(0, os.path.join(os.path.dirname(_PKG), "stream-processor"))
import dataset as D  # noqa: E402
import train as T    # noqa: E402

COUNTERS = ("payee_payers_24h", "payee_payers_7d", "sender_payees_24h",
            "sender_payees_7d", "secs_since_sender_inbound")
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
        print(f"    seed {seed} done", file=sys.stderr, flush=True)
    review = T.choose_review_cutoff(y[fit:cut], expit(np.mean(va, axis=0)))
    return expit(np.mean(te, axis=0)) >= review


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--cache", help="npz of the deployed matrix (X, y, types, names)")
    args = ap.parse_args()
    if args.cache and os.path.exists(args.cache):
        with np.load(args.cache, allow_pickle=False) as z:
            X, y, types = z["X"], z["y"], z["types"]
            names = [str(n) for n in z["names"]]
    else:
        df = D.build_matrix(T.CSV)
        names = list(D.FEATURE_NAMES)
        X, y = df[names].astype("float32").values, df.label.values.astype("int8")
        types = df.fraud_type.astype(str).values.astype("U")
        if args.cache:
            np.savez(args.cache, X=X, y=y, types=types, names=np.array(names))
    if not all(c in names for c in COUNTERS):
        raise SystemExit("the matrix predates the counters - rebuild it")

    cut = T.cut_index(len(y))
    fit = int(cut * T.FIT_SHARE)
    base = [i for i, c in enumerate(names) if c not in COUNTERS]
    flags = {"without": _flags(X, y, base, fit, cut),
             "with": _flags(X, y, list(range(len(names))), fit, cut)}
    yte, tte = y[cut:], types[cut:]
    # The current sender is counted in, so "other payers" is one less.
    others = X[cut:, names.index("payee_payers_7d")] - 1

    for label, f in flags.items():
        tp = int((f & (yte == 1)).sum())
        print(f"{label:<8} {int(f.sum())} alerts, {tp} fraud: "
              f"precision {tp / max(int(f.sum()), 1):.3f}, recall {tp / int(yte.sum()):.3f}")
    print()
    print(f"{'payee other payers, 7d':<24}{'legit':>8}{'false alarms without':>22}"
          f"{'with':>12}{'MULE':>7}{'caught without':>16}{'with':>6}")
    for lo, hi, name in GROUPS:
        g = (others >= lo) & (others <= hi)
        legit, mule = g & (yte == 0), g & (tte == "MULE")
        fa = {k: int((f & legit).sum()) for k, f in flags.items()}
        cm = {k: int((f & mule).sum()) for k, f in flags.items()}
        nl = max(int(legit.sum()), 1)
        print(f"{name:<24}{int(legit.sum()):>8,}{fa['without']:>12} ({fa['without'] / nl:.2%})"
              f"{fa['with']:>6} ({fa['with'] / nl:.2%}){int(mule.sum()):>7}"
              f"{cm['without']:>16}{cm['with']:>6}")


if __name__ == "__main__":
    main()
