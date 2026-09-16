"""How much more fraud does a lower alert cutoff catch, and what does it cost in
false alarms?

train.py puts REVIEW where F1 peaks on the validation rows - one point on a curve.
This prints the useful stretch of that curve for the committee (five seeds, margins
averaged), without and with the multi-day link shapes (experiments/shapes.py), on
the realistic profile and on IBM AML. Every cutoff is chosen on the VALIDATION rows
and read on the TEST rows:

  F1 peak         the current rule
  F2 peak         the same, with recall counted twice as much as precision
  top x%          flag that share of transfers, highest scores first
  same recall     with the shapes only: the cutoff with the fewest alerts whose
                  validation recall reaches the model without them at its F1 peak

Nothing is adopted here. A cutoff trades catching more against alarming more, which
is the owner's choice to make; this prints the menu it is made from.

    python experiments/thresholds.py --cache shapes_realistic.npz
    python experiments/thresholds.py --ibm ../validation/HI-Small_Trans.csv --ibm-cache ../validation/ibm_features.npz
"""
import argparse
import os
import sys

import numpy as np
from scipy.special import expit
from sklearn.metrics import precision_recall_curve

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import shapes as SH  # noqa: E402

T = SH.T
BUDGETS = (0.0005, 0.001, 0.002, 0.005, 0.01)


def _fbeta_cut(y, p, beta):
    prec, rec, thr = precision_recall_curve(y, p)
    b2 = beta * beta
    f = (1 + b2) * prec[:-1] * rec[:-1] / np.maximum(b2 * prec[:-1] + rec[:-1], 1e-12)
    return float(thr[int(np.argmax(f))])


def _recall_cut(y, p, target):
    """The cutoff with the fewest alerts whose recall on these rows reaches target."""
    _, rec, thr = precision_recall_curve(y, p)
    ok = np.flatnonzero(rec[:-1] >= target)
    return float(thr[ok.max()]) if len(ok) else float(thr.min())


def menu(title, X, y, base, fit, cut, seeds, spw):
    res = {"without": SH._fits(X, y, base, fit, cut, seeds, spw),
           "with": SH._fits(X, y, list(range(X.shape[1])), fit, cut, seeds, spw)}
    yva, yte = y[fit:cut], y[cut:]
    print(f"{title}: test {len(yte):,} rows, {int(yte.sum()):,} positives; cutoffs chosen "
          f"on validation ({len(yva):,} rows, {int(yva.sum()):,} positives)")
    print(f"  {'model':<9}{'cutoff':<20}{'alerts':>9}{'of rows':>9}{'caught':>9}{'real':>8}")
    target = None
    for label, (va, te) in res.items():
        pva, pte = expit(np.mean(va, axis=0)), expit(np.mean(te, axis=0))
        cuts = [("F1 peak (current)", _fbeta_cut(yva, pva, 1.0)),
                ("F2 peak", _fbeta_cut(yva, pva, 2.0))]
        cuts += [(f"top {b:.2%}", float(np.quantile(pva, 1 - b))) for b in BUDGETS]
        if target is None:
            target = float(((pva >= cuts[0][1]) & (yva == 1)).sum()) / max(int(yva.sum()), 1)
        else:
            cuts.append(("same recall", _recall_cut(yva, pva, target)))
        for name, c in cuts:
            flag = pte >= c
            n, tp = int(flag.sum()), int((flag & (yte == 1)).sum())
            print(f"  {label:<9}{name:<20}{n:>9,}{n / len(yte):>9.2%}"
                  f"{tp / max(int(yte.sum()), 1):>9.1%}{tp / max(n, 1):>8.1%}")
        print()


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--cache", help="the realistic matrix experiments/shapes.py --cache wrote")
    ap.add_argument("--seeds", type=int, nargs="+", default=[0, 1, 2, 3, 4])
    ap.add_argument("--ibm", help="HI-Small_Trans.csv: print IBM AML's menu instead")
    ap.add_argument("--ibm-cache", help="the matrix ibm_aml_adapter.py --extract-only cached")
    args = ap.parse_args()
    if args.ibm:
        base, S, y = SH.ibm_matrix(args.ibm, args.ibm_cache)
        n = len(y)
        return menu("IBM AML", np.hstack([base, S]), y, list(range(base.shape[1])),
                    int(n * 0.6), int(n * 0.8), args.seeds, 1.0)
    if not (args.cache and os.path.exists(args.cache)):
        raise SystemExit("run experiments/shapes.py --cache first - this reads its matrix")
    z = np.load(args.cache, allow_pickle=False)
    y, n, k = z["y"], len(z["y"]), len(SH.D.FEATURE_NAMES)
    cut = T.cut_index(n)
    fit = int(cut * T.FIT_SHARE)
    spw = T.class_weight(int(y[:fit].sum()), int((y[:fit] == 0).sum()))
    return menu("realistic profile", z["X"], y, list(range(k)), fit, cut, args.seeds, spw)


if __name__ == "__main__":
    main()
