"""How much more fraud does a lower alert cutoff catch, and what does it cost?

train.py puts REVIEW where F1 peaks on the validation rows - one point on a curve.
This prints the useful stretch of that curve for the model that is actually served
(models/model.txt, the merged committee), so the menu describes the deployed system
rather than a refit of it. Every cutoff is chosen on the VALIDATION rows and read on
the held-out TEST rows:

  F1 peak         the current rule, and what thresholds.json ships
  F2 peak         the same, with recall counted twice as much as precision
  top x%          flag that share of transfers, highest scores first

Nothing is adopted here. A cutoff trades catching more against alarming more, which
is the owner's choice; this prints the menu it is made from.

    cd ml
    python experiments/thresholds.py --cache counters_realistic.npz
"""
import argparse
import json
import os
import sys

import numpy as np
from sklearn.metrics import precision_recall_curve

_PKG = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ROOT = os.path.dirname(_PKG)
sys.path.insert(0, _PKG)
sys.path.insert(0, os.path.join(ROOT, "stream-processor"))

import dataset as D  # noqa: E402
import train as T  # noqa: E402

BUDGETS = (0.0005, 0.001, 0.002, 0.005, 0.01)


def _fbeta_cut(y, p, beta):
    prec, rec, thr = precision_recall_curve(y, p)
    b2 = beta * beta
    f = (1 + b2) * prec[:-1] * rec[:-1] / np.maximum(b2 * prec[:-1] + rec[:-1], 1e-12)
    return float(thr[int(np.argmax(f))])


def _row(label, thr, y, p):
    flag = p >= thr
    tp = int((flag & (y == 1)).sum())
    alerts = int(flag.sum())
    return (f"  {label:<22}{thr:>10.4f}{alerts:>9,}{alerts / len(y):>9.2%}"
            f"{tp / max(int(y.sum()), 1):>9.1%}{tp / max(alerts, 1):>8.1%}"
            f"{alerts / len(y) * 100_000:>12,.0f}")


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--cache", help="npz with X, y and names (experiments/counters.py)")
    ap.add_argument("--model", default=os.path.join(_PKG, "models", "model.txt"))
    args = ap.parse_args()

    if args.cache and os.path.exists(args.cache):
        with np.load(args.cache, allow_pickle=False) as z:
            X, y, names = z["X"], z["y"], [str(n) for n in z["names"]]
    else:
        df = D.build_matrix(T.CSV)
        names = list(D.FEATURE_NAMES)
        X, y = df[names].astype("float32").values, df["label"].values.astype("int8")
    if names != list(D.FEATURE_NAMES):
        raise SystemExit(f"the cache holds {len(names)} columns, the registry "
                         f"{len(D.FEATURE_NAMES)}: rebuild it before reading a menu")

    import lightgbm as lgb
    model = lgb.Booster(model_file=args.model)
    cut = T.cut_index(len(y))
    fit = int(cut * T.FIT_SHARE)
    pva, pte = model.predict(X[fit:cut]), model.predict(X[cut:])
    yva, yte = y[fit:cut], y[cut:]

    shipped = json.load(open(os.path.join(_PKG, "models", "thresholds.json")))
    print(f"served model: {os.path.basename(args.model)}, REVIEW = "
          f"{shipped.get('review'):.4f} (thresholds.json)")
    print(f"validation {len(yva):,} rows / {int(yva.sum())} fraud; "
          f"test {len(yte):,} rows / {int(yte.sum())} fraud\n")
    print(f"  {'cutoff':<22}{'score':>10}{'alerts':>9}{'of rows':>9}"
          f"{'caught':>9}{'real':>8}{'per 100k':>12}")
    print(_row("F1 peak (current)", _fbeta_cut(yva, pva, 1.0), yte, pte))
    print(_row("F2 peak", _fbeta_cut(yva, pva, 2.0), yte, pte))
    for b in BUDGETS:
        k = max(1, int(round(b * len(yva))))
        thr = float(np.sort(pva)[::-1][k - 1])
        print(_row(f"top {b:.2%} of transfers".replace(".00%", "%"), thr, yte, pte))


if __name__ == "__main__":
    main()
