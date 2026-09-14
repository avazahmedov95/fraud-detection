"""Which class weight at a realistic base rate?

train.py weights fraud by negatives over positives. At this project's 1.5% that is
about 65, and validated; at IBM AML's 0.10% it is about 870 and every fit
collapsed, and below 0.5% train.py now refuses (class_weight). A realistic
generator runs near 0.2%, so the weight has to be chosen rather than derived.
Candidates, on IBM AML's published split with the 14 features this contract
computes there, five seeds each:

  1       unweighted - the recipe that held on PaySim and on IBM AML
  10, 30  in between
  65      the weight validated on this project's data, kept as a cap
  full    negatives / positives, as train.py computes it

Reported: PR-AUC (ranking, threshold-free); F1 at the threshold that maximises it
on the validation 20% (the benchmark's own reading); and, at the job's fixed
cutoffs, the share of test rows alerted with the precision and recall there -
a weight moves the probability scale those fixed cutoffs read.

Rule, fixed before the run: the weight chosen is the largest whose PR-AUC,
paired by seed against the unweighted fit, is not lower beyond the 95% interval
(the interval of the difference reaches zero or above). If only 1 qualifies, the
recipe at a realistic base rate is unweighted, with thresholds set from a
validation slice rather than fixed.

    python experiments/class_weight.py --ibm-cache <ibm_features.npz>
"""
import argparse
import os
import sys
import warnings

import numpy as np
from sklearn.metrics import average_precision_score, f1_score, precision_recall_curve

_PKG = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _PKG)
import dataset as D  # noqa: E402,F401 - puts stream-processor on the path
import train as T    # noqa: E402
import config as SC  # noqa: E402 - stream-processor's: the job's own cutoffs

warnings.filterwarnings("ignore", message="X does not have valid feature names")
WEIGHTS = ("1", "10", "30", "65", "full")


def _ci95(xs):
    from scipy import stats
    xs = np.asarray(xs, dtype=float)
    return float(stats.t.ppf(0.975, len(xs) - 1) * xs.std(ddof=1) / np.sqrt(len(xs)))


def _tuned(y, p):
    prec, rec, thr = precision_recall_curve(y, p)
    f1 = 2 * prec[:-1] * rec[:-1] / np.maximum(prec[:-1] + rec[:-1], 1e-12)
    return float(thr[int(np.argmax(f1))]) if len(thr) else 0.5


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--ibm-cache", required=True)
    ap.add_argument("--seeds", type=int, nargs="+", default=[0, 1, 2, 3, 4])
    args = ap.parse_args()

    z = np.load(args.ibm_cache, allow_pickle=False)
    X, y = z["X"], z["y"].astype("int8")
    a, b = int(len(y) * 0.6), int(len(y) * 0.8)
    ytr, yva, yte = y[:a], y[a:b], y[b:]
    full = int((ytr == 0).sum()) / max(int(ytr.sum()), 1)
    cut_r, cut_b = SC.FINAL_REVIEW_THRESHOLD, SC.FINAL_BLOCK_THRESHOLD
    print(f"train {a:,} rows ({ytr.mean():.3%} fraud), test {len(yte):,} "
          f"({int(yte.sum()):,} fraud); full weight {full:.0f}; the job's cutoffs "
          f"{cut_r} / {cut_b}\n")

    res = {w: [] for w in WEIGHTS}
    for w in WEIGHTS:
        spw = full if w == "full" else float(w)
        for s in args.seeds:
            m = T.make_model(spw, random_state=s)
            m.fit(X[:a], ytr)
            pva, pte = m.predict_proba(X[a:b])[:, 1], m.predict_proba(X[b:])[:, 1]
            t = _tuned(yva, pva)
            row = {"ap": average_precision_score(yte, pte),
                   "f1": 100 * f1_score(yte, pte >= t, zero_division=0)}
            for name, cut in (("review", cut_r), ("block", cut_b)):
                flag = pte >= cut
                tp = int((flag & (yte == 1)).sum())
                row[name] = (float(flag.mean()), tp / max(int(flag.sum()), 1),
                             tp / max(int(yte.sum()), 1))
            res[w].append(row)
            print(f"  weight {w} seed {s} done", file=sys.stderr, flush=True)

    base = np.array([r["ap"] for r in res["1"]])
    print(f"{'weight':<8}{'PR-AUC':>18}{'vs unweighted':>26}{'F1 tuned':>12}"
          f"{'at ' + str(cut_r) + ': alerted / prec / recall':>40}"
          f"{'at ' + str(cut_b):>30}")
    qualifies = []
    for w in WEIGHTS:
        aps = np.array([r["ap"] for r in res[w]])
        d = aps - base
        lo = d.mean() - (_ci95(d) if w != "1" else 0.0)
        if w == "1" or d.mean() + _ci95(d) >= 0:
            qualifies.append(w)
        cells = [np.mean([r[k][i] for r in res[w]]) for k in ("review", "block") for i in range(3)]
        print(f"{w:<8}{aps.mean():>12.4f} +/- {_ci95(aps):.4f}"
              f"{'' if w == '1' else f'{d.mean():+.4f} [{lo:+.4f}, {d.mean() + _ci95(d):+.4f}]':>26}"
              f"{np.mean([r['f1'] for r in res[w]]):>12.1f}"
              f"{cells[0]:>18.3%} {cells[1]:>9.1%} {cells[2]:>9.1%}"
              f"{cells[3]:>12.3%} {cells[4]:>8.1%} {cells[5]:>8.1%}")
    chosen = qualifies[-1]
    print("\nverdict, as fixed in the module docstring before the run:")
    print(f"  weights within the unweighted fit's noise: {', '.join(qualifies)}")
    print(f"  chosen: {chosen}"
          + ("  (unweighted: thresholds must come from a validation slice)"
             if chosen == "1" else ""))


if __name__ == "__main__":
    main()
