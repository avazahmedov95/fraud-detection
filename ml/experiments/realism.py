"""What the realistic profile does to the model.

The baseline dataset separates too easily (docs/generator-spec.md 8 and 10): its
held-out figures describe the data more than the method. This asks the same
question of the realistic profile, split by time into 64% train, 16% validation
(the thresholds are chosen there) and 20% test, two ways:

  deployed   the model as served today, trained on the baseline dataset - what it
             would do on traffic shaped like the realistic profile
  retrained  train.py's recipe fitted on the realistic training part, at each class
             weight given (--weight: numbers, or "full" for negatives over
             positives), over several seeds - with the validation PR-AUC beside
             each, since a weight is chosen there and not on the test part

Reported for each: PR-AUC; the share of rows flagged, precision and recall at the
job's review and block cutoffs and at the threshold that maximises F1 on the
validation part; recall by fraud type at that threshold.

The replay takes minutes at this size, so the matrix is cached (--cache) and
every later run reads it.

    python experiments/realism.py --csv <realistic transactions.csv> --cache <m.npz> --weight 65
"""
import argparse
import os
import sys
import warnings

import joblib
import numpy as np
from scipy.special import expit
from sklearn.metrics import average_precision_score, precision_recall_curve

_PKG = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _PKG)
import dataset as D  # noqa: E402 - also puts stream-processor on the path
import train as T    # noqa: E402
import config as SC  # noqa: E402 - stream-processor's: the job's own cutoffs

warnings.filterwarnings("ignore", message="X does not have valid feature names")


def _matrix(csv, cache):
    if cache and os.path.exists(cache):
        # The file is this script's own output; pickle only reads a `types` column
        # written before it was stored fixed-width.
        z = np.load(cache, allow_pickle=True)
        return z["X"], z["y"], z["types"].astype("U")
    df = D.build_matrix(csv, age_unknown=T.age_unknown_rows)
    X = df[D.FEATURE_NAMES].astype("float32").values
    y = df["label"].values.astype("int8")
    types = df["fraud_type"].astype(str).values.astype("U")   # fixed-width, no pickle
    if cache:
        np.savez(cache, X=X, y=y, types=types)
    return X, y, types


def _tuned(y, p):
    prec, rec, thr = precision_recall_curve(y, p)
    f1 = 2 * prec[:-1] * rec[:-1] / np.maximum(prec[:-1] + rec[:-1], 1e-12)
    return float(thr[int(np.argmax(f1))]) if len(thr) else 0.5


def _at(y, p, cut):
    flag = p >= cut
    tp = int((flag & (y == 1)).sum())
    return flag.mean(), tp / max(int(flag.sum()), 1), tp / max(int(y.sum()), 1)


def _report(name, y, types, runs):
    """`runs`: [(test scores, tuned threshold)], one per fit."""
    print(f"\n{name}")
    aps = [average_precision_score(y, p) for p, _ in runs]
    print(f"  PR-AUC {np.mean(aps):.4f}" + (f"  (seeds {min(aps):.4f}-{max(aps):.4f})"
                                            if len(runs) > 1 else ""))
    cuts = (("review cutoff", lambda t: SC.FINAL_REVIEW_THRESHOLD),
            ("block cutoff", lambda t: SC.FINAL_BLOCK_THRESHOLD),
            ("tuned on validation", lambda t: t))
    for label, cut in cuts:
        vals = np.mean([_at(y, p, cut(t)) for p, t in runs], axis=0)
        shown = (f"{cut(runs[0][1]):.2f}" if label != "tuned on validation"
                 else f"{np.mean([t for _, t in runs]):.3f}")
        print(f"  {label:<22} at {shown:>6}: flagged {vals[0]:.3%}, "
              f"precision {vals[1]:.1%}, recall {vals[2]:.1%}")
    fraud = y == 1
    kinds = sorted(set(types[fraud]))
    print("  recall by type, tuned threshold: " + ", ".join(
        f"{k} {np.mean([np.mean((p >= t)[fraud & (types == k)]) for p, t in runs]):.0%}"
        f" (n={int((fraud & (types == k)).sum())})" for k in kinds))


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--csv", required=True, help="the realistic profile's transactions.csv")
    ap.add_argument("--cache", help="npz for the replayed matrix")
    ap.add_argument("--weight", nargs="+", default=["30"],
                    help='class weights to fit, or "full"')
    ap.add_argument("--seeds", type=int, nargs="+", default=[0, 1, 2])
    args = ap.parse_args()

    X, y, types = _matrix(args.csv, args.cache)
    n = len(y)
    cut = T.cut_index(n)
    v = int(cut * 0.8)
    yva, yte, tte = y[v:cut], y[cut:], types[cut:]
    print(f"{n:,} rows, {y.mean():.3%} labelled fraud; train {v:,}, validation "
          f"{cut - v:,} ({int(yva.sum())} fraud), test {n - cut:,} ({int(yte.sum())} fraud)")

    deployed = joblib.load(os.path.join(T.MODELS_DIR, "model.joblib"))
    p_te = deployed.predict_proba(X[cut:])[:, 1]
    p_va = deployed.predict_proba(X[v:cut])[:, 1]
    _report("deployed - trained on the baseline dataset", yte, tte,
            [(p_te, _tuned(yva, p_va))])

    ytr = y[:v]
    full = int((ytr == 0).sum()) / max(int(ytr.sum()), 1)
    for w in args.weight:
        spw = full if w == "full" else float(w)
        runs, margins = [], []
        for s in args.seeds:
            m = T.make_model(spw, random_state=s)
            m.fit(X[:v], ytr)
            mte = m.predict(X[cut:], raw_score=True)
            mva = m.predict(X[v:cut], raw_score=True)
            margins.append((mte, mva))
            runs.append((expit(mte), _tuned(yva, expit(mva))))
            print(f"  weight {w} seed {s} done", file=sys.stderr, flush=True)
        _report(f"retrained on the realistic profile - class weight {spw:.0f}, "
                f"{len(runs)} seeds", yte, tte, runs)
        print(f"  validation PR-AUC "
              f"{np.mean([average_precision_score(yva, b) for _, b in margins]):.4f}")
        # The fits' log-odds averaged: on IBM AML that scored far above any single
        # fit (experiments/recipe.py).
        avg_va = np.mean([b for _, b in margins], axis=0)
        avg = expit(np.mean([a for a, _ in margins], axis=0))
        _report(f"the same {len(runs)} fits, averaged", yte, tte,
                [(avg, _tuned(yva, expit(avg_va)))])
        print(f"  validation PR-AUC {average_precision_score(yva, avg_va):.4f}")


if __name__ == "__main__":
    main()
