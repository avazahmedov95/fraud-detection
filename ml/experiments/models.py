"""Does another model beat the served committee on this data?

Read-only by construction: it fits its own models on the same rows train.py fits
on, reads the same held-out slice, and writes nothing the system serves. The
served model, its cutoff and the running job are untouched whatever it prints.

Every model gets the same split (64% fit / 16% cutoff / 20% test, by time), the
same 21 features and the same rule for its cutoff - the F1 peak on the cutoff
rows - so the column that differs is the model. Trees take the features as they
are; the distance-based models need them scaled and their gaps filled, which is
part of what they cost.

    cd ml
    python experiments/models.py --cache collectors_matrix.npz
"""
import argparse
import os
import sys
import time

import numpy as np
from scipy.special import expit
from sklearn.ensemble import HistGradientBoostingClassifier, RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression, SGDClassifier
from sklearn.metrics import average_precision_score, precision_recall_curve, roc_auc_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.svm import SVC

_PKG = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _PKG)
sys.path.insert(0, os.path.join(os.path.dirname(_PKG), "stream-processor"))
import dataset as D  # noqa: E402
import train as T    # noqa: E402

#: An RBF kernel is quadratic in the rows; the full fit does not finish on this
#: machine, so it is fitted on a stratified subsample and said so in the table.
RBF_ROWS = 40_000


def _load(cache):
    if cache and os.path.exists(cache):
        with np.load(cache, allow_pickle=False) as z:
            names = [str(n) for n in z["names"]]
            if names != list(D.FEATURE_NAMES):
                raise SystemExit(f"{cache} holds other columns - delete it and re-run")
            return z["X"], z["y"]
    df = D.build_matrix(T.CSV)
    X = df[list(D.FEATURE_NAMES)].astype("float32").values
    y = df["label"].values.astype("int8")
    if cache:
        np.savez_compressed(cache, X=X, y=y, names=np.array(list(D.FEATURE_NAMES)))
        print(f"cached the matrix in {cache}")
    return X, y


def _scaled(model):
    """What a distance-based model needs before it can be fitted at all."""
    return make_pipeline(SimpleImputer(strategy="median"), StandardScaler(), model)


def _score_of(model, X):
    """A ranking score, whether the model offers probabilities or not."""
    if hasattr(model, "predict_proba"):
        return model.predict_proba(X)[:, 1]
    return expit(model.decision_function(X))


def _cut(y, p):
    prec, rec, thr = precision_recall_curve(y, p)
    f1 = 2 * prec[:-1] * rec[:-1] / np.maximum(prec[:-1] + rec[:-1], 1e-12)
    return float(thr[int(np.argmax(f1))]) if len(thr) else 0.5


def _committee(Xfit, yfit, spw):
    """train.py's own recipe, so the reference row is the served model's."""
    members = []
    for seed in T.COMMITTEE_SEEDS:
        m = T.make_model(spw, random_state=seed)
        m.fit(Xfit, yfit)
        members.append(m)
    return members


def _committee_score(members, X):
    return expit(np.mean([m.predict(X, raw_score=True) for m in members], axis=0))


def run(cache, rbf_rows=RBF_ROWS):
    X, y = _load(cache)
    n = len(y)
    cut, fit = T.cut_index(n), int(T.cut_index(n) * T.FIT_SHARE)
    Xfit, yfit = X[:fit], y[:fit]
    Xva, yva = X[fit:cut], y[fit:cut]
    Xte, yte = X[cut:], y[cut:]
    spw = T.class_weight(int(yfit.sum()), int((yfit == 0).sum()))
    print(f"fit {fit:,} rows ({int(yfit.sum())} fraud) | cutoff {cut - fit:,} "
          f"({int(yva.sum())}) | test {n - cut:,} ({int(yte.sum())})")
    print(f"scale_pos_weight {spw:.1f}, as train.py computes it\n")

    rng = np.random.default_rng(42)
    sub = np.concatenate([np.flatnonzero(yfit == 1),
                          rng.choice(np.flatnonzero(yfit == 0),
                                     size=min(rbf_rows, int((yfit == 0).sum())),
                                     replace=False)])

    candidates = [
        ("LightGBM committee (served)", None, None),
        ("logistic regression", _scaled(LogisticRegression(max_iter=2000, class_weight=None)), None),
        ("linear SVM", _scaled(SGDClassifier(loss="hinge", max_iter=50, tol=1e-3,
                                             random_state=42)), None),
        (f"RBF SVM, {len(sub):,} rows", _scaled(SVC(kernel="rbf", cache_size=800)), sub),
        ("random forest", make_pipeline(SimpleImputer(strategy="median"),
                                        RandomForestClassifier(n_estimators=300, n_jobs=-1,
                                                               min_samples_leaf=5,
                                                               random_state=42)), None),
        ("sklearn boosting", HistGradientBoostingClassifier(max_iter=400, learning_rate=0.05,
                                                            random_state=42), None),
    ]

    print(f"{'model':<30}{'PR-AUC':>9}{'ROC-AUC':>9}{'caught':>9}{'real':>8}"
          f"{'alerts':>9}{'fit s':>8}{'ms/1k':>8}")
    for name, model, rows in candidates:
        started = time.time()
        if model is None:
            members = _committee(Xfit, yfit, spw)
            fitted = time.time() - started
            pva = _committee_score(members, Xva)
            scored = time.time()
            pte = _committee_score(members, Xte)
            per_1k = 1000 * (time.time() - scored) / max(len(yte), 1) * 1000
        else:
            idx = rows if rows is not None else slice(None)
            model.fit(Xfit[idx], yfit[idx])
            fitted = time.time() - started
            pva = _score_of(model, Xva)
            scored = time.time()
            pte = _score_of(model, Xte)
            per_1k = 1000 * (time.time() - scored) / max(len(yte), 1) * 1000
        t = _cut(yva, pva)
        flag = pte >= t
        tp = int((flag & (yte == 1)).sum())
        alerts = int(flag.sum())
        print(f"{name:<30}{average_precision_score(yte, pte):>9.3f}"
              f"{roc_auc_score(yte, pte):>9.3f}"
              f"{tp / max(int(yte.sum()), 1):>8.1%}{tp / max(alerts, 1):>8.1%}"
              f"{alerts:>9,}{fitted:>8.0f}{per_1k:>8.1f}")

    print("\nNothing here is adopted. Replacing the served model would need the same "
          "gate\nevery capability went through: the rule written down first, then five "
          "paired\nfits, then the decision (ml/README.md).")


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--cache", help="npz of the deployed matrix (X, y, names)")
    ap.add_argument("--rbf-rows", dest="rbf_rows", type=int, default=RBF_ROWS,
                    help="legitimate rows the RBF kernel is fitted on")
    args = ap.parse_args()
    run(args.cache, args.rbf_rows)


if __name__ == "__main__":
    main()
