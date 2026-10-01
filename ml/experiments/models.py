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
    python experiments/models.py --cache models_matrix.npz
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
#: TabPFN reads its training rows as context at prediction time, and refuses more
#: than five thousand of them on a CPU, so it gets a subsample of that size.
TABPFN_ROWS = 5_000
#: How long a model may take to score the two slices before it is left unscored.
#: A model that cannot score 180,000 rows in this budget cannot serve a stream.
SCORING_BUDGET_S = 1800


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


def _subsample(yfit, legit_rows, seed=42):
    """Every fraud row and a random sample of the rest, for the models that cannot
    take the whole training slice."""
    rng = np.random.default_rng(seed)
    legit = np.flatnonzero(yfit == 0)
    keep = rng.choice(legit, size=min(legit_rows, len(legit)), replace=False)
    return np.concatenate([np.flatnonzero(yfit == 1), keep])


def _extra(yfit, tabpfn_model=None):
    """XGBoost and TabPFN, when the environment has them. Neither is a dependency
    of this project: they live in an environment of their own, and a run without
    them prints the same table two rows shorter. TabPFN's weights are a gated
    download, so `tabpfn_model` points at a checkpoint fetched by hand."""
    out = []
    try:
        from xgboost import XGBClassifier
        out.append(("XGBoost", XGBClassifier(
            n_estimators=400, learning_rate=0.05, max_depth=6,
            colsample_bytree=0.8, reg_lambda=10.0, tree_method="hist",
            n_jobs=-1, random_state=42, eval_metric="aucpr"), None, False))
    except ImportError:
        print("  (xgboost not installed here - skipped)")
    try:
        from tabpfn import TabPFNClassifier
        rows = _subsample(yfit, TABPFN_ROWS - int(yfit.sum()))
        model = (TabPFNClassifier(model_path=tabpfn_model) if tabpfn_model
                 else TabPFNClassifier())
        out.append((f"TabPFN, {len(rows):,} rows", _scaled(model), rows, True))
    except ImportError:
        print("  (tabpfn not installed here - skipped)")
    except Exception as exc:                           # noqa: BLE001
        # Its weights are a gated download: the account and the licence are the
        # user's to accept, not this script's.
        print(f"  (tabpfn is installed but its model is not available here: "
              f"{type(exc).__name__} - skipped)")
    return out


def _guarded_scores(model, Xva, Xte):
    """Score both slices, unless a probe says the model would take longer than the
    budget: a model that cannot score 180,000 rows in half an hour cannot serve a
    stream, and that is the finding, not a failure."""
    probe = min(1000, len(Xva))
    started = time.time()
    _score_of(model, Xva[:probe])
    per_row = (time.time() - started) / probe
    needed = per_row * (len(Xva) + len(Xte))
    if needed > SCORING_BUDGET_S:
        raise TimeoutError(f"would need {needed / 60:.0f} min to score both slices "
                           f"on this machine")
    pva = _score_of(model, Xva)
    started = time.time()
    pte = _score_of(model, Xte)
    return pva, pte, 1000 * (time.time() - started) / max(len(Xte), 1) * 1000


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


def _slices(cache):
    """The three slices train.py cuts, in its order: fit, cutoff, held-out test."""
    X, y = _load(cache)
    n = len(y)
    cut, fit = T.cut_index(n), int(T.cut_index(n) * T.FIT_SHARE)
    return (X[:fit], y[:fit], X[fit:cut], y[fit:cut], X[cut:], y[cut:])


def small(cache, tabpfn_model=None, fit_rows=TABPFN_ROWS, eval_legit=3000):
    """Every model on TabPFN's terms: the rows it can take, the rows it can score.

    TabPFN refuses more than 5,000 training rows on a CPU and needs 207 ms for each
    row it scores, so on the full slices it cannot be measured at all. Here every
    model is fitted on the same small sample and read on the same small one - all
    the fraud, and a sample of the rest.

    The rates below are NOT the rates of the full table: keeping every fraud row
    and sampling the others raises the fraud rate from 0.18% to about 5%, which
    lifts precision for every model alike. Read the order between models, not the
    numbers against the table above."""
    Xfit, yfit, Xva, yva, Xte, yte = _slices(cache)
    fit_idx = _subsample(yfit, fit_rows - int(yfit.sum()))
    va_idx = _subsample(yva, eval_legit, seed=7)
    te_idx = _subsample(yte, eval_legit, seed=13)
    Xfit, yfit = Xfit[fit_idx], yfit[fit_idx]
    Xva, yva = Xva[va_idx], yva[va_idx]
    Xte, yte = Xte[te_idx], yte[te_idx]
    spw = T.class_weight(int(yfit.sum()), int((yfit == 0).sum()))
    print(f"fit {len(yfit):,} rows ({int(yfit.sum())} fraud, "
          f"{yfit.mean():.1%}) | cutoff {len(yva):,} ({int(yva.sum())}) | "
          f"test {len(yte):,} ({int(yte.sum())})")
    print("all the fraud, a sample of the rest: precision here is not the "
          "precision of the full table\n")
    _table(_candidates(yfit, tabpfn_model, rbf_rows=len(yfit)),
           Xfit, yfit, Xva, yva, Xte, yte, spw)


def _pieces(yfit, rows=TABPFN_ROWS, seed=42):
    """The training slice in pieces TabPFN can read: every fraud row in each piece
    and the ordinary rows dealt out between them, so that together they hold the
    whole slice and none holds more rows than TabPFN takes on a CPU."""
    fraud = np.flatnonzero(yfit == 1)
    legit = np.random.default_rng(seed).permutation(np.flatnonzero(yfit == 0))
    count = -(-len(legit) // (rows - len(fraud)))
    return [np.concatenate([fraud, part]) for part in np.array_split(legit, count)]


def chunked(cache, tabpfn_model=None, eval_legit=3000, curve=(1, 5, 20)):
    """TabPFN on the whole training slice, a piece at a time - the mentor's way
    round its row limit (2026-09-30).

    Each piece is read by a one-member TabPFN and the members' probabilities are
    averaged. At each point of the curve the served recipe is fitted on the same
    rows - every piece read so far - so both models have seen the same data. Read
    on the rows `small` reads, with its caveat: all the fraud, a sample of the
    rest."""
    from tabpfn import TabPFNClassifier

    Xfit, yfit, Xva, yva, Xte, yte = _slices(cache)
    va_idx = _subsample(yva, eval_legit, seed=7)
    te_idx = _subsample(yte, eval_legit, seed=13)
    yva, yte = yva[va_idx], yte[te_idx]
    Xev = np.concatenate([Xva[va_idx], Xte[te_idx]])
    pieces = _pieces(yfit)
    print(f"fit {len(yfit):,} rows ({int(yfit.sum())} fraud) in {len(pieces)} pieces "
          f"of at most {TABPFN_ROWS:,}, each holding all the fraud | test "
          f"{len(yte):,} ({int(yte.sum())}), the rows of --small\n")
    print(f"{'pieces':>7}{'rows':>10}{'TabPFN':>9}{'committee':>11}   PR-AUC")

    total, scoring = np.zeros(len(Xev)), 0.0
    for k, rows in enumerate(pieces, 1):
        model = _scaled(TabPFNClassifier(model_path=tabpfn_model or "auto",
                                         n_estimators=1, random_state=k))
        model.fit(Xfit[rows], yfit[rows])
        started = time.time()
        total += model.predict_proba(Xev)[:, 1]
        scoring += time.time() - started
        print(f"  piece {k} of {len(pieces)}", file=sys.stderr, flush=True)
        if k not in curve and k != len(pieces):
            continue
        seen = np.unique(np.concatenate(pieces[:k]))
        spw = T.class_weight(int(yfit[seen].sum()), int((yfit[seen] == 0).sum()))
        committee = _committee_score(_committee(Xfit[seen], yfit[seen], spw), Xev)
        print(f"{k:>7}{len(seen):>10,}"
              f"{average_precision_score(yte, total[len(yva):] / k):>9.3f}"
              f"{average_precision_score(yte, committee[len(yva):]):>11.3f}", flush=True)

    tab, n = total / len(pieces), len(yva)
    print()
    for name, p in (("TabPFN", tab), ("committee", committee)):
        flag = p[n:] >= _cut(yva, p[:n])
        tp = int((flag & (yte == 1)).sum())
        print(f"{name:<11}caught {tp / int(yte.sum()):.1%}, "
              f"real {tp / max(int(flag.sum()), 1):.1%} at its own cutoff")
    rng = np.random.default_rng(0)
    diffs = []
    for _ in range(2000):
        i = rng.integers(0, len(yte), len(yte))
        diffs.append(average_precision_score(yte[i], tab[n:][i])
                     - average_precision_score(yte[i], committee[n:][i]))
    lo, hi = np.percentile(diffs, [2.5, 97.5])
    print(f"PR-AUC, TabPFN minus committee: "
          f"{average_precision_score(yte, tab[n:]) - average_precision_score(yte, committee[n:]):+.3f}"
          f" [{lo:+.3f}, {hi:+.3f}] over 2,000 resamples of the test rows")
    print(f"TabPFN took {1000 * scoring / len(Xev):,.0f} ms a row for all "
          f"{len(pieces)} pieces, in batches of {len(Xev):,} on this machine")


def _candidates(yfit, tabpfn_model=None, rbf_rows=RBF_ROWS):
    sub = _subsample(yfit, rbf_rows)
    return [
        ("LightGBM committee (served)", None, None, False),
        ("logistic regression", _scaled(LogisticRegression(max_iter=2000, class_weight=None)),
         None, False),
        ("linear SVM", _scaled(SGDClassifier(loss="hinge", max_iter=50, tol=1e-3,
                                             random_state=42)), None, False),
        (f"RBF SVM, {len(sub):,} rows", _scaled(SVC(kernel="rbf", cache_size=800)), sub, False),
        ("random forest", make_pipeline(SimpleImputer(strategy="median"),
                                        RandomForestClassifier(n_estimators=300, n_jobs=-1,
                                                               min_samples_leaf=5,
                                                               random_state=42)), None, False),
        ("sklearn boosting", HistGradientBoostingClassifier(max_iter=400, learning_rate=0.05,
                                                            random_state=42), None, False),
    ] + _extra(yfit, tabpfn_model)


def run(cache, rbf_rows=RBF_ROWS, tabpfn_model=None):
    Xfit, yfit, Xva, yva, Xte, yte = _slices(cache)
    spw = T.class_weight(int(yfit.sum()), int((yfit == 0).sum()))
    print(f"fit {len(yfit):,} rows ({int(yfit.sum())} fraud) | cutoff {len(yva):,} "
          f"({int(yva.sum())}) | test {len(yte):,} ({int(yte.sum())})")
    print(f"scale_pos_weight {spw:.1f}, as train.py computes it\n")
    _table(_candidates(yfit, tabpfn_model, rbf_rows), Xfit, yfit, Xva, yva, Xte, yte, spw)


def _table(candidates, Xfit, yfit, Xva, yva, Xte, yte, spw):
    print(f"{'model':<30}{'PR-AUC':>9}{'ROC-AUC':>9}{'caught':>9}{'real':>8}"
          f"{'alerts':>9}{'fit s':>8}{'ms/1k':>8}")
    for name, model, rows, guarded in candidates:
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
            try:
                model.fit(Xfit[idx], yfit[idx])
            except Exception as exc:                   # noqa: BLE001
                # One model that cannot run here must not take the table with it.
                print(f"{name:<30}  did not fit: {type(exc).__name__}: "
                      f"{str(exc).splitlines()[0][:60]}")
                continue
            fitted = time.time() - started
            if guarded:
                try:
                    pva, pte, per_1k = _guarded_scores(model, Xva, Xte)
                except TimeoutError as slow:
                    print(f"{name:<30}  not scored: {slow}")
                    continue
            else:
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


def paired(cache, sets=5):
    """The forest against the served recipe, five times over, on the same rows.

    One table row cannot separate models four thousandths apart, so this repeats
    the pair with a different seed each time and reads the difference within each
    pair - the rule every capability in this project went through. Measured on the
    cutoff rows, never on the held-out slice: a decision read there is a decision
    taken on the test set."""
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from ablate_seeds import ci95

    X, y = _load(cache)
    n = len(y)
    cut, fit = T.cut_index(n), int(T.cut_index(n) * T.FIT_SHARE)
    Xfit, yfit, Xva, yva = X[:fit], y[:fit], X[fit:cut], y[fit:cut]
    spw = T.class_weight(int(yfit.sum()), int((yfit == 0).sum()))
    print(f"{sets} pairs on the same {fit:,} rows; read on the {cut - fit:,} cutoff "
          f"rows ({int(yva.sum())} fraud)\n")
    print(f"{'pair':<8}{'committee':>12}{'forest':>10}{'difference':>13}")

    diffs = []
    for k in range(sets):
        base = 42 + 5 * k
        members = [T.make_model(spw, random_state=s).fit(Xfit, yfit)
                   for s in range(base, base + 5)]
        committee = average_precision_score(yva, _committee_score(members, Xva))
        rf = make_pipeline(SimpleImputer(strategy="median"),
                           RandomForestClassifier(n_estimators=300, n_jobs=-1,
                                                  min_samples_leaf=5, random_state=base))
        rf.fit(Xfit, yfit)
        forest = average_precision_score(yva, rf.predict_proba(Xva)[:, 1])
        diffs.append(forest - committee)
        print(f"seeds {base:<3}{committee:>12.4f}{forest:>10.4f}{diffs[-1]:>+13.4f}",
              flush=True)

    mean = sum(diffs) / len(diffs)
    half = ci95(diffs)
    better = sum(1 for d in diffs if d > 0)
    print(f"\npaired difference, forest minus committee: {mean:+.4f} "
          f"[{mean - half:+.4f}, {mean + half:+.4f}], forest ahead on {better} of {sets}")
    if abs(mean) <= half:
        print("The interval spans zero: on this data the two are not distinguishable.")
    else:
        print("The interval clears zero: the difference is real at this sample size.")
    print("Either way nothing changes here - a swap would be its own decision, with "
          "its rule\nwritten down before the run.")


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--cache", help="npz of the deployed matrix (X, y, names)")
    ap.add_argument("--rbf-rows", dest="rbf_rows", type=int, default=RBF_ROWS,
                    help="legitimate rows the RBF kernel is fitted on")
    ap.add_argument("--tabpfn-model", dest="tabpfn_model", default=None,
                    help="a TabPFN checkpoint downloaded by hand; its weights are "
                         "gated, so the file is fetched from the vendor's page "
                         "and passed in here")
    ap.add_argument("--small", action="store_true",
                    help="every model on TabPFN's terms: the rows it can take and "
                         "the rows it can score")
    ap.add_argument("--chunks", action="store_true",
                    help="TabPFN on the whole training slice, a piece at a time, "
                         "against the served recipe on the same rows")
    ap.add_argument("--paired", action="store_true",
                    help="the forest against the served recipe, five paired fits")
    ap.add_argument("--sets", type=int, default=5, help="how many pairs")
    args = ap.parse_args()
    if args.small:
        return small(args.cache, args.tabpfn_model)
    if args.chunks:
        return chunked(args.cache, args.tabpfn_model)
    if args.paired:
        return paired(args.cache, args.sets)
    run(args.cache, args.rbf_rows, args.tabpfn_model)


if __name__ == "__main__":
    main()
