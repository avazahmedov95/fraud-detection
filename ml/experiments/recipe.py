"""Which training recipe: train.py's, the same with row sampling actually on, or
an average of several fits?

train.py passes subsample=0.8 and no subsample_freq. LightGBM samples rows only
when that frequency is non-zero, so the 80% row sampling the recipe reads as has
never run: every tree has seen every row. On IBM AML one fit's spread was +/-4 F1
points, and the average of twenty fits scored far above any one of them
(validation/README.md 4) - variance, which row sampling and averaging both reduce.

Recipes, each fitted over the same seeds:
  current            train.py's make_model as it is
  bagged             the same with subsample_freq=1
  current, averaged  the mean log-odds of every `current` fit
  bagged, averaged   the mean log-odds of every `bagged` fit

Scored threshold-free, by PR-AUC, on:
  own  this project's held-out 20%, built and weighted exactly as train.py does
  ibm  IBM AML's published split - trained on the first 60%, scored on the last
       20% - on the 14 features this contract computes there, unweighted
       (weighting collapses at a 0.1% base rate, README 4)

Rule, fixed before the run: `bagged` replaces `current` if its IBM PR-AUC gain,
paired by seed, has a 95% interval above zero, and its own-data PR-AUC falls by
no more than 0.005 on the mean. The averages are reported beside the single fits
and decide nothing: adopting one would change the exported artefact and the
explainer, so it is a finding here, not a switch.

    python experiments/recipe.py --ibm-cache <ibm_features.npz>
"""
import argparse
import os
import sys
import warnings

import numpy as np
from sklearn.metrics import average_precision_score

_PKG = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _PKG)
import dataset as D  # noqa: E402
import train as T    # noqa: E402

# sklearn warns on every predict that LightGBM was fitted without feature names.
warnings.filterwarnings("ignore", message="X does not have valid feature names")
DEFAULT_SEEDS = tuple(range(10))


def _ci95(xs):
    """t-based half-width, the convention of ablate_seeds.py."""
    from scipy import stats
    xs = np.asarray(xs, dtype=float)
    if len(xs) < 2:
        return float("nan")
    return float(stats.t.ppf(0.975, len(xs) - 1) * xs.std(ddof=1) / np.sqrt(len(xs)))


def _margins(Xtr, ytr, Xte, spw, seed, bagged):
    m = T.make_model(spw, random_state=seed)
    if bagged:
        m.set_params(subsample_freq=1)
    m.fit(Xtr, ytr)
    return m.predict(Xte, raw_score=True)


def compare(name, Xtr, ytr, Xte, yte, spw, seeds):
    ms = {}
    for recipe, bagged in (("current", False), ("bagged", True)):
        ms[recipe] = []
        for s in seeds:
            ms[recipe].append(_margins(Xtr, ytr, Xte, spw, s, bagged))
            print(f"  {name}: {recipe} seed {s} done", file=sys.stderr, flush=True)
    ap = {r: np.array([average_precision_score(yte, m) for m in ms[r]]) for r in ms}
    avg = {r: average_precision_score(yte, np.mean(ms[r], axis=0)) for r in ms}
    delta = ap["bagged"] - ap["current"]
    return {"ap": ap, "avg": avg, "delta": delta, "half": _ci95(delta),
            "up": int((delta > 0).sum())}


def own_data():
    df = D.build_matrix(T.CSV, age_unknown=T.age_unknown_rows)
    feats, cut = D.FEATURE_NAMES, T.cut_index(len(df))
    X, y = df[feats].astype("float32").values, df["label"].values
    ytr = y[:cut]
    spw = T.class_weight(int(ytr.sum()), int((ytr == 0).sum()))
    return X[:cut], ytr, X[cut:], y[cut:], spw


def ibm_data(cache):
    z = np.load(cache, allow_pickle=False)
    X, y = z["X"], z["y"].astype("int8")
    a, b = int(len(y) * 0.6), int(len(y) * 0.8)
    return X[:a], y[:a], X[b:], y[b:], 1.0


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--ibm-cache", required=True,
                    help="validation/ibm_aml_adapter.py --extract-only output")
    ap.add_argument("--seeds", type=int, nargs="+", default=list(DEFAULT_SEEDS))
    args = ap.parse_args()

    res = {"own": compare("own", *own_data(), args.seeds),
           "ibm": compare("ibm", *ibm_data(args.ibm_cache), args.seeds)}

    print(f"\nPR-AUC, {len(args.seeds)} seeds: mean +/- 95% CI (min-max)\n")
    print(f"{'recipe':<20}{'own data':>36}{'IBM AML':>36}")
    for r in ("current", "bagged"):
        cells = [f"{v['ap'][r].mean():.4f} +/- {_ci95(v['ap'][r]):.4f} "
                 f"({v['ap'][r].min():.4f}-{v['ap'][r].max():.4f})" for v in res.values()]
        print(f"{r:<20}" + "".join(f"{c:>36}" for c in cells))
    cells = [f"{v['delta'].mean():+.4f} [{v['delta'].mean() - v['half']:+.4f}, "
             f"{v['delta'].mean() + v['half']:+.4f}] {v['up']}/{len(args.seeds)} up"
             for v in res.values()]
    print(f"{'bagged - current':<20}" + "".join(f"{c:>36}" for c in cells))
    for r in ("current", "bagged"):
        print(f"{r + ', averaged':<20}" + "".join(f"{v['avg'][r]:>36.4f}" for v in res.values()))

    own, ibm = res["own"], res["ibm"]
    adopt = (ibm["delta"].mean() - ibm["half"] > 0) and (own["delta"].mean() >= -0.005)
    print("\nverdict, as fixed in the module docstring before the run:")
    print(f"  bagged {'REPLACES' if adopt else 'does NOT replace'} current")


if __name__ == "__main__":
    main()
