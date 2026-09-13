"""Does an unknown payee age make the model raise MORE alerts, and does training
with the age withheld on some rows stop it?

docs/irp-framing.md 7.7a saw alerts rise with Neo4j down, in one arm of the
dependency matrix, and named the test that would separate the mechanism from
noise: replay the held-out slice offline with the age forced to unknown. This is
that replay, for two training recipes, each with train.py's hyperparameters:

  sentinel  every training age known; an unknown one encoded receiver_age = -1,
            receiver_is_fresh = 0 - the recipe until 2026-09-13
  missing   the age withheld on a random share of training rows; an unknown one
            encoded NaN - the recipe since

Each model is scored on the held-out slice twice - every payee age known, and
none - across seeds. The verdicts are fixed here, before the run:

  mechanism  the sentinel recipe raises more false alarms without the ages than
             with them, on every seed;
  fix        the missing recipe raises, averaged over seeds, no more false alarms
             without the ages than with them, and fewer than the sentinel recipe
             without them on every seed;
  cost       the missing recipe's PR-AUC with the ages known is within 0.01 of the
             sentinel recipe's, averaged over seeds.

The replay is built twice - every age, and none - and each row's features are
taken from one build or the other. That holds only if nothing a row's age moves
reaches another row's features; the script checks it rather than assuming it.

    python experiments/receiver_age_outage.py
    python experiments/receiver_age_outage.py --reference sentinel=old/model.joblib
"""
import argparse
import os
import sys
import warnings

import joblib
import numpy as np
from sklearn.metrics import average_precision_score

_PKG = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _PKG)
import dataset as D  # noqa: E402
import train as T    # noqa: E402

# sklearn warns on every predict that LightGBM was fitted without feature names;
# twenty identical warnings bury the table they interrupt.
warnings.filterwarnings("ignore", message="X does not have valid feature names")

AGE = ("receiver_age", "receiver_is_fresh")
DEFAULT_SEEDS = (42, 7, 13, 99, 2026)


def _sentinel(X, a, f):
    """The encoding until 2026-09-13: an unknown age -1.0, and not fresh."""
    X = X.copy()
    unknown = np.isnan(X[:, a])
    X[unknown, a], X[unknown, f] = -1.0, 0.0
    return X


def _score(model, X, y, types):
    p = model.predict_proba(X)[:, 1]
    alert, fraud = p >= 0.50, y == 1
    return dict(alerts=int(alert.sum()), fp=int((alert & ~fraud).sum()),
                recall=float(alert[fraud].mean()),
                pr_auc=float(average_precision_score(y, p)),
                by_type={t: float(alert[types == t].mean())
                         for t in np.unique(types[fraud])},
                proba=p)


def _check_rowwise(known, none, feats):
    """Withholding the ages may move a row's own age features, and its cep_score
    only where FRESH_RECEIVER could have fired. Anything else moving means a row's
    age reaches other rows, and mixing the two builds row by row is invalid."""
    other = [c for c in feats if c not in AGE + ("cep_score",)]
    leaked = [c for c in other
              if not np.allclose(known[c].values, none[c].values, equal_nan=True)]
    cep_moved = known["cep_score"].values != none["cep_score"].values
    stray = int((cep_moved & (known["receiver_is_fresh"].values != 1)).sum())
    if leaked or stray:
        raise SystemExit(f"withholding the ages moved {leaked or 'cep_score'} beyond "
                         f"the rows' own age ({stray} stray cep_score rows): the two "
                         "builds cannot be mixed row by row")
    return int(cep_moved.sum())


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--seeds", type=int, nargs="+", default=list(DEFAULT_SEEDS))
    ap.add_argument("--share", type=float, default=T.AGE_UNKNOWN_SHARE,
                    help="training rows with the age withheld, for the missing recipe")
    ap.add_argument("--reference", nargs="*", default=[], metavar="RECIPE=MODEL",
                    help="a saved model.joblib the seed-42 fit of RECIPE must reproduce")
    args = ap.parse_args()
    refs = dict(r.split("=", 1) for r in args.reference)

    feats = D.FEATURE_NAMES
    if not all(c in feats for c in AGE):
        raise SystemExit("the receiver_age capability is off: no age to withhold")
    a, f = feats.index(AGE[0]), feats.index(AGE[1])

    print("building the replay twice: every payee age known, and none ...")
    known = D.build_matrix(T.CSV)
    none = D.build_matrix(T.CSV, age_unknown=lambda n: np.ones(n, dtype=bool))
    moved = _check_rowwise(known, none, feats)

    y, types = known["label"].values, known["fraud_type"].values
    n = len(y)
    cut = T.cut_index(n)
    XK, XN = (b[feats].astype("float32").values for b in (known, none))
    ytr, yte, tte = y[:cut], y[cut:], types[cut:]
    spw = int((ytr == 0).sum()) / max(int(ytr.sum()), 1)
    print(f"{n:,} rows, held out {n - cut:,} with {int(yte.sum())} fraud; without the "
          f"ages cep_score drops on {moved:,} rows (FRESH_RECEIVER)\n")

    code = {"sentinel": lambda X: _sentinel(X, a, f), "missing": lambda X: X}
    runs = {r: [] for r in code}
    for seed in args.seeds:
        withheld = T.age_unknown_rows(n, seed=seed, share=args.share)[:cut]
        train = {"sentinel": XK[:cut],
                 "missing": np.where(withheld[:, None], XN[:cut], XK[:cut])}
        for recipe, enc in code.items():
            model = T.make_model(spw, random_state=seed)
            model.fit(enc(train[recipe]), ytr)
            runs[recipe].append((_score(model, enc(XK[cut:]), yte, tte),
                                 _score(model, enc(XN[cut:]), yte, tte)))
            if seed == 42 and recipe in refs:
                ref = joblib.load(refs[recipe]).predict_proba(enc(XK[cut:]))[:, 1]
                d = float(np.max(np.abs(ref - runs[recipe][-1][0]["proba"])))
                print(f"  {recipe}, seed 42, against {refs[recipe]}: max |delta| = {d:.2e}")
        print(f"  seed {seed:<5} false alarms, ages known -> none:   " + "   ".join(
            f"{r} {runs[r][-1][0]['fp']} -> {runs[r][-1][1]['fp']}" for r in code))

    def cell(recipe, i, key, fmt):
        xs = [run[i][key] for run in runs[recipe]]
        return f"{format(np.mean(xs), fmt)} ({format(min(xs), fmt)}-{format(max(xs), fmt)})"

    print(f"\nmean over {len(args.seeds)} seeds (min-max), threshold 0.50\n")
    print(f"{'recipe':<10}{'payee ages':<12}{'alerts':>18}{'false alarms':>18}"
          f"{'recall':>22}{'PR-AUC':>22}")
    for recipe in code:
        for i, label in ((0, "known"), (1, "none")):
            print(f"{recipe:<10}{label:<12}{cell(recipe, i, 'alerts', '.0f'):>18}"
                  f"{cell(recipe, i, 'fp', '.0f'):>18}"
                  f"{cell(recipe, i, 'recall', '.3f'):>22}"
                  f"{cell(recipe, i, 'pr_auc', '.3f'):>22}")

    print("\nrecall by fraud type, mean over seeds")
    print(f"  {'':<14}" + "".join(f"{r + ', ' + s:>20}"
                                  for r in code for s in ("known", "none")))
    for t in sorted(runs["missing"][0][0]["by_type"]):
        print(f"  {t:<14}" + "".join(
            f"{np.mean([run[i]['by_type'][t] for run in runs[r]]):>20.1%}"
            for r in code for i in (0, 1)))

    fp = {r: [(h["fp"], d["fp"]) for h, d in runs[r]] for r in code}
    mechanism = all(d > h for h, d in fp["sentinel"])
    fixed = (np.mean([d for _, d in fp["missing"]]) <= np.mean([h for h, _ in fp["missing"]])
             and all(m[1] < s[1] for m, s in zip(fp["missing"], fp["sentinel"])))
    cost = float(np.mean([h["pr_auc"] for h, _ in runs["missing"]])
                 - np.mean([h["pr_auc"] for h, _ in runs["sentinel"]]))
    print("\nverdicts, as fixed in the module docstring before the run")
    print(f"  mechanism  {'CONFIRMED' if mechanism else 'NOT CONFIRMED'}")
    print(f"  fix        {'WORKS' if fixed else 'DOES NOT WORK'}")
    print(f"  cost       PR-AUC {cost:+.4f}, {'within' if cost >= -0.01 else 'OUTSIDE'} 0.01")


if __name__ == "__main__":
    main()
