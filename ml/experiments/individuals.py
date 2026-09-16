"""Is the IBM AML caveat on the multi-day link shapes about its companies?

On IBM AML the shapes catch more only while under about 0.1% of transfers are
flagged (ml/README.md, "The alert cutoff"). This project is about transfers between
people, and IBM AML also holds banks and companies. So the same models - five seeds
each, without and with the shapes, fitted on every training row as in
experiments/shapes.py - are read only on transfers where both accounts look like
people: in the 96 hours before the transfer the sender paid at most 20 accounts and
was paid by at most 20, and the same holds for the payee. Those are the shapes' own
counts, so the filter looks only at the past, never at a label.

Measure: recall at a budget - the share of the subset's laundering found in the top
0.1%, 0.2%, 0.5% and 1% of the subset's transfers by committee score - on the test
rows, and for contrast on all test rows.

Rule, fixed before the run: companies explain the caveat if, on the people-like test
rows, the model with the shapes finds at least as much laundering as the model
without them at every one of the four budgets. If so, building the shapes into the
stream goes ahead; if not, it does not, and another way to raise detection is looked
for.

    python experiments/individuals.py --ibm ../validation/HI-Small_Trans.csv --ibm-cache ../validation/ibm_features.npz
"""
import argparse
import os
import sys

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import shapes as SH  # noqa: E402

BUDGETS = (0.001, 0.002, 0.005, 0.01)
PEOPLE = 20


def recall_at(y, score, budget):
    """Share of the positives among the top `budget` of rows by score."""
    k = max(1, int(round(budget * len(y))))
    top = np.argsort(-score, kind="stable")[:k]
    return float(y[top].sum()) / max(int(y.sum()), 1)


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--ibm", required=True, help="HI-Small_Trans.csv")
    ap.add_argument("--ibm-cache", required=True,
                    help="the matrix ibm_aml_adapter.py --extract-only cached")
    ap.add_argument("--seeds", type=int, nargs="+", default=[0, 1, 2, 3, 4])
    args = ap.parse_args()

    base, S, y = SH.ibm_matrix(args.ibm, args.ibm_cache)
    X = np.hstack([base, S])
    n = len(y)
    a, b = int(n * 0.6), int(n * 0.8)
    people = (S[:, :4] <= PEOPLE).all(axis=1)
    res = {"without": SH._fits(X, y, list(range(base.shape[1])), a, b, args.seeds, 1.0),
           "with": SH._fits(X, y, list(range(X.shape[1])), a, b, args.seeds, 1.0)}
    yte, mte = y[b:], people[b:]
    print(f"IBM AML test rows: {len(yte):,}, {int(yte.sum()):,} laundering. People-like: "
          f"{int(mte.sum()):,} ({mte.mean():.1%}), holding {int(yte[mte].sum()):,} "
          f"laundering ({yte[mte].sum() / max(int(yte.sum()), 1):.1%} of it)")
    scores = {k: np.mean(te, axis=0) for k, (_, te) in res.items()}

    passed = True
    for title, m in (("people-like test rows", mte),
                     ("all test rows, for contrast", np.ones(len(yte), dtype=bool))):
        print()
        print(f"  {title}: share of laundering in the top x% of transfers")
        print(f"  {'top':<8}{'without':>10}{'with':>10}")
        for budget in BUDGETS:
            r = {k: recall_at(yte[m], s[m], budget) for k, s in scores.items()}
            print(f"  {budget:<8.1%}{r['without']:>10.1%}{r['with']:>10.1%}")
            if m is mte and r["with"] < r["without"]:
                passed = False
    print()
    print("verdict, as fixed in the module docstring before the run:")
    print("  companies " + ("EXPLAIN the caveat - build the shapes into the stream" if passed
                            else "do NOT explain the caveat - do not build the shapes yet"))


if __name__ == "__main__":
    main()
