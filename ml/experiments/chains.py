"""Do features that follow the money one hop further earn their place?

Three features read the receiver-keyed store a day back, and read it for the
sender as well as for the payee (stream-processor capability `money_chains`):

  rcv_distinct_senders_24h    distinct people paying this payee in a day - the
                              collection a one-hour window sees a slice of
  sender_inflow_24h           money the sender itself received in a day
  sender_distinct_payers_24h  how many people paid the sender in a day - a mule
                              paying on what many paid in

Asked on the realistic profile's time split - 64% fit, 16% validation, 20% test -
over five seeds with train.py's recipe, with and without the three columns. The
rest of the matrix is identical either way: switching the capability on changes
no other feature and no rule, so one replay serves both.

Rule, fixed before the run, and it is the owner's condition - adopt only if it
does not get worse: the features are adopted if, on the VALIDATION rows, the
per-seed paired PR-AUC difference has a mean at or above zero and a 95% interval
whose lower bound is above -0.01, and the averaged committee's validation PR-AUC
is not lower with them. The test rows are reported and decide nothing. IBM AML is
asked the same question afterwards, and only if this passes.

    python experiments/chains.py [--cache chains.npz]
"""
import argparse
import os
import sys
import warnings

os.environ["CAP_MONEY_CHAINS"] = "on"     # before the registry is imported

import numpy as np  # noqa: E402
from scipy.special import expit  # noqa: E402
from sklearn.metrics import average_precision_score, precision_recall_curve  # noqa: E402

_PKG = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _PKG)
import dataset as D  # noqa: E402
import train as T    # noqa: E402

warnings.filterwarnings("ignore", message="X does not have valid feature names")
CHAIN = ("rcv_distinct_senders_24h", "sender_inflow_24h", "sender_distinct_payers_24h")


def _ci95(xs):
    from scipy import stats
    xs = np.asarray(xs, dtype=float)
    return float(stats.t.ppf(0.975, len(xs) - 1) * xs.std(ddof=1) / np.sqrt(len(xs)))


def _cut(y, p):
    prec, rec, thr = precision_recall_curve(y, p)
    f1 = 2 * prec[:-1] * rec[:-1] / np.maximum(prec[:-1] + rec[:-1], 1e-12)
    return float(thr[int(np.argmax(f1))])


def _matrix(cache):
    if cache and os.path.exists(cache):
        z = np.load(cache, allow_pickle=False)
        return z["X"], z["y"], z["types"], [str(n) for n in z["names"]]
    df = D.build_matrix(T.CSV, age_unknown=T.age_unknown_rows)
    X = df[D.FEATURE_NAMES].astype("float32").values
    y = df["label"].values.astype("int8")
    types = df["fraud_type"].astype(str).values.astype("U")
    if cache:
        np.savez(cache, X=X, y=y, types=types, names=np.array(D.FEATURE_NAMES))
    return X, y, types, list(D.FEATURE_NAMES)


def ibm(cache, seeds):
    """The second gate: IBM AML's published split - fit on the first 60%, validate
    on the next 20%, test on the last 20% - with the chain columns and without,
    unweighted as that benchmark's fits must be (validation/README.md 4). Rule,
    fixed before the run: adopted only if the paired per-seed validation PR-AUC
    difference is not below zero on the mean - the owner's "not worse". The cache
    is validation/ibm_aml_adapter.py --extract-only run with CAP_MONEY_CHAINS=on."""
    z = np.load(cache, allow_pickle=False)
    X, y, names = z["X"], z["y"].astype("int8"), [str(c) for c in z["names"]]
    assert all(c in names for c in CHAIN), "the cache was extracted without money_chains"
    without = [i for i, c in enumerate(names) if c not in CHAIN]
    a, b = int(len(y) * 0.6), int(len(y) * 0.8)
    res = {}
    for label, cols in (("without", without), ("with", list(range(len(names))))):
        res[label] = []
        for s in seeds:
            m = T.make_model(1.0, random_state=s)
            m.fit(X[:a][:, cols], y[:a])
            res[label].append((m.predict(X[a:b][:, cols], raw_score=True),
                               m.predict(X[b:][:, cols], raw_score=True)))
            print(f"  IBM {label} seed {s} done", file=sys.stderr, flush=True)
    ap = {k: (np.array([average_precision_score(y[a:b], v) for v, _ in r]),
              np.array([average_precision_score(y[b:], t) for _, t in r]))
          for k, r in res.items()}
    print(f"IBM AML: {len(y):,} rows, {len(names)} features with the chains; "
          f"test {len(y) - b:,} rows, {int(y[b:].sum()):,} laundering")
    for k, (v, t) in ap.items():
        cv = average_precision_score(y[a:b], np.mean([p for p, _ in res[k]], axis=0))
        ct = average_precision_score(y[b:], np.mean([p for _, p in res[k]], axis=0))
        print(f"  {k:<8} validation {v.mean():.4f} +/-{_ci95(v):.4f}   test {t.mean():.4f} "
              f"+/-{_ci95(t):.4f}   committee {cv:.4f} / {ct:.4f}")
    dv, dt = ap["with"][0] - ap["without"][0], ap["with"][1] - ap["without"][1]
    print(f"  with - without: validation {dv.mean():+.4f} [{dv.mean() - _ci95(dv):+.4f}, "
          f"{dv.mean() + _ci95(dv):+.4f}]   test {dt.mean():+.4f} [{dt.mean() - _ci95(dt):+.4f}, "
          f"{dt.mean() + _ci95(dt):+.4f}]")
    print("\nverdict, as fixed in this function's docstring before the run:")
    print(f"  on IBM AML money_chains {'does NOT make it worse - adopted' if dv.mean() >= 0 else 'makes it WORSE - not adopted'}")


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--cache", help="npz for the replayed matrix")
    ap.add_argument("--seeds", type=int, nargs="+", default=[0, 1, 2, 3, 4])
    ap.add_argument("--ibm-cache", help="run the IBM AML gate on this extraction instead")
    args = ap.parse_args()
    if args.ibm_cache:
        return ibm(args.ibm_cache, args.seeds)

    X, y, types, names = _matrix(args.cache)
    assert all(c in names for c in CHAIN), "money_chains did not reach the vector"
    without = [i for i, c in enumerate(names) if c not in CHAIN]
    n = len(y)
    cut = T.cut_index(n)
    fit = int(cut * T.FIT_SHARE)
    yva, yte, tte = y[fit:cut], y[cut:], types[cut:]
    spw = T.class_weight(int(y[:fit].sum()), int((y[:fit] == 0).sum()))
    print(f"{n:,} rows; fit {fit:,}, validation {cut - fit:,} ({int(yva.sum())} fraud), "
          f"test {n - cut:,} ({int(yte.sum())} fraud); weight {spw:.0f}")

    res = {}
    for label, cols in (("without", without), ("with", list(range(len(names))))):
        va, te = [], []
        for s in args.seeds:
            m = T.make_model(spw, random_state=s)
            m.fit(X[:fit][:, cols], y[:fit])
            va.append(m.predict(X[fit:cut][:, cols], raw_score=True))
            te.append(m.predict(X[cut:][:, cols], raw_score=True))
            print(f"  {label} seed {s} done", file=sys.stderr, flush=True)
        res[label] = (va, te)

    def aps(margins, yy):
        return np.array([average_precision_score(yy, m) for m in margins])

    print(f"\n{'':<10}{'validation PR-AUC':>26}{'test PR-AUC':>26}{'committee val / test':>26}")
    for label, (va, te) in res.items():
        v, t = aps(va, yva), aps(te, yte)
        cv = average_precision_score(yva, np.mean(va, axis=0))
        ct = average_precision_score(yte, np.mean(te, axis=0))
        print(f"{label:<10}{v.mean():>18.4f} +/-{_ci95(v):.4f}{t.mean():>18.4f} +/-{_ci95(t):.4f}"
              f"{cv:>17.4f} / {ct:.4f}")
    dv = aps(res["with"][0], yva) - aps(res["without"][0], yva)
    dt = aps(res["with"][1], yte) - aps(res["without"][1], yte)
    print(f"{'with - without':<16} validation {dv.mean():+.4f} [{dv.mean() - _ci95(dv):+.4f}, "
          f"{dv.mean() + _ci95(dv):+.4f}]   test {dt.mean():+.4f} [{dt.mean() - _ci95(dt):+.4f}, "
          f"{dt.mean() + _ci95(dt):+.4f}]")

    print("\ncommittee at the validation-chosen cutoff, on the test rows:")
    for label, (va, te) in res.items():
        cv, ct = expit(np.mean(va, axis=0)), expit(np.mean(te, axis=0))
        flag = ct >= _cut(yva, cv)
        fraud = yte == 1
        tp = int((flag & fraud).sum())
        kinds = ", ".join(f"{k} {np.mean(flag[fraud & (tte == k)]):.0%}"
                          for k in sorted(set(tte[fraud])))
        print(f"  {label:<8} precision {tp / max(int(flag.sum()), 1):.1%}, recall "
              f"{tp / max(int(fraud.sum()), 1):.1%}   by type: {kinds}")

    committee_ok = (average_precision_score(yva, np.mean(res["with"][0], axis=0))
                    >= average_precision_score(yva, np.mean(res["without"][0], axis=0)))
    adopt = dv.mean() >= 0 and dv.mean() - _ci95(dv) > -0.01 and committee_ok
    print("\nverdict, as fixed in the module docstring before the run:")
    print(f"  money_chains {'IS ADOPTED - IBM AML asked next' if adopt else 'is NOT adopted'}")


if __name__ == "__main__":
    main()
