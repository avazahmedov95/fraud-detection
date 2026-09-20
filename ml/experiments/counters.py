"""Do the counterparty counters earn a place in the vector?

The rule is in ml/README.md ("Counting counterparties over days"), committed in
7247a79 before this ran: five paired fits on the realistic profile with the mean
validation PR-AUC difference at or above zero, its 95% interval above -0.01 and
the averaged committee not lower; then the same on PaySim's published split, with
the committee finding at least as much fraud in its top 0.1%.

    cd ml
    python experiments/counters.py --cache counters_realistic.npz
    python experiments/counters.py --cache counters_realistic.npz \
        --paysim ../validation/PS_20174392719_1491204439457_log.csv \
        --paysim-cache counters_paysim.npz
"""
import argparse
import os
import subprocess
import sys

os.environ["CAP_COUNTERPARTY_HISTORY"] = "on"      # before the registry is imported

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from sklearn.metrics import average_precision_score  # noqa: E402

_PKG = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ROOT = os.path.dirname(_PKG)
sys.path.insert(0, _PKG)
sys.path.insert(0, os.path.join(ROOT, "stream-processor"))

import dataset as D  # noqa: E402
import train as T  # noqa: E402

#: The columns under test; everything else is the arm they are measured against.
NEW = ("payee_payers_24h", "payee_payers_7d", "sender_payees_24h",
       "sender_payees_7d", "secs_since_sender_inbound")


def _ci95(xs):
    from scipy import stats
    xs = np.asarray(xs, dtype=float)
    return float(stats.t.ppf(0.975, len(xs) - 1) * xs.std(ddof=1) / np.sqrt(len(xs)))


def _top_share(y, score, share):
    """The share of the positives among the top `share` of rows by score."""
    k = max(1, int(round(share * len(y))))
    top = np.argsort(-score, kind="stable")[:k]
    return float(y[top].sum()) / max(int(y.sum()), 1)


def _fits(X, y, cols, fit, cut, seeds, spw):
    out = []
    for s in seeds:
        m = T.make_model(spw, random_state=s)
        m.fit(X[:fit][:, cols], y[:fit])
        out.append(m.predict(X[fit:cut][:, cols], raw_score=True))
        print(f"    seed {s} done", file=sys.stderr, flush=True)
    return out


def _compare(title, X, y, names, fit, cut, seeds, subset=NEW):
    """Paired fits without and with the counters, on the validation rows."""
    base = [i for i, n in enumerate(names) if n not in NEW]
    with_cols = base + [names.index(n) for n in subset]
    spw = T.class_weight(int(y[:fit].sum()), int((y[:fit] == 0).sum()))
    print(f"\n{title}: fit {fit:,} rows (pos={int(y[:fit].sum()):,}), "
          f"validate {cut - fit:,} (pos={int(y[fit:cut].sum()):,}), "
          f"scale_pos_weight={spw:.1f}")
    runs = {"without": _fits(X, y, base, fit, cut, seeds, spw),
            "with": _fits(X, y, with_cols, fit, cut, seeds, spw)}
    yva = y[fit:cut]

    def aps(ms):
        return np.array([average_precision_score(yva, m) for m in ms])

    for k, ms in runs.items():
        v = aps(ms)
        print(f"  {k:<8} validation PR-AUC {v.mean():.4f} +/-{_ci95(v):.4f}"
              f"   committee {average_precision_score(yva, np.mean(ms, axis=0)):.4f}"
              f"   top 0.1% {_top_share(yva, np.mean(ms, axis=0), 0.001):.1%}")
    dv = aps(runs["with"]) - aps(runs["without"])
    print(f"  with - without: {dv.mean():+.4f} "
          f"[{dv.mean() - _ci95(dv):+.4f}, {dv.mean() + _ci95(dv):+.4f}], "
          f"better on {int((dv > 0).sum())} of {len(dv)} seeds")
    committee = {k: average_precision_score(yva, np.mean(ms, axis=0))
                 for k, ms in runs.items()}
    top = {k: _top_share(yva, np.mean(ms, axis=0), 0.001) for k, ms in runs.items()}
    return dv, committee["with"] >= committee["without"], top["with"] >= top["without"]


def _matrix(cache, build):
    if cache and os.path.exists(cache):
        z = np.load(cache, allow_pickle=False)
        return z["X"], z["y"], [str(n) for n in z["names"]]
    X, y, names = build()
    if cache:
        np.savez(cache, X=X, y=y, names=np.array(names))
    return X, y, names


def _existing_columns_unchanged(X, names, rows=20000):
    """Condition 1 of the rule: switching the capability on leaves the sixteen
    columns a trained model already reads exactly as they were. Rebuilt in a child
    process, because the registry is read once at import."""
    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_off_head.npz")
    env = dict(os.environ, CAP_COUNTERPARTY_HISTORY="off", PYTHONIOENCODING="utf-8")
    code = (f"import sys; sys.path[:0] = [{_PKG!r}, {os.path.join(ROOT, 'stream-processor')!r}];"
            f"import numpy as np, dataset as D, train as T;"
            f"df = D.build_matrix(T.CSV, nrows={rows});"
            f"np.savez({out!r}, X=df[D.FEATURE_NAMES].astype('float32').values,"
            f" names=np.array(list(D.FEATURE_NAMES)))")
    subprocess.run([sys.executable, "-c", code], check=True, env=env)
    with np.load(out, allow_pickle=False) as z:      # closed before the file is removed
        off_names = [str(n) for n in z["names"]]
        assert not [n for n in off_names if n in NEW]
        cols = [names.index(n) for n in off_names]
        differ = int(np.sum(X[:rows][:, cols] != z["X"]))
    os.remove(out)
    print(f"parity of the existing columns on {rows:,} rows: {differ:,} cells differ "
          f"(required: 0)")
    return differ == 0


def _realistic_matrix(cache):
    def build():
        df = D.build_matrix(T.CSV)
        names = list(D.FEATURE_NAMES)
        return (df[names].astype("float32").values,
                df["label"].values.astype("int8"), names)

    return _matrix(cache, build)


def decompose(cache, seeds):
    """Which half carries the gain: the four counts, or the transit interval. Not a
    gate - the rule judges the five columns together - but a column that does nothing
    is a column to drop, and one that does everything is one to look at twice."""
    X, y, names = _realistic_matrix(cache)
    cut = T.cut_index(len(y))
    fit = int(cut * T.FIT_SHARE)
    for title, subset in (("counts only", NEW[:4]), ("transit only", NEW[4:])):
        _compare(f"realistic profile, {title}", X, y, names, fit, cut, seeds, subset)


def realistic(cache, seeds):
    X, y, names = _realistic_matrix(cache)
    assert all(n in names for n in NEW), names[-6:]
    if not _existing_columns_unchanged(X, names):
        raise SystemExit("parity failed - nothing below is measured")
    cut = T.cut_index(len(y))
    dv, committee_ok, _ = _compare("realistic profile", X, y, names,
                                   int(cut * T.FIT_SHARE), cut, seeds)
    return bool(dv.mean() >= 0 and dv.mean() - _ci95(dv) > -0.01 and committee_ok)


def paysim(path, cache, seeds):
    def build():
        sys.path.insert(0, os.path.join(ROOT, "validation"))
        import paysim_adapter as P
        P.RP.capability_profile("myid_kinship", "geo_telemetry", "session_telemetry")
        df = pd.read_csv(path).sort_values("step", kind="stable").reset_index(drop=True)
        keep, names = P.RP.available_features()
        Xb, yb, _ = P.RP.extract_features(P.to_events(df, P.scale_factor(df.amount)),
                                          total=len(df))
        steps = df.step.values.astype("int64")
        return Xb[:, keep], np.asarray(yb).astype("int8"), names, steps

    if cache and os.path.exists(cache):
        z = np.load(cache, allow_pickle=False)
        X, y, names, steps = z["X"], z["y"], [str(n) for n in z["names"]], z["steps"]
    else:
        X, y, names, steps = build()
        if cache:
            np.savez(cache, X=X, y=y, names=np.array(names), steps=steps)
    sys.path.insert(0, os.path.join(ROOT, "validation"))
    import paysim_adapter as P
    cut = int((steps <= P.BASELINE["cut_step"]).sum())     # the published 24-day split
    dv, _, top_ok = _compare("PaySim", X, y, names, int(cut * 0.8), cut, seeds)
    return bool(dv.mean() >= 0 and top_ok)


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--cache", help="npz for the realistic matrix")
    ap.add_argument("--paysim", help="PaySim's CSV; omitted, only this data is read")
    ap.add_argument("--paysim-cache", help="npz for PaySim's matrix")
    ap.add_argument("--seeds", type=int, nargs="+", default=[0, 1, 2, 3, 4])
    ap.add_argument("--decompose", action="store_true",
                    help="counts and transit measured apart, after the gate")
    args = ap.parse_args()

    if args.decompose:
        decompose(args.cache, args.seeds)
        return
    results = {"realistic profile": realistic(args.cache, args.seeds)}
    if args.paysim:
        results["PaySim"] = paysim(args.paysim, args.paysim_cache, args.seeds)

    print("\nverdict, by the rule fixed in ml/README.md before the run:")
    for k, v in results.items():
        print(f"  {k:<20}{'pass' if v else 'FAIL'}")
    if args.paysim:
        print("  counterparty_history " + ("is SWITCHED ON - retrain the served model"
                                           if all(results.values()) else "stays OFF"))


if __name__ == "__main__":
    main()
