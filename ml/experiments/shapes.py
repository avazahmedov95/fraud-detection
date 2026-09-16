"""Do multi-day link shapes - money moving in a circle, or split and then gathered
again - earn a place beside the features the job computes?

Seven columns per transfer s -> r, counted over the transfers strictly before it
within 96 hours, on the identity the live job keys its state on (the card on the
realistic profile, the account on IBM AML). 96 hours is the smallest whole number
of days covering IBM AML's median collection span, 86.8 hours - measured on that
whole file, so the window is informed by the second gate's data.

  fan_in_96h          distinct payers of r
  fan_out_96h         distinct payees of s
  src_in_96h          distinct payers of s - gathered, now paying on
  dst_out_96h         distinct payees of r - already paying on
  cycle2_96h          transfers r -> s: the money goes straight back
  cycle3_96h          accounts x with r -> x no later than x -> s: a three-step circle
  scatter_gather_96h  other payers of r that share a payer with s: split, then gathered

Counts stop at 20. A three-step circle is not searched when both sides have more
than 500 neighbours, nor a split-and-gather when either side has more than 30
payers: those cells are NaN, as a live store would have to give up on a hub too.
The columns are computed offline in replay order, so this asks whether they are
worth building into the stream, not how to build them.

Rule, fixed before the run - money_chains' rule, the owner's "adopt only if it does
not get worse":
  1. realistic profile, train.py's split (64% fit, 16% validation, 20% test), five
     seeds: on the VALIDATION rows the paired per-seed PR-AUC difference has a mean
     at or above zero and a 95% interval whose lower bound is above -0.01, and the
     averaged committee's validation PR-AUC is not lower with the columns;
  2. only if 1 passes - IBM AML's published split (60/20/20), unweighted, five
     seeds: the paired per-seed validation PR-AUC difference is not below zero on
     the mean.
Test rows are reported and decide nothing. Passing both means building the columns
into the stream and measuring again through the deployed extractor; failing either
means this file is deleted and the verdict written into ml/README.md.

    python experiments/shapes.py --cache shapes_realistic.npz
    python experiments/shapes.py --ibm ../validation/HI-Small_Trans.csv --ibm-cache ../validation/ibm_features.npz
"""
import argparse
import os
import sys
import warnings
from bisect import bisect_left

import numpy as np
import pandas as pd
from scipy.special import expit
from sklearn.metrics import average_precision_score, precision_recall_curve

_PKG = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _PKG)
import dataset as D  # noqa: E402
import train as T    # noqa: E402

warnings.filterwarnings("ignore", message="X does not have valid feature names")
WINDOW_S = 96 * 3600
CAP, CYCLE_LIMIT, GATHER_LIMIT = 20, 500, 30
SHAPES = ("fan_in_96h", "fan_out_96h", "src_in_96h", "dst_out_96h",
          "cycle2_96h", "cycle3_96h", "scatter_gather_96h")


def shape_features(ts, src, dst, window=WINDOW_S):
    """The seven columns for rows in time order. A row sees only transfers with an
    earlier timestamp: rows sharing one are scored before any of them is added."""
    ts, src, dst = list(ts), [int(v) for v in src], [int(v) for v in dst]
    n, empty, nan = len(ts), {}, float("nan")
    cols = [[0.0] * n for _ in SHAPES]
    out_c, in_c, times = {}, {}, {}
    e = i = 0
    while i < n:
        t, j = ts[i], i
        while j < n and ts[j] == t:
            j += 1
        lo = t - window
        while ts[e] < lo:                       # the window slides past event e
            for store, a, b in ((out_c, src[e], dst[e]), (in_c, dst[e], src[e])):
                c = store[a]
                if c[b] > 1:
                    c[b] -= 1
                else:
                    del c[b]
                    if not c:
                        del store[a]
            e += 1
        for k in range(i, j):
            s, r = src[k], dst[k]
            ins, outs = in_c.get(s, empty), out_c.get(s, empty)
            inr, outr = in_c.get(r, empty), out_c.get(r, empty)
            cols[0][k], cols[1][k] = len(inr), len(outs)
            cols[2][k], cols[3][k] = len(ins), len(outr)
            back = times.get((r << 32) | s)
            cols[4][k] = min(CAP, len(back) - bisect_left(back, lo)) if back else 0
            small, big = (outr, ins) if len(outr) <= len(ins) else (ins, outr)
            if len(small) > CYCLE_LIMIT:
                cols[5][k] = nan
            else:
                c3 = 0
                for x in small:
                    if x in big and x != s and x != r:
                        rx = times[(r << 32) | x]
                        if rx[bisect_left(rx, lo)] <= times[(x << 32) | s][-1]:
                            c3 += 1
                            if c3 == CAP:
                                break
                cols[5][k] = c3
            if len(inr) > GATHER_LIMIT or len(ins) > GATHER_LIMIT:
                cols[6][k] = nan
            else:
                sg = 0
                for x in inr:
                    if x != s and any(a in in_c.get(x, empty) for a in ins):
                        sg += 1
                        if sg == CAP:
                            break
                cols[6][k] = sg
        for k in range(i, j):
            s, r = src[k], dst[k]
            out_c.setdefault(s, {})[r] = out_c.get(s, empty).get(r, 0) + 1
            in_c.setdefault(r, {})[s] = in_c.get(r, empty).get(s, 0) + 1
            times.setdefault((s << 32) | r, []).append(t)
        i = j
    return np.array(cols, dtype="float32").T


def _ci95(xs):
    from scipy import stats
    xs = np.asarray(xs, dtype=float)
    return float(stats.t.ppf(0.975, len(xs) - 1) * xs.std(ddof=1) / np.sqrt(len(xs)))


def _cut(y, p):
    prec, rec, thr = precision_recall_curve(y, p)
    f1 = 2 * prec[:-1] * rec[:-1] / np.maximum(prec[:-1] + rec[:-1], 1e-12)
    return float(thr[int(np.argmax(f1))])


def _describe(S, y):
    """Each column on each class - a column that is all zero or all NaN would fail
    the rule for the wrong reason."""
    print(f"  {'column':<20}{'fraud: mean  nonzero  NaN':>30}{'legit: mean  nonzero  NaN':>30}")
    for c, name in enumerate(SHAPES):
        parts = []
        for m in (y == 1, y == 0):
            v = S[m, c]
            parts.append(f"{np.nanmean(v):>10.2f}{np.mean(v > 0):>10.1%}{np.mean(np.isnan(v)):>10.1%}")
        print(f"  {name:<20}{parts[0]:>30}{parts[1]:>30}")


def _fits(X, y, cols, fit, cut, seeds, spw):
    va, te = [], []
    for s in seeds:
        m = T.make_model(spw, random_state=s)
        m.fit(X[:fit][:, cols], y[:fit])
        va.append(m.predict(X[fit:cut][:, cols], raw_score=True))
        te.append(m.predict(X[cut:][:, cols], raw_score=True))
        print(f"    seed {s} done", file=sys.stderr, flush=True)
    return va, te


def _compare(X, y, base, fit, cut, seeds, spw, types=None):
    """Paired fits without and with the shapes. Returns the per-seed validation
    PR-AUC differences and whether the averaged committee is not lower with them."""
    res = {"without": _fits(X, y, base, fit, cut, seeds, spw),
           "with": _fits(X, y, list(range(X.shape[1])), fit, cut, seeds, spw)}
    yva, yte = y[fit:cut], y[cut:]

    def aps(margins, yy):
        return np.array([average_precision_score(yy, m) for m in margins])

    print()
    print(f"  {'':<10}{'validation PR-AUC':>24}{'test PR-AUC':>24}{'committee val / test':>26}")
    for k, (va, te) in res.items():
        v, t = aps(va, yva), aps(te, yte)
        cv = average_precision_score(yva, np.mean(va, axis=0))
        ct = average_precision_score(yte, np.mean(te, axis=0))
        print(f"  {k:<10}{v.mean():>16.4f} +/-{_ci95(v):.4f}{t.mean():>16.4f} +/-{_ci95(t):.4f}"
              f"{cv:>17.4f} / {ct:.4f}")
    dv = aps(res["with"][0], yva) - aps(res["without"][0], yva)
    dt = aps(res["with"][1], yte) - aps(res["without"][1], yte)
    print(f"  with - without: validation {dv.mean():+.4f} [{dv.mean() - _ci95(dv):+.4f}, "
          f"{dv.mean() + _ci95(dv):+.4f}]   test {dt.mean():+.4f} [{dt.mean() - _ci95(dt):+.4f}, "
          f"{dt.mean() + _ci95(dt):+.4f}]")
    print()
    print("  committee at the validation-chosen cutoff, on the test rows:")
    for k, (va, te) in res.items():
        cv, ct = expit(np.mean(va, axis=0)), expit(np.mean(te, axis=0))
        flag, pos = ct >= _cut(yva, cv), yte == 1
        tp = int((flag & pos).sum())
        kinds = ""
        if types is not None:
            tte = types[cut:]
            kinds = "   by type: " + ", ".join(
                f"{t} {np.mean(flag[pos & (tte == t)]):.0%}" for t in sorted(set(tte[pos])))
        print(f"    {k:<8} precision {tp / max(int(flag.sum()), 1):.1%}, "
              f"recall {tp / max(int(pos.sum()), 1):.1%}{kinds}")
    committee_ok = (average_precision_score(yva, np.mean(res["with"][0], axis=0))
                    >= average_precision_score(yva, np.mean(res["without"][0], axis=0)))
    return dv, committee_ok


def realistic(cache, seeds):
    if cache and os.path.exists(cache):
        z = np.load(cache, allow_pickle=False)
        X, y, types = z["X"], z["y"], z["types"]
    else:
        df = D.build_matrix(T.CSV, age_unknown=T.age_unknown_rows)
        raw = (pd.read_csv(T.CSV, usecols=["event_time", "sender_card", "receiver_card",
                                           "label_is_fraud"])
               .sort_values("event_time").reset_index(drop=True))
        if not np.array_equal(raw.label_is_fraud.values.astype("int8"),
                              df.label.values.astype("int8")):
            raise SystemExit("the replay and the CSV are not in the same order")
        ts = pd.to_datetime(raw.event_time, format="ISO8601").astype("int64").values / 1e9
        ids, _ = pd.factorize(np.concatenate([raw.sender_card.astype(str).values,
                                              raw.receiver_card.astype(str).values]))
        S = shape_features(ts, ids[:len(raw)], ids[len(raw):])
        X = np.hstack([df[D.FEATURE_NAMES].astype("float32").values, S])
        y = df.label.values.astype("int8")
        types = df.fraud_type.astype(str).values.astype("U")
        if cache:
            np.savez(cache, X=X, y=y, types=types)
    n, k = len(y), len(D.FEATURE_NAMES)
    cut = T.cut_index(n)
    fit = int(cut * T.FIT_SHARE)
    spw = T.class_weight(int(y[:fit].sum()), int((y[:fit] == 0).sum()))
    print(f"realistic profile: {n:,} rows; fit {fit:,}, validation {cut - fit:,} "
          f"({int(y[fit:cut].sum())} fraud), test {n - cut:,} ({int(y[cut:].sum())} fraud); "
          f"weight {spw:.0f}")
    _describe(X[:, k:], y)
    dv, committee_ok = _compare(X, y, list(range(k)), fit, cut, seeds, spw, types)
    adopt = dv.mean() >= 0 and dv.mean() - _ci95(dv) > -0.01 and committee_ok
    print()
    print("verdict, as fixed in the module docstring before the run:")
    print(f"  on the realistic profile the shapes "
          f"{'PASS - IBM AML is asked next' if adopt else 'do NOT pass - not adopted'}")


def ibm(path, cache, seeds):
    sys.path.insert(0, os.path.join(os.path.dirname(_PKG), "validation"))
    import ibm_aml_adapter as A
    d, _, _ = A.load(path)
    z = np.load(cache, allow_pickle=False)
    base, y = z["X"], z["y"].astype("int8")
    file_ts = d.ts.values.astype("datetime64[s]").astype("int64")
    if (len(y) != len(d) or (z["ts"].astype("int64") != file_ts).any()
            or (y != d.label.values.astype("int8")).any()):
        raise SystemExit("the cache is not row-aligned with this file - re-run --extract-only")
    ids, _ = pd.factorize(np.concatenate([d.sender.values, d.receiver.values]))
    S = shape_features(file_ts, ids[:len(d)], ids[len(d):])
    X = np.hstack([base, S])
    n = len(y)
    a, b = int(n * 0.6), int(n * 0.8)
    print(f"IBM AML: {n:,} rows; fit {a:,}, validation {b - a:,} ({int(y[a:b].sum()):,} "
          f"laundering), test {n - b:,} ({int(y[b:].sum()):,} laundering)")
    _describe(S, y)
    dv, _ = _compare(X, y, list(range(base.shape[1])), a, b, seeds, 1.0)
    print()
    print("verdict, as fixed in the module docstring before the run:")
    print(f"  on IBM AML the shapes "
          f"{'do NOT make it worse - build them into the stream' if dv.mean() >= 0 else 'make it WORSE - not adopted'}")


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--cache", help="npz for the realistic matrix with the shapes appended")
    ap.add_argument("--seeds", type=int, nargs="+", default=[0, 1, 2, 3, 4])
    ap.add_argument("--ibm", help="HI-Small_Trans.csv: ask the second gate instead")
    ap.add_argument("--ibm-cache", help="the matrix ibm_aml_adapter.py --extract-only cached")
    args = ap.parse_args()
    if args.ibm:
        return ibm(args.ibm, args.ibm_cache, args.seeds)
    return realistic(args.cache, args.seeds)


if __name__ == "__main__":
    main()
