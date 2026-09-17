"""Does link_history, as built, keep what the offline shapes measured?

Step 1 of the multi-day shapes (ml/README.md) is in the stream as the link_history
capability: five of the seven offline columns - the distinct payers and payees of
both parties, and money going straight back, over the 96 hours before a transfer.
Two things are asked before it is switched on.

Parity - the deployed extractor's columns against the offline ones, row for row:
  realistic profile, through ml/dataset.py: every one of the existing columns
  unchanged, and the three keyed by card on both sides exactly (the payees, and
  money back). The payers are counted by person in the store and by card offline,
  so they are reported, not required;
  PaySim, through validation/harness.py's extractor: all five, exactly.
A parity failure stops the run.

Value - the earlier gates, applied to the five columns, fixed before the run:
  1. realistic profile, through the deployed extractor, train.py's split, five
     seeds: the validation paired PR-AUC difference has a mean at or above zero and
     a 95% interval lower bound above -0.01, and the averaged committee's validation
     PR-AUC is not lower;
  2. PaySim, through the deployed extractor, the published split's first 24 days
     (fit on the earliest 80%, validate on the rest), unweighted, five seeds: the
     validation paired PR-AUC difference is not below zero on the mean, and the
     averaged committee finds at least as much fraud in the top 0.1% of the
     validation transfers;
  3. IBM AML, on the offline columns - parity on PaySim, keyed by account as IBM AML
     is, stands in for its 5.5-hour replay - the published split, unweighted, five
     seeds: the same two conditions as PaySim.
All three pass: link_history is switched on and the served model retrained. Any
fails: it stays off, and the verdict is written into ml/README.md.

    python experiments/links.py --realistic-cache links_realistic.npz --realistic-shapes shapes_realistic.npz --paysim ../validation/PS_20174392719_1491204439457_log.csv --ibm ../validation/HI-Small_Trans.csv --ibm-cache ../validation/ibm_features.npz
"""
import argparse
import os
import sys

os.environ["CAP_LINK_HISTORY"] = "on"      # before the registry is imported

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import shapes as SH  # noqa: E402

D, T = SH.D, SH.T
#: Each built column, and the offline column (shapes.SHAPES index) it was measured as.
OFFLINE = {"payee_payers_96h": 0, "sender_payees_96h": 1, "sender_payers_96h": 2,
           "payee_payees_96h": 3, "money_back_96h": 4}
LINKS = tuple(OFFLINE)


def _differ(a, b):
    return int(np.sum(~((a == b) | (np.isnan(a) & np.isnan(b)))))


def _parity(title, deployed, offline, required):
    ok = True
    print(f"parity, {title}:")
    for name in LINKS:
        diff = _differ(deployed[name], offline[:, OFFLINE[name]])
        need = name in required
        print(f"  {name:<20}{diff:>10,} rows differ{'   (required: 0)' if need else ''}")
        ok = ok and (diff == 0 or not need)
    return ok


def _strict_gate(title, X, y, base, fit, cut, seeds):
    dv, _, res = SH._compare(X, y, base, fit, cut, seeds, 1.0)
    yva = y[fit:cut]
    top = {k: SH._top_share(yva, np.mean(va, axis=0), 0.001) for k, (va, _) in res.items()}
    print(f"  top 0.1% of the validation transfers: without {top['without']:.1%}, "
          f"with {top['with']:.1%}")
    return dv.mean() >= 0 and top["with"] >= top["without"]


def realistic(cache, shapes_cache, seeds):
    if cache and os.path.exists(cache):
        z = np.load(cache, allow_pickle=False)
        X, y, types, names = z["X"], z["y"], z["types"], [str(n) for n in z["names"]]
    else:
        df = D.build_matrix(T.CSV, age_unknown=T.age_unknown_rows)
        names = list(D.FEATURE_NAMES)
        X = df[names].astype("float32").values
        y = df.label.values.astype("int8")
        types = df.fraud_type.astype(str).values.astype("U")
        if cache:
            np.savez(cache, X=X, y=y, types=types, names=np.array(names))
    offline = np.load(shapes_cache, allow_pickle=False)["X"]
    base = [i for i, n in enumerate(names) if n not in LINKS]
    changed = _differ(X[:, base], offline[:, :len(base)])
    print(f"parity, realistic profile: {changed:,} cells differ across the {len(base)} "
          f"existing columns (required: 0)")
    ok = _parity("realistic profile", {n: X[:, names.index(n)] for n in LINKS},
                 offline[:, len(base):],
                 required=("sender_payees_96h", "payee_payees_96h", "money_back_96h"))
    if changed or not ok:
        raise SystemExit("parity failed on the realistic profile - nothing is measured")
    n = len(y)
    cut = T.cut_index(n)
    fit = int(cut * T.FIT_SHARE)
    spw = T.class_weight(int(y[:fit].sum()), int((y[:fit] == 0).sum()))
    dv, committee_ok, _ = SH._compare(X, y, base, fit, cut, seeds, spw, types)
    return dv.mean() >= 0 and dv.mean() - SH._ci95(dv) > -0.01 and committee_ok


def paysim(path, seeds):
    sys.path.insert(0, os.path.join(os.path.dirname(SH._PKG), "validation"))
    import paysim_adapter as P
    P.RP.capability_profile("receiver_age", "myid_kinship", "device_telemetry",
                            "geo_telemetry", "session_telemetry")
    df = pd.read_csv(path).sort_values("step", kind="stable").reset_index(drop=True)
    keep, names = P.RP.available_features()
    Xb, y, _ = P.RP.extract_features(P.to_events(df, P.scale_factor(df.amount)),
                                     total=len(df))
    X, y = Xb[:, keep], np.asarray(y).astype("int8")
    ids, _ = pd.factorize(np.concatenate([df.nameOrig.values, df.nameDest.values]))
    S = SH.shape_features(df.step.values.astype("int64") * 3600, ids[:len(df)], ids[len(df):])
    if not _parity("PaySim", {n: X[:, names.index(n)] for n in LINKS}, S, required=LINKS):
        raise SystemExit("parity failed on PaySim - nothing below is measured")
    base = [i for i, n in enumerate(names) if n not in LINKS]
    cut = int((df.step.values <= P.BASELINE["cut_step"]).sum())
    return _strict_gate("PaySim", X, y, base, int(cut * 0.8), cut, seeds)


def ibm(path, cache, seeds):
    base, S, y = SH.ibm_matrix(path, cache)
    n = len(y)
    return _strict_gate("IBM AML", np.hstack([base, S[:, :5]]), y,
                        list(range(base.shape[1])), int(n * 0.6), int(n * 0.8), seeds)


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--realistic-cache", help="npz for the deployed realistic matrix")
    ap.add_argument("--realistic-shapes", required=True,
                    help="the matrix experiments/shapes.py --cache wrote")
    ap.add_argument("--paysim", required=True, help="PaySim's CSV")
    ap.add_argument("--ibm", required=True, help="HI-Small_Trans.csv")
    ap.add_argument("--ibm-cache", required=True,
                    help="the matrix ibm_aml_adapter.py --extract-only cached")
    ap.add_argument("--seeds", type=int, nargs="+", default=[0, 1, 2, 3, 4])
    args = ap.parse_args()
    results = {"realistic profile": realistic(args.realistic_cache, args.realistic_shapes,
                                              args.seeds)}
    results["PaySim"] = paysim(args.paysim, args.seeds)
    results["IBM AML"] = ibm(args.ibm, args.ibm_cache, args.seeds)
    print()
    print("verdict, as fixed in the module docstring before the run:")
    for k, v in results.items():
        print(f"  {k:<18}{'pass' if v else 'FAIL'}")
    print("  link_history " + ("is SWITCHED ON - retrain the served model"
                               if all(results.values()) else "stays OFF"))


if __name__ == "__main__":
    main()
