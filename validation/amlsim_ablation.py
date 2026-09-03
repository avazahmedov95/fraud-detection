"""Ablation over the AMLSim features, plus the two screens the run produced:
leakage_screen() and drift_screen(). Negative and structural: validation/README.md 3.
"""

import argparse
import math
import os
import sys
from collections import defaultdict, deque

import numpy as np
import pandas as pd

# Windows in DAYS. The deployed rule uses one hour, but the adapter run put the
# pattern at a median 363 days, so a single horizon would decide the answer by
# assumption.
WINDOWS_D = (1, 7, 30, 90)

RECEIVER_COLS = ([f"rcv_senders_{w}d" for w in WINDOWS_D]
                 + [f"rcv_inflow_{w}d" for w in WINDOWS_D]
                 + [f"rcv_txcount_{w}d" for w in WINDOWS_D])


def _truthy(s):
    return s.astype(str).str.lower().isin(("true", "1", "t", "yes"))


def build_features(dirpath):
    """One row per transaction in time order, both blocks from a single stream pass.
    Mirrors the SHAPE of stream-processor/features.py rather than importing it: that
    extractor is fixed to this project's contract and its one-hour window."""
    tx = pd.read_csv(os.path.join(dirpath, "transactions.csv"))
    acct = pd.read_csv(os.path.join(dirpath, "accounts.csv"))
    alert_path = os.path.join(dirpath, "alert_transactions.csv")
    alerts = pd.read_csv(alert_path) if os.path.exists(alert_path) else None

    ts = pd.to_datetime(tx["tran_timestamp"], errors="coerce")
    tx = tx.assign(ts=(ts.astype("int64") // 10**9)).sort_values("ts")
    tx = tx.reset_index(drop=True)

    opened = pd.to_datetime(acct.set_index("acct_id")["open_dt"], errors="coerce")
    opened_s = (opened.astype("int64") // 10**9).to_dict()

    typ = {}
    if alerts is not None and "alert_type" in alerts.columns:
        typ = dict(zip(alerts["tran_id"], alerts["alert_type"]))

    max_w = max(WINDOWS_D) * 86400
    rcv_hist = defaultdict(deque)      # receiver -> (ts, sender, amount)
    snd_hist = defaultdict(deque)      # sender   -> (ts, receiver, amount)
    snd_payees = defaultdict(set)
    snd_n = defaultdict(int)
    snd_mean = defaultdict(float)
    snd_m2 = defaultdict(float)

    rows = []
    for tid, t, orig, bene, amt in zip(
            tx["tran_id"], tx["ts"], tx["orig_acct"], tx["bene_acct"],
            tx["base_amt"].astype(float)):

        rh, sh = rcv_hist[bene], snd_hist[orig]
        while rh and rh[0][0] < t - max_w:
            rh.popleft()
        while sh and sh[0][0] < t - max_w:
            sh.popleft()

        feat = {}
        for w in WINDOWS_D:
            cut = t - w * 86400
            senders, inflow, n = set(), 0.0, 0
            for (ts_i, s_i, a_i) in reversed(rh):
                if ts_i < cut:
                    break
                senders.add(s_i); inflow += a_i; n += 1
            feat[f"rcv_senders_{w}d"] = len(senders)
            feat[f"rcv_inflow_{w}d"] = inflow
            feat[f"rcv_txcount_{w}d"] = n

        feat["log_amount"] = math.log1p(amt)
        feat["snd_tx_30d"] = sum(1 for (ts_i, _, _) in reversed(sh)
                                 if ts_i >= t - 30 * 86400)
        feat["snd_payees_all"] = len(snd_payees[orig])
        feat["is_new_payee"] = int(bene not in snd_payees[orig])
        feat["secs_since_last_snd"] = (t - sh[-1][0]) if sh else -1.0
        n_s, mean_s, m2_s = snd_n[orig], snd_mean[orig], snd_m2[orig]
        sd = math.sqrt(m2_s / (n_s - 1)) if n_s > 1 else 0.0
        feat["amount_z"] = ((amt - mean_s) / sd) if sd > 0 else 0.0
        o = opened_s.get(bene)
        feat["receiver_age_days"] = ((t - o) / 86400.0) if o is not None and not pd.isna(o) else -1.0

        rows.append(feat)

        # state update AFTER extraction, as the deployed extractor does
        rh.append((t, orig, amt)); sh.append((t, bene, amt))
        snd_payees[orig].add(bene)
        snd_n[orig] = n_s + 1
        d = amt - mean_s
        snd_mean[orig] = mean_s + d / (n_s + 1)
        snd_m2[orig] = m2_s + d * (amt - snd_mean[orig])

    X = pd.DataFrame(rows)
    X["_y"] = _truthy(tx["is_sar"]).astype(int).values
    X["_ts"] = tx["ts"].values
    X["_typ"] = [typ.get(i, "") for i in tx["tran_id"]]
    return X


def univariate_auc(X, y, col):
    """Rank AUC of one feature against the label: does one column nearly determine it?"""
    r = X[col].rank().values
    n1, n0 = int(y.sum()), int((1 - y).sum())
    if n1 == 0 or n0 == 0:
        return float("nan")
    return (r[y == 1].sum() - n1 * (n1 + 1) / 2) / (n1 * n0)


LEAK_AT = 0.85          # |AUC| beyond this is a near-deterministic separator


def leakage_screen(X, y, cols):
    """Print per-feature separation and flag construction artefacts.

    The first run of this script reported PR-AUC 0.88 for the arm WITHOUT receiver
    features at a 0.339% positive rate; the cause was `is_new_payee` at AUC 0.906.
    AMLSim injects each alert as fresh graph edges while ordinary traffic reuses
    established ones, so 97% of SAR rows are to a never-before-paid payee against 16% of
    legitimate ones - planted, not behavioural. ml/README.md records the same failure in
    this project's OWN generator: `is_family` ranked #1 by SHAP, an artefact of a
    generator that sent no fraud to relatives.
    """
    scored = sorted(((c, univariate_auc(X, y, c)) for c in cols),
                    key=lambda t: -abs(t[1] - 0.5))
    flagged = [c for c, a in scored if abs(a - 0.5) >= (LEAK_AT - 0.5)]
    print("    univariate separation (rank AUC; 0.5 = none, 0 and 1 = perfect)")
    for c, a in scored[:6]:
        mark = "  <-- NEAR-DETERMINISTIC" if abs(a - 0.5) >= (LEAK_AT - 0.5) else ""
        print(f"      {c:<22}{a:>7.3f}{mark}")
    if flagged:
        print()
        print(f"    !! {len(flagged)} feature(s) separate the classes almost perfectly:")
        print(f"       {', '.join(flagged)}")
        print("       On synthetic data that is usually how the generator PLANTED the")
        print("       positives, not how the positives BEHAVE. An ablation measured on")
        print("       top of such a feature answers a different question than the one")
        print("       asked. Re-run with --drop to exclude them, and report both.")
    print()
    return flagged


def drift_screen(X, y, cols, deciles=10):
    """Are the features - and the label - stationary across the run?

    A time-ordered split is the right protocol and is exactly what turns non-stationarity
    into a measurement error: the model trains on one regime and is scored on another.
    Found the expensive way. On the AMLSim 10K profile `rcv_txcount_90d` runs
    67 -> 199 -> 102 across time deciles while the SAR rate runs 0.54% -> 0.28%, and the
    ablation delta swung from +0.069 to -0.336 on a change of bagging parameters alone.
    """
    d = pd.qcut(X["_ts"], deciles, labels=False, duplicates="drop")
    rate = pd.Series(y).groupby(d).mean()
    print("    stationarity across time deciles")
    print(f"      SAR rate      first {rate.iloc[0]:.4%}   last {rate.iloc[-1]:.4%}"
          f"   ratio {rate.iloc[0] / max(rate.iloc[-1], 1e-12):.1f}x")
    worst = []
    for c in cols:
        m = X[c].groupby(d).mean()
        lo, hi = m.min(), m.max()
        if abs(lo) + abs(hi) > 0:
            worst.append((abs(hi - lo) / (abs(m.mean()) + 1e-9), c, m.iloc[0], m.max(), m.iloc[-1]))
    worst.sort(reverse=True)
    for swing, c, a, mx, z in worst[:4]:
        print(f"      {c:<22} {a:>9.1f} -> {mx:>9.1f} -> {z:>9.1f}   swing {swing:.1f}x of mean")
    if worst and worst[0][0] > 1.0:
        print()
        print("    !! Features drift by more than their own mean across the run.")
        print("       With a time-ordered split the model is trained in one regime and")
        print("       scored in another, so the delta measures the simulation's shape")
        print("       as much as the feature's value. Fixing this by switching to a")
        print("       random split would trade a visible error for an invisible one.")
    print()


def fit_once(X, y, cols, seed):
    """PR-AUC on a time-ordered held-out tail. No tuning, no early stopping on
    the test slice, identical settings in both arms."""
    from lightgbm import LGBMClassifier
    from sklearn.metrics import average_precision_score

    cut = int(len(X) * 0.7)
    Xtr, Xte = X.iloc[:cut][cols], X.iloc[cut:][cols]
    ytr, yte = y[:cut], y[cut:]
    if ytr.sum() == 0 or yte.sum() == 0:
        return float("nan")
    pos_w = float((len(ytr) - ytr.sum()) / max(ytr.sum(), 1))
    # subsample/colsample are ON so that `seed` perturbs something. Without them LightGBM
    # is deterministic given the data, every seed returns the identical model, and the
    # "95% CI" over seeds comes out with ZERO width - which reads as a very tight interval
    # and is in fact no measurement at all. Observed on the first run of this script.
    m = LGBMClassifier(n_estimators=300, learning_rate=0.05, num_leaves=31,
                       subsample=0.8, subsample_freq=1, colsample_bytree=0.8,
                       scale_pos_weight=pos_w, random_state=seed,
                       n_jobs=-1, verbose=-1)
    m.fit(Xtr, ytr)
    p = m.predict_proba(Xte)[:, 1]
    return average_precision_score(yte, p), m, Xte, yte, p


def run_dataset(dirpath, seeds, drop=()):
    X = build_features(dirpath)
    y = X["_y"].values
    all_cols = [c for c in X.columns if not c.startswith("_") and c not in drop]
    base_cols = [c for c in all_cols if c not in RECEIVER_COLS]

    name = os.path.basename(os.path.normpath(dirpath))
    print(f"  {name:<20} "
          f"{len(X):>8,} tx  {int(y.sum()):>5,} SAR ({y.mean():.3%})  "
          f"{len(all_cols)} features, {len(RECEIVER_COLS)} of them receiver-side")

    # Direction, printed on every run because it is most easily assumed. The deployed rule
    # can only read this feature one way - MORE senders is MORE suspicious - and on this
    # dataset the relationship runs the other way at every horizon.
    print(f"    {'window':<10}{'mean, SAR':>12}{'mean, legit':>14}   direction")
    for w in WINDOWS_D:
        c = f"rcv_senders_{w}d"
        a, b = X.loc[y == 1, c].mean(), X.loc[y == 0, c].mean()
        arrow = "SAR higher" if a > b else "SAR LOWER  <- opposite to the rule"
        print(f"    {str(w) + 'd':<10}{a:>12.2f}{b:>14.2f}   {arrow}")
    print()

    leakage_screen(X, y, all_cols)
    drift_screen(X, y, all_cols)

    out = []
    for s in seeds:
        with_r = fit_once(X, y, all_cols, s)[0]
        without = fit_once(X, y, base_cols, s)[0]
        out.append((with_r, without, with_r - without))
    return X, out


def report(results, seeds, n_dirs):
    deltas = [d for _, _, d in results if not math.isnan(d)]
    withs = [a for a, _, _ in results if not math.isnan(a)]
    withouts = [b for _, b, _ in results if not math.isnan(b)]
    if not deltas:
        print("\nNo usable folds - too few positives in a split.")
        return

    n = len(deltas)
    mean = float(np.mean(deltas))
    sem = float(np.std(deltas, ddof=1) / math.sqrt(n)) if n > 1 else float("nan")
    lo, hi = (mean - 1.96 * sem, mean + 1.96 * sem) if n > 1 else (float("nan"),) * 2
    pos = sum(1 for d in deltas if d > 0)

    print("\n" + "=" * 72)
    print("ABLATION: receiver-side features, added to an otherwise identical model")
    print("=" * 72)
    print(f"  PR-AUC with receiver block   : {np.mean(withs):.4f}")
    print(f"  PR-AUC without               : {np.mean(withouts):.4f}")
    print(f"  delta (mean over {n:>2} fits)     : {mean:+.4f}"
          + (f"  95% CI [{lo:+.4f}, {hi:+.4f}]" if n > 1 else ""))
    print(f"  sign consistency             : {pos}/{n} positive")

    print("\n  Reading it:")
    if n > 1 and lo > 0:
        print("  The interval excludes zero and the sign is stable. Receiver-side")
        print("  aggregation is informative on data this project did not produce,")
        print("  even though the DEPLOYED RULE inverted on the same data.")
        print("  Read that together with the direction table above: if the feature")
        print("  helps a model while running OPPOSITE to how the rule reads it,")
        print("  what transfers is the QUANTITY, and what does not transfer is the")
        print("  SIGN. A hand-written rule fixes the direction; a model does not,")
        print("  and that is the operational difference, not a detail of tuning.")
    elif n > 1 and hi < 0:
        print("  The interval excludes zero with the WRONG SIGN. Adding the block")
        print("  makes the model worse, which no story about thresholds explains.")
        print("  This would be evidence against the idea, not against the rule.")
    else:
        print("  The interval spans zero. Nothing is established either way. Do not")
        print("  report this as support: an interval containing zero is the shape a")
        print("  null result has, and this project has twice been wrong by treating")
        print("  a single unresolved measurement as a finding.")

    print("\n  What this number is NOT:")
    print("  - not comparable in magnitude to the -0.032 measured on our own")
    print("    generator. Different data, different label (laundering typologies,")
    print("    not customer fraud), different feature set, different baseline.")
    print("  - not a system evaluation. A throwaway model, fitted to answer one")
    print("    question, is not the deployed pipeline.")

    if n_dirs < 2:
        print("\n  !! SEED CAVEAT - read before quoting the interval.")
        print("  All fits come from ONE AMLSim simulation, so the seeds vary only")
        print("  the model's randomness, not the data. ml/README.md records this")
        print("  project getting the same thing wrong once already: a single-dataset")
        print("  sweep put receiver_age at -0.043 against an honest -0.025, and it")
        print("  took 20 generator seeds to establish. The interval printed above is")
        print("  therefore NARROWER THAN IT SHOULD BE. Generate several AMLSim runs")
        print("  with different random_seed in conf.json and pass --dirs to fix it.")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", help="one AMLSim outputs/<name> directory")
    ap.add_argument("--dirs", nargs="+", help="several, ideally different seeds")
    ap.add_argument("--seeds", type=int, default=5, help="model seeds per dataset")
    ap.add_argument("--drop", nargs="*", default=[],
                    help="feature names to exclude, e.g. --drop is_new_payee. "
                         "Use for columns the leakage screen flags; report the "
                         "run with and without, never only the flattering one.")
    ap.add_argument("--by-typology", action="store_true",
                    help="also break held-out recall down by alert_type")
    args = ap.parse_args()

    dirs = args.dirs or ([args.dir] if args.dir else None)
    if not dirs:
        raise SystemExit("pass --dir or --dirs")
    for d in dirs:
        if not os.path.isdir(d):
            raise SystemExit(f"{d} is not a directory")

    print(f"windows (days): {WINDOWS_D}")
    print(f"receiver-side features under test: {len(RECEIVER_COLS)}")
    if args.drop:
        print(f"excluded by --drop: {', '.join(args.drop)}")
    print()
    print("datasets:")
    seeds = list(range(args.seeds))
    results, last_X = [], None
    for d in dirs:
        last_X, res = run_dataset(d, seeds, drop=set(args.drop))
        results.extend(res)

    report(results, seeds, len(dirs))

    if args.by_typology and last_X is not None:
        y = last_X["_y"].values
        cols = [c for c in last_X.columns if not c.startswith("_")]
        _, _, Xte, yte, p = fit_once(last_X, y, cols, 0)
        te = last_X.iloc[len(last_X) - len(Xte):]
        thr = np.quantile(p, 1 - 0.0314)   # same alert budget the rules spent
        print("\n" + "=" * 72)
        print("HELD-OUT RECALL BY TYPOLOGY, at the alert rate the CEP layer spent")
        print("=" * 72)
        print("  The rules flagged 3.14% of traffic and caught 0.3% of SAR. Holding")
        print("  the alert budget fixed makes the two comparable.\n")
        flagged = p >= thr
        for t, idx in te.groupby("_typ").groups.items():
            if not t:
                continue
            m = te.index.isin(idx)
            if m.sum():
                print(f"  {t:<12} n={int(m.sum()):>6,}  recall {flagged[m].mean():>6.1%}")


if __name__ == "__main__":
    main()
