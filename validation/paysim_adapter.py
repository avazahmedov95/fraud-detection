"""Replays the deployed rules over PaySim, mapping its schema onto the event shape
features.py expects. What transfers and what does not: validation/README.md 2.

Everything downstream of a translated event - the unit conversion, the replay, the
report sections - is in harness.py, shared with the other two adapters. The second
half of this file is not shared and should not be: fitting a model on foreign data
answers a different question, and only PaySim has a published baseline to fit
against.
"""

import argparse
import os
import time

import pandas as pd

import harness as RP
from harness import CAP, Event, scale_factor      # noqa: F401  (scale_factor: tests)


def to_events(df, scale):
    """PaySim rows -> translated events.

    Only fields PaySim actually has are populated; the rest are left absent and their
    capabilities switched off (see main), so nothing fires on a fabricated value.
    """
    for r in df.itertuples(index=False):
        yield Event(
            ev={"amount_uzs": float(r.amount) * scale,
                "sender_pinfl": r.nameOrig,
                "receiver_pinfl": r.nameDest},
            # `step` is PaySim's hour index (1..744 over 30 days) and the sharpest
            # limitation of this cross-check: same-step events share a timestamp, so
            # the 10-minute and 1-hour windows see nearly the same set and
            # secs_since_last is 0. Sub-hour patterns are invisible here - a property
            # of PaySim, not of the rules.
            ts=int(r.step) * 3600,
            label=int(r.isFraud))


def run(path, txn_types, limit):
    df = pd.read_csv(path)
    if txn_types:
        df = df[df.type.isin(txn_types)]
    if limit:
        df = df.head(limit)
    df = df.sort_values("step").reset_index(drop=True)

    scale = scale_factor(df.amount)
    print(f"{len(df):,} PaySim rows, types {sorted(df.type.unique())}")
    print(f"fraud: {df.isFraud.sum():,} ({df.isFraud.mean():.3%})")
    print(f"amount scale factor: {scale:.1f}x "
          f"(median {df.amount.median():,.0f} -> {df.amount.median()*scale:,.0f})\n")

    return RP.replay(to_events(df, scale))


def report(res, hits):
    RP.section_lift(res, hits, positive="fraud", width=70)
    print()
    RP.section_decision(res, hits, positive="fraud", width=70)


#: The published baseline this project calibrates its own PR-AUC against, and the
#: split it was measured on. 24 days train / 7 days test is a cut at PaySim's
#: hourly step 576; that reproduces their stated rates (train 0.103%, test
#: 1.142%, 1,854 holdout frauds) closely enough to confirm it is the split.
BASELINE = dict(source="ris3abh/aml-p2p-fraud-detection (MIT)", cut_step=576,
                pr_auc=0.380, roc_auc=0.908, recall_at_2pct=0.494,
                lift_at_decile=7.0, pr_auc_with_leakage=0.988)


def _fit_report(name, X, y, step, cut, drop=(), names=None, weighted=True):
    """Train and score one configuration on the published split.

    `weighted` is not a tuning knob; it is what makes the rows comparable at all.
    train.py sets scale_pos_weight from the class ratio, which at PaySim's 0.129%
    prevalence is ~974, and heavy positive weighting flattens the top of the
    ranking - exactly what AUPRC reads. The published baseline was fitted
    unweighted. Quoting only the weighted row beside it would compare this
    project's TRAINING RECIPE against their FEATURE SET and report the difference
    as though it were about features.
    """
    import numpy as np
    import lightgbm as lgb
    from sklearn.metrics import roc_auc_score, average_precision_score

    keep = [i for i, n in enumerate(names) if n not in drop]
    Xk = X[:, keep]
    tr, te = step <= cut, step > cut
    ytr, yte = y[tr], y[te]
    spw = ((ytr == 0).sum() / max(int(ytr.sum()), 1)) if weighted else 1.0
    m = lgb.LGBMClassifier(n_estimators=400, learning_rate=0.05, num_leaves=31,
                           subsample=0.8, colsample_bytree=0.8,
                           min_child_samples=30, scale_pos_weight=spw,
                           random_state=42, n_jobs=-1, verbose=-1)
    m.fit(Xk[tr], ytr)
    p = m.predict_proba(Xk[te])[:, 1]
    order = np.argsort(-p)
    k = int(0.02 * len(yte))
    rec2 = yte[order[:k]].sum() / max(1, yte.sum())
    lift = yte[order[:len(yte) // 10]].mean() / yte.mean()
    ap = average_precision_score(yte, p)
    print(f"  {name:<34}{len(keep):>4}{ap:>9.3f}{roc_auc_score(yte, p):>10.3f}"
          f"{rec2:>11.1%}{lift:>9.1f}x")
    return ap


def our_model(path, limit=None):
    """This project's own feature extractor and model, trained on PaySim.

    The question the rules replay above cannot answer: not "do the rules fire on
    foreign data" but "how does this system score on it". 14 of the 20 features
    survive - PaySim carries identifiers on both sides, so the per-sender history
    and the receiver-side aggregation both compute; what it cannot supply is
    device, geo, session, channel, receiver age and kinship, and those
    capabilities are switched off rather than defaulted.

    Trained on the SAME split as the published baseline (24 days / 7 days, a cut
    at step 576), so the numbers sit beside `--baseline` on one axis.

    The second configuration is the point of running this at all. Receiver-side
    aggregation is this project's largest measured effect and the one with no
    external evidence, because the only foreign dataset run so far could not
    express the pattern. Dropping those two features here asks whether the
    finding reproduces off this project's own generator - the one legitimate
    training use of foreign data, per validation/README.md 1.
    """
    import numpy as np
    from collections import defaultdict
    sys.path.insert(0, _SP)
    import features as F                       # noqa: E402
    from rules import SenderState, ReceiverState   # noqa: E402

    df = pd.read_csv(path)
    if limit:
        df = df.head(limit)
    df = df.sort_values("step", kind="stable").reset_index(drop=True)
    scale = scale_factor(df.amount)

    # features.FEATURE_NAMES is fixed at IMPORT from the capability registry, so
    # switching modes in main() cannot shrink it - the contract was already built.
    # Rather than require the caller to get environment variables right, extract
    # the full 24 and drop the columns whose capability is unavailable here. Those
    # columns are computed from data PaySim does not carry, so what is dropped is
    # exactly what would otherwise be a fabricated zero.
    off = {f for cap in CAP.REGISTRY if CAP.MODES.get(cap.key) == "off"
           for f in cap.features}
    # Intersect with the contract: a capability that was ALREADY off at import
    # (myid_kinship, by default) contributed no column to drop, and listing it
    # as dropped would make the arithmetic in the line below not add up.
    unavailable = off & set(F.FEATURE_NAMES)
    names = [n for n in F.FEATURE_NAMES if n not in unavailable]
    keep_idx = [i for i, n in enumerate(F.FEATURE_NAMES) if n not in unavailable]

    print(f"{len(df):,} PaySim rows, {int(df.isFraud.sum()):,} fraud "
          f"({df.isFraud.mean():.3%})")
    print(f"features this project can compute here: {len(names)} of "
          f"{len(F.FEATURE_NAMES)}")
    print(f"  kept    : {', '.join(names)}")
    print(f"  dropped : {', '.join(sorted(unavailable))}\n")

    X = np.zeros((len(df), len(F.FEATURE_NAMES)), dtype="float32")
    y = df.isFraud.values.astype("int8")
    step = df.step.values
    senders, receivers = defaultdict(SenderState), defaultdict(ReceiverState)
    t0 = time.time()
    for i, ev in enumerate(to_events(df, scale)):
        ev.pop("_label"); ts = ev.pop("_ts"); ev.pop("_type")
        key = F.payee_key(ev)
        sender = senders[ev["sender_pinfl"]]
        X[i] = F.to_vector(F.extract(ev, None, sender, ts, receivers[key]))
        F.update_state(sender, ev, ts)
        F.update_receiver_state(receivers[key], ev, ts)
        if i and i % 500_000 == 0:
            print(f"  ... {i:,} rows ({time.time()-t0:.0f}s)", flush=True)
    print(f"  extracted {len(df):,} rows in {time.time()-t0:.0f}s\n")
    X = X[:, keep_idx]

    cut = BASELINE["cut_step"]
    if not (step > cut).any():
        raise SystemExit(
            f"no rows past step {cut}. PaySim is ordered by step, so a "
            f"--limit keeps only the training side of the published "
            f"split and leaves nothing to score. Run without --limit.")
    # PaySim puts ALL of its fraud in TRANSFER and CASH_OUT, so the transaction
    # type carries much of the available signal - and this project's contract has
    # no column for it. The channel one-hots are the nearest thing and PaySim has
    # no channels, so they were dropped. Adding PaySim's own type tests whether
    # the gap is the feature SET, rather than asserting it.
    types = np.zeros((len(df), 5), dtype="float32")
    tnames = ["CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER"]
    for j, t in enumerate(tnames):
        types[:, j] = (df.type.values == t).astype("float32")
    Xt = np.hstack([X, types])
    tn = names + [f"type_{t}" for t in tnames]
    RCV = ("rcv_distinct_senders_1h", "rcv_inflow_1h")

    print(f"  {'configuration':<34}{'feat':>4}{'PR-AUC':>9}{'ROC-AUC':>10}"
          f"{'rec@2%':>11}{'lift':>9}")
    print("  -- scale_pos_weight from the class ratio, as train.py fits it --")
    full = _fit_report("this project, all it can compute", X, y, step, cut,
                       names=names)
    less = _fit_report("  without receiver aggregation", X, y, step, cut,
                       drop=RCV, names=names)
    _fit_report("  + PaySim's own transaction type", Xt, y, step, cut, names=tn)

    print("  -- unweighted, as the published baseline was fitted --")
    full_u = _fit_report("this project, all it can compute", X, y, step, cut,
                         names=names, weighted=False)
    less_u = _fit_report("  without receiver aggregation", X, y, step, cut,
                         drop=RCV, names=names, weighted=False)
    _fit_report("  + PaySim's own transaction type", Xt, y, step, cut, names=tn,
                weighted=False)
    print(f"  {'published generic baseline':<34}{'~24':>4}"
          f"{BASELINE['pr_auc']:>9.3f}{BASELINE['roc_auc']:>10.3f}"
          f"{BASELINE['recall_at_2pct']:>11.1%}"
          f"{BASELINE['lift_at_decile']:>9.1f}x")

    print(f"\n  receiver aggregation on PaySim: {less - full:+.3f} PR-AUC weighted, "
          f"{less_u - full_u:+.3f} unweighted")
    print("  On this project's own data the same removal costs -0.040, the")
    print("  largest effect in the ablation. Whether the sign and rough size")
    print("  survive on a dataset this project did not produce is the whole")
    print("  reason for the run - and PaySim drains one account straight to")
    print("  cash-out with no collection stage, so a null here is a fact about")
    print("  PaySim's fraud model, not a refutation. See validation/README.md 1.")


def baseline(path):
    """Retrain the published PaySim baseline, because the file to do it with is
    already here.

    related-work.md 6 quoted AUPRC 0.380 for a year with "not independently
    reproduced here" attached, while the full 6.36M-row log sat in this
    directory. Two things come out of actually running it: the figure is real,
    and the prevalence it was quoted against was not - 0.380 belongs to a holdout
    slice at 1.142%, not to PaySim's 0.129% overall, and AUPRC's floor IS the
    prevalence.

    The balance columns are excluded, which is what their leakage removal did.
    Left in they do not merely help, they ARE the label: same split, same model,
    PR-AUC 1.000.
    """
    import numpy as np
    import lightgbm as lgb
    from sklearn.metrics import roc_auc_score, average_precision_score

    df = pd.read_csv(path).sort_values("step", kind="stable").reset_index(drop=True)
    X = pd.DataFrame(index=df.index)
    X["amount"] = df.amount.values
    X["log_amount"] = np.log1p(df.amount.values)
    X["hour"] = df.step.values % 24
    for t in ["CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER"]:
        X[f"type_{t}"] = (df.type.values == t).astype("int8")
    X["dest_is_merchant"] = df.nameDest.str.startswith("M").astype("int8")
    # `step` itself is deliberately not a feature: across a temporal split the
    # test steps are all unseen, and across a random one it encodes the fraud
    # concentration in PaySim's tail.

    y = df.isFraud.values
    tr = df.step.values <= BASELINE["cut_step"]
    te = ~tr
    print(f"train {int(tr.sum()):,} rows {y[tr].mean():.4%} fraud")
    print(f"test  {int(te.sum()):,} rows {y[te].mean():.4%} fraud "
          f"({int(y[te].sum()):,}) - reported 1.142%, 1,854\n")

    m = lgb.LGBMClassifier(n_estimators=400, learning_rate=0.05, num_leaves=63,
                           subsample=0.8, colsample_bytree=0.8,
                           min_child_samples=20, random_state=42, n_jobs=-1,
                           verbose=-1)
    m.fit(X[tr].values.astype("float32"), y[tr])
    p = m.predict_proba(X[te].values.astype("float32"))[:, 1]
    yte = y[te]
    order = np.argsort(-p)
    k = int(0.02 * len(yte))
    rec2 = yte[order[:k]].sum() / max(1, yte.sum())
    lift = yte[order[:len(yte) // 10]].mean() / yte.mean()

    print(f"{'':<26}{'reported':>10}{'here':>10}")
    for label, key, got in (
            ("PR-AUC", "pr_auc", average_precision_score(yte, p)),
            ("ROC-AUC", "roc_auc", roc_auc_score(yte, p)),
            ("recall @2% budget", "recall_at_2pct", rec2),
            ("lift @top decile", "lift_at_decile", lift)):
        print(f"  {label:<24}{BASELINE[key]:>10.3f}{got:>10.3f}")
    print(f"\n  source: {BASELINE['source']}")
    print("\n  Their 0.380 is measured at 1.142% prevalence, against this")
    print("  project's 1.23% on its own held-out slice - a matched comparison,")
    print("  which the 0.129% dataset rate this was once quoted against was not.")
    print("  Read 0.966 next to their PRE-leak-removal 0.988, not their 0.380:")
    print("  a PR-AUC in the high nineties is what a broken model scores.")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True, help="PaySim CSV")
    ap.add_argument("--types", default="TRANSFER",
                    help="comma-separated PaySim types; TRANSFER is the P2P "
                         "analogue. CASH_OUT also carries fraud but is a "
                         "withdrawal, not a transfer between two customers.")
    ap.add_argument("--limit", type=int, default=None,
                    help="first N rows (the full file is 6.3M)")
    ap.add_argument("--our-model", dest="our_model", action="store_true",
                    help="train THIS project's feature set and model on PaySim, "
                         "on the published baseline's split - and again without "
                         "the receiver-side features, to see whether the "
                         "largest finding reproduces on foreign data")
    ap.add_argument("--baseline", action="store_true",
                    help="retrain the published PaySim baseline instead of "
                         "replaying the rules - verifies the AUPRC 0.380 that "
                         "related-work.md 6 calibrates against")
    args = ap.parse_args()

    if not os.path.exists(args.file):
        raise SystemExit(f"{args.file} not found")

    if args.our_model:
        # Only what PaySim actually carries; the rest are switched off rather
        # than defaulted, so no feature is computed from a fabricated value.
        RP.capability_profile("receiver_age", "myid_kinship", "device_telemetry",
                              "geo_telemetry", "session_telemetry")
        return our_model(args.file, args.limit)
    if args.baseline:
        return baseline(args.file)

    # PaySim has account ids, amounts and a clock, nothing else this project uses. What
    # is not backed by real data is switched off, not defaulted: nothing fires on a zero.
    RP.capability_profile("receiver_age", "myid_kinship", "device_telemetry",
                          "geo_telemetry", "session_telemetry")

    types = [t.strip() for t in args.types.split(",") if t.strip()]
    res, hits = run(args.file, types, args.limit)
    report(res, hits)

    print("\nWhat this does and does not show:")
    print("  - PaySim is synthetic, so this is not production validation.")
    print("  - It IS an independent generator: these rules were written against")
    print("    a different dataset and are run here unchanged, so a result here")
    print("    is not circular in the way a result on our own data would be.")
    print("  - Hourly timestamps collapse the sub-hour windows (see to_events).")


if __name__ == "__main__":
    main()
