"""Trains LightGBM on a time-ordered split and writes metrics.json.
Calibration is reported beside the AUCs because a rank statistic cannot see a
score that ranks well yet cannot order a queue - docs/irp-framing.md 9.1.
"""

import json
import os

import joblib
import numpy as np
import lightgbm as lgb
from sklearn.metrics import (roc_auc_score, average_precision_score,
                             precision_recall_fscore_support, confusion_matrix)

import dataset as D

# Overridable so a sweep writes elsewhere instead of clobbering the deployed model.
MODELS_DIR = os.getenv(
    "MODELS_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), "models"))
CSV = os.getenv(
    "DATASET_CSV", os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", "data-generator", "out", "transactions.csv"))
REVIEW_THRESHOLD = 0.40   # CEP flag cutoff, for the head-to-head comparison
TRAIN_SHARE = 0.80        # the earliest 80% trains; the rest is the held-out slice

#: Share of TRAINING rows replayed with the payee's age withheld, as the live job
#: sees every event while Neo4j cannot be read. Without such rows the model has no
#: missing branch to learn: LightGBM sends NaN in a feature never missing in
#: training down the 0.0 side of every split - an account opened today - and the
#: -1 this replaced sorted below every real age (docs/irp-framing.md 7.7a).
#: Random, independent of the label, and only before the cut, so the held-out
#: figures still describe a healthy graph; `_graph_outage` measures the other case.
AGE_UNKNOWN_SHARE = 0.10
AGE_UNKNOWN_SEED = 42

#: The class weighting - negatives over positives - is validated on this project's
#: data, at 1.5% fraud (a weight near 65). At the rates real card traffic runs at
#: it collapses: PaySim at 0.13% (weight ~974) fell from PR-AUC 0.267 unweighted to
#: 0.032, IBM AML at 0.10% (~870) from F1 17.5 to 1.5-2.2 (validation/README.md 1,
#: 4). Nothing between 0.13% and 1.5% has been measured. The line sits between
#: them - a guard, not a measured boundary - and below it training stops rather
#: than hand over a collapsed model that looks like any other.
MIN_WEIGHTED_POSITIVE_RATE = 0.005
#: auto: weight above the line, stop below it. off: fit unweighted, the recipe that
#: held on both datasets where the weighted one collapsed. on: weight regardless.
CLASS_WEIGHTING = os.getenv("CLASS_WEIGHTING", "auto")


def cut_index(n):
    return int(n * TRAIN_SHARE)


def age_unknown_rows(n, outage=False, seed=AGE_UNKNOWN_SEED, share=AGE_UNKNOWN_SHARE):
    """Rows replayed with no payee age: a random share of the training slice, and
    with `outage` the whole held-out slice as well - Neo4j down from the cut on."""
    rows = np.random.default_rng(seed).random(n) < share
    rows[cut_index(n):] = outage
    return rows


def class_weight(pos, neg, mode=None):
    """scale_pos_weight for the training slice, or a stop with the reason.

    Below MIN_WEIGHTED_POSITIVE_RATE the default refuses rather than guesses. The
    unweighted alternative is not a drop-in: its probabilities sit near the base
    rate, so the 0.50 cutoff the metrics use flags too little and the decision
    thresholds have to be set again from data - a choice for whoever deploys it.
    """
    mode = CLASS_WEIGHTING if mode is None else mode
    if mode not in ("auto", "on", "off"):
        raise ValueError(f"CLASS_WEIGHTING={mode!r}: expected auto, on or off")
    if mode == "off":
        return 1.0
    rate = pos / max(pos + neg, 1)
    if mode == "auto" and rate < MIN_WEIGHTED_POSITIVE_RATE:
        raise SystemExit(
            f"fraud is {rate:.3%} of the training rows, below the "
            f"{MIN_WEIGHTED_POSITIVE_RATE:.1%} this recipe's class weighting is "
            "trusted at; it collapsed at 0.13% and 0.10% (validation/README.md 1, "
            "4). Rerun with CLASS_WEIGHTING=off to fit unweighted, then set the "
            "decision thresholds from a validation slice - the 0.50 cutoff will "
            "flag too little - or with CLASS_WEIGHTING=on to weight anyway.")
    return neg / max(pos, 1)


def make_model(scale_pos_weight, random_state=42):
    """The deployed recipe, in one place for the experiments that refit it."""
    return lgb.LGBMClassifier(
        n_estimators=400, learning_rate=0.05, num_leaves=31,
        subsample=0.8, colsample_bytree=0.8, min_child_samples=30,
        scale_pos_weight=scale_pos_weight, random_state=random_state,
        n_jobs=-1, verbose=-1)


def _metrics(y, proba, thr):
    pred = (proba >= thr).astype(int)
    p, r, f1, _ = precision_recall_fscore_support(y, pred, average="binary", zero_division=0)
    tn, fp, fn, tp = confusion_matrix(y, pred, labels=[0, 1]).ravel()
    return dict(threshold=thr, precision=p, recall=r, f1=f1,
                tp=int(tp), fp=int(fp), fn=int(fn), tn=int(tn))


def _calibration(y, proba, review_thr=REVIEW_THRESHOLD, block_thr=0.80):
    """How usable the probabilities are AS MAGNITUDES, not just as a ranking.
    The AUCs are rank statistics and hid this: 89% of alerts tied at 1.000, leaving
    arrival order as the only tiebreak when a work queue tried to order by score.
    Not a defect of the method: synthetic fraud is close to separable.
    """
    alert = proba >= review_thr
    n_alert = int(alert.sum())
    pa = proba[alert]
    return dict(
        brier=float(np.mean((proba - y) ** 2)),
        n_alerts=n_alert,
        saturated_share=(float(np.mean(pa >= 0.9995)) if n_alert else None),
        distinct_scores=(int(len(np.unique(np.round(pa, 3)))) if n_alert else 0),
        review_band=int(((pa >= review_thr) & (pa < block_thr)).sum()) if n_alert else 0,
        median_alert_score=(float(np.median(pa)) if n_alert else None),
    )


def _graph_outage(model, feats, yte, healthy_proba):
    """The held-out slice replayed as if Neo4j were down throughout: no payee has an
    age, so FRESH_RECEIVER cannot fire and the model sees the age as missing. A
    dependency that fails open should cost alerts, not add them - under the -1
    encoding it added them (docs/irp-framing.md 7.7a, 7.7c)."""
    df = D.build_matrix(CSV, age_unknown=lambda n: age_unknown_rows(n, outage=True))
    test = df.iloc[cut_index(len(df)):]
    if not np.array_equal(test["label"].values, yte):
        raise RuntimeError("the outage replay does not line up with the held-out slice")
    proba = model.predict_proba(test[feats].astype("float32").values)[:, 1]
    m = _metrics(yte, proba, 0.50)
    return dict(pr_auc=float(average_precision_score(yte, proba)), at_0_50=m,
                alerts_healthy=int((healthy_proba >= 0.50).sum()),
                alerts_outage=m["tp"] + m["fp"])


def main():
    os.makedirs(MODELS_DIR, exist_ok=True)
    print("building feature matrix ...")
    df = D.build_matrix(CSV, age_unknown=age_unknown_rows)
    feats = D.FEATURE_NAMES

    cut = cut_index(len(df))
    train, test = df.iloc[:cut], df.iloc[cut:]
    Xtr, ytr = train[feats].astype("float32").values, train["label"].values
    Xte, yte = test[feats].astype("float32").values, test["label"].values

    pos, neg = int(ytr.sum()), int((ytr == 0).sum())
    spw = class_weight(pos, neg)
    print(f"train {len(ytr):,} (pos={pos}) | test {len(yte):,} (pos={int(yte.sum())}) | scale_pos_weight={spw:.1f}")

    if "receiver_age" in feats:
        print(f"payee age withheld on {int(train['receiver_age'].isna().sum()):,} "
              f"training rows ({AGE_UNKNOWN_SHARE:.0%} drawn, plus any the data lacks)")

    model = make_model(spw)
    model.fit(Xtr, ytr)

    proba = model.predict_proba(Xte)[:, 1]
    auc = roc_auc_score(yte, proba)
    ap = average_precision_score(yte, proba)

    print("\n=== ML model on held-out (later) test slice - DESIGN TARGETS ===")
    print(f"ROC-AUC: {auc:.3f}   PR-AUC (avg precision): {ap:.3f}")
    m05 = _metrics(yte, proba, 0.50)
    print(f"@0.50  precision={m05['precision']:.3f}  recall={m05['recall']:.3f}  "
          f"f1={m05['f1']:.3f}  (tp={m05['tp']} fp={m05['fp']} fn={m05['fn']})")

    best = max((_metrics(yte, proba, t) for t in np.linspace(0.05, 0.95, 19)),
               key=lambda mm: mm["recall"] if mm["precision"] >= 0.90 else -1)
    print(f"@{best['threshold']:.2f}  precision={best['precision']:.3f}  "
          f"recall={best['recall']:.3f}  f1={best['f1']:.3f}  (>=0.90 precision target)")

    cep_flag = (test["cep_score"].values >= REVIEW_THRESHOLD).astype(int)
    cep = _metrics(yte, cep_flag.astype(float), 0.5)
    print("\n=== CEP-only vs ML (same test slice) ===")
    print(f"CEP rules : precision={cep['precision']:.3f}  recall={cep['recall']:.3f}")
    print(f"ML @0.50  : precision={m05['precision']:.3f}  recall={m05['recall']:.3f}   "
          f"<- fusion (phase 6) combines both")

    cal = _calibration(yte, proba)
    print("\n=== calibration - are the probabilities usable as MAGNITUDES? ===")
    print(f"Brier score            : {cal['brier']:.5f}")
    if cal["n_alerts"]:
        print(f"alerts (>= {REVIEW_THRESHOLD:.2f})        : {cal['n_alerts']}")
        print(f"  rounding to 1.000    : {cal['saturated_share']:.1%}")
        print(f"  distinct scores      : {cal['distinct_scores']}")
        print(f"  in the REVIEW band   : {cal['review_band']}")
        print(f"  median alert score   : {cal['median_alert_score']:.6f}")
        if cal["saturated_share"] and cal["saturated_share"] > 0.5:
            print("  WARNING: most alerts are tied at the top of the scale. "
                  "Ranking is fine (see AUC) but the score cannot ORDER work, "
                  "and the REVIEW/BLOCK split is nominal. See _calibration().")

    print("\nrecall by fraud type (ML @0.50):")
    tdf = test.copy(); tdf["pred"] = (proba >= 0.50).astype(int)
    by_type = {}
    for ftype, grp in tdf[tdf.label == 1].groupby("fraud_type"):
        by_type[ftype] = {"recall": float(grp["pred"].mean()),
                          "caught": int(grp["pred"].sum()), "n": int(len(grp))}
        print(f"  {ftype:<12} {grp['pred'].mean():.1%}  (n={len(grp)})")
    # Counts beside the rate: a per-type recall on a few dozen events has a wide
    # binomial interval.

    print("\nreplaying the held-out slice with Neo4j down (every payee age withheld) ...")
    outage = _graph_outage(model, feats, yte, proba)
    om = outage["at_0_50"]
    print(f"alerts @0.50: {outage['alerts_healthy']} with the graph -> "
          f"{outage['alerts_outage']} without   precision={om['precision']:.3f}  "
          f"recall={om['recall']:.3f}  PR-AUC={outage['pr_auc']:.3f}")

    joblib.dump(model, os.path.join(MODELS_DIR, "model.joblib"))
    with open(os.path.join(MODELS_DIR, "feature_names.json"), "w") as fh:
        json.dump(feats, fh, indent=2)
    with open(os.path.join(MODELS_DIR, "metrics.json"), "w") as fh:
        json.dump(dict(roc_auc=auc, pr_auc=ap, at_0_50=m05,
                       high_recall=best, cep_only=cep, calibration=cal,
                       by_fraud_type=by_type, graph_outage=outage),
                  fh, indent=2)
    print(f"\nsaved model + feature_names + metrics to {MODELS_DIR}/")
    print("NOTE: metrics are design targets on synthetic data, not validated findings.")


if __name__ == "__main__":
    main()
