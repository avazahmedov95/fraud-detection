"""Trains the scoring committee on a time-ordered split and writes the model, the
cutoff the job decides with, and metrics.json.

The earliest 64% of rows fit the committee, the next 16% choose the REVIEW
cutoff, and every printed figure is measured on the last 20%. Calibration
is reported beside the AUCs because a rank statistic cannot see a score that
ranks well yet cannot order a queue.
"""

import argparse
import json
import os

import joblib
import numpy as np
import lightgbm as lgb
from sklearn.metrics import (roc_auc_score, average_precision_score,
                             precision_recall_fscore_support, confusion_matrix,
                             precision_recall_curve)

import committee
import dataset as D

# Overridable so a sweep writes elsewhere instead of clobbering the deployed model.
MODELS_DIR = os.getenv(
    "MODELS_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), "models"))
CSV = os.getenv(
    "DATASET_CSV", os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", "data-generator", "out", "transactions.csv"))
REVIEW_THRESHOLD = 0.40   # CEP flag cutoff, for the head-to-head comparison
TRAIN_SHARE = 0.80        # the earliest 80% trains; the rest is the held-out slice
FIT_SHARE = 0.80          # of the training slice: the committee fits on this much,
                          # and the cutoffs are chosen on the rest

#: Class weighting (negatives over positives) is validated at 1.5% fraud; below
#: this rate it collapses the ranking, so the default fits unweighted and the
#: cutoffs are chosen on validation rows (thresholds.json), not fixed.
MIN_WEIGHTED_POSITIVE_RATE = 0.005
#: auto: weight above the line, unweighted below it. on / off: force either.
CLASS_WEIGHTING = os.getenv("CLASS_WEIGHTING", "auto")

#: Five fits averaged into one booster (committee.py).
COMMITTEE_SEEDS = tuple(range(42, 47))


def cut_index(n):
    return int(n * TRAIN_SHARE)


def class_weight(pos, neg, mode=None):
    """scale_pos_weight for the rows the committee fits on."""
    mode = CLASS_WEIGHTING if mode is None else mode
    if mode not in ("auto", "on", "off"):
        raise ValueError(f"CLASS_WEIGHTING={mode!r}: expected auto, on or off")
    if mode == "off" or (mode == "auto"
                         and pos / max(pos + neg, 1) < MIN_WEIGHTED_POSITIVE_RATE):
        return 1.0
    return neg / max(pos, 1)


def make_model(scale_pos_weight, random_state=42):
    """The deployed recipe, in one place for the experiments that refit it. The L2
    penalty bounds the leaf values: without it, Newton steps over vanishing second
    derivatives reached 18,111 on this data (ml/README.md, Feature importance)."""
    return lgb.LGBMClassifier(
        n_estimators=400, learning_rate=0.05, num_leaves=31,
        colsample_bytree=0.8, min_child_samples=30, reg_lambda=10.0,
        scale_pos_weight=scale_pos_weight, random_state=random_state,
        n_jobs=-1, verbose=-1)


def choose_review_cutoff(y, p):
    """REVIEW maximises F1 on the validation rows. There is no BLOCK: the system
    never blocks on its own."""
    prec, rec, thr = precision_recall_curve(y, p)
    f1 = 2 * prec[:-1] * rec[:-1] / np.maximum(prec[:-1] + rec[:-1], 1e-12)
    return float(thr[int(np.argmax(f1))])


def _metrics(y, proba, thr):
    pred = (proba >= thr).astype(int)
    p, r, f1, _ = precision_recall_fscore_support(y, pred, average="binary", zero_division=0)
    tn, fp, fn, tp = confusion_matrix(y, pred, labels=[0, 1]).ravel()
    return dict(threshold=thr, precision=p, recall=r, f1=f1,
                tp=int(tp), fp=int(fp), fn=int(fn), tn=int(tn))


def _calibration(y, proba, review_thr):
    """How usable the probabilities are as magnitudes, not just as a ranking."""
    alert = proba >= review_thr
    n_alert = int(alert.sum())
    pa = proba[alert]
    return dict(
        brier=float(np.mean((proba - y) ** 2)),
        n_alerts=n_alert,
        saturated_share=(float(np.mean(pa >= 0.9995)) if n_alert else None),
        distinct_scores=(int(len(np.unique(np.round(pa, 3)))) if n_alert else 0),
        median_alert_score=(float(np.median(pa)) if n_alert else None),
    )


def main():
    os.makedirs(MODELS_DIR, exist_ok=True)
    print("building feature matrix ...")
    df = D.build_matrix(CSV)
    feats = D.FEATURE_NAMES
    cut = cut_index(len(df))
    fit = int(cut * FIT_SHARE)
    X = df[feats].astype("float32").values
    y = df["label"].values
    test, Xte, yte = df.iloc[cut:], X[cut:], y[cut:]

    pos, neg = int(y[:fit].sum()), int((y[:fit] == 0).sum())
    spw = class_weight(pos, neg)
    print(f"fit {fit:,} (pos={pos}, {pos / (pos + neg):.3%}) | cutoffs {cut - fit:,} "
          f"(pos={int(y[fit:cut].sum())}) | test {len(yte):,} (pos={int(yte.sum())}) "
          f"| scale_pos_weight={spw:.1f}")

    members = []
    for seed in COMMITTEE_SEEDS:
        m = make_model(spw, random_state=seed)
        m.fit(X[:fit], y[:fit])
        members.append(m.booster_)
    model = committee.merge(members)
    committee.check(model, members, Xte[:5000])

    review = choose_review_cutoff(y[fit:cut], model.predict(X[fit:cut]))
    proba = model.predict(Xte)
    auc = roc_auc_score(yte, proba)
    ap = average_precision_score(yte, proba)
    member_ap = [float(average_precision_score(yte, b.predict(Xte))) for b in members]

    print(f"\n=== committee of {len(members)} on the held-out (later) test slice "
          f"- DESIGN TARGETS ===")
    print(f"ROC-AUC: {auc:.3f}   PR-AUC (avg precision): {ap:.3f}   "
          f"(one fit alone: {min(member_ap):.3f}-{max(member_ap):.3f})")
    mr = _metrics(yte, proba, review)
    print(f"REVIEW at {review:.4f}  precision={mr['precision']:.3f}  "
          f"recall={mr['recall']:.3f}  f1={mr['f1']:.3f}  "
          f"(tp={mr['tp']} fp={mr['fp']} fn={mr['fn']})")

    cep_flag = (test["cep_score"].values >= REVIEW_THRESHOLD).astype(int)
    cep = _metrics(yte, cep_flag.astype(float), 0.5)
    print("\n=== CEP-only vs ML (same test slice) ===")
    print(f"CEP rules    : precision={cep['precision']:.3f}  recall={cep['recall']:.3f}")
    print(f"ML at REVIEW : precision={mr['precision']:.3f}  recall={mr['recall']:.3f}"
          f"   <- fusion.py combines both")

    cal = _calibration(yte, proba, review)
    print("\n=== calibration - are the probabilities usable as MAGNITUDES? ===")
    print(f"Brier score            : {cal['brier']:.5f}")
    if cal["n_alerts"]:
        print(f"alerts (>= REVIEW)     : {cal['n_alerts']}")
        print(f"  rounding to 1.000    : {cal['saturated_share']:.1%}")
        print(f"  distinct scores      : {cal['distinct_scores']}")
        print(f"  median alert score   : {cal['median_alert_score']:.6f}")

    print("\nrecall by fraud type (ML at REVIEW):")
    tdf = test.copy()
    tdf["pred"] = (proba >= review).astype(int)
    # Counts beside the rate: a per-type recall on a few dozen events has a wide
    # binomial interval.
    by_type = {}
    for ftype, grp in tdf[tdf.label == 1].groupby("fraud_type"):
        by_type[ftype] = {"recall": float(grp["pred"].mean()),
                          "caught": int(grp["pred"].sum()), "n": int(len(grp))}
        print(f"  {ftype:<12} {grp['pred'].mean():.1%}  (n={len(grp)})")

    joblib.dump(model, os.path.join(MODELS_DIR, "model.joblib"))
    with open(os.path.join(MODELS_DIR, "feature_names.json"), "w") as fh:
        json.dump(feats, fh, indent=2)
    with open(os.path.join(MODELS_DIR, "thresholds.json"), "w") as fh:
        json.dump(dict(review=review,
                       chosen_on=dict(rows=cut - fit, fraud=int(y[fit:cut].sum())),
                       committee=len(members)), fh, indent=2)
    with open(os.path.join(MODELS_DIR, "metrics.json"), "w") as fh:
        json.dump(dict(roc_auc=auc, pr_auc=ap, thresholds=dict(review=review),
                       at_review=mr, cep_only=cep, calibration=cal,
                       by_fraud_type=by_type,
                       committee=dict(seeds=list(COMMITTEE_SEEDS),
                                      member_pr_auc=member_ap)),
                  fh, indent=2)
    print(f"\nsaved model + thresholds + feature_names + metrics to {MODELS_DIR}/")
    print("NOTE: metrics are design targets on synthetic data, not validated findings.")


if __name__ == "__main__":
    # Refuses unknown flags: a mistyped one would otherwise retrain the model.
    argparse.ArgumentParser(description=__doc__).parse_args()
    main()
