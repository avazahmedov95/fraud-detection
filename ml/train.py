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


def main():
    os.makedirs(MODELS_DIR, exist_ok=True)
    print("building feature matrix ...")
    df = D.build_matrix(CSV)
    feats = D.FEATURE_NAMES

    cut = int(len(df) * 0.80)
    train, test = df.iloc[:cut], df.iloc[cut:]
    Xtr, ytr = train[feats].astype("float32").values, train["label"].values
    Xte, yte = test[feats].astype("float32").values, test["label"].values

    pos, neg = int(ytr.sum()), int((ytr == 0).sum())
    spw = neg / max(pos, 1)
    print(f"train {len(ytr):,} (pos={pos}) | test {len(yte):,} (pos={int(yte.sum())}) | scale_pos_weight={spw:.1f}")

    model = lgb.LGBMClassifier(
        n_estimators=400, learning_rate=0.05, num_leaves=31,
        subsample=0.8, colsample_bytree=0.8, min_child_samples=30,
        scale_pos_weight=spw, random_state=42, n_jobs=-1, verbose=-1)
    model.fit(Xtr, ytr)

    proba = model.predict_proba(Xte)[:, 1]
    auc = roc_auc_score(yte, proba)
    ap = average_precision_score(yte, proba)

    print("\n=== ML model on held-out (later) test slice — DESIGN TARGETS ===")
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

    joblib.dump(model, os.path.join(MODELS_DIR, "model.joblib"))
    with open(os.path.join(MODELS_DIR, "feature_names.json"), "w") as fh:
        json.dump(feats, fh, indent=2)
    with open(os.path.join(MODELS_DIR, "metrics.json"), "w") as fh:
        json.dump(dict(roc_auc=auc, pr_auc=ap, at_0_50=m05,
                       high_recall=best, cep_only=cep, calibration=cal,
                       by_fraud_type=by_type),
                  fh, indent=2)
    print(f"\nsaved model + feature_names + metrics to {MODELS_DIR}/")
    print("NOTE: metrics are design targets on synthetic data, not validated findings.")


if __name__ == "__main__":
    main()
