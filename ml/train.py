"""Trains the scoring committee on a time-ordered split and writes the model, the
cutoffs the job decides with, and metrics.json.

The earliest 64% of rows fit the committee, the next 16% choose the REVIEW and
BLOCK cutoffs, and every printed figure is measured on the last 20%. Calibration
is reported beside the AUCs because a rank statistic cannot see a score that
ranks well yet cannot order a queue.
"""

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

#: Share of training rows replayed with the payee's age withheld, as the live job
#: sees them while Neo4j is down, so the model learns where "unknown" goes. Random,
#: label-independent and before the cut; `_graph_outage` measures the other case.
AGE_UNKNOWN_SHARE = 0.10
AGE_UNKNOWN_SEED = 42

#: Class weighting (negatives over positives) is validated at 1.5% fraud; below
#: this rate it collapses the ranking, so the default fits unweighted and the
#: cutoffs are chosen on validation rows (thresholds.json), not fixed.
MIN_WEIGHTED_POSITIVE_RATE = 0.005
#: auto: weight above the line, unweighted below it. on / off: force either.
CLASS_WEIGHTING = os.getenv("CLASS_WEIGHTING", "auto")

#: Five fits averaged into one booster (committee.py).
COMMITTEE_SEEDS = tuple(range(42, 47))
#: BLOCK stops a customer, so it asks more than REVIEW: the lowest cutoff at or
#: above REVIEW whose validation precision is at least this.
BLOCK_PRECISION = 0.90


def cut_index(n):
    return int(n * TRAIN_SHARE)


def age_unknown_rows(n, outage=False, seed=AGE_UNKNOWN_SEED, share=AGE_UNKNOWN_SHARE):
    """Rows replayed with no payee age: a random share of the training slice, and
    with `outage` the whole held-out slice as well - Neo4j down from the cut on."""
    rows = np.random.default_rng(seed).random(n) < share
    rows[cut_index(n):] = outage
    return rows


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
    """The deployed recipe, in one place for the experiments that refit it."""
    return lgb.LGBMClassifier(
        n_estimators=400, learning_rate=0.05, num_leaves=31,
        subsample=0.8, colsample_bytree=0.8, min_child_samples=30,
        scale_pos_weight=scale_pos_weight, random_state=random_state,
        n_jobs=-1, verbose=-1)


def choose_cutoffs(y, p, block_precision=BLOCK_PRECISION):
    """REVIEW maximises F1 on validation rows; BLOCK is the lowest cutoff at or above
    it with validation precision >= `block_precision`, or None - the model then never
    blocks on its own."""
    prec, rec, thr = precision_recall_curve(y, p)
    f1 = 2 * prec[:-1] * rec[:-1] / np.maximum(prec[:-1] + rec[:-1], 1e-12)
    review = float(thr[int(np.argmax(f1))])
    sure = [t for t, pr in zip(thr, prec[:-1]) if t >= review and pr >= block_precision]
    return review, (float(min(sure)) if sure else None)


def _metrics(y, proba, thr):
    pred = (proba >= thr).astype(int)
    p, r, f1, _ = precision_recall_fscore_support(y, pred, average="binary", zero_division=0)
    tn, fp, fn, tp = confusion_matrix(y, pred, labels=[0, 1]).ravel()
    return dict(threshold=thr, precision=p, recall=r, f1=f1,
                tp=int(tp), fp=int(fp), fn=int(fn), tn=int(tn))


def _calibration(y, proba, review_thr, block_thr):
    """How usable the probabilities are as magnitudes, not just as a ranking; the
    review band is the alerts below BLOCK (all of them when there is no BLOCK)."""
    alert = proba >= review_thr
    n_alert = int(alert.sum())
    pa = proba[alert]
    top = np.inf if block_thr is None else block_thr
    return dict(
        brier=float(np.mean((proba - y) ** 2)),
        n_alerts=n_alert,
        saturated_share=(float(np.mean(pa >= 0.9995)) if n_alert else None),
        distinct_scores=(int(len(np.unique(np.round(pa, 3)))) if n_alert else 0),
        review_band=int((pa < top).sum()) if n_alert else 0,
        median_alert_score=(float(np.median(pa)) if n_alert else None),
    )


def _graph_outage(model, feats, yte, healthy_proba, review):
    """The held-out slice replayed as if Neo4j were down throughout: no payee age, so
    FRESH_RECEIVER cannot fire and the model sees the age as missing."""
    df = D.build_matrix(CSV, age_unknown=lambda n: age_unknown_rows(n, outage=True))
    test = df.iloc[cut_index(len(df)):]
    if not np.array_equal(test["label"].values, yte):
        raise RuntimeError("the outage replay does not line up with the held-out slice")
    proba = model.predict(test[feats].astype("float32").values)
    m = _metrics(yte, proba, review)
    return dict(pr_auc=float(average_precision_score(yte, proba)), at_review=m,
                alerts_healthy=int((healthy_proba >= review).sum()),
                alerts_outage=m["tp"] + m["fp"])


def main():
    os.makedirs(MODELS_DIR, exist_ok=True)
    print("building feature matrix ...")
    df = D.build_matrix(CSV, age_unknown=age_unknown_rows)
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
    if "receiver_age" in feats:
        print(f"payee age withheld on {int(df.iloc[:cut]['receiver_age'].isna().sum()):,} "
              f"training rows ({AGE_UNKNOWN_SHARE:.0%} drawn, plus any the data lacks)")

    members = []
    for seed in COMMITTEE_SEEDS:
        m = make_model(spw, random_state=seed)
        m.fit(X[:fit], y[:fit])
        members.append(m.booster_)
    model = committee.merge(members)
    committee.check(model, members, Xte[:5000])

    review, block = choose_cutoffs(y[fit:cut], model.predict(X[fit:cut]))
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
    mb = _metrics(yte, proba, block) if block is not None else None
    if mb:
        print(f"BLOCK  at {block:.4f}  precision={mb['precision']:.3f}  "
              f"recall={mb['recall']:.3f}")
    else:
        print(f"BLOCK  none: no cutoff reached {BLOCK_PRECISION:.0%} precision on "
              f"the validation rows, so the model only ever sends to review")

    cep_flag = (test["cep_score"].values >= REVIEW_THRESHOLD).astype(int)
    cep = _metrics(yte, cep_flag.astype(float), 0.5)
    print("\n=== CEP-only vs ML (same test slice) ===")
    print(f"CEP rules    : precision={cep['precision']:.3f}  recall={cep['recall']:.3f}")
    print(f"ML at REVIEW : precision={mr['precision']:.3f}  recall={mr['recall']:.3f}"
          f"   <- fusion (phase 6) combines both")

    cal = _calibration(yte, proba, review, block)
    print("\n=== calibration - are the probabilities usable as MAGNITUDES? ===")
    print(f"Brier score            : {cal['brier']:.5f}")
    if cal["n_alerts"]:
        print(f"alerts (>= REVIEW)     : {cal['n_alerts']}")
        print(f"  rounding to 1.000    : {cal['saturated_share']:.1%}")
        print(f"  distinct scores      : {cal['distinct_scores']}")
        print(f"  in the REVIEW band   : {cal['review_band']}")
        print(f"  median alert score   : {cal['median_alert_score']:.6f}")

    print("\nrecall by fraud type (ML at REVIEW):")
    tdf = test.copy()
    tdf["pred"] = (proba >= review).astype(int)
    by_type = {}
    for ftype, grp in tdf[tdf.label == 1].groupby("fraud_type"):
        by_type[ftype] = {"recall": float(grp["pred"].mean()),
                          "caught": int(grp["pred"].sum()), "n": int(len(grp))}
        print(f"  {ftype:<12} {grp['pred'].mean():.1%}  (n={len(grp)})")
    # Counts beside the rate: a per-type recall on a few dozen events has a wide
    # binomial interval.

    print("\nreplaying the held-out slice with Neo4j down (every payee age withheld) ...")
    outage = _graph_outage(model, feats, yte, proba, review)
    om = outage["at_review"]
    print(f"alerts at REVIEW: {outage['alerts_healthy']} with the graph -> "
          f"{outage['alerts_outage']} without   precision={om['precision']:.3f}  "
          f"recall={om['recall']:.3f}  PR-AUC={outage['pr_auc']:.3f}")

    joblib.dump(model, os.path.join(MODELS_DIR, "model.joblib"))
    with open(os.path.join(MODELS_DIR, "feature_names.json"), "w") as fh:
        json.dump(feats, fh, indent=2)
    with open(os.path.join(MODELS_DIR, "thresholds.json"), "w") as fh:
        json.dump(dict(review=review, block=block, block_precision=BLOCK_PRECISION,
                       chosen_on=dict(rows=cut - fit, fraud=int(y[fit:cut].sum())),
                       committee=len(members)), fh, indent=2)
    with open(os.path.join(MODELS_DIR, "metrics.json"), "w") as fh:
        json.dump(dict(roc_auc=auc, pr_auc=ap, thresholds=dict(review=review, block=block),
                       at_review=mr, at_block=mb, cep_only=cep, calibration=cal,
                       by_fraud_type=by_type, graph_outage=outage,
                       committee=dict(seeds=list(COMMITTEE_SEEDS),
                                      member_pr_auc=member_ap)),
                  fh, indent=2)
    print(f"\nsaved model + thresholds + feature_names + metrics to {MODELS_DIR}/")
    print("NOTE: metrics are design targets on synthetic data, not validated findings.")


if __name__ == "__main__":
    main()
