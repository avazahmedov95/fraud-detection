"""Add calibration figures to metrics.json for a model that is ALREADY trained.

Why this exists as a separate step rather than only inside train.py:

Calibration describes a MODEL, not a training run. Retraining to obtain it would
rewrite model.joblib while model.onnx - the artefact that actually serves - stayed
where it was, so the two would diverge and case-manager/explain.py would start
refusing to explain (MODEL_MISMATCH, by design). It would also move every
reported figure in docs/, because a retrain is a retrain: PR-AUC, F1 and the
per-type recalls would all shift and the documents would no longer describe the
deployed model. Adding a metric is not a reason to change the thing being
measured.

So this scores the DEPLOYED model over the same time-ordered held-out slice
train.py uses, computes train._calibration - the same function, so there is one
definition and not two - and merges the result into metrics.json without touching
any other key.

Scores through model.onnx by default, because that is what the pipeline serves;
the native model agrees to 3.3e-07 across 50,000 events, so the choice does not
move the figures, but it means they describe the artefact in production rather
than one beside it.

  python calibration_report.py
  python calibration_report.py --native      # score with model.joblib instead
  python calibration_report.py --dry-run     # print, do not write
"""

import argparse
import json
import os

import numpy as np

import dataset as D
import train as T

MODELS_DIR = os.getenv(
    "MODELS_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), "models"))
CSV = os.getenv(
    "DATASET_CSV", os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", "data-generator", "out", "transactions.csv"))
METRICS = os.path.join(MODELS_DIR, "metrics.json")


def _onnx_proba(X):
    import onnxruntime as ort
    path = os.path.join(MODELS_DIR, "model.onnx")
    sess = ort.InferenceSession(path, providers=["CPUExecutionProvider"])
    outputs = sess.run(None, {sess.get_inputs()[0].name: X})
    for out in outputs:
        arr = np.asarray(out)
        if arr.ndim == 2 and arr.shape[1] == 2:
            return arr[:, 1].astype(float), os.path.basename(path)
    for out in outputs:                                   # ZipMap fallback
        if isinstance(out, list) and out and isinstance(out[0], dict):
            return (np.array([r.get(1, r.get("1", 0.0)) for r in out], dtype=float),
                    os.path.basename(path))
    raise RuntimeError("could not locate the probability output in model.onnx")


def _native_proba(X):
    import warnings

    import joblib
    path = os.path.join(MODELS_DIR, "model.joblib")
    clf = joblib.load(path)
    with warnings.catch_warnings():
        # "X does not have valid feature names, but LGBMClassifier was fitted
        # with feature names" - emitted on some sklearn/lightgbm combinations
        # and benign here, VERIFIED rather than assumed:
        #
        #   * the model was fitted on a bare numpy array (train.py), so its
        #     booster carries positional names, Column_0..23;
        #   * feature_names.json matches capabilities.feature_names() exactly,
        #     which is the order those columns were in;
        #   * scoring this same slice through model.onnx returns identical
        #     figures to five decimal places.
        #
        # Suppressed narrowly, and only here, because a warning on every run
        # that never means anything teaches an operator to ignore warnings.
        # stream-processor/test_artefact_consistency.py is what actually
        # guards the hazard the message gestures at.
        warnings.filterwarnings("ignore", message=".*valid feature names.*")
        proba = clf.predict_proba(X.astype(np.float64))[:, 1]
    return proba, os.path.basename(path)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--native", action="store_true",
                    help="score with model.joblib instead of model.onnx")
    ap.add_argument("--dry-run", action="store_true",
                    help="print the figures without writing metrics.json")
    args = ap.parse_args()

    df = D.build_matrix(CSV)
    # The SAME split train.py uses. A calibration figure computed over a
    # different slice than the AUCs it sits beside would invite comparison
    # between numbers that were never measured on the same events.
    cut = int(len(df) * 0.80)
    test = df.iloc[cut:]
    X = test[D.FEATURE_NAMES].astype("float32").values
    y = test["label"].values

    proba, source = (_native_proba(X) if args.native else _onnx_proba(X))
    cal = T._calibration(y, proba)
    cal["scored_with"] = source

    print(f"held-out slice : {len(y):,} events, {int(y.sum())} fraud")
    print(f"scored with    : {source}")
    print(f"Brier score            : {cal['brier']:.5f}")
    if cal["n_alerts"]:
        print(f"alerts (>= {T.REVIEW_THRESHOLD:.2f})        : {cal['n_alerts']}")
        print(f"  rounding to 1.000    : {cal['saturated_share']:.1%}")
        print(f"  distinct scores      : {cal['distinct_scores']}")
        print(f"  in the REVIEW band   : {cal['review_band']}")
        print(f"  median alert score   : {cal['median_alert_score']:.6f}")

    if args.dry_run:
        print("\n--dry-run: metrics.json not written")
        return
    if not os.path.exists(METRICS):
        raise SystemExit(f"{METRICS} not found - train the model first")
    with open(METRICS, encoding="utf-8") as fh:
        metrics = json.load(fh)
    # Merge, never rewrite: every other key belongs to the training run that
    # produced this model and must keep describing it.
    metrics["calibration"] = cal
    with open(METRICS, "w", encoding="utf-8") as fh:
        json.dump(metrics, fh, indent=2)
    print(f"\nmerged into {METRICS} (only the 'calibration' key was touched)")


if __name__ == "__main__":
    main()
