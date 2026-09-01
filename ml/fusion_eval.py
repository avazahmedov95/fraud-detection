"""Evaluates score-fusion strategies against the model alone.
"""

import os
import sys

import numpy as np
import onnxruntime as ort
from sklearn.metrics import (precision_recall_fscore_support, roc_auc_score,
                             average_precision_score, confusion_matrix)

import dataset as D            # noqa: E402  (also inserts stream-processor on sys.path)
import config as C            # noqa: E402  (resolved from stream-processor)
import fusion as FU           # noqa: E402  (resolved from stream-processor)

HERE = os.path.dirname(os.path.abspath(__file__))
MODELS = os.path.join(HERE, "models")
CSV = os.path.join(HERE, "..", "data-generator", "out", "transactions.csv")


def _onnx_proba(outputs):
    for out in outputs:
        arr = np.asarray(out)
        if arr.ndim == 2 and arr.shape[1] == 2:
            return arr[:, 1]
    raise RuntimeError("no probability tensor in ONNX output")


def _pr(y, flag):
    p, r, f1, _ = precision_recall_fscore_support(y, flag, average="binary", zero_division=0)
    tn, fp, fn, tp = confusion_matrix(y, flag, labels=[0, 1]).ravel()
    return p, r, f1, int(tp), int(fp), int(fn)


def main():
    df = D.build_matrix(CSV)
    test = df.iloc[int(len(df) * 0.80):].reset_index(drop=True)
    feats = D.FEATURE_NAMES
    X = test[feats].astype("float32").values
    y = test["label"].values.astype(int)
    cep = test["cep_score"].values.astype(float)

    sess = ort.InferenceSession(os.path.join(MODELS, "model.onnx"),
                                providers=["CPUExecutionProvider"])
    ml = _onnx_proba(sess.run(None, {sess.get_inputs()[0].name: X})).astype(float)

    final = np.array([FU.final_score(c, m) for c, m in zip(cep, ml)])

    # Compliance must-flags reconstructed from features (structuring / daily limit).
    mandatory = (test["sub_threshold_1h"].values >= C.STRUCTURING_MIN_COUNT) | \
                (test["daily_sum_ratio"].values > 1.0)

    cep_flag = (cep >= C.REVIEW_THRESHOLD).astype(int)
    ml_flag = (ml >= 0.50).astype(int)
    fused_flag = ((final >= C.FINAL_REVIEW_THRESHOLD) | mandatory).astype(int)
    block = int((final >= C.FINAL_BLOCK_THRESHOLD).sum())

    print(f"test events: {len(y):,}   positives: {int(y.sum())}\n")
    print(f"final_score ranking quality:  ROC-AUC {roc_auc_score(y, final):.3f}   "
          f"PR-AUC {average_precision_score(y, final):.3f}\n")

    print(f"{'layer':<20}{'precision':>10}{'recall':>9}{'f1':>7}    (tp/fp/fn)")
    for name, flag in (("CEP rules only", cep_flag),
                       ("ML only @0.50", ml_flag),
                       ("Fused (final)", fused_flag)):
        p, r, f1, tp, fp, fn = _pr(y, flag)
        print(f"{name:<20}{p:>10.3f}{r:>9.3f}{f1:>7.3f}    ({tp}/{fp}/{fn})")

    flagged = int(fused_flag.sum())
    print(f"\nfused decision split:  ALLOW {len(y)-flagged}   "
          f"REVIEW {flagged-block}   BLOCK {block}")

    print("\nfused recall by fraud type:")
    tdf = test.copy(); tdf["flag"] = fused_flag
    for ftype, g in tdf[tdf.label == 1].groupby("fraud_type"):
        print(f"  {ftype:<12} {g['flag'].mean():.1%}  (n={len(g)})")

    print("\nNote: design targets on synthetic data, not validated production metrics.")


if __name__ == "__main__":
    main()
