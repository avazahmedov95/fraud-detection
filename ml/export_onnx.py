"""Exports the trained model to ONNX for serving inside Flink, plus model.txt
for the case-manager's explanations, and checks both against the native model.
"""

import json
import os

import joblib
import numpy as np
import onnxruntime as ort
from onnxmltools import convert_lightgbm
from onnxmltools.convert.common.data_types import FloatTensorType

import dataset as D

MODELS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "models")
CSV = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                   "..", "data-generator", "out", "transactions.csv")
ONNX_PATH = os.path.join(MODELS_DIR, "model.onnx")

#: The booster as a plain text model, for case-manager/explain.py.
#:
#: Not model.joblib: unpickling an LGBMClassifier drags in scikit-learn, which
#: would put ~30 MB of training dependency into a service that only reads trees.
#: `Booster(model_file=...)` needs lightgbm alone. It is written from the SAME
#: object that is converted to ONNX two lines below, so the explanation and the
#: decision cannot come from different trees - and explain.py re-checks that at
#: runtime anyway by recomputing the probability.
BOOSTER_PATH = os.path.join(MODELS_DIR, "model.txt")


def _onnx_positive_proba(outputs):
    """Pull the fraud-class probability out of whatever shape the model emits."""
    for out in outputs:
        arr = np.asarray(out)
        if arr.ndim == 2 and arr.shape[1] == 2:           # [n, 2] probability tensor
            return arr[:, 1]
    # Fallback: ZipMap output (a list of {label: prob} dicts).
    for out in outputs:
        if isinstance(out, list) and out and isinstance(out[0], dict):
            return np.array([row.get(1, row.get("1", 0.0)) for row in out])
    raise RuntimeError("could not locate probability output in ONNX model")


def main():
    model = joblib.load(os.path.join(MODELS_DIR, "model.joblib"))
    feats = json.load(open(os.path.join(MODELS_DIR, "feature_names.json")))
    n = len(feats)

    initial_types = [("input", FloatTensorType([None, n]))]
    try:
        onx = convert_lightgbm(model, initial_types=initial_types, zipmap=False)
    except TypeError:
        onx = convert_lightgbm(model, initial_types=initial_types)

    with open(ONNX_PATH, "wb") as fh:
        fh.write(onx.SerializeToString())
    print(f"exported {ONNX_PATH}  ({os.path.getsize(ONNX_PATH) / 1024:.0f} KB)")

    booster = model.booster_ if hasattr(model, "booster_") else model
    booster.save_model(BOOSTER_PATH)
    print(f"exported {BOOSTER_PATH}  "
          f"({os.path.getsize(BOOSTER_PATH) / 1024:.0f} KB, "
          f"{booster.num_trees()} trees)")

    # --- parity check on a sample of the test slice ---
    df = D.build_matrix(CSV)
    sample = df.iloc[int(len(df) * 0.80):][feats].astype("float32").values[:2000]

    native = model.predict_proba(sample)[:, 1]
    sess = ort.InferenceSession(ONNX_PATH, providers=["CPUExecutionProvider"])
    onnx_proba = _onnx_positive_proba(sess.run(None, {sess.get_inputs()[0].name: sample}))

    max_diff = float(np.max(np.abs(native - onnx_proba)))
    print(f"parity vs native LightGBM:  max |Δ probability| = {max_diff:.2e}  over {len(sample)} rows")
    assert max_diff < 1e-3, "ONNX/native mismatch too large"
    print("ONNX model matches the native model — ready for in-Flink serving (phase 6).")

    json.dump(feats, open(os.path.join(MODELS_DIR, "feature_names.json"), "w"), indent=2)


if __name__ == "__main__":
    main()
