"""
Export the trained LightGBM model to ONNX for serving inside Flink via ONNX
Runtime (already bundled in the Flink image). Verifies that the ONNX model
reproduces the native model's probabilities, so phase-6 serving is faithful.

Produces:
  models/model.onnx
  + parity check against the native model

  python export_onnx.py
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
