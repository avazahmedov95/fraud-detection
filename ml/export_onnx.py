"""Exports the trained committee to ONNX for serving inside Flink, plus model.txt
for the case-manager's explanations, and checks the ONNX against the native model.
"""

import argparse
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
PARITY_ROWS = 60_000

#: Plain-text booster for case-manager/explain.py - no scikit-learn needed there -
#: written from the same object that is converted to ONNX below.
BOOSTER_PATH = os.path.join(MODELS_DIR, "model.txt")


def _onnx_positive_proba(outputs):
    """The fraud-class column of the [n, 2] probability tensor (zipmap off)."""
    for out in outputs:
        arr = np.asarray(out)
        if arr.ndim == 2 and arr.shape[1] == 2:
            return arr[:, 1]
    raise RuntimeError("could not locate probability output in ONNX model")


def main():
    model = joblib.load(os.path.join(MODELS_DIR, "model.joblib"))
    feats = json.load(open(os.path.join(MODELS_DIR, "feature_names.json")))
    n = len(feats)

    initial_types = [("input", FloatTensorType([None, n]))]
    onx = convert_lightgbm(model, initial_types=initial_types, zipmap=False)

    with open(ONNX_PATH, "wb") as fh:
        fh.write(onx.SerializeToString())
    print(f"exported {ONNX_PATH}  ({os.path.getsize(ONNX_PATH) / 1024:.0f} KB)")

    model.save_model(BOOSTER_PATH)
    print(f"exported {BOOSTER_PATH}  "
          f"({os.path.getsize(BOOSTER_PATH) / 1024:.0f} KB, "
          f"{model.num_trees()} trees)")

    # A parity check needs valid inputs, not the whole replay: the first rows of the
    # file, ten minutes shorter at the realistic profile's size.
    df = D.build_matrix(CSV, nrows=PARITY_ROWS)
    sample = df.iloc[int(len(df) * 0.80):][feats].astype("float32").values[:2000]

    native = model.predict(sample)          # a merged lgb.Booster (committee.py)
    sess = ort.InferenceSession(ONNX_PATH, providers=["CPUExecutionProvider"])
    onnx_proba = _onnx_positive_proba(sess.run(None, {sess.get_inputs()[0].name: sample}))

    max_diff = float(np.max(np.abs(native - onnx_proba)))
    print(f"parity vs native LightGBM:  max |delta probability| = {max_diff:.2e}  over {len(sample)} rows")
    assert max_diff < 1e-3, "ONNX/native mismatch too large"
    print("ONNX model matches the native model - ready for in-Flink serving.")

    # Last, so it records the artefacts as they finally are.
    import manifest
    manifest.write()


if __name__ == "__main__":
    # Refuses unknown flags: a mistyped one would otherwise re-export the model.
    argparse.ArgumentParser(description=__doc__).parse_args()
    main()
