"""Exports the trained committee to ONNX for serving inside Flink, plus model.txt
for the sink writer's explanations, and checks the ONNX against the native model.
Both are written with the split thresholds moved to float32, as ONNX keeps them
(committee.float32_thresholds): no decision changes.
"""

import argparse
import json
import os

import joblib
import numpy as np
import onnxruntime as ort
from onnxmltools import convert_lightgbm
from onnxmltools.convert.common.data_types import FloatTensorType

import committee
import dataset as D

MODELS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "models")
CSV = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                   "..", "data-generator", "out", "transactions.csv")
ONNX_PATH = os.path.join(MODELS_DIR, "model.onnx")
PARITY_ROWS = 60_000

#: Plain-text booster for sink-writer/explain.py - no scikit-learn needed there -
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
    measured = joblib.load(os.path.join(MODELS_DIR, "model.joblib"))
    model = committee.float32_thresholds(measured)
    feats = json.load(open(os.path.join(MODELS_DIR, "feature_names.json")))
    n = len(feats)

    initial_types = [("input", FloatTensorType([None, n]))]
    onx = convert_lightgbm(model, initial_types=initial_types, zipmap=False)

    # Written beside the served files and moved over them once the check passes, so
    # a failed export leaves the served model as it was.
    new_onnx, new_txt = ONNX_PATH + ".new", BOOSTER_PATH + ".new"
    try:
        with open(new_onnx, "wb") as fh:
            fh.write(onx.SerializeToString())
        model.save_model(new_txt)

        # A parity check needs valid inputs, not the whole replay: the file's first rows.
        df = D.build_matrix(CSV, nrows=PARITY_ROWS)
        sample = df.iloc[int(len(df) * 0.80):][feats].astype("float32").values[:2000]

        native = measured.predict(sample)       # the merged booster train.py measured
        sess = ort.InferenceSession(new_onnx, providers=["CPUExecutionProvider"])
        onnx_proba = _onnx_positive_proba(sess.run(None, {sess.get_inputs()[0].name: sample}))

        max_diff = float(np.max(np.abs(native - onnx_proba)))
        print(f"parity vs native LightGBM:  max |delta probability| = {max_diff:.2e}  "
              f"over {len(sample)} rows")
        if max_diff >= 1e-3:
            raise SystemExit("ONNX/native mismatch too large: the served model is unchanged")
    except BaseException:
        for path in (new_onnx, new_txt):
            if os.path.exists(path):
                os.remove(path)
        raise
    os.replace(new_onnx, ONNX_PATH)
    os.replace(new_txt, BOOSTER_PATH)
    print(f"exported {ONNX_PATH}  ({os.path.getsize(ONNX_PATH) / 1024:.0f} KB)")
    print(f"exported {BOOSTER_PATH}  ({os.path.getsize(BOOSTER_PATH) / 1024:.0f} KB, "
          f"{model.num_trees()} trees)")
    print("ONNX model matches the native model - ready for in-Flink serving.")

    # Last, so it records the artefacts as they finally are.
    import manifest
    manifest.write()


if __name__ == "__main__":
    # Refuses unknown flags: a mistyped one would otherwise re-export the model.
    argparse.ArgumentParser(description=__doc__).parse_args()
    main()
