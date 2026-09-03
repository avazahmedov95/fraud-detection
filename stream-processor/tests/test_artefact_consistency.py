"""The deployed artefacts must agree with each other and with the code.

Three files describe the same 24 columns in the same order: the capability
registry (which generates FEATURE_NAMES), feature_names.json (which labels the
model's contributions in case-manager/explain.py), and the model itself. The
vector is passed positionally, so a reordered registry trains and serves happily
while every SHAP contribution is attributed to the wrong feature - not a crash,
but a confident, specific, wrong reason. Skipped when the artefacts are absent.
"""

import json
import os

import pytest

import features as F

# ../../ml/models: this file sits two levels below the repository root.
_MODELS = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                       "..", "..", "ml", "models")
_NAMES = os.path.join(_MODELS, "feature_names.json")


def _names():
    if not os.path.exists(_NAMES):
        pytest.skip("no feature_names.json; model has not been exported")
    with open(_NAMES, encoding="utf-8") as fh:
        return json.load(fh)


def test_exported_names_match_the_capability_registry():
    """feature_names.json is written at export from the registry's order; a change
    without a re-export would otherwise stay silent, the vector being positional."""
    assert _names() == list(F.FEATURE_NAMES), (
        "feature_names.json no longer matches capabilities.feature_names(). "
        "Re-run ml/train.py and ml/export_onnx.py, or revert the registry "
        "change - a model served against a different order scores nonsense.")


def test_the_booster_has_the_same_number_of_features_as_the_names():
    """explain.py labels contribution i with names[i], and the booster carries only
    positional names (Column_0..N), so the count is all that catches a stale one."""
    booster_path = os.path.join(_MODELS, "model.txt")
    if not os.path.exists(booster_path):
        pytest.skip("no model.txt; run ml/export_onnx.py")
    lgb = pytest.importorskip("lightgbm")
    n = lgb.Booster(model_file=booster_path).num_feature()
    assert n == len(_names()), (
        f"model.txt expects {n} features, feature_names.json lists "
        f"{len(_names())}. One of the two artefacts is stale; explanations "
        f"would be attributed to the wrong features.")


def test_the_onnx_model_takes_the_same_width():
    onnx_path = os.path.join(_MODELS, "model.onnx")
    if not os.path.exists(onnx_path):
        pytest.skip("no model.onnx; run ml/export_onnx.py")
    ort = pytest.importorskip("onnxruntime")
    sess = ort.InferenceSession(onnx_path, providers=["CPUExecutionProvider"])
    shape = sess.get_inputs()[0].shape
    assert shape[-1] == len(_names()), (
        f"model.onnx takes {shape[-1]} inputs, feature_names.json lists "
        f"{len(_names())}. The serving artefact and the training contract "
        f"disagree.")
