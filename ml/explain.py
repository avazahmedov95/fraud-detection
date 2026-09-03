"""SHAP over the trained model: global importance plots and a per-alert view."""

import json
import os

import joblib
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import shap

import dataset as D

MODELS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "models")
CSV = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                   "..", "data-generator", "out", "transactions.csv")


def _positive_class(sv):
    """Normalise SHAP output to a 2-D (n, n_features) array for the fraud class."""
    if isinstance(sv, list):
        sv = sv[1] if len(sv) > 1 else sv[0]
    sv = np.asarray(sv)
    if sv.ndim == 3:
        sv = sv[:, :, 1] if sv.shape[2] == 2 else sv[:, :, 0]
    return sv


def main():
    model = joblib.load(os.path.join(MODELS_DIR, "model.joblib"))
    feats = json.load(open(os.path.join(MODELS_DIR, "feature_names.json")))

    df = D.build_matrix(CSV)
    test = df.iloc[int(len(df) * 0.80):]
    X = test[feats].astype("float32").reset_index(drop=True)
    y = test["label"].values

    explainer = shap.TreeExplainer(model)
    shap_values = _positive_class(explainer.shap_values(X))

    plt.figure()
    shap.summary_plot(shap_values, X, max_display=15, show=False)
    plt.tight_layout(); plt.savefig(os.path.join(MODELS_DIR, "shap_summary.png"), dpi=130); plt.close()

    plt.figure()
    shap.summary_plot(shap_values, X, plot_type="bar", max_display=15, show=False)
    plt.tight_layout(); plt.savefig(os.path.join(MODELS_DIR, "shap_importance.png"), dpi=130); plt.close()

    mean_abs = np.abs(shap_values).mean(axis=0)
    order = np.argsort(mean_abs)[::-1]
    print("global feature importance (mean |SHAP|):")
    for i in order[:10]:
        print(f"  {feats[i]:<20} {mean_abs[i]:.4f}")

    proba = model.predict_proba(X.values)[:, 1]
    fraud_idx = [i for i in range(len(y)) if y[i] == 1 and proba[i] >= 0.5]
    if fraud_idx:
        j = max(fraud_idx, key=lambda i: proba[i])
        contrib = sorted(zip(feats, shap_values[j], X.iloc[j].values),
                         key=lambda t: abs(t[1]), reverse=True)
        print(f"\nper-alert explanation (fraud, model p={proba[j]:.3f}) — top reason codes:")
        for name, sv, val in contrib[:6]:
            direction = "+risk" if sv > 0 else "-risk"
            print(f"  {name:<20} value={val:<10.3f} SHAP={sv:+.3f}  ({direction})")

    print(f"\nsaved shap_summary.png and shap_importance.png to {MODELS_DIR}/")


if __name__ == "__main__":
    main()
