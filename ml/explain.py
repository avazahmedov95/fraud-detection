"""Exact tree contributions over the served model: global importance and one alert.

These are SHAP values - LightGBM's own TreeSHAP (`pred_contrib`), not the `shap`
package, which cannot load on the owner's machine (its numba DLL is blocked by an
application-control policy). Same algorithm, and the same call the case view uses
per alert (`case-manager/explain.py`), so the global picture and the sentence an
analyst reads come from one implementation.
"""

import json
import os

import lightgbm as lgb
import matplotlib
import numpy as np

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

import dataset as D  # noqa: E402

MODELS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "models")
CSV = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                   "..", "data-generator", "out", "transactions.csv")
TOP = 15


def _bar(mean_abs, feats, path):
    order = np.argsort(mean_abs)[::-1][:TOP][::-1]
    plt.figure(figsize=(7, 0.32 * len(order) + 1))
    plt.barh([feats[i] for i in order], mean_abs[order], color="#3b6ea5")
    plt.xlabel("mean |contribution| to the log-odds")
    plt.title("Global importance, held-out slice")
    plt.tight_layout(); plt.savefig(path, dpi=130); plt.close()


def _beeswarm(contrib, X, mean_abs, feats, path):
    """One row per feature: every held-out transfer's contribution, coloured by where
    that transfer sits in the feature's own distribution."""
    order = np.argsort(mean_abs)[::-1][:TOP][::-1]
    rng = np.random.default_rng(0)
    plt.figure(figsize=(7.5, 0.42 * len(order) + 1.2))
    for row, i in enumerate(order):
        v = X[:, i]
        lo, hi = np.percentile(v, 1), np.percentile(v, 99)
        colour = np.clip((v - lo) / max(hi - lo, 1e-9), 0, 1)
        plt.scatter(contrib[:, i], row + rng.normal(0, 0.08, len(v)),
                    c=colour, cmap="coolwarm", s=2, alpha=0.35, linewidths=0)
    plt.yticks(range(len(order)), [feats[i] for i in order], fontsize=8)
    plt.axvline(0, color="#888", lw=0.6)
    plt.xlabel("contribution to the log-odds (red = high value of that feature)")
    plt.title("Where each feature pushes the score, held-out slice")
    plt.tight_layout(); plt.savefig(path, dpi=130); plt.close()


def main():
    booster = lgb.Booster(model_file=os.path.join(MODELS_DIR, "model.txt"))
    feats = json.load(open(os.path.join(MODELS_DIR, "feature_names.json")))

    df = D.build_matrix(CSV)
    test = df.iloc[int(len(df) * 0.80):].reset_index(drop=True)
    X = test[feats].astype("float32").values
    y = test["label"].values

    # (n, n_features + 1): the last column is the model's base value.
    contrib = booster.predict(X, pred_contrib=True)[:, :len(feats)]
    mean_abs = np.abs(contrib).mean(axis=0)

    print(f"held-out slice: {len(y):,} transfers, {int(y.sum())} fraud")
    print("\nglobal importance (mean |contribution|):")
    for i in np.argsort(mean_abs)[::-1][:TOP]:
        print(f"  {feats[i]:<28}{mean_abs[i]:.4f}")

    _bar(mean_abs, feats, os.path.join(MODELS_DIR, "shap_importance.png"))
    _beeswarm(contrib, X, mean_abs, feats, os.path.join(MODELS_DIR, "shap_summary.png"))

    proba = booster.predict(X)
    caught = [i for i in range(len(y)) if y[i] == 1 and proba[i] >= 0.5]
    if caught:
        j = caught[0]
        print(f"\none caught fraud (score {proba[j]:.3f}), what moved it:")
        for name, c, v in sorted(zip(feats, contrib[j], X[j]),
                                 key=lambda t: -abs(t[1]))[:6]:
            print(f"  {name:<28}{c:+.3f}   value {v:,.2f}")
    print(f"\nsaved shap_importance.png and shap_summary.png to {MODELS_DIR}/")


if __name__ == "__main__":
    main()
