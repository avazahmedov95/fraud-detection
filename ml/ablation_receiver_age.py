"""Ablation over the receiver_age modes: always / on_us / off.
on_us is the honest default - the age is resolvable for 6.85% of transfers.
"""

import json
import os
import shutil
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
MODELS = os.path.join(HERE, "models")
OUT_DIR = os.path.join(MODELS, "ablation")
MODES = ("always", "on_us", "off")
ARTEFACTS = ("model.joblib", "model.onnx", "feature_names.json", "metrics.json")


def _train(mode):
    """Train under one mode in a fresh interpreter (the contract is import-time)."""
    env = dict(os.environ, RECEIVER_AGE_MODE=mode)
    proc = subprocess.run([sys.executable, os.path.join(HERE, "train.py")],
                          cwd=HERE, env=env, capture_output=True, text=True)
    if proc.returncode != 0:
        sys.stderr.write(proc.stdout[-2000:] + proc.stderr[-2000:])
        raise SystemExit(f"training failed for mode={mode}")

    with open(os.path.join(MODELS, "metrics.json")) as fh:
        metrics = json.load(fh)
    with open(os.path.join(MODELS, "feature_names.json")) as fh:
        feats = json.load(fh)

    # train.py prints "  APP   83.9%  (n=62)" per fraud type.
    by_type = {}
    for line in proc.stdout.splitlines():
        parts = line.split()
        if len(parts) == 3 and parts[1].endswith("%") and parts[2].startswith("(n="):
            by_type[parts[0]] = parts[1]

    os.makedirs(OUT_DIR, exist_ok=True)
    with open(os.path.join(OUT_DIR, f"{mode}.json"), "w") as fh:
        json.dump({"mode": mode, "n_features": len(feats), "features": feats,
                   "metrics": metrics, "recall_by_type": by_type}, fh, indent=2)
    return metrics, feats, by_type


def _on_us_share():
    """Share of transfers the sending bank could resolve in-house.
    From the PANs via bins.py, not the CSV's bank-name columns: the issuer the PIPELINE
    sees comes from the BIN table, so a number taken from those columns would describe
    something other than what ran.
    """
    import pandas as pd
    import bins as B
    csv = os.path.join(ROOT, "data-generator", "out", "transactions.csv")
    d = pd.read_csv(csv, usecols=["sender_card", "receiver_card"], dtype=str)
    s = d.sender_card.map(B.issuer_of)
    r = d.receiver_card.map(B.issuer_of)
    return ((s != "") & (r != "") & (s == r)).mean()


def main():
    backup = os.path.join(MODELS, "_pre_ablation")
    os.makedirs(backup, exist_ok=True)
    for name in ARTEFACTS:
        src = os.path.join(MODELS, name)
        if os.path.exists(src):
            shutil.copy2(src, os.path.join(backup, name))

    try:
        share = _on_us_share()
        print(f"on-us share in the dataset: {share:.1%}   <- the ceiling on "
              f"receiver_age coverage in 'on_us' mode\n")

        results = []
        for mode in MODES:
            print(f"training mode={mode} ...", flush=True)
            results.append((mode,) + _train(mode))

        base_pr = results[0][1]["pr_auc"]
        print(f"\n{'mode':<9}{'feats':>6}{'PR-AUC':>9}{'delta':>8}"
              f"{'prec@0.50':>11}{'recall':>9}{'F1':>8}")
        for mode, metrics, feats, _ in results:
            a = metrics["at_0_50"]
            print(f"{mode:<9}{len(feats):>6}{metrics['pr_auc']:>9.3f}"
                  f"{metrics['pr_auc'] - base_pr:>+8.3f}"
                  f"{a['precision']:>11.3f}{a['recall']:>9.3f}{a['f1']:>8.3f}")

        types = {mode: by_type for mode, _, _, by_type in results}
        all_types = sorted({t for v in types.values() for t in v})
        if all_types:
            print("\nrecall by fraud type (ML @0.50):")
            print(f"{'type':<14}" + "".join(f"{m:>10}" for m in MODES))
            for t in all_types:
                print(f"{t:<14}" + "".join(f"{types[m].get(t, '-'):>10}"
                                           for m in MODES))

        print(f"\nper-mode detail written to {OUT_DIR}/")
        print("\nNote: each mode is a separately trained model on synthetic "
              "data; figures are design targets, not validated findings.")
        print("The gap between 'always' and 'on_us' is the cost of the missing "
              "inter-bank data exchange, not a deficiency of the model.")
    finally:
        for name in ARTEFACTS:
            src = os.path.join(backup, name)
            if os.path.exists(src):
                shutil.copy2(src, os.path.join(MODELS, name))
        shutil.rmtree(backup, ignore_errors=True)
        print("\nmodels/ restored to the pre-ablation state.")


if __name__ == "__main__":
    main()
