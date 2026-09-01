"""Sweeps capability profiles, retraining and re-exporting for each, and reports
what each capability is worth.
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
ARTEFACTS = ("model.joblib", "model.onnx", "feature_names.json", "metrics.json")

sys.path.insert(0, os.path.join(ROOT, "stream-processor"))
import capabilities as CAP  # noqa: E402


def _train(label, env_overrides):
    """Train one configuration in a fresh interpreter (contracts are import-time)."""
    env = dict(os.environ, **env_overrides)
    proc = subprocess.run([sys.executable, os.path.join(HERE, "train.py")],
                          cwd=HERE, env=env, capture_output=True, text=True)
    if proc.returncode != 0:
        sys.stderr.write(proc.stdout[-2000:] + proc.stderr[-2000:])
        raise SystemExit(f"training failed for {label}")

    with open(os.path.join(MODELS, "metrics.json")) as fh:
        metrics = json.load(fh)
    with open(os.path.join(MODELS, "feature_names.json")) as fh:
        feats = json.load(fh)

    by_type = {}
    for line in proc.stdout.splitlines():
        parts = line.split()
        if len(parts) == 3 and parts[1].endswith("%") and parts[2].startswith("(n="):
            by_type[parts[0]] = parts[1]

    os.makedirs(OUT_DIR, exist_ok=True)
    safe = label.replace("=", "_").replace(" ", "_")
    with open(os.path.join(OUT_DIR, f"{safe}.json"), "w") as fh:
        json.dump({"label": label, "env": env_overrides, "n_features": len(feats),
                   "features": feats, "metrics": metrics,
                   "recall_by_type": by_type}, fh, indent=2)
    return metrics, feats, by_type


def _plan(target):
    """(label, env) pairs to train: baseline first, then the variations."""
    baseline = ("baseline", {})
    if target:
        cap = CAP.BY_KEY.get(target)
        if cap is None:
            raise SystemExit(f"unknown capability {target!r}; "
                             f"choose from {', '.join(CAP.BY_KEY)}")
        if cap.always_on:
            raise SystemExit(f"{target} cannot be switched off: {cap.rationale}")
        var = f"CAP_{cap.key.upper()}"
        return [(f"{cap.key}={m}", {var: m}) for m in cap.modes]

    plan = [baseline]
    for cap in CAP.REGISTRY:
        if cap.always_on:
            continue
        # Flip each capability away from its default, one at a time.
        other = "on" if CAP.MODES[cap.key] == "off" else "off"
        plan.append((f"{cap.key}={other}", {f"CAP_{cap.key.upper()}": other}))
    return plan


def _report(results):
    base_pr = results[0][1]["pr_auc"]
    print(f"\n{'configuration':<24}{'feats':>6}{'PR-AUC':>9}{'delta':>8}"
          f"{'prec@0.50':>11}{'recall':>9}{'F1':>8}")
    for label, metrics, feats, _ in results:
        a = metrics["at_0_50"]
        print(f"{label:<24}{len(feats):>6}{metrics['pr_auc']:>9.3f}"
              f"{metrics['pr_auc'] - base_pr:>+8.3f}"
              f"{a['precision']:>11.3f}{a['recall']:>9.3f}{a['f1']:>8.3f}")

    labels = [r[0] for r in results]
    types = {r[0]: r[3] for r in results}
    all_types = sorted({t for v in types.values() for t in v})
    if all_types:
        # Column heads keep the distinguishing part of each label: when one
        # capability is swept they all share a prefix, so show the mode.
        heads = [l.split("=")[-1] if len(set(
            x.split("=")[0] for x in labels)) == 1 else l for l in labels]
        width = max(10, max(len(h) for h in heads) + 2)
        print("\nrecall by fraud type (ML @0.50):")
        print(f"{'type':<14}" + "".join(f"{h:>{width}}" for h in heads))
        for t in all_types:
            print(f"{t:<14}" + "".join(
                f"{types[l].get(t, '-'):>{width}}" for l in labels))


def main():
    target = sys.argv[1] if len(sys.argv) > 1 else None
    plan = _plan(target)

    backup = os.path.join(MODELS, "_pre_ablation")
    os.makedirs(backup, exist_ok=True)
    for name in ARTEFACTS:
        src = os.path.join(MODELS, name)
        if os.path.exists(src):
            shutil.copy2(src, os.path.join(backup, name))

    try:
        print(CAP.describe())
        print()
        results = []
        for label, env in plan:
            print(f"training {label} ...", flush=True)
            results.append((label,) + _train(label, env))
        _report(results)
        print(f"\nper-run detail written to {OUT_DIR}/")
        print("\nNote: each row is a separately trained model on synthetic data; "
              "figures are design targets, not validated findings. A delta is "
              "the cost of a missing integration, not a flaw in the model.")
    finally:
        for name in ARTEFACTS:
            src = os.path.join(backup, name)
            if os.path.exists(src):
                shutil.copy2(src, os.path.join(MODELS, name))
        shutil.rmtree(backup, ignore_errors=True)
        print("\nmodels/ restored to the pre-ablation state.")


if __name__ == "__main__":
    main()
