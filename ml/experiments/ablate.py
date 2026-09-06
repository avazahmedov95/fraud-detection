"""Sweeps capability profiles, retraining for each, and reports what each is worth.

    python ablate.py                 every capability, against its poorest mode
    python ablate.py receiver_age    one capability across all its modes

The receiver_age sweep used to be a second file, and it had gone stale in a way
that does not announce itself: it set `RECEIVER_AGE_MODE`, the switch this
project used before capabilities.py existed. Nothing reads that name from the
environment now - `capabilities._configured` reads `CAP_RECEIVER_AGE` - so the
harness trained the DEFAULT configuration three times and printed the
differences between three training runs as the cost of losing an integration.
It ran to completion, wrote three files, and raised nothing. The JSONs already
in models/ablation/ predate the migration and were valid when taken; the harness
was not repaired but replaced by this one, which drives the same three modes
through the switch that works.
"""

import json
import os
import shutil
import subprocess
import sys

# A harness lives one level down. _PKG is the ml package it drives - where
# train.py and models/ are - and ROOT is the repository. Everything this
# writes belongs to the package, not to this directory.
_PKG = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ROOT = os.path.dirname(_PKG)
MODELS = os.path.join(_PKG, "models")
OUT_DIR = os.path.join(MODELS, "ablation")
ARTEFACTS = ("model.joblib", "model.onnx", "feature_names.json", "metrics.json")

sys.path.insert(0, os.path.join(ROOT, "stream-processor"))
import capabilities as CAP  # noqa: E402


def _train(label, env_overrides):
    """Train one configuration in a fresh interpreter (contracts are import-time)."""
    env = dict(os.environ, **env_overrides)
    proc = subprocess.run([sys.executable, os.path.join(_PKG, "train.py")],
                          cwd=_PKG, env=env, capture_output=True, text=True)
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


def _alternative(cap):
    """The mode to compare the active one against: the POOREST the capability
    declares, since the question is what losing it costs.

    Not the literal string "off". Two capabilities do not have one - receiver_age
    is (always, on_us, off) and payee_identity is (card, pinfl) - and assuming it
    made `ablate.py` with no argument die on the payee_identity arm, because
    capabilities._configured rejects a mode outside the declared set. It died
    loudly, which is why this is a bug rather than an entry in the silent-failure
    catalogue.
    """
    poorest = cap.modes[-1]
    return poorest if poorest != CAP.MODES[cap.key] else cap.modes[0]


def _plan(target):
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
        other = _alternative(cap)
        plan.append((f"{cap.key}={other}", {f"CAP_{cap.key.upper()}": other}))
    return plan


def _on_us_share():
    """Share of transfers the sending bank could resolve in-house - the ceiling
    on receiver_age coverage in 'on_us' mode.

    From the PANs via bins.py, not the CSV's bank-name columns: the issuer the
    PIPELINE sees comes from the BIN table, so a number taken from those columns
    would describe something other than what ran.
    """
    import pandas as pd
    import bins as B
    csv = os.path.join(ROOT, "data-generator", "out", "transactions.csv")
    d = pd.read_csv(csv, usecols=["sender_card", "receiver_card"], dtype=str)
    s = d.sender_card.map(B.issuer_of)
    r = d.receiver_card.map(B.issuer_of)
    return ((s != "") & (r != "") & (s == r)).mean()


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
        # Column heads show the mode: when one capability is swept the labels share a prefix.
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
        if target == "receiver_age":
            print(f"\non-us share in the dataset: {_on_us_share():.1%}   <- the "
                  f"ceiling on receiver_age coverage in 'on_us' mode")
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
        if target == "receiver_age":
            print("The gap between 'always' and 'on_us' is the cost of the "
                  "missing inter-bank data exchange, not a deficiency of the "
                  "model.")
    finally:
        for name in ARTEFACTS:
            src = os.path.join(backup, name)
            if os.path.exists(src):
                shutil.copy2(src, os.path.join(MODELS, name))
        shutil.rmtree(backup, ignore_errors=True)
        print("\nmodels/ restored to the pre-ablation state.")


if __name__ == "__main__":
    main()
