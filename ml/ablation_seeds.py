"""Runs the capability ablation across generator seeds and reports paired deltas
with intervals, so an effect is not read off a single run.
"""

import argparse
import hashlib
import json
import math
import os
import statistics
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
GEN_DIR = os.path.join(ROOT, "data-generator")
STATE = os.path.join(HERE, "models", "ablation", "seeds.json")
SCRATCH = "/tmp/ablation_seeds"

sys.path.insert(0, os.path.join(ROOT, "stream-processor"))
import capabilities as CAP  # noqa: E402

DEFAULT_SEEDS = (42, 7, 13, 99, 2026)


def _configurations(only=None):
    """Baseline plus each capability flipped away from its default, one at a time.
    `only` restricts the sweep - resolving one borderline delta needs many seeds."""
    plan = [("baseline", {})]
    for cap in CAP.REGISTRY:
        if cap.always_on or (only and cap.key not in only):
            continue
        other = "on" if CAP.MODES[cap.key] == "off" else "off"
        plan.append((f"{cap.key}={other}", {f"CAP_{cap.key.upper()}": other}))
    return plan


def _dataset(seed):
    """Generate (once per generator version) the dataset for a seed."""
    # Cached under the generator fingerprint, so a generator change produces fresh data.
    tag = _contract_fingerprint().split("gen-")[-1]
    out = os.path.join(SCRATCH, f"gen{tag}", f"seed{seed}")
    csv = os.path.join(out, "transactions.csv")
    if os.path.exists(csv):
        return csv
    os.makedirs(out, exist_ok=True)
    proc = subprocess.run(
        [sys.executable, "generator.py", "--seed", str(seed), "--out", out],
        cwd=GEN_DIR, capture_output=True, text=True)
    if proc.returncode != 0:
        sys.stderr.write(proc.stdout[-1500:] + proc.stderr[-1500:])
        raise SystemExit(f"generation failed for seed={seed}")
    return csv


def _train(csv, env_overrides):
    models = os.path.join(SCRATCH, "models")
    os.makedirs(models, exist_ok=True)
    env = dict(os.environ, DATASET_CSV=csv, MODELS_DIR=models, **env_overrides)
    proc = subprocess.run([sys.executable, os.path.join(HERE, "train.py")],
                          cwd=HERE, env=env, capture_output=True, text=True)
    if proc.returncode != 0:
        sys.stderr.write(proc.stdout[-1500:] + proc.stderr[-1500:])
        raise SystemExit("training failed")
    with open(os.path.join(models, "metrics.json")) as fh:
        metrics = json.load(fh)
    with open(os.path.join(models, "feature_names.json")) as fh:
        n_feats = len(json.load(fh))
    return {"pr_auc": metrics["pr_auc"], "roc_auc": metrics["roc_auc"],
            "precision": metrics["at_0_50"]["precision"],
            "recall": metrics["at_0_50"]["recall"],
            "f1": metrics["at_0_50"]["f1"], "n_features": n_feats}


GEN_SOURCES = ("config.py", "persons.py", "events.py", "fraud_patterns.py",
               "generator.py", "travel.py")


def _contract_fingerprint():
    """What the stored results depend on: the feature set AND the generator.
    Adding a feature moves the baseline; so does changing the generator, and old deltas
    then describe a different experiment. Both were learned the hard way."""
    feats = "|".join(sorted(f for cap in CAP.REGISTRY for f in cap.features))
    h = hashlib.sha256()
    for name in GEN_SOURCES:
        path = os.path.join(GEN_DIR, name)
        if os.path.exists(path):
            with open(path, "rb") as fh:
                h.update(fh.read())
    return f"{feats}::gen-{h.hexdigest()[:12]}"


def _load():
    if not os.path.exists(STATE):
        return {}
    with open(STATE) as fh:
        blob = json.load(fh)
    stored = blob.get("_contract")
    if stored != _contract_fingerprint():
        raise SystemExit(
            "stored results were produced with a different feature set or a "
            "different generator, so their baseline no longer applies.\n"
            "Re-run with --reset to discard them, or move "
            "models/ablation/seeds.json aside to keep them.")
    return {k: v for k, v in blob.items() if not k.startswith("_")}


def _save(results):
    os.makedirs(os.path.dirname(STATE), exist_ok=True)
    with open(STATE, "w") as fh:
        json.dump({"_contract": _contract_fingerprint(), **results}, fh, indent=2)


def run(seeds, budget, only=None):
    results = _load()
    configs = _configurations(only)
    deadline = time.time() + budget
    todo = [(s, label, env) for s in seeds for label, env in configs
            if str(s) not in results or label not in results.get(str(s), {})]

    if not todo:
        print("grid complete.")
        return results, True

    print(f"{len(todo)} run(s) left in the grid")
    for seed, label, env in todo:
        if time.time() >= deadline:
            left = sum(1 for s, l, _ in todo
                       if l not in results.get(str(s), {}))
            print(f"\nbudget reached — {left} run(s) left; "
                  f"invoke again to continue.")
            return results, False
        csv = _dataset(seed)
        print(f"  seed={seed:<6} {label}", flush=True)
        results.setdefault(str(seed), {})[label] = _train(csv, env)
        _save(results)

    return results, True


def report(results, only=None):
    if not results:
        print("no results yet.")
        return
    configs = [label for label, _ in _configurations(only)]
    seeds = sorted(results, key=int)
    # Only seeds carrying every configuration compared, so the paired deltas come from
    # one consistent set of datasets.
    seeds = [s for s in seeds if all(c in results[s] for c in configs)]
    if not seeds:
        print("no seed has all the requested configurations yet.")
        return
    print(f"\nseeds: {', '.join(seeds)}   ({len(seeds)} dataset regenerations)\n")

    def stat(values):
        m = statistics.mean(values)
        s = statistics.stdev(values) if len(values) > 1 else 0.0
        return m, s

    # Two-sided 95% t critical values by df (n-1) - honest at the small n here.
    T95 = {1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571, 6: 2.447,
           7: 2.365, 8: 2.306, 9: 2.262, 10: 2.228, 11: 2.201, 12: 2.179,
           13: 2.160, 14: 2.145, 15: 2.131, 16: 2.120, 17: 2.110, 18: 2.101,
           19: 2.093, 20: 2.086, 24: 2.064, 29: 2.045, 39: 2.023}

    def ci95(values):
        """Half-width of the 95% confidence interval for the MEAN.
        Governed by the standard error (sd/sqrt(n)), not the spread of individual deltas:
        an earlier revision compared a mean against a standard deviation, understating
        the evidence and calling established effects unresolved."""
        n = len(values)
        if n < 2:
            return float("inf")
        se = statistics.stdev(values) / math.sqrt(n)
        df = n - 1
        t = T95.get(df) or min(T95[k] for k in T95 if k >= df) if df <= 39 else 1.96
        return t * se

    print(f"{'configuration':<24}{'PR-AUC mean+/-sd':>20}"
          f"{'delta (95% CI)':>28}{'sign':>8}{'verdict':>13}")
    for label in configs:
        vals = [results[s][label]["pr_auc"] for s in seeds if label in results[s]]
        if not vals:
            continue
        m, sd = stat(vals)
        if label == "baseline":
            print(f"{label:<24}{m:>13.3f} +/-{sd:<4.3f}"
                  f"{'':>28}{'':>8}{'':>13}")
            continue
        # Paired deltas: same dataset, so the between-seed variation drops out.
        pairs = [results[s][label]["pr_auc"] - results[s]["baseline"]["pr_auc"]
                 for s in seeds if label in results[s] and "baseline" in results[s]]
        dm, _ = stat(pairs)
        half = ci95(pairs)
        agree = sum(1 for p in pairs if (p < 0) == (dm < 0))

        # Three outcomes, not two: "cannot tell yet" is not "no effect".
        excludes_zero = abs(dm) > half
        if excludes_zero and abs(dm) >= 0.005:
            verdict = "real"
        elif excludes_zero or abs(dm) < 0.005:
            verdict = "no effect"
        else:
            verdict = "unresolved"

        lo, hi = dm - half, dm + half
        print(f"{label:<24}{m:>13.3f} +/-{sd:<4.3f}"
              f"{dm:>+11.3f}  [{lo:+.3f},{hi:+.3f}]"
              f"{agree:>5}/{len(pairs):<3}{verdict:>13}")

    print("\nDeltas are paired within each seed, so between-seed variation "
          "cancels out.\nThe interval is a 95% CI for the MEAN delta (t-based), "
          "not the spread of\nindividual deltas — the question is whether the "
          "mean differs from zero.\n'sign' counts seeds agreeing with the "
          "mean's direction.\n")
    print("  real        CI excludes zero and |mean| >= 0.005 PR-AUC")
    print("  no effect   CI excludes zero but the effect is too small to act on,")
    print("              or the mean is under 0.005 either way")
    print("  unresolved  CI straddles zero — add seeds before quoting it")


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    ap.add_argument("--seeds", default=",".join(str(s) for s in DEFAULT_SEEDS))
    ap.add_argument("--budget", type=float, default=35.0,
                    help="seconds to work before saving and exiting")
    ap.add_argument("--report", action="store_true", help="print results only")
    ap.add_argument("--reset", action="store_true", help="discard progress")
    ap.add_argument("--only", default=None,
                    help="comma-separated capabilities to sweep (default: all)")
    args = ap.parse_args()
    only = ([c.strip() for c in args.only.split(",")] if args.only else None)

    if args.reset and os.path.exists(STATE):
        os.remove(STATE)
        print("progress discarded.")

    if args.report:
        report(_load(), only)
        return

    seeds = [int(s) for s in args.seeds.split(",") if s.strip()]
    results, done = run(seeds, args.budget, only)
    if done:
        report(results, only)


if __name__ == "__main__":
    main()
