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

# _PKG: the ml package driven (train.py, models/); ROOT: the repository.
_PKG = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ROOT = os.path.dirname(_PKG)
GEN_DIR = os.path.join(ROOT, "data-generator")
STATE = os.path.join(_PKG, "models", "ablation", "seeds.json")
SCRATCH = "/tmp/ablation_seeds"

sys.path.insert(0, os.path.join(ROOT, "stream-processor"))
import capabilities as CAP  # noqa: E402

DEFAULT_SEEDS = (42, 7, 13, 99, 2026)


def _alternative(cap):
    """The mode to compare the active one against: the POOREST the capability
    declares - not "off", which payee_identity does not have."""
    poorest = cap.modes[-1]
    return poorest if poorest != CAP.MODES[cap.key] else cap.modes[0]


def _configurations(only=None):
    """Baseline plus each capability flipped away from its default, one at a time.
    `only` restricts the sweep, and sweeps each capability it names across all of
    its declared modes."""
    plan = [("baseline", {})]
    for cap in CAP.REGISTRY:
        if cap.always_on or (only and cap.key not in only):
            continue
        others = ([m for m in cap.modes if m != CAP.MODES[cap.key]] if only
                  else [_alternative(cap)])
        plan += [(f"{cap.key}={m}", {f"CAP_{cap.key.upper()}": m}) for m in others]
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
    proc = subprocess.run([sys.executable, os.path.join(_PKG, "train.py")],
                          cwd=_PKG, env=env, capture_output=True, text=True)
    if proc.returncode != 0:
        sys.stderr.write(proc.stdout[-1500:] + proc.stderr[-1500:])
        raise SystemExit("training failed")
    with open(os.path.join(models, "metrics.json")) as fh:
        metrics = json.load(fh)
    with open(os.path.join(models, "feature_names.json")) as fh:
        n_feats = len(json.load(fh))
    return {"pr_auc": metrics["pr_auc"], "roc_auc": metrics["roc_auc"],
            "precision": metrics["at_review"]["precision"],
            "recall": metrics["at_review"]["recall"],
            "f1": metrics["at_review"]["f1"], "n_features": n_feats}


# Two-sided 95% t critical values by df (n-1) - honest at the small n here.
T95 = {1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571, 6: 2.447,
       7: 2.365, 8: 2.306, 9: 2.262, 10: 2.228, 11: 2.201, 12: 2.179,
       13: 2.160, 14: 2.145, 15: 2.131, 16: 2.120, 17: 2.110, 18: 2.101,
       19: 2.093, 20: 2.086, 24: 2.064, 29: 2.045, 39: 2.023}


def ci95(values):
    """Half-width of the 95% confidence interval for the MEAN delta (standard error,
    not the spread of individual deltas); shared by both sweeps."""
    n = len(values)
    if n < 2:
        return float("inf")
    se = statistics.stdev(values) / math.sqrt(n)
    df = n - 1
    t = T95.get(df) or min(T95[k] for k in T95 if k >= df) if df <= 39 else 1.96
    return t * se


def sweep_features(seeds):
    """What each COLUMN contributes, which the capability sweep cannot ask: eleven
    features are always on, so no toggle removes them. Reported one at a time (the
    marginal value - redundant columns score zero) and together (the joint value of
    everything the first pass called negligible), because either alone misleads."""
    import lightgbm as lgb
    from sklearn.metrics import average_precision_score
    sys.path.insert(0, _PKG)
    import dataset as D

    feats = list(D.FEATURE_NAMES)
    per = {f: [] for f in feats}
    bases = []
    for seed in seeds:
        df = D.build_matrix(_dataset(seed))
        cut = int(len(df) * 0.80)
        tr, te = df.iloc[:cut], df.iloc[cut:]
        ytr, yte = tr.label.values, te.label.values

        def fit(cols):
            m = lgb.LGBMClassifier(
                n_estimators=400, learning_rate=0.05, num_leaves=31,
                colsample_bytree=0.8, min_child_samples=30,
                scale_pos_weight=(ytr == 0).sum() / max(int(ytr.sum()), 1),
                random_state=42, n_jobs=-1, verbose=-1)
            m.fit(tr[cols].astype("float32").values, ytr)
            return average_precision_score(
                yte, m.predict_proba(te[cols].astype("float32").values)[:, 1])

        base = fit(feats)
        bases.append(base)
        for f in feats:
            per[f].append(fit([x for x in feats if x != f]) - base)
        print(f"  seed {seed:>5}  baseline {base:.4f}", flush=True)

    print(f"\nbaseline {statistics.mean(bases):.4f} "
          f"+/-{statistics.stdev(bases):.4f} over {len(seeds)} seeds")
    print("\nDelta is what DROPPING the column does; negative means it carried")
    print("signal. Paired within seed, 95% t-interval for the mean.\n")
    print(f"{'column dropped':<26}{'mean':>9}{'95% CI':>22}{'sign':>7}  verdict")
    negligible = []
    for f in sorted(feats, key=lambda k: statistics.mean(per[k])):
        d = per[f]
        m, half = statistics.mean(d), ci95(d)
        agree = sum(1 for x in d if (x > 0) == (m > 0))
        if m + half < 0:
            v = "carries signal"
        else:
            v = "not distinguishable alone"
            negligible.append(f)
        print(f"{f:<26}{m:>+9.4f}   [{m - half:+.4f},{m + half:+.4f}]"
              f"{agree:>5}/{len(d)}  {v}")

    if not negligible:
        return
    print(f"\nAll {len(negligible)} 'not distinguishable alone' columns removed "
          f"TOGETHER:")
    joint = []
    keep = [f for f in feats if f not in negligible]
    for seed in seeds:
        df = D.build_matrix(_dataset(seed))
        cut = int(len(df) * 0.80)
        tr, te = df.iloc[:cut], df.iloc[cut:]
        ytr, yte = tr.label.values, te.label.values

        def fit(cols):
            m = lgb.LGBMClassifier(
                n_estimators=400, learning_rate=0.05, num_leaves=31,
                colsample_bytree=0.8, min_child_samples=30,
                scale_pos_weight=(ytr == 0).sum() / max(int(ytr.sum()), 1),
                random_state=42, n_jobs=-1, verbose=-1)
            m.fit(tr[cols].astype("float32").values, ytr)
            return average_precision_score(
                yte, m.predict_proba(te[cols].astype("float32").values)[:, 1])
        joint.append(fit(keep) - fit(feats))
    m, half = statistics.mean(joint), ci95(joint)
    print(f"  {len(keep)} columns instead of {len(feats)}: "
          f"{m:+.4f} [{m - half:+.4f},{m + half:+.4f}]")
    print("  Compare that against the largest SINGLE delta above. If it is "
          "bigger,\n  the columns are redundant with each other, not worthless "
          "- and the\n  one-at-a-time column is not a removal list.")


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
            print(f"\nbudget reached - {left} run(s) left; "
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

        # Four outcomes, "nothing was measured" first - it reads like "no effect" and
        # means the opposite. A delta of EXACTLY zero on every seed means both arms
        # trained the same model: a statement about the harness or the dataset, not the
        # capability.
        untested = all(p == 0.0 for p in pairs) and len(pairs) > 1

        excludes_zero = abs(dm) > half
        if untested:
            verdict = "NOT MEASURED"
        elif excludes_zero and abs(dm) >= 0.005:
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
          "not the spread of\nindividual deltas - the question is whether the "
          "mean differs from zero.\n'sign' counts seeds agreeing with the "
          "mean's direction.\n")
    print("  real          CI excludes zero and |mean| >= 0.005 PR-AUC")
    print("  no effect     CI excludes zero but the effect is too small to act on,")
    print("                or the mean is under 0.005 either way")
    print("  unresolved    CI straddles zero - add seeds before quoting it")
    print("  NOT MEASURED  every seed gave a delta of exactly zero, so both arms")
    print("                trained the same model. Nothing about the capability")
    print("                follows from this row - fix the data, not the reading.")


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    ap.add_argument("--seeds", default=",".join(str(s) for s in DEFAULT_SEEDS))
    ap.add_argument("--budget", type=float, default=35.0,
                    help="seconds to work before saving and exiting")
    ap.add_argument("--report", action="store_true", help="print results only")
    ap.add_argument("--reset", action="store_true", help="discard progress")
    ap.add_argument("--features", action="store_true",
                    help="sweep individual FEATURE columns instead of "
                         "capabilities - the only way the eleven in "
                         "core_history get measured at all")
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
    if args.features:
        return sweep_features(seeds)

    results, done = run(seeds, args.budget, only)
    if done:
        report(results, only)


if __name__ == "__main__":
    main()
