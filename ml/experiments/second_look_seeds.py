"""Does the second look pay on data it was not chosen on? The realistic profile -
the dataset of record's recipe - under twenty generator seeds: other people, other
fraud, about 176 held-out frauds each. Every seed is read as the system decides on
its own data: the served recipe fitted, its cut-off and the band chosen on the
cutoff rows, TabPFN's context and cut-off as ml/second_look.py chooses them, and the
held-out rows read once. Three systems on each:

  model    the served model alone, as before the second look
  lowered  the model with its cut-off lowered to raise as many alerts as TabPFN
  second   the model with the second look

Two environments, as ml/ is split: `prepare` generates, featurises and fits in the
project's (.venv311, where the generator's numpy is pinned); `look` asks TabPFN in
.venv-models and stores each seed as it finishes; `report` reads what is stored.
A seed done is not done again, and results from another feature set or generator
are refused.

    python experiments/second_look_seeds.py prepare
    python experiments/second_look_seeds.py look --tabpfn-model <checkpoint>
    python experiments/second_look_seeds.py report
"""
import argparse
import hashlib
import json
import os
import statistics
import subprocess
import sys
import time

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import models as M                                      # noqa: E402
import second_opinion as SO                             # noqa: E402
from ablate_seeds import GEN_DIR, _contract_fingerprint, ci95  # noqa: E402
from recall import DEFAULT_SEEDS                        # noqa: E402

STATE = os.path.join(M._PKG, "models", "ablation", "second_look_seeds.json")
SYSTEMS = ("model", "lowered", "second")


def _scratch(seed):
    """Prepared seeds live under the contract they were made with."""
    tag = hashlib.sha256(_contract_fingerprint().encode()).hexdigest()[:12]
    return os.path.join("/tmp/second_look_seeds", tag, f"seed{seed}")


def prepare(seed):
    """Generate, featurise and fit one seed, and keep only what `look` reads: the
    context pieces, the two bands and the model's own figures. The CSV goes."""
    import second_look as SL
    from sklearn.metrics import average_precision_score
    out = _scratch(seed)
    done = os.path.join(out, "prepared.npz")
    if os.path.exists(done):
        return
    os.makedirs(out, exist_ok=True)
    started = time.time()
    subprocess.run([sys.executable, "generator.py", "--profile", "realistic",
                    "--seed", str(seed), "--out", out],
                   cwd=GEN_DIR, check=True, capture_output=True)
    generated = time.time()
    df = M.D.build_matrix(os.path.join(out, "transactions.csv"))
    X = df[list(M.D.FEATURE_NAMES)].astype("float32").values
    y = df["label"].values.astype("int8")
    featurised = time.time()

    cut = M.T.cut_index(len(y))
    fit = int(cut * M.T.FIT_SHARE)
    Xfit, yfit, Xva, yva, Xte, yte = X[:fit], y[:fit], X[fit:cut], y[fit:cut], X[cut:], y[cut:]
    members = M._committee(Xfit, yfit, M.T.class_weight(int(yfit.sum()), int((yfit == 0).sum())))
    pva, pte = M._committee_score(members, Xva), M._committee_score(members, Xte)
    review = M.T.choose_review_cutoff(yva, pva)
    band_va = SO.band_of(pva, review, SO.served_band(len(yva)))
    floor = pva[band_va[-1]]
    band_te = np.flatnonzero((pte >= floor) & (pte < review))
    band_te = band_te[np.argsort(-pte[band_te])]
    pieces = M._pieces(yfit)[:SL.PIECES]
    alert = pte >= review
    np.savez_compressed(
        done, ctx_X=Xfit[np.concatenate(pieces)], ctx_y=yfit[np.concatenate(pieces)],
        piece=np.repeat(np.arange(len(pieces)), [len(p) for p in pieces]),
        va_X=Xva[band_va], va_y=yva[band_va], te_X=Xte[band_te], te_y=yte[band_te],
        pr_auc=average_precision_score(yte, pte), alerts=int(alert.sum()),
        caught=int(yte[alert].sum()), frauds=int(yte.sum()))
    for name in ("transactions.csv", "persons.csv"):
        os.remove(os.path.join(out, name))
    print(f"seed {seed}: generated in {generated - started:.0f} s, featurised in "
          f"{featurised - generated:.0f} s, fitted in {time.time() - featurised:.0f} s; "
          f"{int(yte.sum())} held-out frauds, the band {len(band_te)} rows", flush=True)


def _rates(alerts, caught, frauds):
    p, r = caught / max(alerts, 1), caught / max(frauds, 1)
    return {"alerts": alerts, "caught": caught, "precision": p, "recall": r,
            "f1": 2 * p * r / max(p + r, 1e-12)}


def look(seed, tabpfn_model):
    """TabPFN on one prepared seed, and the three systems read on its held-out rows."""
    import second_look as SL
    with np.load(os.path.join(_scratch(seed), "prepared.npz")) as z:
        z = dict(z)
    n = len(z["va_y"])
    pieces = [np.flatnonzero(z["piece"] == k) for k in range(int(z["piece"].max()) + 1)]
    *_, (_, tab, _) = M._in_pieces(z["ctx_X"], z["ctx_y"],
                                   np.concatenate([z["va_X"], z["te_X"]]), pieces, tabpfn_model)
    # A cutoff band with no fraud gives TabPFN nothing to set its cut-off on: no hold.
    t = M._cut(z["va_y"], tab[:n]) - SL.TIE if z["va_y"].any() else np.inf
    flag, y = tab[n:] >= t, z["te_y"]
    m, alerts, caught = int(flag.sum()), int(z["alerts"]), int(z["caught"])
    found = {"model": (alerts, caught),
             "lowered": (alerts + m, caught + int(y[:m].sum())),
             "second": (alerts + m, caught + int(y[flag].sum()))}
    return {"pr_auc": float(z["pr_auc"]), "frauds": int(z["frauds"]), "band": len(y),
            "band_fraud": int(y.sum()),
            **{s: _rates(a, c, int(z["frauds"])) for s, (a, c) in found.items()}}


def _load():
    if not os.path.exists(STATE):
        return {}
    with open(STATE, encoding="utf-8") as fh:
        blob = json.load(fh)
    if blob["_contract"] != _contract_fingerprint():
        raise SystemExit(f"{STATE} was read under another feature set or generator")
    return blob["seeds"]


def _save(results):
    with open(STATE, "w", encoding="utf-8") as fh:
        json.dump({"_contract": _contract_fingerprint(), "seeds": results}, fh, indent=1)


def report(results):
    if not results:
        print("nothing stored yet")
        return
    runs = list(results.values())
    frauds = sum(r["frauds"] for r in runs)
    print(f"{len(runs)} seeds, {frauds:,} held-out frauds; the band held "
          f"{sum(r['band_fraud'] for r in runs)} of them\n")
    print(f"{'system':<9}{'PR-AUC':>8}{'recall':>18}{'precision':>18}{'F1':>18}"
          f"{'alerts':>9}{'caught':>8}")
    for s in SYSTEMS:
        cells = "".join(f"{statistics.mean(r[s][k] for r in runs):>10.1%} +/-"
                        f"{ci95([r[s][k] for r in runs]):>5.1%}"
                        for k in ("recall", "precision", "f1"))
        print(f"{s:<9}{statistics.mean(r['pr_auc'] for r in runs):>8.3f}{cells}"
              f"{sum(r[s]['alerts'] for r in runs):>9,}{sum(r[s]['caught'] for r in runs):>8,}")
    print("\nthe second look against each, paired by seed: mean difference, 95% CI, "
          "seeds where it is higher")
    for other in ("model", "lowered"):
        for k in ("recall", "precision", "f1"):
            d = [r["second"][k] - r[other][k] for r in runs]
            print(f"  vs {other:<8}{k:<10}{statistics.mean(d):>+8.2%} "
                  f"[{statistics.mean(d) - ci95(d):+.2%}, {statistics.mean(d) + ci95(d):+.2%}]"
                  f"  {sum(x > 0 for x in d)} of {len(d)}")


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("stage", choices=("prepare", "look", "report"))
    ap.add_argument("--seeds", default=",".join(map(str, DEFAULT_SEEDS)))
    ap.add_argument("--tabpfn-model", dest="tabpfn_model",
                    help="for `look`: the checkpoint the second-look service loads")
    args = ap.parse_args()
    seeds = [int(s) for s in args.seeds.split(",")]
    if args.stage == "prepare":
        for seed in seeds:
            prepare(seed)
    elif args.stage == "look":
        results = _load()
        for seed in seeds:
            if str(seed) in results:
                continue
            if not os.path.exists(os.path.join(_scratch(seed), "prepared.npz")):
                print(f"seed {seed} is not prepared yet")
                continue
            results[str(seed)] = look(seed, args.tabpfn_model)
            _save(results)
            print(f"seed {seed}: done", flush=True)
    report(_load())


if __name__ == "__main__":
    main()
