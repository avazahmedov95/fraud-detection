"""
Paired comparison of the two MULE_FAN_IN threshold modes, across generator seeds.

WHY A SEPARATE SCRIPT. `replay_eval.py` reports one dataset under one mode. The
first comparison done that way gave +8.2 points of MULE recall for the relative
mode - measured on ONE dataset, after sweeping three quantiles and reporting the
best. This project has been wrong exactly that way twice before (`ml/README.md`:
a single-seed sweep put receiver_age at -0.043 against an honest -0.025), so the
number is not quotable until it is paired across seeds with an interval.

Deltas are paired WITHIN each seed: the same dataset scored under both modes, so
the comparison removes everything that varies between datasets and leaves the
mode. The interval is a t-based 95% CI for the mean delta, the same convention
`ablation_seeds.py` settled on after an earlier version compared the mean
against the standard deviation instead of the standard error.

    # generate the seeds first (each takes a couple of minutes)
    cd ../data-generator
    for %s in (1 2 3 4 5) do python generator.py --seed %s --out ./out_seed%s

    cd ../stream-processor
    python fan_in_mode_eval.py --files ../data-generator/out_seed*/transactions.csv

The quantile is NOT swept here. Sweeping it per seed and reporting the best
would reintroduce the selection this script exists to remove; pass --quantile
once, and if a different value is tried, report both runs.
"""

import argparse
import glob
import math
import statistics as st
from collections import defaultdict

import pandas as pd

import config as C
from rules import SenderState, ReceiverState, PopulationBaseline, evaluate
from replay_eval import _as_bool, _as_age


def score(path, mode, quantile):
    """One dataset, one mode. Returns the numbers the modes differ on."""
    saved_mode, saved_q = C.MULE_FAN_IN_MODE, C.MULE_FAN_IN_QUANTILE
    C.MULE_FAN_IN_MODE, C.MULE_FAN_IN_QUANTILE = mode, quantile
    try:
        df = pd.read_csv(path).sort_values("event_time").reset_index(drop=True)
        states, rstates = defaultdict(SenderState), defaultdict(ReceiverState)
        pop = PopulationBaseline()
        flagged, hits = [], 0
        for row in df.itertuples(index=False):
            r = row._asdict()
            ev = {
                "amount_uzs": r["amount_uzs"],
                "sender_pinfl": r["sender_pinfl"], "receiver_pinfl": r["receiver_pinfl"],
                "device_id": r["device_id"], "sender_region": r["sender_region"],
                "channel": r.get("channel", "MOBILE_APP"),
                "sender_network": r.get("sender_network", ""),
                "receiver_network": r.get("receiver_network", ""),
                "active_call": _as_bool(r.get("active_call")),
                "secs_login_to_confirm": r.get("secs_login_to_confirm", 0.0),
                "sender_bank_name": r.get("sender_bank_name", ""),
                "receiver_bank_name": r.get("receiver_bank_name", ""),
                "is_family_transfer": _as_bool(r.get("is_family_transfer")),
            }
            res = evaluate(ev, _as_age(r.get("receiver_account_age_days")),
                           states[r["sender_card"]],
                           pd.Timestamp(r["event_time"]).timestamp(),
                           rstates[r["receiver_pinfl"]], population=pop)
            flagged.append(res["decision"] in ("REVIEW", "BLOCK"))
            hits += ("MULE_FAN_IN" in res["rule_hits"])
        df["flagged"] = flagged
        fraud = df["label_is_fraud"].astype(int) == 1
        mule = fraud & (df.get("label_fraud_type", "") == "MULE")
        return {
            "mule_recall": float(df.loc[mule, "flagged"].mean()) if mule.any() else float("nan"),
            "fraud_recall": float(df.loc[fraud, "flagged"].mean()),
            "fp_rate": float(df.loc[~fraud, "flagged"].mean()),
            "fan_in_hits": hits,
            "threshold": pop.threshold(quantile, C.MULE_FAN_IN_MIN_SENDERS),
        }
    finally:
        C.MULE_FAN_IN_MODE, C.MULE_FAN_IN_QUANTILE = saved_mode, saved_q


def ci(vals):
    n = len(vals)
    if n < 2:
        return float("nan"), float("nan")
    m, sem = st.mean(vals), st.stdev(vals) / math.sqrt(n)
    # t for 95%, small-n; falls back to 1.96 beyond the table
    t = {2: 12.71, 3: 4.30, 4: 3.18, 5: 2.78, 6: 2.57, 7: 2.45,
         8: 2.36, 9: 2.31, 10: 2.26}.get(n, 1.96)
    return m - t * sem, m + t * sem


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--files", nargs="+", required=True)
    ap.add_argument("--quantile", type=float, default=C.MULE_FAN_IN_QUANTILE)
    args = ap.parse_args()

    files = [f for pat in args.files for f in sorted(glob.glob(pat))] or args.files
    print(f"{len(files)} dataset(s), quantile {args.quantile}, "
          f"absolute threshold {C.MULE_FAN_IN_MIN_SENDERS}\n")
    print(f"{'dataset':<26}{'thr':>5}{'MULE abs':>10}{'MULE rel':>10}{'delta':>9}"
          f"{'FP abs':>9}{'FP rel':>9}")

    rows = []
    for f in files:
        a = score(f, "absolute", args.quantile)
        r = score(f, "relative", args.quantile)
        rows.append((a, r))
        name = f.replace("\\\\", "/").split("/")[-2]
        print(f"{name:<26}{r['threshold']:>5}{a['mule_recall']:>10.1%}"
              f"{r['mule_recall']:>10.1%}{r['mule_recall'] - a['mule_recall']:>+9.1%}"
              f"{a['fp_rate']:>9.2%}{r['fp_rate']:>9.2%}")

    print("\n" + "=" * 72)
    for key, label in (("mule_recall", "MULE recall"),
                       ("fraud_recall", "overall fraud recall"),
                       ("fp_rate", "false-positive rate")):
        d = [r[key] - a[key] for a, r in rows if not math.isnan(a[key])]
        if not d:
            continue
        lo, hi = ci(d)
        pos = sum(1 for x in d if x > 0)
        verdict = ("real" if (len(d) > 1 and (lo > 0 or hi < 0)) else "unresolved")
        print(f"{label:<24} delta {st.mean(d):+.4f}"
              + (f"  95% CI [{lo:+.4f}, {hi:+.4f}]" if len(d) > 1 else "")
              + f"  sign {pos}/{len(d)}  -> {verdict}")

    print("\nRead the false-positive row first. A recall gain bought with alerts")
    print("is not a gain; it is a threshold move, and the decision layer already")
    print("has a knob for that. The claim worth making is a recall delta whose")
    print("interval excludes zero WHILE the false-positive interval contains it.")
    if len(files) < 3:
        print("\n!! Fewer than three datasets. The interval is decoration at this n.")


if __name__ == "__main__":
    main()
