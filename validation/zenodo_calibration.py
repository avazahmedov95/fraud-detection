"""
Calibrate the generator's assumptions against a real production dataset.

Zenodo record 20030065 (DOI 10.5281/zenodo.20030065, CC-BY-4.0): 56,962 online
banking transactions from a live cloud-deployed fraud detection system, 30 days
of January 2026, 98 confirmed fraud (0.172%). Each row also carries that system's
own model outputs, including response latency in milliseconds.

**This does not validate detection.** The dataset carries no account identifiers
— they are exactly what cannot be published — so 14 of this project's 24 features
cannot be computed on it. Attempting a detection comparison would compare a
crippled version of this system against a full one, and the number would mean
nothing.

What it can do is check three assumptions that the synthetic generator states and
that a real dataset can contradict:

  1. **Fraud base rate.** The generator uses 1.5%; this dataset shows 0.172%.
     If the real figure is an order of magnitude lower, every precision figure
     measured on synthetic data is optimistic, because precision depends
     directly on class balance.
  2. **Amount distribution.** The generator assumes lognormal. A real
     right-skewed heavy tail either supports that or does not.
  3. **Latency.** An independently measured production system, reported in ms,
     as an external anchor for this project's own 217 ms p99.

    python zenodo_calibration.py --file fraud_tests_export_20260501_080333.csv

Column names are discovered rather than assumed: the record's description lists
what the columns mean but not what they are called, so the script reports what it
found and works with what is present.
"""

import argparse
import math
import os
import sys

import pandas as pd

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_HERE, "..", "data-generator"))


# Candidate names for each quantity, most likely first. Matching is case- and
# separator-insensitive; whatever is found is printed so a wrong guess is
# visible rather than silent.
CANDIDATES = {
    "label":   ["isfraud", "is_fraud", "fraud", "label", "actualfraud",
                "confirmedfraud", "true_label", "y"],
    "amount":  ["amount", "transactionamount", "amt", "value", "txnamount"],
    "latency": ["responselatencyms", "latencyms", "latency", "responsetime",
                "response_time_ms", "processingtimems", "inferencetimems"],
    "score":   ["fraudprobability", "probability", "score", "fraudscore",
                "riskscore", "confidence"],
    "action":  ["action", "actionrecommendation", "decision", "recommendation"],
}


def _norm(name):
    return "".join(ch for ch in str(name).lower() if ch.isalnum())


def discover(df):
    found, normalised = {}, {_norm(c): c for c in df.columns}
    for key, options in CANDIDATES.items():
        for opt in options:
            if opt in normalised:
                found[key] = normalised[opt]
                break
    return found


def quantile(sorted_vals, q):
    if not sorted_vals:
        return float("nan")
    i = max(0, min(len(sorted_vals) - 1, int(math.ceil(q * len(sorted_vals))) - 1))
    return sorted_vals[i]


def wilson(k, n, z=1.96):
    """Interval for a proportion; correct at the small counts this data has."""
    if n == 0:
        return 0.0, 0.0, 0.0
    p = k / n
    d = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / d
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return p, max(0.0, centre - half), min(1.0, centre + half)


def report_base_rate(df, col):
    labels = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    k, n = int(labels.sum()), len(labels)
    p, lo, hi = wilson(k, n)

    import config as GC
    ours = GC.GeneratorConfig().fraud_rate

    print("\n1. FRAUD BASE RATE")
    print(f"   real       : {k} / {n:,} = {p:.3%}  [95% CI {lo:.3%}, {hi:.3%}]")
    print(f"   generator  : {ours:.3%}")
    print(f"   ratio      : generator is {ours/p:.1f}x higher" if p > 0 else "")
    print("\n   Why it matters: precision depends directly on class balance. At a"
          "\n   base rate ~{:.0f}x lower, the same detector produces far more false"
          "\n   positives per true one. Recall is unaffected; precision measured on"
          "\n   synthetic data at 1.5% is optimistic.".format(ours / p if p > 0 else 0))
    print(f"\n   With only {k} positives, any per-type recall from this dataset "
          f"would carry\n   an interval tens of points wide — which is why it is "
          f"not used for detection.")
    return p


def report_amounts(df, col):
    amt = pd.to_numeric(df[col], errors="coerce").dropna()
    amt = amt[amt > 0]
    if amt.empty:
        print("\n2. AMOUNT DISTRIBUTION — no usable values")
        return
    import numpy as np
    logs = np.log(amt)
    skew_log = float(pd.Series(logs).skew())
    skew_raw = float(amt.skew())

    print("\n2. AMOUNT DISTRIBUTION")
    print(f"   n={len(amt):,}  median {amt.median():,.2f}  "
          f"p95 {amt.quantile(0.95):,.2f}  max {amt.max():,.2f}")
    print(f"   skewness: raw {skew_raw:.2f}, log {skew_log:.2f}")
    verdict = ("consistent with lognormal" if abs(skew_log) < 1.0
               else "NOT well described by lognormal")
    print(f"   -> {verdict}")
    print("\n   The generator draws amounts from LogN (see docs/generator-spec.md"
          "\n   section 4). Log-skew near zero supports that; a large log-skew means"
          "\n   real amounts are more structured than the assumption — most likely"
          "\n   round-number clustering, which the spec already lists as unmodelled.")


def report_latency(df, col):
    lat = pd.to_numeric(df[col], errors="coerce").dropna()
    lat = sorted(lat[lat >= 0])
    if not lat:
        print("\n3. LATENCY — no usable values")
        return
    print("\n3. LATENCY — external anchor")
    print(f"   n={len(lat):,}  median {quantile(lat,0.50):,.1f} ms  "
          f"p95 {quantile(lat,0.95):,.1f}  p99 {quantile(lat,0.99):,.1f}  "
          f"max {max(lat):,.1f}")
    print("   this project (measured, decision path): median 82 ms, p95 159, p99 217")
    print("\n   Not a like-for-like benchmark: different hardware, different model"
          "\n   (CNN-LSTM behind a REST endpoint vs CEP + gradient boosting in a"
          "\n   stream), different load. It is an order-of-magnitude sanity check —"
          "\n   a published production system operating in the same regime.")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True)
    args = ap.parse_args()
    if not os.path.exists(args.file):
        raise SystemExit(f"{args.file} not found. Download from "
                         f"https://zenodo.org/records/20030065 (CC-BY-4.0)")

    df = pd.read_csv(args.file)
    cols = discover(df)

    print(f"loaded {len(df):,} rows, {len(df.columns)} columns\n")
    print("column discovery:")
    for key in CANDIDATES:
        print(f"  {key:<10}-> {cols.get(key, '(not found)')}")

    if len(cols) < len(CANDIDATES):
        # Print everything numeric so an unmatched quantity can be identified by
        # eye rather than by guessing more candidate names.
        print("\nunmatched — numeric columns available, with medians:")
        for c in df.columns:
            if c in cols.values():
                continue
            s = pd.to_numeric(df[c], errors="coerce")
            if s.notna().sum() > len(df) * 0.5:
                print(f"  {c:<34} median {s.median():>12,.3f}  "
                      f"max {s.max():>14,.3f}")
    missing = [k for k in ("label", "amount") if k not in cols]
    if missing:
        print(f"\nall columns: {list(df.columns)}")
        raise SystemExit(f"\ncannot proceed without: {missing}. "
                         f"Add the real names to CANDIDATES.")

    # The identity check that decides what this dataset can be used for.
    id_like = [c for c in df.columns
               if any(t in _norm(c) for t in ("account", "customer", "sender",
                                              "receiver", "origin", "dest",
                                              "userid", "clientid", "cardid"))]
    print(f"\nidentifier-like columns: {id_like or 'NONE'}")
    if not id_like:
        print("  -> confirms the premise: without account identifiers, the 14"
              "\n     relational features cannot be computed, so this dataset"
              "\n     calibrates assumptions rather than validating detection.")

    report_base_rate(df, cols["label"])
    report_amounts(df, cols["amount"])
    if "latency" in cols:
        report_latency(df, cols["latency"])
    else:
        print("\n3. LATENCY — column not found; check the printed column list")

    print("\n" + "-" * 70)
    print("Cross-generator validation of the relational features is a separate"
          "\nquestion, answered by paysim_adapter.py — PaySim is the only public"
          "\ndataset carrying identifiers on both sides.")


if __name__ == "__main__":
    main()
