"""
Provenance check for Zenodo record 20030065.

The record is published as "A Production-Collected Online Banking Fraud
Detection Dataset from a Live Cloud-Based Deep Learning System". Before citing
it as real-world validation in a thesis, the claim is worth testing — a citation
that collapses under a reviewer's question is worse than no citation.

Three things prompted this check:

  1. The columns are named `v1..v28`, which is the signature of a PCA-transformed
     feature space — and PCA anonymisation is exactly why this project rejected
     the ULB/Kaggle credit-card dataset (SHAP over principal components is
     meaningless, and CBU 3759 requires explainability).

  2. The arithmetic lines up with a 1/5 sample of that same ULB dataset:
       284,807 / 5 = 56,961.4   vs 56,962 rows claimed
           492 / 5 = 98.4       vs 98 fraud claimed
       ULB fraud rate 0.1727%   vs 0.172% claimed

  3. The published description promises "response latency in milliseconds" per
     record. No such column exists in the file.

This script tests the PCA hypothesis directly rather than by resemblance.

    python zenodo_provenance.py --file fraud_tests_export_20260501_080333.csv

A finding here does not accuse anyone of anything. It establishes what the data
IS, which determines what it can support.
"""

import argparse
import os

import numpy as np
import pandas as pd


def pca_signature(df, cols):
    """PCA output has two properties this can test.

    Components are (a) mutually uncorrelated by construction and (b) centred at
    zero. Ordinary transaction features are neither: amount correlates with
    balance, counts correlate with velocity, and nothing is centred.
    """
    sub = df[cols].apply(pd.to_numeric, errors="coerce").dropna()
    if sub.empty or len(cols) < 3:
        return None
    corr = sub.corr().values
    off = corr[~np.eye(len(cols), dtype=bool)]
    return {
        "n": len(sub),
        "max_abs_corr": float(np.nanmax(np.abs(off))),
        "mean_abs_corr": float(np.nanmean(np.abs(off))),
        "max_abs_median": float(sub.median().abs().max()),
        "median_of_stds": float(sub.std().median()),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True)
    args = ap.parse_args()
    if not os.path.exists(args.file):
        raise SystemExit(f"{args.file} not found")

    df = pd.read_csv(args.file)
    print(f"{len(df):,} rows, {len(df.columns)} columns\n")

    # --- 1. published description vs the file -----------------------------
    print("=" * 70)
    print("1. FILE vs PUBLISHED DESCRIPTION")
    print("=" * 70)
    claims = {"rows": 56962, "fraud": 98}
    label_col = next((c for c in df.columns if c.lower() in
                      ("is_fraud", "isfraud", "class", "label")), None)
    actual_fraud = int(pd.to_numeric(df[label_col], errors="coerce").fillna(0).sum()) \
        if label_col else None

    print(f"   rows      claimed {claims['rows']:>8,}   actual {len(df):>8,}"
          f"   {'MATCH' if len(df) == claims['rows'] else 'DIFFERS'}")
    if actual_fraud is not None:
        print(f"   fraud     claimed {claims['fraud']:>8,}   actual {actual_fraud:>8,}"
              f"   {'MATCH' if actual_fraud == claims['fraud'] else 'DIFFERS'}")
    has_latency = any("laten" in c.lower() or "response_time" in c.lower()
                      for c in df.columns)
    print(f"   latency   promised in the description: "
          f"{'present' if has_latency else 'ABSENT'}")

    # --- 2. the PCA hypothesis --------------------------------------------
    print("\n" + "=" * 70)
    print("2. ARE THE FEATURES PCA COMPONENTS?")
    print("=" * 70)
    v_cols = [c for c in df.columns if c.lower().startswith("v")
              and c[1:].isdigit()]
    if not v_cols:
        print("   no v-prefixed columns; hypothesis does not apply")
        return

    v_cols = sorted(v_cols, key=lambda c: int(c[1:]))
    # v1..v6 looked structurally different from v7..v28 in the column dump, so
    # the two blocks are tested separately rather than assumed homogeneous.
    early = [c for c in v_cols if int(c[1:]) <= 6]
    late = [c for c in v_cols if int(c[1:]) > 6]

    for name, cols in (("v1-v6", early), ("v7-v28", late)):
        s = pca_signature(df, cols)
        if not s:
            continue
        orthogonal = s["max_abs_corr"] < 0.15
        centred = s["max_abs_median"] < 0.5
        verdict = ("consistent with PCA output" if orthogonal and centred
                   else "NOT PCA-like")
        print(f"\n   {name} ({len(cols)} columns)")
        print(f"     max |correlation| between pairs : {s['max_abs_corr']:.4f}"
              f"   {'(orthogonal)' if orthogonal else '(correlated)'}")
        print(f"     max |median|                    : {s['max_abs_median']:.4f}"
              f"   {'(centred)' if centred else '(not centred)'}")
        print(f"     median std                      : {s['median_of_stds']:.4f}")
        print(f"     -> {verdict}")

    # --- 3. duplicated columns --------------------------------------------
    print("\n" + "=" * 70)
    print("3. DUPLICATED CONTENT")
    print("=" * 70)
    amount_col = next((c for c in df.columns if c.lower() == "amount"), None)
    if amount_col:
        amt = pd.to_numeric(df[amount_col], errors="coerce")
        for c in v_cols:
            other = pd.to_numeric(df[c], errors="coerce")
            if other.notna().sum() < len(df) * 0.5:
                continue
            if np.isclose(float(amt.max()), float(other.max()), rtol=1e-6):
                r = amt.corr(other)
                print(f"   {c} shares its maximum with `{amount_col}` "
                      f"({float(other.max()):,.2f}), correlation {r:.4f}")
                if r > 0.99:
                    print(f"     -> {c} IS the amount column under another name.")

    # --- 4. what follows ---------------------------------------------------
    print("\n" + "=" * 70)
    print("4. WHAT THIS MEANS FOR THE THESIS")
    print("=" * 70)
    print("""
   If the v-columns are PCA components, this dataset inherits the exact
   objection that ruled out the ULB/Kaggle credit-card set: features with no
   meaning cannot support SHAP explanations, and CBU Regulation No. 3759
   requires an explainable decision.

   It also cannot be described as an independent real-world corroboration if
   it is derived from a dataset already in wide circulation — and the row and
   fraud counts sit within rounding of a 1/5 sample of it.

   Usable regardless, with the claim narrowed:
     - the fraud BASE RATE (~0.19%) still contradicts the generator's 1.5%,
       and that finding does not depend on what the features mean;
     - the AMOUNT distribution still tests the lognormal assumption.

   Not usable for:
     - "validated against real production banking data" as a headline claim;
     - anything resting on the promised latency column, which is absent.
""")


if __name__ == "__main__":
    main()
