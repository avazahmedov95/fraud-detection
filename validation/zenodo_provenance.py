"""Checks whether a published dataset is what its paper says it is.

Written after the row count disagreed with the publication; the discrepancy was
resolved exactly - docs/related-work.md 7.
"""

import argparse
import os
import re

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

    # --- 1b. where the extra rows come from -------------------------------
    # The row and fraud counts differ from the description, and the obvious
    # reading is that the description is wrong. It is not. Partitioning by the
    # SHAPE of transaction_id splits the file cleanly into the dataset that was
    # described and a block of rows appended afterwards, and the description is
    # exact for the first. Reporting the raw mismatch without this split would
    # accuse the publisher of miscounting when the real defect is contamination.
    print()
    print("   where the difference comes from - partition by transaction_id shape:")
    tid = df["transaction_id"].astype(str)

    def _shape(v):
        if v.startswith("txn_"):
            return "txn_<epoch>_<n>"          # a demo UI's own id format
        if re.fullmatch(r"[0-9a-f]{8}", v):
            return "8 hex chars"             # repeated canned test record
        return "TXN+base32"                  # the published dataset

    for name, g in df.assign(_s=tid.map(_shape)).groupby("_s"):
        fr = int(pd.to_numeric(g[label_col], errors="coerce").fillna(0).sum()) \
            if label_col else -1
        dates = pd.to_datetime(g["timestamp"], errors="coerce")
        td = g["test_date"].notna().sum() if "test_date" in g else 0
        print(f"     {name:<16} {len(g):>7,} rows  {fr:>4} fraud  "
              f"test_date set on {td:>6,}  "
              f"{dates.min():%Y-%m-%d}..{dates.max():%Y-%m-%d}")

    main_block = df[tid.map(_shape) == "TXN+base32"]
    main_fraud = int(pd.to_numeric(main_block[label_col], errors="coerce")
                     .fillna(0).sum()) if label_col else None
    exact = (len(main_block) == claims["rows"] and main_fraud == claims["fraud"])
    print(f"\n     -> the TXN+base32 block alone: {len(main_block):,} rows, "
          f"{main_fraud} fraud  "
          f"{'EXACTLY as claimed' if exact else 'still does not match'}")
    if exact:
        print("     -> so the description is accurate for the dataset, and the")
        print(f"        remaining {len(df) - len(main_block):,} rows "
              f"({actual_fraud - main_fraud} of them flagged fraud) are")
        print("        somebody's live testing, appended after publication:")
        print("        no test_date, timestamps months past the dataset window,")
        print("        and a different feature schema (see check 3).")

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
