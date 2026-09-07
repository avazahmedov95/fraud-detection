"""Zenodo 20030065: whether it is what it claims, and what survives the answer.

Published as "A Production-Collected Online Banking Fraud Detection Dataset from
a Live Cloud-Based Deep Learning System". It is not usable as that, and the two
halves of finding out belong together: the provenance check says what the file
actually is, and the profile says which of its quantities stay quotable once the
headline claim is withdrawn. They were two scripts, and the second computed
exactly the numbers the first's conclusion named - so finishing one argument
meant opening both.

    python zenodo.py --file <csv>                     both, in order
    python zenodo.py --file <csv> --only provenance
    python zenodo.py --file <csv> --only profile

Findings: docs/related-work.md 7, validation/README.md 2.
"""

import argparse
import math
import os
import re
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", "data-generator"))

# Candidate names per quantity, most likely first. Matching is case- and
# separator-insensitive; what is found is printed so a wrong guess is visible.
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


def _label_col(df):
    return next((c for c in df.columns if c.lower() in
                 ("is_fraud", "isfraud", "class", "label")), None)


# --------------------------------------------------------------------------
# Part 1 - is the file what the publication describes?
# --------------------------------------------------------------------------

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


def provenance(df):
    """What the file is, against what the publication says it is."""
    label_col = _label_col(df)
    actual_fraud = int(pd.to_numeric(df[label_col], errors="coerce").fillna(0).sum()) \
        if label_col else None
    claims = {"rows": 56962, "fraud": 98}

    print("=" * 70)
    print("1. FILE vs PUBLISHED DESCRIPTION")
    print("=" * 70)
    print(f"   rows      claimed {claims['rows']:>8,}   actual {len(df):>8,}"
          f"   {'MATCH' if len(df) == claims['rows'] else 'DIFFERS'}")
    if actual_fraud is not None:
        print(f"   fraud     claimed {claims['fraud']:>8,}   actual {actual_fraud:>8,}"
              f"   {'MATCH' if actual_fraud == claims['fraud'] else 'DIFFERS'}")
    has_latency = any("laten" in c.lower() or "response_time" in c.lower()
                      for c in df.columns)
    print(f"   latency   promised in the description: "
          f"{'present' if has_latency else 'ABSENT'}")

    # The counts differ from the description and the obvious reading is that the
    # description is wrong. It is not. Partitioning by the SHAPE of
    # transaction_id splits the file cleanly into the dataset that was described
    # and a block appended afterwards. Reporting the raw mismatch without this
    # would accuse the publisher of miscounting when the defect is contamination.
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

    print("\n" + "=" * 70)
    print("2. ARE THE FEATURES PCA COMPONENTS?")
    print("=" * 70)
    v_cols = [c for c in df.columns if c.lower().startswith("v") and c[1:].isdigit()]
    if not v_cols:
        print("   no v-prefixed columns; hypothesis does not apply")
        return
    v_cols = sorted(v_cols, key=lambda c: int(c[1:]))

    # v1..v6 looked structurally different from v7..v28 in the column dump, so
    # the two blocks are tested separately rather than assumed homogeneous.
    for name, cols in (("v1-v6", [c for c in v_cols if int(c[1:]) <= 6]),
                       ("v7-v28", [c for c in v_cols if int(c[1:]) > 6])):
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

    print("\n   NOT usable as production data, and the objection is the one that"
          "\n   ruled out ULB/Kaggle: PCA components carry no meaning, so no SHAP"
          "\n   explanation and no compliance with Regulation 3759. Full reasoning:"
          "\n   validation/README.md 2, docs/related-work.md 7."
          "\n\n   What survives is the profile below. It is below rather than in a"
          "\n   second file because this conclusion is what makes it the only"
          "\n   remaining use.")


# --------------------------------------------------------------------------
# Part 2 - what survives the rejection
# --------------------------------------------------------------------------

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
          f"would carry\n   an interval tens of points wide - which is why it is "
          f"not used for detection.")
    return p


def report_amounts(df, col):
    amt = pd.to_numeric(df[col], errors="coerce").dropna()
    amt = amt[amt > 0]
    if amt.empty:
        print("\n2. AMOUNT DISTRIBUTION - no usable values")
        return
    skew_log = float(pd.Series(np.log(amt)).skew())
    verdict = ("consistent with lognormal" if abs(skew_log) < 1.0
               else "NOT well described by lognormal")

    print("\n2. AMOUNT DISTRIBUTION")
    print(f"   n={len(amt):,}  median {amt.median():,.2f}  "
          f"p95 {amt.quantile(0.95):,.2f}  max {amt.max():,.2f}")
    print(f"   skewness: raw {float(amt.skew()):.2f}, log {skew_log:.2f}")
    print(f"   -> {verdict}")
    print("\n   The generator draws amounts from LogN (see docs/generator-spec.md"
          "\n   section 4). Log-skew near zero supports that; a large log-skew means"
          "\n   real amounts are more structured than the assumption - most likely"
          "\n   round-number clustering, which the spec already lists as unmodelled.")


def report_latency(df, col):
    lat = pd.to_numeric(df[col], errors="coerce").dropna()
    lat = sorted(lat[lat >= 0])
    if not lat:
        print("\n3. LATENCY - no usable values")
        return
    print("\n3. LATENCY - external anchor")
    print(f"   n={len(lat):,}  median {quantile(lat,0.50):,.1f} ms  "
          f"p95 {quantile(lat,0.95):,.1f}  p99 {quantile(lat,0.99):,.1f}  "
          f"max {max(lat):,.1f}")
    # The reference run, docs/irp-framing.md 7.1a. This line used to quote
    # 82/159/217, which belonged to a run withdrawn in 7.0 - the comparison was
    # never updated when the withdrawal was.
    print("   this project (7.1a, decision path): median 69 ms, p95 131, p99 176")
    print("\n   Not a like-for-like benchmark: different hardware, different model"
          "\n   (CNN-LSTM behind a REST endpoint vs CEP + gradient boosting in a"
          "\n   stream), different load. It is an order-of-magnitude sanity check -"
          "\n   a published production system operating in the same regime.")


def profile(df):
    """The quantities that stay quotable once the headline claim is withdrawn."""
    cols = discover(df)
    print("column discovery:")
    for key in CANDIDATES:
        print(f"  {key:<10}-> {cols.get(key, '(not found)')}")

    if len(cols) < len(CANDIDATES):
        # Print everything numeric so an unmatched quantity can be identified by eye.
        print("\nunmatched - numeric columns available, with medians:")
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

    # The identity check that decides what this dataset can be used for at all.
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
        print("\n3. LATENCY - column not found; check the printed column list")

    print("\n" + "-" * 70)
    print("Cross-generator validation of the relational features is a separate"
          "\nquestion, answered by paysim_adapter.py - PaySim is the only public"
          "\ndataset carrying identifiers on both sides.")


def main():
    ap = argparse.ArgumentParser(
        description="Zenodo 20030065: provenance check and what survives it.")
    ap.add_argument("--file", required=True)
    ap.add_argument("--only", choices=["provenance", "profile"], default=None,
                    help="run one half; the default runs both, in order")
    args = ap.parse_args()
    if not os.path.exists(args.file):
        raise SystemExit(f"{args.file} not found. Download from "
                         f"https://zenodo.org/records/20030065 (CC-BY-4.0)")

    df = pd.read_csv(args.file)
    print(f"loaded {len(df):,} rows, {len(df.columns)} columns\n")

    if args.only != "profile":
        provenance(df)
    if args.only != "provenance":
        if args.only is None:
            print("\n" + "=" * 70)
            print("WHAT SURVIVES THE REJECTION")
            print("=" * 70)
        profile(df)


if __name__ == "__main__":
    main()
