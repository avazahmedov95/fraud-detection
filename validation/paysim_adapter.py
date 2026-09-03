"""Replays the deployed rules over PaySim, mapping its schema onto the event shape
features.py expects. What transfers and what does not: validation/README.md 2.
"""

import argparse
import os
import sys
from collections import defaultdict, Counter

import pandas as pd

_SP = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                   "..", "stream-processor")
sys.path.insert(0, _SP)

import capabilities as CAP     # noqa: E402
import config as C             # noqa: E402
from rules import SenderState, ReceiverState, evaluate   # noqa: E402


# PaySim's amounts are ~1000x smaller than UZS, so rules with absolute thresholds
# (NEW_PAYEE_ABS_FLOOR, STRUCTURING_THRESHOLD, LIMIT_DAILY) would never fire without
# rescaling. A unit conversion, not tuning: one factor from the medians, every row.
def scale_factor(amounts, our_median_uzs=138_740.0):
    med = float(amounts.median())
    return our_median_uzs / med if med > 0 else 1.0


def to_events(df, scale):
    """PaySim rows -> the event dict this project's extractor expects.
    Only fields PaySim actually has are populated; the rest are left absent and their
    capabilities switched off (see main), so nothing fires on a fabricated value.
    """
    for r in df.itertuples(index=False):
        yield {
            "amount_uzs": float(r.amount) * scale,
            "sender_pinfl": r.nameOrig,
            "receiver_pinfl": r.nameDest,
            # `step` is PaySim's hour index (1..744 over 30 days) and the sharpest limitation
            # of this cross-check: same-step events share a timestamp, so the 10-minute and
            # 1-hour windows see nearly the same set and secs_since_last is 0. Sub-hour
            # patterns are invisible here - a property of PaySim, not of the rules.
            "_ts": int(r.step) * 3600,
            "_label": int(r.isFraud),
            "_type": r.type,
        }


def run(path, txn_types, limit):
    df = pd.read_csv(path)
    if txn_types:
        df = df[df.type.isin(txn_types)]
    if limit:
        df = df.head(limit)
    df = df.sort_values("step").reset_index(drop=True)

    scale = scale_factor(df.amount)
    print(f"{len(df):,} PaySim rows, types {sorted(df.type.unique())}")
    print(f"fraud: {df.isFraud.sum():,} ({df.isFraud.mean():.3%})")
    print(f"amount scale factor: {scale:.1f}x "
          f"(median {df.amount.median():,.0f} -> {df.amount.median()*scale:,.0f})\n")

    senders = defaultdict(SenderState)
    receivers = defaultdict(ReceiverState)
    rows, hits_by_class = [], defaultdict(Counter)

    for ev in to_events(df, scale):
        label, ts = ev.pop("_label"), ev.pop("_ts")
        ev.pop("_type")
        res = evaluate(ev, None, senders[ev["sender_pinfl"]], ts,
                       receivers[ev["receiver_pinfl"]])
        rows.append((label, res["cep_score"], res["decision"]))
        bucket = "fraud" if label else "legit"
        for h in res["rule_hits"]:
            hits_by_class[bucket][h] += 1

    return pd.DataFrame(rows, columns=["label", "cep_score", "decision"]), hits_by_class


def report(res, hits):
    n_fraud = int((res.label == 1).sum())
    n_legit = int((res.label == 0).sum())
    flagged = res.decision.isin(["REVIEW", "BLOCK"])
    fr = flagged[res.label == 1].mean() if n_fraud else 0.0
    lg = flagged[res.label == 0].mean() if n_legit else 0.0

    print("=" * 70)
    print("A. PER-RULE LIFT — does each rule carry signal on foreign data?")
    print("=" * 70)
    print("Threshold-free, and therefore the measure that actually answers the")
    print("question. A rule that fires more often on fraud than on legitimate")
    print("traffic is discriminating, whatever the decision threshold does.\n")
    print(f"{'rule':<24}{'on fraud':>12}{'on legit':>12}{'lift':>9}")
    every = set(hits["fraud"]) | set(hits["legit"])
    lifts = []
    for rule in sorted(every):
        f = hits["fraud"][rule] / max(n_fraud, 1)
        l = hits["legit"][rule] / max(n_legit, 1)
        lift = (f / l) if l > 0 else float("inf")
        lifts.append((rule, f, l, lift))
    for rule, f, l, lift in sorted(lifts, key=lambda x: -x[3]):
        shown = "inf" if lift == float("inf") else f"{lift:.1f}x"
        print(f"{rule:<24}{f:>11.2%}{l:>12.2%}{shown:>9}")

    print("\n" + "=" * 70)
    print("B. DECISION LAYER — does the deployed threshold still work?")
    print("=" * 70)
    print(f"  fraud flagged : {int(flagged[res.label==1].sum()):>7,} / {n_fraud:<7,} ({fr:.1%})")
    print(f"  legit flagged : {int(flagged[res.label==0].sum()):>7,} / {n_legit:<7,} ({lg:.2%})")

    scores = res.cep_score
    print(f"\n  cep_score, fraud : max {scores[res.label==1].max():.2f}   "
          f"mean {scores[res.label==1].mean():.3f}")
    print(f"  cep_score, legit : max {scores[res.label==0].max():.2f}   "
          f"mean {scores[res.label==0].mean():.3f}")

    # The threshold actually applied - under capability scaling not the configured
    # constant. Printing the constant here hid the mechanism this run exercises.
    import rules as _R
    review_at, block_at = _R._thresholds()
    if abs(review_at - C.REVIEW_THRESHOLD) > 1e-9:
        print(f"  REVIEW threshold : {review_at:.2f}  "
              f"(scaled from {C.REVIEW_THRESHOLD:.2f} for this capability profile)")
        print(f"  BLOCK  threshold : {block_at:.2f}  "
              f"(scaled from {C.BLOCK_THRESHOLD:.2f})")
    else:
        print(f"  REVIEW threshold : {review_at:.2f}")

    if fr > 0 and lg > 0:
        print(f"\n  decision-layer lift: {fr/lg:.1f}x")

    top = scores[res.label == 1].max()
    if n_fraud and fr == 0.0 and top > 0:
        print(f"\n  Nothing crossed the threshold: the highest score any fraud")
        print(f"  reached was {top:.2f}, against a REVIEW cutoff of "
              f"{C.REVIEW_THRESHOLD:.2f}.")
        print(f"  The CEP score is ADDITIVE, so crossing it normally takes two")
        print(f"  rules firing together. With this capability profile only")
        print(f"  {len(every)} rule(s) can fire at all, and they rarely co-occur.")
        print(f"\n  This is a finding about threshold calibration, not about the")
        print(f"  features: see the lift table above, where the rules do separate")
        print(f"  the classes. A deployment with fewer integrations does not get a")
        print(f"  slightly worse rule layer — it gets a silent one.")

    if n_fraud and n_legit:
        best = None
        for t in sorted(set(round(v, 2) for v in scores if v > 0)):
            f_at = (scores[res.label == 1] >= t).mean()
            l_at = (scores[res.label == 0] >= t).mean()
            if f_at > 0 and l_at > 0 and f_at / l_at > (best[3] if best else 0):
                best = (t, f_at, l_at, f_at / l_at)
        if best:
            t, f_at, l_at, lift = best
            print(f"\n  Best cutoff on this data: {t:.2f} -> flags {f_at:.1%} of "
                  f"fraud, {l_at:.2%} of legit ({lift:.1f}x lift).")
            print(f"  Reported to size the gap, NOT adopted — tuning a threshold on")
            print(f"  the validation set is what this exercise exists to avoid.")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True, help="PaySim CSV")
    ap.add_argument("--types", default="TRANSFER",
                    help="comma-separated PaySim types; TRANSFER is the P2P "
                         "analogue. CASH_OUT also carries fraud but is a "
                         "withdrawal, not a transfer between two customers.")
    ap.add_argument("--limit", type=int, default=None,
                    help="first N rows (the full file is 6.3M)")
    args = ap.parse_args()

    if not os.path.exists(args.file):
        raise SystemExit(f"{args.file} not found")

    # PaySim has account ids, amounts and a clock, nothing else this project uses. What
    # is not backed by real data is switched off, not defaulted: nothing fires on a zero.
    for key in ("receiver_age", "myid_kinship", "device_telemetry",
                "geo_telemetry", "session_telemetry", "channel"):
        CAP.MODES[key] = "off"

    print("capability profile for this run:")
    print(CAP.describe())
    print()

    types = [t.strip() for t in args.types.split(",") if t.strip()]
    res, hits = run(args.file, types, args.limit)
    report(res, hits)

    print("\nWhat this does and does not show:")
    print("  - PaySim is synthetic, so this is not production validation.")
    print("  - It IS an independent generator: these rules were written against")
    print("    a different dataset and are run here unchanged, so a result here")
    print("    is not circular in the way a result on our own data would be.")
    print("  - Hourly timestamps collapse the sub-hour windows (see to_events).")


if __name__ == "__main__":
    main()
