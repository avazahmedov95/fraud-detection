"""
Run this project's CEP rules on IBM AMLSim, to test the one claim PaySim cannot.

WHY THIS EXISTS. Receiver-side aggregation is this project's largest measured
effect (-0.032 PR-AUC) and its only structural design finding. It has no
external validation, because the one foreign dataset run so far - PaySim -
models fraud as draining an account straight to cash-out and contains no
collection stage for MULE_FAN_IN to see. "The rule has nothing to detect" is
true there, and is also the most convenient possible outcome, which is reason
enough to distrust it.

AMLSim (IBM, Apache-2.0) generates `fan_in` as an explicit typology: several
accounts sending substantial funds to one main account, over a configured
number of steps. It also generates `fan_out` and `cycle` separately, and labels
every transaction with the alert it belongs to. That makes it the first foreign
dataset on which the fan-in/fan-out asymmetry reported in ml/README.md can be
reproduced rather than asserted.

WHAT IT SUPPORTS THAT PAYSIM DID NOT. AMLSim's accounts file carries `open_dt`,
so `receiver_age` is computable from the data instead of being switched off.
That is one more capability of the six PaySim forced off, and it means the
FRESH_RECEIVER rule is exercised here for the first time on foreign data.

SCOPE. AMLSim models INTERBANK anti-money-laundering flows, not consumer
card-to-card transfer. Amounts, cadence and account population all differ. This
tests whether the rules' SHAPE transfers, not their thresholds - which is the
only kind of transfer test a threshold-carrying rule can pass on foreign data,
and the same scope the PaySim run settled on.

    python amlsim_adapter.py --dir /path/to/AMLSim/outputs/<simulation_name>

Nothing is retrained and no threshold is tuned. See validation/README.md,
"Why not train on it".
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


# AMLSim's currency is unspecified and its default amounts are ~100-1000, three
# orders below UZS. Rules with absolute thresholds (NEW_PAYEE_ABS_FLOOR,
# STRUCTURING_THRESHOLD, LIMIT_DAILY) would never fire without rescaling. One
# factor, derived from the medians, applied to every row: a unit conversion,
# not tuning. Identical treatment to paysim_adapter.scale_factor.
def scale_factor(amounts, our_median_uzs=138_740.0):
    med = float(amounts.median())
    return our_median_uzs / med if med > 0 else 1.0


def load(dirpath):
    """Read AMLSim's output triple. Column names follow paramFiles/schema.json."""
    def _find(*names):
        for n in names:
            p = os.path.join(dirpath, n)
            if os.path.exists(p):
                return p
        raise SystemExit(
            f"none of {names} found in {dirpath}. Point --dir at AMLSim's "
            f"outputs/<simulation_name>/ directory.")

    tx = pd.read_csv(_find("transactions.csv", "tx_log.csv"))
    acct = pd.read_csv(_find("accounts.csv"))
    try:
        alerts = pd.read_csv(_find("alert_transactions.csv", "alert_tx.csv"))
    except SystemExit:
        alerts = None            # typology breakdown unavailable; rest still runs
    return tx, acct, alerts


def _epoch(series):
    """AMLSim timestamps are dates derived from `base_date` + step, so the clock
    granularity is ONE DAY by default. That is coarser than PaySim's hourly
    step, and it cuts in a specific direction: RECEIVER_WINDOW_S is 3600 s, so a
    one-hour window covers at most one simulated day of inbound traffic, while
    AMLSim's fan_in typology is configured to spread over min_period..max_period
    STEPS (5-20 in the shipped parameter files). The deployed window therefore
    sees a FRACTION of each fan-in pattern by construction.

    This is reported rather than corrected. Widening the window to fit the
    dataset would be tuning on the validation set, which is what this directory
    exists to avoid; the sensitivity section of the report shows what a wider
    window would have seen, explicitly not adopted.
    """
    dt = pd.to_datetime(series, errors="coerce")
    if dt.notna().any():
        return (dt.astype("int64") // 10**9).astype("int64")
    # Some builds emit a raw integer step instead of a date.
    return (pd.to_numeric(series, errors="coerce").fillna(0) * 86400).astype("int64")


def run(dirpath, limit):
    tx, acct, alerts = load(dirpath)

    open_dt = pd.to_datetime(
        acct.set_index("acct_id")["open_dt"], errors="coerce")
    # NOT a leading-underscore name: itertuples renames such columns to
    # positional ones (_1, _2 ...), silently breaking getattr below.
    tx["ts_epoch"] = _epoch(tx["tran_timestamp"])
    tx = tx.sort_values("ts_epoch").reset_index(drop=True)
    if limit:
        tx = tx.head(limit)

    # Typology per transaction, so recall can be split by leg the way
    # ml/README.md splits it on our own data.
    typ = {}
    if alerts is not None and "alert_type" in alerts.columns:
        typ = dict(zip(alerts["tran_id"], alerts["alert_type"]))

    scale = scale_factor(tx["base_amt"])
    n_sar = int(tx["is_sar"].astype(str).str.lower().isin(["true", "1"]).sum())
    print(f"{len(tx):,} AMLSim transactions, {acct.shape[0]:,} accounts")
    print(f"SAR-labelled: {n_sar:,} ({n_sar/max(len(tx),1):.3%})")
    print(f"amount scale factor: {scale:,.1f}x "
          f"(median {tx['base_amt'].median():,.1f} -> "
          f"{tx['base_amt'].median()*scale:,.0f} UZS)")
    if typ:
        print("typologies present:",
              ", ".join(f"{k}={v}" for k, v in Counter(typ.values()).most_common()))
    print()

    senders, receivers = defaultdict(SenderState), defaultdict(ReceiverState)
    rows, hits_by_class = [], defaultdict(Counter)

    for r in tx.itertuples(index=False):
        ts = int(getattr(r, "ts_epoch"))
        label = 1 if str(getattr(r, "is_sar")).lower() in ("true", "1") else 0
        bene = getattr(r, "bene_acct")

        # Receiver account age in days, from the accounts file. PaySim had no
        # equivalent field, so this capability was forced off there.
        opened = open_dt.get(bene, pd.NaT)
        age_days = None
        if pd.notna(opened):
            age_days = max(0, (pd.Timestamp(ts, unit="s") - opened).days)

        ev = {
            "amount_uzs": float(getattr(r, "base_amt")) * scale,
            "sender_pinfl": getattr(r, "orig_acct"),
            "receiver_pinfl": bene,
        }
        res = evaluate(ev, age_days, senders[ev["sender_pinfl"]], ts,
                       receivers[bene])
        rows.append((label, res["cep_score"], res["decision"],
                     typ.get(getattr(r, "tran_id"), "")))
        for h in res["rule_hits"]:
            hits_by_class["fraud" if label else "legit"][h] += 1

    return (pd.DataFrame(rows, columns=["label", "cep_score", "decision", "typology"]),
            hits_by_class)


def report(res, hits):
    n_fraud = int((res.label == 1).sum())
    n_legit = int((res.label == 0).sum())
    flagged = res.decision.isin(["REVIEW", "BLOCK"])

    print("=" * 72)
    print("A. PER-RULE LIFT - does each rule carry signal on foreign data?")
    print("=" * 72)
    print("Threshold-free, so it answers the question whatever the decision")
    print("layer does. Same measure as the PaySim run, for comparability.\n")
    print(f"{'rule':<26}{'on SAR':>11}{'on legit':>12}{'lift':>9}")
    every = set(hits["fraud"]) | set(hits["legit"])
    lifts = []
    for rule in sorted(every):
        f = hits["fraud"][rule] / max(n_fraud, 1)
        l = hits["legit"][rule] / max(n_legit, 1)
        lifts.append((rule, f, l, (f / l) if l > 0 else float("inf")))
    for rule, f, l, lift in sorted(lifts, key=lambda x: -x[3]):
        shown = "inf" if lift == float("inf") else f"{lift:.1f}x"
        print(f"{rule:<26}{f:>10.2%}{l:>12.2%}{shown:>9}")

    print("\n" + "=" * 72)
    print("B. BY TYPOLOGY - the reason this dataset was chosen")
    print("=" * 72)
    print("PaySim has no collection stage, so MULE_FAN_IN could not be tested")
    print("there at all. AMLSim labels fan_in and fan_out separately, so the leg")
    print("asymmetry reported in ml/README.md can be checked rather than asserted.\n")
    labelled = res[(res.label == 1) & (res.typology != "")]
    if labelled.empty:
        print("  No typology labels found - alert_transactions.csv was missing or")
        print("  carried no alert_type column. Section B is unavailable.")
    else:
        print(f"{'typology':<18}{'n':>8}{'flagged':>10}{'recall':>10}")
        for t, g in labelled.groupby("typology"):
            fl = g.decision.isin(["REVIEW", "BLOCK"])
            print(f"{t:<18}{len(g):>8,}{int(fl.sum()):>10,}{fl.mean():>10.1%}")
        print("\n  Read the fan_in row against fan_out. On our own data the split")
        print("  was 57.8% against 93.8% BEFORE receiver-side state, and the whole")
        print("  design claim is that the gap closes with it. A gap of the same")
        print("  SIGN here reproduces the finding off our generator; no gap, or the")
        print("  opposite sign, falsifies it - which is why this run is worth doing.")

    print("\n" + "=" * 72)
    print("C. DECISION LAYER")
    print("=" * 72)
    fr = flagged[res.label == 1].mean() if n_fraud else 0.0
    lg = flagged[res.label == 0].mean() if n_legit else 0.0
    print(f"  SAR flagged   : {int(flagged[res.label==1].sum()):>7,} / {n_fraud:<7,} ({fr:.1%})")
    print(f"  legit flagged : {int(flagged[res.label==0].sum()):>7,} / {n_legit:<7,} ({lg:.2%})")
    if fr > 0 and lg > 0:
        print(f"  decision-layer lift: {fr/lg:.1f}x")

    import rules as _R
    review_at, block_at = _R._thresholds()
    if abs(review_at - C.REVIEW_THRESHOLD) > 1e-9:
        print(f"  REVIEW threshold : {review_at:.2f} "
              f"(scaled from {C.REVIEW_THRESHOLD:.2f} for this capability profile)")
    else:
        print(f"  REVIEW threshold : {review_at:.2f}")

    print("\n" + "=" * 72)
    print("D. THE WINDOW, AND WHY IT IS NOT WIDENED HERE")
    print("=" * 72)
    print(f"  RECEIVER_WINDOW_S is {C.RECEIVER_WINDOW_S:,} s "
          f"({C.RECEIVER_WINDOW_S/3600:.0f} h).")
    print("  AMLSim's clock advances one DAY per step and its fan_in typology is")
    print("  configured to spread over several steps, so the deployed window sees")
    print("  a fraction of each pattern BY CONSTRUCTION. A weak fan-in result here")
    print("  is therefore ambiguous between 'the rule does not transfer' and 'the")
    print("  window is shorter than the pattern', and the two must not be conflated.")
    print("  Widening the window to fit this dataset would be tuning on the")
    print("  validation set. The honest statement of a null result is: not")
    print("  reproduced AT THIS WINDOW, on a dataset whose time base is coarser")
    print("  than the window. Re-running with a longer window is a legitimate")
    print("  SEPARATE experiment, reported as such.")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", required=True,
                    help="AMLSim outputs/<simulation_name>/ directory")
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()
    if not os.path.isdir(args.dir):
        raise SystemExit(f"{args.dir} is not a directory")

    # AMLSim carries account ids, amounts, a clock and account open dates.
    # Everything not backed by real data is switched OFF rather than defaulted,
    # so no rule can fire on a fabricated zero. receiver_age stays ON - the one
    # capability this dataset supports and PaySim did not.
    for key in ("myid_kinship", "device_telemetry", "geo_telemetry",
                "session_telemetry", "channel"):
        CAP.MODES[key] = "off"

    print("capability profile for this run:")
    print(CAP.describe())
    print()

    res, hits = run(args.dir, args.limit)
    report(res, hits)

    print("\nWhat this does and does not show:")
    print("  - AMLSim is synthetic, so this is not production validation.")
    print("  - It IS an independent generator, built by a different group for a")
    print("    different purpose, and these rules run on it unchanged.")
    print("  - It models INTERBANK AML flows, not consumer card P2P. A result")
    print("    here is about the rules' shape, never about their thresholds.")
    print("  - SAR labels mark laundering typologies, not customer fraud. The")
    print("    fan_in typology is the analogue of our MULE collection stage; the")
    print("    others are present but are not what this run is for.")


if __name__ == "__main__":
    main()
