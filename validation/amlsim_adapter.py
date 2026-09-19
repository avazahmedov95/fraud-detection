"""Replays the deployed rules over an IBM AMLSim run, which generates the mule
collection stage PaySim lacks (README.md 3). The shared replay is in harness.py.
"""

import argparse
import os
from collections import Counter

import pandas as pd

import harness as RP
from harness import CAP, C, Event, scale_factor      # noqa: F401  (scale_factor: tests)


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
    """AMLSim's clock is ONE DAY per step, so the one-hour receiver window sees only a
    fraction of each fan-in pattern - reported, not corrected, since widening it to
    fit the dataset would be tuning on validation."""
    dt = pd.to_datetime(series, errors="coerce")
    if dt.notna().any():
        return (dt.astype("int64") // 10**9).astype("int64")
    # Some builds emit a raw integer step instead of a date.
    return (pd.to_numeric(series, errors="coerce").fillna(0) * 86400).astype("int64")


def run(dirpath, limit):
    tx, acct, alerts = load(dirpath)

    # NOT a leading-underscore name: itertuples renames such columns to
    # positional ones (_1, _2 ...), silently breaking getattr below.
    tx["ts_epoch"] = _epoch(tx["tran_timestamp"])
    tx = tx.sort_values("ts_epoch").reset_index(drop=True)
    if limit:
        tx = tx.head(limit)

    # Typology per transaction, so recall splits by leg the way ml/README.md does.
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

    return RP.replay(_events(tx, typ, scale), total=len(tx))


def _events(tx, typ, scale):
    for r in tx.itertuples(index=False):
        ts = int(getattr(r, "ts_epoch"))
        bene = getattr(r, "bene_acct")

        # str(): AMLSim numbers accounts from 0, and a falsy 0 would give an empty key.
        yield Event(
            ev={"amount_uzs": float(getattr(r, "base_amt")) * scale,
                "sender_pinfl": str(getattr(r, "orig_acct")),
                "receiver_pinfl": str(bene)},
            ts=ts,
            label=1 if str(getattr(r, "is_sar")).lower() in ("true", "1") else 0,
            typology=typ.get(getattr(r, "tran_id"), ""))


BY_TYPOLOGY_NOTES = (
    "PaySim has no collection stage, so MULE_FAN_IN could not be tested",
    "there at all. AMLSim labels fan_in and fan_out separately, so the leg",
    "asymmetry reported in ml/README.md can be checked rather than asserted.",
    "",
    "Read the fan_in row against fan_out. On our own data the split was 57.8%",
    "against 93.8% BEFORE receiver-side state, and the whole design claim is",
    "that the gap closes with it. A gap of the same SIGN here reproduces the",
    "finding off our generator; no gap, or the opposite sign, falsifies it -",
    "which is why this run is worth doing.",
)


def report(res, hits):
    RP.section_lift(res, hits, positive="SAR", width=72)
    print()
    RP.section_by_group(res, "B. BY TYPOLOGY - the reason this dataset was chosen",
                        BY_TYPOLOGY_NOTES, width=72)
    print()
    RP.section_decision(res, hits, positive="SAR", width=72)

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

    RP.capability_profile("myid_kinship", "geo_telemetry",
                          "session_telemetry")

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
