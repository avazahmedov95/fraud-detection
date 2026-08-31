"""
Offline replay of the synthetic dataset through the pure CEP rule engine.

Runs WITHOUT Flink/Kafka/Redis/Neo4j: it reads transactions.csv (which already
carries the enrichment columns the live Neo4j lookup would return) and feeds each
event, in time order, through `rules.evaluate`, keeping per-sender state in a
plain dict — exactly the logic the Flink job runs, minus the runtime.

Output is the *design behaviour* of the CEP layer on synthetic data: how it
separates fraud from legitimate traffic, and which rules drive it. These are
tuning targets, not validated production metrics.

  python replay_eval.py --file ../data-generator/out/transactions.csv
"""

import argparse
from collections import defaultdict, Counter

import pandas as pd

import config as C
from rules import (SenderState, ReceiverState, PopulationBaseline,
                   evaluate)


def _as_bool(v):
    return str(v).strip().lower() in ("true", "1")


def _as_age(v):
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def run(path):
    df = pd.read_csv(path)
    df = df.sort_values("event_time").reset_index(drop=True)
    has_labels = "label_is_fraud" in df.columns

    states = defaultdict(SenderState)
    # Keyed by payee, mirroring the shared store the live job reads.
    receiver_states = defaultdict(ReceiverState)
    # One histogram for the whole stream, mirroring the shared counter the live
    # job would keep. Only consulted when MULE_FAN_IN_MODE is "relative"; it is
    # built unconditionally so the two modes replay identical code paths apart
    # from the threshold itself.
    population = PopulationBaseline()
    decisions, hit_counter = [], defaultdict(Counter)

    for row in df.itertuples(index=False):
        r = row._asdict()
        event = {
            "amount_uzs": r["amount_uzs"],
            "sender_pinfl": r["sender_pinfl"],
            "receiver_pinfl": r["receiver_pinfl"],
            "device_id": r["device_id"],
            "sender_region": r["sender_region"],
            "channel": r.get("channel", "MOBILE_APP"),
            "sender_network": r.get("sender_network", ""),
            "receiver_network": r.get("receiver_network", ""),
            # behavioural session signals (backlog #7) — required for the
            # COACHED_SESSION rule and the secs_login_z baseline.
            "active_call": _as_bool(r.get("active_call")),
            "secs_login_to_confirm": r.get("secs_login_to_confirm", 0.0),
            # issuer identity, for the on-us test behind the receiver_age
            # capability; kinship, for the myid_kinship capability.
            "sender_bank_name": r.get("sender_bank_name", ""),
            "receiver_bank_name": r.get("receiver_bank_name", ""),
            "is_family_transfer": _as_bool(r.get("is_family_transfer")),
        }
        ts = pd.Timestamp(r["event_time"]).timestamp()
        res = evaluate(event,
                       receiver_age_days=_as_age(r.get("receiver_account_age_days")),
                       state=states[r["sender_card"]],
                       now=ts,
                       receiver_state=receiver_states[r["receiver_pinfl"]],
                       population=population)
        decisions.append(res["decision"])
        bucket = "fraud" if (has_labels and int(r["label_is_fraud"]) == 1) else "legit"
        for h in res["rule_hits"]:
            hit_counter[bucket][h] += 1

    df["decision"] = decisions
    thr = population.threshold(C.MULE_FAN_IN_QUANTILE, C.MULE_FAN_IN_MIN_SENDERS)
    print(f"MULE_FAN_IN mode: {C.MULE_FAN_IN_MODE}"
          + (f"  (q={C.MULE_FAN_IN_QUANTILE}, threshold settled at "
             f"{thr} senders/h over {population.n:,} observations)"
             if C.MULE_FAN_IN_MODE == "relative"
             else f"  (fixed at {C.MULE_FAN_IN_MIN_SENDERS} senders/h)"))
    _report(df, hit_counter, has_labels)


def _report(df, hit_counter, has_labels):
    n = len(df)
    print(f"events scored : {n:,}")
    print("\noverall decisions:")
    print(df["decision"].value_counts().reindex(["ALLOW", "REVIEW", "BLOCK"]).fillna(0).astype(int).to_string())

    if not has_labels:
        print("\n(no labels in file — run the producer/CSV with labels for a fraud/legit breakdown)")
        return

    flagged = df["decision"].isin(["REVIEW", "BLOCK"])
    fraud = df["label_is_fraud"] == 1
    legit = ~fraud

    fr_flagged = (flagged & fraud).sum()
    lg_flagged = (flagged & legit).sum()
    print("\nfraud vs legit (flagged = REVIEW or BLOCK):")
    print(f"  fraud flagged : {fr_flagged:>6} / {fraud.sum():<6}  ({fr_flagged / max(fraud.sum(),1):.1%})")
    print(f"  legit flagged : {lg_flagged:>6} / {legit.sum():<6}  ({lg_flagged / max(legit.sum(),1):.2%})   <- false positives")

    print("\nflagged rate by fraud type:")
    for ftype, grp in df[fraud].groupby("label_fraud_type"):
        f = grp["decision"].isin(["REVIEW", "BLOCK"]).mean()
        print(f"  {ftype:<12} {f:.1%}  (n={len(grp)})")

    print("\ntop rule hits among fraud:")
    for rule, c in hit_counter["fraud"].most_common(8):
        print(f"  {rule:<22} {c}")
    print("\ntop rule hits among legit:")
    for rule, c in hit_counter["legit"].most_common(8):
        print(f"  {rule:<22} {c}")

    print("\nNote: design behaviour of the CEP layer on synthetic data — tuning "
          "targets, not validated production metrics. ML fusion (phase 6) lifts recall further.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Replay the dataset through the CEP rule engine")
    ap.add_argument("--file", default="../data-generator/out/transactions.csv")
    run(ap.parse_args().file)
