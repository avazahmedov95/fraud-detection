"""Compares seeded against unseeded APP episodes within one dataset, to bound how
much is_new_payee owes to the generator rather than to behaviour.
"""

import argparse
import glob
import math
import statistics as st
from collections import defaultdict

import pandas as pd

from rules import SenderState, ReceiverState, evaluate
from replay_eval import _as_bool, _as_age
import features as F

MAX_SEED_LAG_DAYS = 21     # must match maybe_seed_payee in fraud_patterns.py


def replay(path):
    df = pd.read_csv(path).sort_values("event_time").reset_index(drop=True)
    seen, stream_new = defaultdict(set), []
    # Resolved through features.payee_key rather than by naming a column: this
    # recomputation exists to be compared against what the RULE layer sees, and
    # a cross-check keyed on a different identity than the thing it checks is
    # not a cross-check.
    payees = [F.payee_key({"receiver_pinfl": p, "receiver_card": c})
              for p, c in zip(df["receiver_pinfl"], df["receiver_card"])]
    for card, rcv in zip(df["sender_card"], payees):
        stream_new.append(0 if rcv in seen[card] else 1)
        seen[card].add(rcv)
    df["stream_new"] = stream_new

    states, rstates, flagged = defaultdict(SenderState), defaultdict(ReceiverState), []
    for r in df.to_dict("records"):
        ev = {"amount_uzs": r["amount_uzs"], "sender_pinfl": r["sender_pinfl"],
              "receiver_pinfl": r["receiver_pinfl"], "device_id": r["device_id"],
              "sender_region": r["sender_region"], "channel": r.get("channel", "MOBILE_APP"),
              "sender_network": r.get("sender_network", ""),
              "receiver_network": r.get("receiver_network", ""),
              "active_call": _as_bool(r.get("active_call")),
              "secs_login_to_confirm": r.get("secs_login_to_confirm", 0.0),
              "sender_card": r.get("sender_card", ""),
              "receiver_card": r.get("receiver_card", ""),
              "is_family_transfer": _as_bool(r.get("is_family_transfer"))}
        res = evaluate(ev, _as_age(r.get("receiver_account_age_days")),
                       states[r["sender_card"]],
                       pd.Timestamp(r["event_time"]).timestamp(),
                       rstates[F.payee_key(ev)])
        flagged.append(res["decision"] in ("REVIEW", "BLOCK"))
    df["flagged"] = flagged
    # ISO8601: the generator omits microseconds when they are zero, so the
    # column is not one fixed format.
    df["t"] = pd.to_datetime(df["event_time"], format="ISO8601")
    return df


def ci(vals):
    n = len(vals)
    if n < 2:
        return float("nan"), float("nan")
    m, sem = st.mean(vals), st.stdev(vals) / math.sqrt(n)
    t = {2: 12.71, 3: 4.30, 4: 3.18, 5: 2.78, 6: 2.57, 7: 2.45,
         8: 2.36, 9: 2.31, 10: 2.26}.get(n, 1.96)
    return m - t * sem, m + t * sem


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--files", nargs="+", required=True)
    args = ap.parse_args()
    files = [f for pat in args.files for f in sorted(glob.glob(pat))] or args.files

    print(f"{len(files)} dataset(s); APP episodes after day {MAX_SEED_LAG_DAYS} only\n")
    print(f"{'dataset':<22}{'n new':>7}{'recall':>9}{'n seeded':>10}{'recall':>9}{'delta':>9}")
    deltas, pooled = [], []
    for f in files:
        df = replay(f)
        app = df[(df["label_is_fraud"] == 1) & (df["label_fraud_type"] == "APP")].copy()
        app["day"] = (app["t"] - df["t"].min()).dt.days
        late = app[app["day"] >= MAX_SEED_LAG_DAYS]
        new, seeded = late[late.stream_new == 1], late[late.stream_new == 0]
        if len(new) == 0 or len(seeded) == 0:
            print(f"{f:<22}  skipped - one group empty")
            continue
        d = seeded.flagged.mean() - new.flagged.mean()
        deltas.append(d)
        pooled.append((int(new.flagged.sum()), len(new),
                       int(seeded.flagged.sum()), len(seeded)))
        name = f.replace("\\", "/").split("/")[-2]
        print(f"{name:<22}{len(new):>7}{new.flagged.mean():>9.1%}"
              f"{len(seeded):>10}{seeded.flagged.mean():>9.1%}{d:>+9.1%}")

    if not deltas:
        raise SystemExit("nothing measured")

    lo, hi = ci(deltas)
    neg = sum(1 for d in deltas if d < 0)
    hn, nn, hs, ns = (sum(x[i] for x in pooled) for i in range(4))
    print("\n" + "=" * 66)
    print(f"pooled   new payee {hn}/{nn} = {hn/nn:.1%}   "
          f"seeded {hs}/{ns} = {hs/ns:.1%}")
    print(f"delta    {st.mean(deltas):+.1%}"
          + (f"  95% CI [{lo:+.1%}, {hi:+.1%}]" if len(deltas) > 1 else "")
          + f"  sign {neg}/{len(deltas)} negative")
    print("\nA negative delta is the evasion working: the same episode, with one")
    print("small prior transfer, is detected less often. The threat model called")
    print("this control cheap to evade from reasoning alone; this is the price.")
    if len(deltas) < 3:
        print("\n!! Fewer than three seeds. The interval is decoration at this n.")


if __name__ == "__main__":
    main()
