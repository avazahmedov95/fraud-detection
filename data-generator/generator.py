"""Builds the dataset: population, normal behaviour, travel, injected fraud.

Every parameter and the dataset of record: docs/generator-spec.md.
"""

import argparse
import os
from collections import defaultdict
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from config import (GeneratorConfig, AMOUNT_MIN, AMOUNT_MAX,
                    CHANNELS, CHANNEL_WEIGHTS, FAMILY_PAYEE_SHARE)
from events import EVENT_FIELDS, make_event
import persons as P
import travel as T
from persons import build_population, build_fraud_accounts
from fraud_patterns import inject_fraud


def _normalise(weights):
    a = np.asarray(weights, dtype=float)
    return a / a.sum()


def _assign_payees(persons, rng):
    """Each person gets 3..8 frequent payees.

    A share of them are relatives (FAMILY_PAYEE_SHARE): people send money to
    family more than to anyone else, so a kinship signal that never appeared in
    legitimate traffic would be as unrealistic as one that never appeared in
    fraud. The rest are drawn from the population at large.
    """
    by_household = P.households(persons)
    payees = {}
    for p in persons:
        kin = P.relatives_of(p, by_household)
        chosen = set()
        k = int(rng.integers(3, 9))
        guard = 0
        while len(chosen) < k and guard < 100 * k:
            guard += 1
            if kin and rng.random() < FAMILY_PAYEE_SHARE:
                q = kin[int(rng.integers(len(kin)))].pinfl
            else:
                q = persons[int(rng.integers(len(persons)))].pinfl
            if q != p.pinfl:
                chosen.add(q)
        # sorted(), not list(): iterating a set of strings orders them by hash,
        # and Python randomises string hashing per process (PYTHONHASHSEED).
        # Under list() this payee list came out shuffled differently on every
        # run, so the same seed, the same code and the same pinned versions
        # still produced a different receiver for most transactions. Measured
        # 2026-08-30 on two runs differing only in PYTHONHASHSEED: persons.csv
        # byte-identical, 36,072 of 50,000 transaction rows different. Sorting
        # makes the order a property of the data rather than of the process.
        # NOTE: this fix changes the RNG stream, so it does not regenerate the
        # frozen dataset of record - see the hashes in docs/generator-spec.md.
        payees[p.pinfl] = sorted(chosen)
    return payees


def generate_normal(config, persons, by_pinfl, n_normal, rng, start_dt, trips):
    """Behaviourally-consistent legitimate traffic."""
    payees = _assign_payees(persons, rng)
    known = {p.pinfl: set(payees[p.pinfl]) for p in persons}  # already-seen payees

    # Heavy-tailed activity: a few very active senders.
    activity = _normalise(rng.random(len(persons)) ** 3)
    sender_idx = rng.choice(len(persons), size=n_normal, p=activity)
    channel_p = _normalise(CHANNEL_WEIGHTS)
    span_seconds = config.days * 24 * 3600

    events = []
    for i in range(n_normal):
        sender = persons[int(sender_idx[i])]

        # Mostly pay a frequent payee; occasionally a brand-new one.
        rp = str(rng.choice(payees[sender.pinfl]))
        if rng.random() < 0.05:  # noise: a genuinely new legit payee
            cand = persons[int(rng.integers(len(persons)))].pinfl
            if cand != sender.pinfl:
                rp = cand

        # Hard negatives: legitimate but suspicious-looking transfers (e.g. a rent
        # deposit, a one-off purchase) to a brand-new, unrelated payee. These
        # overlap APP fraud on the obvious signals, so the model can't separate
        # the classes trivially.
        hard_neg = rng.random() < config.hard_negative_share
        if hard_neg:
            cand = persons[int(rng.integers(len(persons)))].pinfl
            if cand != sender.pinfl:
                rp = cand

        receiver = by_pinfl[rp]
        # NOTE the two senses of "new payee", which are NOT the same thing and
        # differ on ~28% of rows. This one is generator-internal: is the
        # receiver outside the sender's ASSIGNED regular-payee set. The feature
        # the model and the rules actually use (features.py) is stream-derived:
        # has this sender sent to this receiver BEFORE, within the observed
        # window. A person's regular payee is still stream-new the first time
        # they are paid here. The column below records the first; the signal
        # check at the end of this file reports the second, because that is the
        # one anything downstream reads.
        is_new = rp not in known[sender.pinfl]
        known[sender.pinfl].add(rp)

        amount = float(np.clip(
            np.exp(rng.normal(np.log(max(sender.typical_amount, 1000)), 0.5)),
            AMOUNT_MIN, AMOUNT_MAX))
        if hard_neg and rng.random() < 0.5:          # a large legitimate one-off
            amount = float(np.clip(np.exp(rng.normal(15.0, 0.5)), AMOUNT_MIN, AMOUNT_MAX))

        ts = start_dt + timedelta(seconds=float(rng.random() * span_seconds))
        ts = ts.replace(
            hour=int(rng.integers(sender.active_start_hour, sender.active_end_hour)),
            minute=int(rng.integers(0, 60)),
            second=int(rng.integers(0, 60)))

        # Travellers transact from where they are. Events falling mid-journey
        # are re-timed to just after arrival: a transaction at the origin and
        # the next at the destination minutes later would look like impossible
        # travel in perfectly legitimate traffic.
        ts = T.settle_after_transit(sender, trips, ts, rng)
        region, _ = T.locate(sender, trips, ts)

        ev = make_event(
            sender, receiver, amount, ts,
            channel=str(rng.choice(CHANNELS, p=channel_p)),
            device_id=f"dev-{sender.pinfl[-8:]}",        # the sender's usual device
            is_new_payee=is_new,
            balance_before=amount * float(rng.uniform(1.2, 8.0)), rng=rng)
        ev["sender_region"] = region
        events.append(ev)
    return events


def build_dataset(config):
    rng = np.random.default_rng(config.seed)
    persons, by_pinfl = build_population(config, rng)
    fraud_accounts = build_fraud_accounts(max(50, config.n_persons // 25), rng)
    start_dt = datetime.fromisoformat(config.start_date)

    n_fraud = int(config.fraud_rate * config.n_transactions)
    n_normal = config.n_transactions - n_fraud

    trips = T.plan_trips(persons, rng, start_dt, config.days)
    normal = generate_normal(config, persons, by_pinfl, n_normal, rng, start_dt,
                             trips)

    # When and where each person was last legitimately active. A session
    # hijack is defined relative to that: the takeover continues an account
    # whose owner was just seen somewhere else.
    legit_activity = defaultdict(list)
    for e in normal:
        legit_activity[e["sender_pinfl"]].append(
            (e["event_time"], e["sender_region"]))
    for v in legit_activity.values():
        v.sort()

    fraud = inject_fraud(config, persons, by_pinfl, fraud_accounts, n_fraud, rng,
                         start_dt, legit_activity)

    df = (pd.DataFrame(normal + fraud, columns=EVENT_FIELDS)
            .sort_values("event_time")
            .reset_index(drop=True))

    persons_df = pd.DataFrame([{
        "pinfl": p.pinfl, "card": p.card, "network": p.network,
        "full_name": p.full_name,
        "bank_code": p.bank_code, "bank_name": p.bank_name,
        "region": p.region, "account_age_days": p.account_age_days,
        "is_fraud_account": p.is_fraud_account,
    } for p in persons + fraud_accounts])

    return df, persons_df


def _summary(df):
    n = len(df)
    n_fraud = int(df["label_is_fraud"].sum())
    print(f"transactions      : {n:,}")
    print(f"fraudulent        : {n_fraud:,}  ({n_fraud / n:.2%})")
    print("\nby fraud type:")
    print(df.loc[df.label_is_fraud == 1, "label_fraud_type"].value_counts().to_string())
    _signal_check(df)


def _signal_check(df):
    """Report `is_new_payee` as the PIPELINE computes it, not as the column
    records it.

    The two disagreed on 14,201 of 50,000 rows and the check was reporting the
    wrong one: the column said 8.11% of legitimate traffic went to a new payee,
    while the value features.py derives from the stream says 36.93%. A check
    that reports on a quantity nothing downstream reads is worse than no check,
    because it looks like verification. Both are printed now, and the gap
    between them is the point.
    """
    seen, computed = {}, []
    for card, rcv in zip(df["sender_card"], df["receiver_pinfl"]):
        s = seen.setdefault(card, set())
        computed.append(0 if rcv in s else 1)
        s.add(rcv)
    df = df.assign(_computed=computed)
    f = df.label_is_fraud == 1
    print("\nsignal check  is_new_payee")
    print(f"  as computed from the stream (what the model and rules see):"
          f"  fraud={df.loc[f, '_computed'].mean():.2%}"
          f"  legit={df.loc[~f, '_computed'].mean():.2%}")
    print(f"  as recorded in the column (outside the assigned payee set):"
          f"     fraud={df.loc[f, 'is_new_payee'].mean():.2%}"
          f"  legit={df.loc[~f, 'is_new_payee'].mean():.2%}")
    disagree = (df["is_new_payee"].astype(int) != df["_computed"]).sum()
    print(f"  the two senses disagree on {disagree:,} of {len(df):,} rows"
          f" ({disagree / len(df):.1%}) - see the note beside `is_new` above")
    if df.loc[f, "_computed"].mean() > 0.98:
        print("  !! fraud is essentially ALWAYS to a stream-new payee. The"
              " threat model (docs/threat-model.md 4) rates that control"
              " 'low cost to evade - a prior small transfer establishes the"
              " payee', and this generator does not produce that evasion, so"
              " the feature's measured value is an upper bound.")


def parse_args():
    cfg = GeneratorConfig()
    ap = argparse.ArgumentParser(description="Uzbekistan P2P synthetic data generator")
    ap.add_argument("--persons", type=int, default=cfg.n_persons)
    ap.add_argument("--transactions", type=int, default=cfg.n_transactions)
    ap.add_argument("--fraud-rate", type=float, default=cfg.fraud_rate)
    ap.add_argument("--days", type=int, default=cfg.days)
    ap.add_argument("--seed", type=int, default=cfg.seed)
    ap.add_argument("--out", type=str, default="./out")
    return ap.parse_args()


def main():
    args = parse_args()
    config = GeneratorConfig(
        n_persons=args.persons, n_transactions=args.transactions,
        fraud_rate=args.fraud_rate, days=args.days, seed=args.seed)

    df, persons_df = build_dataset(config)

    os.makedirs(args.out, exist_ok=True)
    df.to_csv(os.path.join(args.out, "transactions.csv"), index=False)
    persons_df.to_csv(os.path.join(args.out, "persons.csv"), index=False)

    _summary(df)
    print(f"\nwritten to {os.path.abspath(args.out)}/  "
          f"(transactions.csv, persons.csv)")


if __name__ == "__main__":
    main()