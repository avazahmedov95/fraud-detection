"""Builds the dataset: population, normal behaviour, travel, injected fraud.
Every parameter and the dataset of record: docs/generator-spec.md."""

import argparse
import os
from collections import defaultdict
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

import config as C
from config import (GeneratorConfig, AMOUNT_MIN, AMOUNT_MAX, STRUCTURING_THRESHOLD,
                    FAMILY_PAYEE_SHARE, SECOND_DEVICE_USE_RATE)
from events import EVENT_FIELDS, make_event, round_like_a_person
import persons as P
import travel as T
from persons import build_population, build_fraud_accounts, _normalise
from fraud_patterns import inject_fraud


def _assign_payees(persons, rng):
    """Each person gets 3..8 frequent payees.

    A share are relatives (FAMILY_PAYEE_SHARE): a kinship signal that never appeared in
    legitimate traffic would be as unrealistic as one that never appeared in fraud.
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
        # sorted(), not list(): iterating a set of strings orders them by hash, and
        # Python randomises string hashing per process (PYTHONHASHSEED), so under list()
        # the same seed and pinned versions still produced a different receiver for most
        # transactions. Measured 2026-08-30 on two runs differing only in PYTHONHASHSEED:
        # persons.csv byte-identical, 36,072 of 50,000 transaction rows different.
        # NOTE: this fix changes the RNG stream, so it does not regenerate the frozen
        # dataset of record - see the hashes in docs/generator-spec.md.
        payees[p.pinfl] = sorted(chosen)
    return payees


def _lookalikes(config, persons, known, rng, start_dt, trips):
    """Legitimate transfers with fraud's shapes (generator-spec.md 10): collections
    - many people paying one person within hours, for a wedding, a gift, a joint
    purchase - and large payments split into parts, a car or a deposit paid in
    instalments. Without them MULE_FAN_IN and STRUCTURING can fire only on fraud,
    which is the label under another name. Off in the baseline profile."""
    events = []
    span = max(1, config.days - 2) * 24 * 3600

    def one(sender, receiver, amount, ts):
        is_new = receiver.pinfl not in known[sender.pinfl]
        known[sender.pinfl].add(receiver.pinfl)
        ev = make_event(sender, receiver, amount, ts,
                        device_id=f"dev-{sender.pinfl[-8:]}", is_new_payee=is_new,
                        balance_before=amount * float(rng.uniform(1.5, 6.0)), rng=rng)
        ev["sender_region"] = T.locate(sender, trips, ts)[0]
        events.append(ev)

    for p in persons:
        if config.collector_share > 0 and rng.random() < config.collector_share:
            t0 = start_dt + timedelta(seconds=float(rng.random() * span))
            for i in range(int(rng.integers(5, 16))):
                s = persons[int(rng.integers(len(persons)))]
                if s.pinfl != p.pinfl:
                    one(s, p, float(np.clip(np.exp(rng.normal(12.3, 0.6)),
                                            AMOUNT_MIN, AMOUNT_MAX)),
                        t0 + timedelta(minutes=float(i * rng.uniform(5, 120))))
        if config.split_payment_share > 0 and rng.random() < config.split_payment_share:
            q = persons[int(rng.integers(len(persons)))]
            t0 = start_dt + timedelta(seconds=float(rng.random() * span))
            if q.pinfl != p.pinfl:
                for i in range(int(rng.integers(3, 6))):
                    one(p, q, float(STRUCTURING_THRESHOLD * rng.uniform(0.60, 0.99)),
                        t0 + timedelta(minutes=float(i * rng.uniform(10, 120))))
    return events


def generate_normal(config, persons, by_pinfl, n_normal, rng, start_dt, trips):
    """Behaviourally-consistent legitimate traffic."""
    payees = _assign_payees(persons, rng)
    known = {p.pinfl: set(payees[p.pinfl]) for p in persons}  # already-seen payees
    span_seconds = config.days * 24 * 3600
    # Drawn first and taken out of the same budget; empty, without a draw, in the
    # baseline profile.
    events = _lookalikes(config, persons, known, rng, start_dt, trips)
    phone_changed = {}
    if config.phone_change_share > 0:
        for p in persons:
            if rng.random() < config.phone_change_share:
                phone_changed[p.pinfl] = start_dt + timedelta(
                    seconds=float(rng.random() * span_seconds))
    n_regular = n_normal - len(events)

    # Heavy-tailed activity: a few very active senders.
    activity = _normalise(rng.random(len(persons)) ** 3)
    sender_idx = rng.choice(len(persons), size=n_regular, p=activity)

    for i in range(n_regular):
        sender = persons[int(sender_idx[i])]

        rp = str(rng.choice(payees[sender.pinfl]))
        if rng.random() < 0.05:  # noise: a genuinely new legit payee
            cand = persons[int(rng.integers(len(persons)))].pinfl
            if cand != sender.pinfl:
                rp = cand

        # Hard negatives: legitimate but suspicious-looking transfers (rent deposit, one-off
        # purchase) to a new unrelated payee - they overlap APP fraud, so not separable.
        hard_neg = rng.random() < config.hard_negative_share
        if hard_neg:
            cand = persons[int(rng.integers(len(persons)))].pinfl
            if cand != sender.pinfl:
                rp = cand

        receiver = by_pinfl[rp]
        # TWO senses of "new payee", NOT the same thing - they differ on ~28% of rows. This
        # column is generator-internal: is the receiver outside the sender's ASSIGNED payee
        # set. The one the model and rules use (features.py) is stream-derived: has this
        # sender sent to this receiver BEFORE within the observed window.
        is_new = rp not in known[sender.pinfl]
        known[sender.pinfl].add(rp)

        amount = float(np.clip(
            np.exp(rng.normal(np.log(max(sender.typical_amount, 1000)), 0.5)),
            AMOUNT_MIN, AMOUNT_MAX))
        if hard_neg and rng.random() < 0.5:          # a large legitimate one-off
            amount = float(np.clip(np.exp(rng.normal(15.0, 0.5)), AMOUNT_MIN, AMOUNT_MAX))
        if config.round_amount_share > 0 and rng.random() < config.round_amount_share:
            amount = round_like_a_person(amount)

        ts = start_dt + timedelta(seconds=float(rng.random() * span_seconds))
        ts = ts.replace(
            hour=int(rng.integers(sender.active_start_hour, sender.active_end_hour)),
            minute=int(rng.integers(0, 60)),
            second=int(rng.integers(0, 60)))

        ts = T.settle_after_transit(sender, trips, ts, rng)
        region, _ = T.locate(sender, trips, ts)

        # Most events come from the sender's usual device; those who own a second
        # one use it sometimes. Without this every legitimate event carried one
        # device forever and device_is_new became a fraud-only signal - see the
        # SECOND_DEVICE_* note in config.py.
        device_id = f"dev-{sender.pinfl[-8:]}"
        if sender.pinfl in phone_changed and ts >= phone_changed[sender.pinfl]:
            device_id = f"dev-{sender.pinfl[-8:]}-n"          # a new phone, kept
        if sender.has_second_device and rng.random() < SECOND_DEVICE_USE_RATE:
            device_id = f"dev-{sender.pinfl[-8:]}-b"

        ev = make_event(
            sender, receiver, amount, ts,
            device_id=device_id,
            is_new_payee=is_new,
            balance_before=amount * float(rng.uniform(1.2, 8.0)), rng=rng)
        ev["sender_region"] = region
        events.append(ev)
    return events


def build_dataset(config):
    C.PROFILE = config                    # the session knobs make_event reads
    rng = np.random.default_rng(config.seed)
    persons, by_pinfl = build_population(config, rng)
    fraud_accounts = build_fraud_accounts(max(50, config.n_persons // 25), rng,
                                          aged_share=config.aged_fraud_share)
    start_dt = datetime.fromisoformat(config.start_date)

    n_fraud = int(config.fraud_rate * config.n_transactions)
    n_normal = config.n_transactions - n_fraud

    trips = T.plan_trips(persons, rng, start_dt, config.days)
    normal = generate_normal(config, persons, by_pinfl, n_normal, rng, start_dt,
                             trips)

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
        # Empty for the ~80% who hold one card. Written out because a
        # reader checking payee_identity needs to see which people can
        # receive under two keys - see config.SECOND_CARD_SHARE.
        "card2": p.card2, "bank_code2": p.bank_code2,
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
    """Report `is_new_payee` as the PIPELINE computes it, not as the column records it.

    The two disagreed on 14,201 of 50,000 rows and the check reported the wrong one: the
    column said 8.11% of legitimate traffic went to a new payee, while the value features.py
    derives from the stream says 36.93%. Both are printed now.
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
    ap = argparse.ArgumentParser(description="Uzbekistan P2P synthetic data generator")
    ap.add_argument("--profile", choices=("baseline", "realistic"), default="baseline",
                    help="realistic: docs/generator-spec.md 10")
    # No defaults here: an unset size comes from the profile, not from baseline.
    ap.add_argument("--persons", type=int)
    ap.add_argument("--transactions", type=int)
    ap.add_argument("--fraud-rate", type=float)
    ap.add_argument("--days", type=int)
    ap.add_argument("--seed", type=int)
    ap.add_argument("--out", type=str, default="./out")
    return ap.parse_args()


def main():
    args = parse_args()
    given = {k: v for k, v in (("n_persons", args.persons),
                               ("n_transactions", args.transactions),
                               ("fraud_rate", args.fraud_rate), ("days", args.days),
                               ("seed", args.seed)) if v is not None}
    config = (C.realistic(**given) if args.profile == "realistic"
              else GeneratorConfig(**given))

    df, persons_df = build_dataset(config)

    os.makedirs(args.out, exist_ok=True)
    # LF explicitly, not os.linesep. pandas takes its terminator from the host, so
    # the same seed on Windows and Linux produced files that differed in every
    # line ending and therefore in SHA-256 - generator-spec.md 9 records the
    # dataset of record as "not produced on the Windows host" for exactly this
    # reason. A hash pin that only holds on one operating system is not a pin.
    df.to_csv(os.path.join(args.out, "transactions.csv"), index=False,
              lineterminator="\n")
    persons_df.to_csv(os.path.join(args.out, "persons.csv"), index=False,
                      lineterminator="\n")

    _summary(df)
    print(f"\nwritten to {os.path.abspath(args.out)}/  "
          f"(transactions.csv, persons.csv)")


if __name__ == "__main__":
    main()