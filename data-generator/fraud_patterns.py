"""
Fraud-pattern injection.

Produces labelled fraudulent events covering four patterns relevant to instant
P2P payments. Each is shaped so that the *enriched* and behavioural signals the
Flink + CEP + ML pipeline is designed to catch are actually present in the data:

  APP          Authorized Push Payment — the genuine account-holder is socially
               engineered into sending a single, unusually large amount to a
               new, unrelated, freshly-created payee. (Key research target.)

  ATO          Account takeover — a session from a NEW device in a DIFFERENT
               region, followed by 2–4 rapid transfers out to fresh accounts.

  STRUCTURING  One actor splits funds into many transfers kept just under the
               control threshold, within a short window.

  MULE         A mule account fans IN from many distinct senders, then quickly
               fans OUT to a few destinations.

These are design fixtures for prototype experimentation, not measured findings.
"""

import numpy as np
from datetime import datetime, timedelta

from config import (AMOUNT_MIN, AMOUNT_MAX, STRUCTURING_THRESHOLD, CHANNELS,
                    REGIONS, FAMILY_FRAUD_SHARE, MULE_RECRUITED_SHARE,
                    SEEDED_PAYEE_SHARE)
from events import make_event
from persons import households, relatives_of
from travel import hijack_origin


# Target share of *fraudulent transactions* per pattern. APP is weighted up
# because it is the central research target, yet each APP episode is a single
# transaction (whereas mule/structuring episodes emit many).
FRAUD_MIX = {"APP": 0.35, "ATO": 0.20, "STRUCTURING": 0.20, "MULE": 0.25}


def inject_fraud(config, persons, by_pinfl, fraud_accounts, n_fraud, rng, start_dt,
                 legit_activity=None):
    """Generate fraud episodes until each pattern hits its transaction budget.

    `legit_activity` maps pinfl -> sorted [(event_time, region)] of the person's
    legitimate transactions. ATO uses it to anchor a hijacked session to a real
    moment in the victim's history, which is what makes the resulting journey
    impossible rather than merely unusual.
    """
    legit_activity = legit_activity or {}
    events = []
    span_seconds = config.days * 24 * 3600
    budget = {k: int(round(v * n_fraud)) for k, v in FRAUD_MIX.items()}
    produced = {k: 0 for k in FRAUD_MIX}

    by_household = households(persons)

    def rand_time():
        return start_dt + timedelta(seconds=float(rng.random() * span_seconds))

    def maybe_relative(person, fallback):
        """Route a minority of fraud legs through a genuine relative.

        Real fraud is not kinship-free: mule networks recruit inside families,
        and relatives' accounts are used as drops. Without this, `is_family`
        would separate fraud perfectly by construction and any importance it
        showed would be an artefact of this generator rather than a finding.
        """
        kin = relatives_of(person, by_household)
        if kin and rng.random() < FAMILY_FRAUD_SHARE:
            return kin[int(rng.integers(len(kin)))]
        return fallback

    def pick(pool):
        return pool[int(rng.integers(len(pool)))]

    # Seed transfers live in their own list: `produced[kind]` counts everything
    # appended to `events` inside a pattern block, so putting a non-fraud event
    # there would silently consume the fraud budget and shrink the dataset's
    # fraud count.
    seeds = []

    def maybe_seed_payee(victim, payee, fraud_ts, balance):
        """Establish the payee before the fraud, the way the threat model says
        an adversary would.

        docs/threat-model.md 4 rates NEW_PAYEE_HIGH_AMOUNT "low cost to evade -
        a prior small transfer establishes the payee". This produces exactly
        that: a small transfer from the victim to the same destination, days
        earlier, which leaves the payee no longer new to the stream by the time
        the real transfer arrives.

        The seed is labelled is_fraud=0 ON PURPOSE. No loss happens on it, and
        under this project's label definition a detector firing on it is a false
        positive. Labelling it fraud would hand the model a second positive per
        episode and quietly inflate recall.

        Known unrealism, recorded rather than modelled: receiver_account_age_days
        is a static property of the Person, so the destination looks the same age
        at seed time as at fraud time. Modelling it would mean ageing accounts
        along the timeline, which affects every event in the dataset for the sake
        of one.
        """
        if SEEDED_PAYEE_SHARE <= 0 or rng.random() >= SEEDED_PAYEE_SHARE:
            return
        seed_ts = fraud_ts - timedelta(days=float(rng.uniform(1.0, 21.0)))
        if seed_ts < start_dt:
            # Outside the observed window: the payee would still be stream-new,
            # so emitting nothing is the honest outcome rather than clamping the
            # timestamp and pretending the seeding happened inside it.
            return
        # Small enough that the seed itself trips neither the absolute floor of
        # NEW_PAYEE_HIGH_AMOUNT nor the personal AMOUNT_DEVIATION baseline: an
        # evasion that raises its own alert is not an evasion.
        amount = float(np.clip(np.exp(rng.normal(10.8, 0.4)), AMOUNT_MIN, AMOUNT_MAX))
        seeds.append(make_event(
            victim, payee, amount, seed_ts, "MOBILE_APP",
            device_id=f"dev-{victim.pinfl[-8:]}",
            is_new_payee=True, balance_before=balance,
            is_fraud=0, fraud_type="NONE", rng=rng))

    # Round-robin over patterns whose budget is not yet met.
    while sum(produced.values()) < n_fraud:
        remaining = [k for k in FRAUD_MIX if produced[k] < budget[k]]
        if not remaining:
            break
        kind = str(rng.choice(remaining))
        before = len(events)

        if kind == "APP":
            victim = pick(persons)
            # Usually a purpose-made account; sometimes a complicit relative's,
            # which is a documented APP variant (the payee is family, the
            # instruction still came from a fraudster).
            fraudster = maybe_relative(victim, pick(fraud_accounts))
            balance = float(np.exp(rng.normal(15.5, 0.6)))          # larger-than-usual balance
            # Most APP fraud drains a large amount, but a portion is moderate so it
            # overlaps ordinary large transfers and isn't trivially separable.
            if rng.random() < 0.40:
                amount = float(np.clip(np.exp(rng.normal(14.0, 0.5)), AMOUNT_MIN, AMOUNT_MAX))
            else:
                amount = float(np.clip(balance * rng.uniform(0.5, 0.95), AMOUNT_MIN, AMOUNT_MAX))
            ts = rand_time()
            maybe_seed_payee(victim, fraudster, ts, balance)
            events.append(make_event(
                victim, fraudster, amount, ts, "MOBILE_APP",
                device_id=f"dev-{victim.pinfl[-8:]}",
                is_new_payee=True, balance_before=balance,
                is_fraud=1, fraud_type="APP", rng=rng))

        elif kind == "ATO":
            victim = pick(persons)
            # 40% "stealth" takeover: the fraudster operates from the victim's own
            # device and region (e.g. on-device malware), leaving only behavioural
            # signals (velocity, amount) — much harder to catch.
            stealth = rng.random() < 0.40
            device = f"dev-{victim.pinfl[-8:]}" if stealth else f"dev-NEW-{int(rng.integers(10**6))}"
            region, base = victim.region, rand_time()

            if not stealth:
                # Anchor the takeover to a real moment in the victim's history:
                # the session continues minutes after they were last seen, from
                # somewhere they could not have travelled to in that time. This
                # is what a hijacked session looks like — not a random region at
                # a random hour, which only produces an impossible journey by
                # coincidence.
                seen = legit_activity.get(victim.pinfl)
                if seen:
                    when, where = seen[int(rng.integers(len(seen)))]
                    gap_min = float(rng.uniform(3, 45))
                    origin = hijack_origin(where, gap_min, rng)
                    if origin is not None:
                        region = origin
                        base = datetime.fromisoformat(when) + timedelta(
                            minutes=gap_min)
                    else:
                        # No region is far enough to be unreachable in the time
                        # available. Leave the session where it is rather than
                        # inventing a journey the geography does not support.
                        region = where
                else:
                    region = str(rng.choice(REGIONS))
            for i in range(int(rng.integers(2, 5))):
                fraudster = pick(fraud_accounts)
                amount = float(np.clip(np.exp(rng.normal(14.5, 0.5)), AMOUNT_MIN, AMOUNT_MAX))
                ts = base + timedelta(minutes=float(i * rng.uniform(1, 4)))
                ev = make_event(
                    victim, fraudster, amount, ts, "MOBILE_APP",
                    device_id=device,
                    is_new_payee=True,
                    balance_before=amount * rng.uniform(1.1, 3.0),
                    is_fraud=1, fraud_type="ATO", rng=rng)
                ev["sender_region"] = region
                events.append(ev)

        elif kind == "STRUCTURING":
            actor = pick(persons)
            base = rand_time()
            for i in range(int(rng.integers(5, 12))):
                fraudster = pick(fraud_accounts)
                amount = float(STRUCTURING_THRESHOLD * rng.uniform(0.85, 0.99))  # just under limit
                ts = base + timedelta(minutes=float(i * rng.uniform(3, 15)))
                events.append(make_event(
                    actor, fraudster, amount, ts, str(rng.choice(CHANNELS)),
                    device_id=f"dev-{actor.pinfl[-8:]}",
                    is_new_payee=True,
                    balance_before=amount * rng.uniform(1.05, 2.0),
                    is_fraud=1, fraud_type="STRUCTURING", rng=rng))

        else:  # MULE — fan-in then fan-out
            # A share of mules are ordinary people recruited into the network
            # rather than purpose-made accounts. Only those have relatives, so
            # only those can show family fan-in.
            recruited = rng.random() < MULE_RECRUITED_SHARE
            mule = pick(persons) if recruited else pick(fraud_accounts)
            base = rand_time()
            n_in = int(rng.integers(4, 9))
            collected = 0.0
            for i in range(n_in):
                # Recruitment runs through personal networks, so some of the
                # senders feeding a recruited mule are their own relatives.
                sender = maybe_relative(mule, pick(persons))
                amount = float(np.clip(np.exp(rng.normal(13.5, 0.6)), AMOUNT_MIN, AMOUNT_MAX))
                collected += amount
                ts = base + timedelta(minutes=float(i * rng.uniform(1, 6)))
                events.append(make_event(
                    sender, mule, amount, ts, str(rng.choice(CHANNELS)),
                    device_id=f"dev-{sender.pinfl[-8:]}",
                    is_new_payee=True,
                    balance_before=amount * rng.uniform(1.1, 3.0),
                    is_fraud=1, fraud_type="MULE", rng=rng))
            n_out = int(rng.integers(1, 3))
            for j in range(n_out):
                dest = pick(fraud_accounts)
                amount = float(collected / n_out * rng.uniform(0.80, 0.98))
                ts = base + timedelta(minutes=float((n_in + j) * rng.uniform(1, 6)))
                events.append(make_event(
                    mule, dest, amount, ts, "MOBILE_APP",
                    device_id=f"dev-{mule.pinfl[-8:]}",
                    is_new_payee=True,
                    balance_before=collected,
                    is_fraud=1, fraud_type="MULE", rng=rng))

        produced[kind] += len(events) - before

    return events + seeds
