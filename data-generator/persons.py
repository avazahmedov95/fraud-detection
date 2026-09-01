"""The synthetic population: PINFL, a valid-Luhn card whose BIN encodes the
issuer, region, device, household, and a per-person decision-time baseline.
"""

from dataclasses import dataclass
import numpy as np

from config import (CARD_LENGTH, CARD_NETWORKS, BANKS_SOURCE, REGIONS,
                    REGION_WEIGHTS, BANKS, BANK_WEIGHTS,
                    MALE_FIRST, FEMALE_FIRST, SURNAME_STEMS,
                    DECISION_TIME_MEDIAN_SEC, DECISION_TIME_CLIENT_SPREAD)

# Fast BIN -> bank lookup (the bank is derivable from the first 6 PAN digits).
BANK_BY_BIN = {b["bin"]: b for b in BANKS}


@dataclass
class Person:
    pinfl: str
    card: str
    network: str
    region: str
    account_age_days: int
    typical_amount: float          # personal spend baseline (UZS)
    active_start_hour: int         # behavioural active window
    active_end_hour: int
    household_id: int
    decision_time_median: float = 40.0   # personal login->confirm baseline (sec)
    full_name: str = ""
    bank_code: str = ""
    bank_name: str = ""
    is_fraud_account: bool = False


def _normalise(weights):
    a = np.asarray(weights, dtype=float)
    return a / a.sum()


def gen_pinfl(rng):
    """14-digit synthetic personal identifier (PINFL-like). Not a real PINFL."""
    return "".join(str(int(d)) for d in rng.integers(0, 10, size=14))


def gen_full_name(rng):
    """Synthetic Uzbek full name 'Surname First Father{o'g'li|qizi}'.

    The male/female split is only to keep the name grammatically correct
    (surname -ov/-ova, patronymic o'g'li/qizi); no gender is stored.
    """
    is_male = rng.random() < 0.5
    first = str(rng.choice(MALE_FIRST if is_male else FEMALE_FIRST))
    surname = str(rng.choice(SURNAME_STEMS)) + ("ov" if is_male else "ova")
    father = str(rng.choice(MALE_FIRST))
    patronymic = father + (" o'g'li" if is_male else " qizi")
    return f"{surname} {first} {patronymic}"


def network_from_bin(bin6):
    """Card network from the BIN's leading digits, per config.CARD_NETWORKS.

    Refuses an unrecognised prefix instead of guessing. This used to read
    `"UZCARD" if bin6.startswith("8600") else "HUMO"`, which had two faults at
    once: it labelled ANY unknown BIN as HUMO without a word, and it left
    CARD_NETWORKS defined and read by nothing, so a reader who corrected that
    table changed nothing at all.

    A BIN outside the registry's networks is worth stopping for rather than
    absorbing: the network feeds `cross_network`, which the model uses as a
    feature, so a mislabelled card does not fail - it quietly shifts a feature.
    """
    for name, prefix in CARD_NETWORKS.items():
        if bin6.startswith(prefix):
            return name
    known = ", ".join(f"{n}={p}" for n, p in CARD_NETWORKS.items())
    raise ValueError(
        f"BIN {bin6!r} matches no network in CARD_NETWORKS ({known}). "
        f"Check {BANKS_SOURCE}: a card whose network cannot be resolved would "
        f"silently distort the cross_network feature.")


def gen_card(rng):
    """Return (network, 16-digit PAN). The PAN starts with a real bank BIN, so the
    issuing bank can be recovered from the number via `bank_from_card`."""
    # Sampled by market share, not uniformly: see WEIGHT_BANKS_BY_CARD_SHARE in
    # config.py for why the concentration of the card market matters here.
    bank = BANKS[int(rng.choice(len(BANKS), p=BANK_WEIGHTS))]
    bin6 = bank["bin"]
    network = network_from_bin(bin6)
    n_random = CARD_LENGTH - len(bin6) - 1          # 16 - 6 - 1 = 9
    base = bin6 + "".join(str(int(d)) for d in rng.integers(0, 10, size=n_random))
    # Luhn: rightmost base digit sits in an even position w.r.t. the check digit.
    total = 0
    for idx, ch in enumerate(reversed(base)):
        d = int(ch)
        if idx % 2 == 0:
            d *= 2
            if d > 9:
                d -= 9
        total += d
    check = (10 - total % 10) % 10
    return network, base + str(check)


def bank_from_card(pan):
    """Recover (bank_code, bank_name) from the PAN's 6-digit BIN prefix."""
    bank = BANK_BY_BIN.get(str(pan)[:6])
    return (bank["code"], bank["name"]) if bank else ("00000", "UNKNOWN")


def _make_person(rng, region, age, household_id, is_fraud=False,
                 typical_amount=0.0, active_start=0, active_end=23):
    network, card = gen_card(rng)
    bank_code, bank_name = bank_from_card(card)
    return Person(
        pinfl=gen_pinfl(rng), card=card, network=network, region=region,
        account_age_days=age, typical_amount=typical_amount,
        active_start_hour=active_start, active_end_hour=active_end,
        household_id=household_id,
        # Each person has their own habitual pace: a fast 20-year-old and a slow
        # pensioner are both "normal". secs_login_z is measured against THIS
        # baseline, not the population's, so the signal isn't trivially separable.
        decision_time_median=float(
            DECISION_TIME_MEDIAN_SEC * np.exp(rng.normal(0, DECISION_TIME_CLIENT_SPREAD))
        ),
        full_name=gen_full_name(rng),
        bank_code=bank_code, bank_name=bank_name, is_fraud_account=is_fraud,
    )


def households(persons):
    """Map household_id -> members. Household membership stands in for the
    MyID-verified kinship a bank with that integration could look up."""
    out = {}
    for p in persons:
        out.setdefault(p.household_id, []).append(p)
    return out


def relatives_of(person, by_household):
    """Household members other than the person; empty for a single-person one."""
    return [q for q in by_household.get(person.household_id, ())
            if q.pinfl != person.pinfl]


def build_population(config, rng):
    """Return (persons, by_pinfl). Persons are drawn in household-sized clusters:
    members share a region, and are treated as relatives of one another."""
    persons, by_pinfl = [], {}
    region_p = _normalise(REGION_WEIGHTS)
    hh_id = 0
    while len(persons) < config.n_persons:
        size = int(rng.integers(1, 7))          # clusters of 1..6 share a region
        region = str(rng.choice(REGIONS, p=region_p))
        for _ in range(size):
            if len(persons) >= config.n_persons:
                break
            # A share of legitimate accounts are freshly opened -> "fresh receiver"
            # is no longer a fraud-exclusive signal.
            if rng.random() < config.new_account_share:
                age = int(rng.integers(1, 30))
            else:
                age = int(rng.integers(30, 3650))
            person = _make_person(
                rng, region=region, age=age, household_id=hh_id,
                typical_amount=float(np.exp(rng.normal(11.8, 0.6))),  # ~130k UZS median
                active_start=int(rng.integers(6, 11)),
                active_end=int(rng.integers(18, 24)))
            persons.append(person)
            by_pinfl[person.pinfl] = person
        hh_id += 1
    return persons, by_pinfl


def build_fraud_accounts(n, rng, aged_share=0.30):
    """Pool of accounts used as fraud receivers / mules.

    Most are freshly created, but a fraction are AGED (established mules) so a
    low account age is not a perfect fraud indicator.
    """
    accounts = []
    for i in range(n):
        if rng.random() < aged_share:
            age = int(rng.integers(100, 1500))     # established mule
        else:
            age = int(rng.integers(1, 45))          # freshly created
        # Each fraud account is its own household. A shared id would make every
        # fraud account a "relative" of every other one, so mule fan-out would
        # register as a family transfer.
        accounts.append(_make_person(
            rng, region=str(rng.choice(REGIONS)), age=age, household_id=-(i + 1),
            is_fraud=True))
    return accounts