"""Configuration for the synthetic P2P generator. Real-world figures here are
chosen placeholders, not sourced values."""

from dataclasses import dataclass
from collections import Counter
import csv
import os
import sys

# --- Behavioural session signals --------------------------------------------
DECISION_TIME_MEDIAN_SEC = 40.0       # population median, login -> confirm
DECISION_TIME_SIGMA = 0.55            # lognormal sigma: spread within one client
DECISION_TIME_CLIENT_SPREAD = 0.35    # how far client medians sit from each other
ACTIVE_CALL_BASE_RATE = 0.03          # ordinary transactions
ACTIVE_CALL_APP_RATE = 0.70           # an APP victim is on the phone
APP_TIME_STRETCH = (2.5, 5.0)         # victim listening to instructions: slower
ATO_TIME_COMPRESS = (0.4, 0.7)        # attacker in a hurry: faster
SECS_LOGIN_FLOOR = 3.0                # physical minimum

# --- Kinship (households stand in for MyID-verified relatives) ---------------
# Both shares must stay non-zero: with no fraud going to relatives, `is_family`
# separated the classes perfectly and topped SHAP - an artefact, not a finding.
FAMILY_PAYEE_SHARE = 0.35     # frequent payees who are relatives
FAMILY_FRAUD_SHARE = 0.10     # eligible fraud legs routed to a relative
MULE_RECRUITED_SHARE = 0.30   # mules who are recruited people, not made accounts

# Seeds the payee before an APP transfer - the documented cheap evasion of
# NEW_PAYEE_HIGH_AMOUNT (threat-model.md 4), which the generator does not
# otherwise produce: 99.20% of fraud goes to a stream-new payee against 36.93%
# of legitimate traffic, making the feature's measured value a ceiling.
# Default 0.0 - it moves every figure in irp-framing and the ablation tables,
# and the dataset of record is hash-pinned. APP only, so at share S it seeds
# 0.35*S of all fraud: a lower bound on what full evasion costs.
SEEDED_PAYEE_SHARE = float(os.getenv("SEEDED_PAYEE_SHARE", "0.0"))

# --- Card networks (BIN prefixes) -------------------------------------------
# CONFIRM against current UzCard / HUMO network specifications.
CARD_NETWORKS = {
    "UZCARD": "8600",
    "HUMO": "9860",
}
CARD_LENGTH = 16  # 16-digit PAN with a valid Luhn check digit

# --- Currency & amounts (UZS) -----------------------------------------------
# Global clips; each person also gets a personal typical_amount baseline.
AMOUNT_MIN = 1_000
AMOUNT_MAX = 50_000_000

# --- Transfer thresholds (UZS) ----------------------------------------------
# NOT from Regulation 3759 - an earlier revision cited it and was wrong; 3759
# sets no sum thresholds. The real traceability threshold is in BRV, so it moves
# (25 BRV was ~10.3M UZS in March 2026); this is a chosen round figure and
# should become a BRV multiple with a dated BRV beside it.
# Load-bearing: fraud_patterns.py places STRUCTURING at 0.85-0.99 of it and
# rules.py watches the same constant, so that recall is partly by construction.
STRUCTURING_THRESHOLD = 10_000_000

# Bank limits, not regulatory. Only LIMIT_DAILY is read (DAILY_LIMIT_BREACH).
LIMIT_DAILY = 100_000_000
LIMIT_PER_TRANSACTION = 30_000_000   # UNUSED
LIMIT_MONTHLY = 500_000_000          # UNUSED

# --- Channels ----------------------------------------------------------------
CHANNELS = ["MOBILE_APP", "USSD", "WEB", "ATM"]
CHANNEL_WEIGHTS = [0.70, 0.12, 0.13, 0.05]

# --- Geography (Uzbekistan regions) -----------------------------------------
REGIONS = [
    "Tashkent City", "Tashkent Region", "Samarkand", "Bukhara", "Andijan",
    "Fergana", "Namangan", "Kashkadarya", "Surkhandarya", "Navoi",
    "Jizzakh", "Syrdarya", "Khorezm", "Karakalpakstan",
]
# Rough population weighting (Tashkent heaviest).
REGION_WEIGHTS = [0.18, 0.09, 0.11, 0.05, 0.10, 0.11, 0.09, 0.10,
                  0.04, 0.03, 0.04, 0.02, 0.03, 0.01]


@dataclass
class GeneratorConfig:
    n_persons: int = 5_000
    n_transactions: int = 50_000
    fraud_rate: float = 0.015          # ~1.5% positive class (realistic imbalance)
    days: int = 30
    seed: int = 42
    # Class overlap, so the benchmark is not trivially separable.
    new_account_share: float = 0.12    # legit accounts opened recently
    hard_negative_share: float = 0.03  # legit transfers that look suspicious
    start_date: str = "2025-01-01"


# --- Issuing banks ------------------------------------------------------------
# Header `bin,code,name,cards_mln`; one row per BIN, cards_mln repeated. No
# fallback table on purpose - a synthetic one put on-us at 8.3% against the
# registry's 6.9%, moving receiver_age coverage by a fifth, silently.
BANKS_SOURCE = "banks.csv"


def _load_banks():
    """Bank BIN/MFO table from BANKS_SOURCE. Raises if it is missing or empty."""
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), BANKS_SOURCE)
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"bank registry '{BANKS_SOURCE}' not found at {path}. Nothing is "
            f"substituted for it: market structure decides the on-us rate, and "
            f"the on-us rate decides how much traffic receiver_age can cover. "
            f"Supply the UzCard/HUMO BIN registry as CSV with the header "
            f"bin,code,name,cards_mln.")
    with open(path, newline="", encoding="utf-8") as fh:
        rows = [{"bin": r["bin"].strip(), "code": r["code"].strip(),
                 "name": r["name"].strip(),
                 "cards_mln": float(r.get("cards_mln") or 0.0)}
                for r in csv.DictReader(fh) if r.get("bin", "").strip()]
    if not rows:
        raise ValueError(
            f"bank registry '{BANKS_SOURCE}' at {path} carries no BIN row. An "
            f"empty table is the silent substitution this file exists to prevent.")
    return rows


BANKS = _load_banks()

# The on-us rate bounds receiver_age coverage, so assignment is not cosmetic:
# uniform gives ~1/n_banks (~3%), a property of the list, while card-share
# weighting reproduces the real concentration (Xalq ~16%). False = uniform, a
# legitimate control. Cards in circulation proxies volume - hence the toggle.
WEIGHT_BANKS_BY_CARD_SHARE = True


def _bank_weights():
    """Per-BIN weights from each bank's cards in circulation, split evenly across
    its BINs."""
    n = len(BANKS)
    if not WEIGHT_BANKS_BY_CARD_SHARE:
        return [1.0 / n] * n

    bins_per_bank = Counter(b["name"] for b in BANKS)
    raw = [float(b.get("cards_mln") or 0.0) / bins_per_bank[b["name"]] for b in BANKS]
    total = sum(raw)
    if total <= 0:
        # Uniform is a legitimate control; it is only dangerous unchosen.
        print(f"WARNING: {BANKS_SOURCE} carries no cards_mln figures; bank "
              f"assignment falls back to UNIFORM weights. The on-us rate drops "
              f"to about 1/n_banks, a property of the list rather than of the "
              f"market, and receiver_age coverage moves with it.", file=sys.stderr)
        return [1.0 / n] * n
    return [w / total for w in raw]


BANK_WEIGHTS = _bank_weights()

# --- Name components (synthetic) - split is for grammar only, no gender stored
MALE_FIRST = ["Sardor", "Jasur", "Bekzod", "Aziz", "Otabek", "Sherzod", "Akmal",
              "Bobur", "Doniyor", "Ulugbek", "Farrux", "Javohir", "Sanjar",
              "Rustam", "Timur"]
FEMALE_FIRST = ["Nilufar", "Zilola", "Malika", "Dilnoza", "Sevara", "Gulnora",
                "Shahnoza", "Kamola", "Feruza", "Madina", "Charos", "Nigora",
                "Zarina", "Laylo", "Oysha"]
SURNAME_STEMS = ["Karim", "Rahim", "Yusup", "Tursun", "Umar", "Nazar", "Sulton",
                 "Xolmat", "Qodir", "Mirza", "Yormat", "Saidjon", "Ibragim",
                 "Bekmurod", "Toshpulat"]
