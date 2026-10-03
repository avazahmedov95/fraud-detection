"""Configuration for the synthetic P2P generator. Real-world figures here are
chosen placeholders, not sourced values."""

from dataclasses import dataclass
from collections import Counter
import csv
import os
import sys

# --- Behavioural session signals --------------------------------------------
DECISION_TIME_MEDIAN_SEC = 40.0       # population median, login -> confirm
DECISION_TIME_CLIENT_SPREAD = 0.35    # how far client medians sit from each other
ATO_TIME_COMPRESS = (0.4, 0.7)        # attacker in a hurry: faster
SECS_LOGIN_FLOOR = 3.0                # physical minimum

# --- Devices ----------------------------------------------------------------
# Legitimate people own a second device and replace phones, so a new one is not fraud.
SECOND_DEVICE_SHARE = 0.25    # people who use a second device at all
SECOND_DEVICE_USE_RATE = 0.15 # share of THEIR transactions sent from it

# --- Second card ------------------------------------------------------------
# Some people receive on cards at two banks, so PAN and PINFL keys can differ
# (payee_identity). Receiving only: a second sending card splits the history.
SECOND_CARD_SHARE = 0.20      # people who hold a second card
SECOND_CARD_USE_RATE = 0.40   # share of transfers TO them that arrive on it

# --- Kinship (households stand in for MyID-verified relatives) ---------------
# Both shares stay non-zero, or is_family would separate the classes by construction.
FAMILY_PAYEE_SHARE = 0.35     # frequent payees who are relatives
FAMILY_FRAUD_SHARE = 0.10     # eligible fraud legs routed to a relative

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
# A chosen round figure, not from Regulation 3759 (which sets no sums); it should
# become a dated BRV multiple. STRUCTURING is placed at 0.85-0.99 of it and the
# rule watches the same constant, so that recall is partly by construction.
STRUCTURING_THRESHOLD = 10_000_000

# A bank limit, not a regulatory one (DAILY_LIMIT_BREACH).
LIMIT_DAILY = 100_000_000

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
    """The dataset of record: fraud at the rate real card traffic runs at,
    legitimate transfers that share fraud's shapes, fraud that shares legitimate
    ones, and labels as incomplete as real ones."""
    n_persons: int = 50_000
    n_transactions: int = 500_000
    fraud_rate: float = 0.002
    days: int = 30
    seed: int = 42
    # Class overlap, so the benchmark is not trivially separable.
    new_account_share: float = 0.12    # legit accounts opened recently
    hard_negative_share: float = 0.08  # legit transfers that look suspicious
    start_date: str = "2025-01-01"

    active_call_base_rate: float = 0.10      # ordinary transactions
    active_call_app_rate: float = 0.45       # an APP victim is on the phone
    app_time_stretch: tuple = (1.0, 3.5)     # victim listening to instructions: slower
    decision_time_sigma: float = 0.75        # lognormal spread within one client
    app_moderate_share: float = 0.60
    aged_fraud_share: float = 0.50
    ato_stealth_share: float = 0.60
    mule_recruited_share: float = 0.50       # recruited people, not made accounts
    mule_senders: tuple = (3, 11)            # integers(lo, hi)
    mule_gap_minutes: tuple = (5, 60)
    structuring_events: tuple = (3, 9)
    structuring_gap_minutes: tuple = (10, 90)
    structuring_fraction: tuple = (0.70, 0.99)
    phone_change_share: float = 0.04
    collector_share: float = 0.01
    split_payment_share: float = 0.003
    round_amount_share: float = 0.90
    unreported_fraud_share: float = 0.10


#: The profile being generated, set by generator.build_dataset; make_event reads it.
PROFILE = GeneratorConfig()


# --- Issuing banks ------------------------------------------------------------
# Header `bin,code,name,cards_mln`, one row per BIN. No fallback table: a synthetic
# one would silently change the bank mix of every generated card.
BANKS_SOURCE = "banks.csv"


def _load_banks():
    """Bank BIN/MFO table from BANKS_SOURCE. Raises if it is missing or empty."""
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), BANKS_SOURCE)
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"bank registry '{BANKS_SOURCE}' not found at {path}. Nothing is "
            f"substituted for it: market structure decides which bank issues "
            f"each generated card. "
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

# Card-share weighting reproduces the real bank concentration; False = uniform,
# a control.
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
              f"market.", file=sys.stderr)
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
