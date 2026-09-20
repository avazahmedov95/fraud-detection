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

# --- Devices ----------------------------------------------------------------
# Legitimate people own a second device and replace phones; with one device each,
# a first-seen device marked only fraud - the label wearing a feature's name.
SECOND_DEVICE_SHARE = 0.25    # people who use a second device at all
SECOND_DEVICE_USE_RATE = 0.15 # share of THEIR transactions sent from it

# --- Second card ------------------------------------------------------------
# Some people receive on cards at two banks, so keying the payee by PAN and by
# PINFL can differ (the payee_identity capability). Receiving only: a sender with
# two cards would fragment the per-sender history.
SECOND_CARD_SHARE = 0.20      # people who hold a second card
SECOND_CARD_USE_RATE = 0.40   # share of transfers TO them that arrive on it

# --- Kinship (households stand in for MyID-verified relatives) ---------------
# Both shares stay non-zero, or is_family would separate the classes by construction.
FAMILY_PAYEE_SHARE = 0.35     # frequent payees who are relatives
FAMILY_FRAUD_SHARE = 0.10     # eligible fraud legs routed to a relative
MULE_RECRUITED_SHARE = 0.30   # mules who are recruited people, not made accounts

# Seeds the payee before an APP transfer - the cheap evasion of NEW_PAYEE_HIGH_AMOUNT
# (threat-model.md 4). Default 0: the dataset of record is hash-pinned.
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
# A chosen round figure, not from Regulation 3759 (which sets no sums); it should
# become a dated BRV multiple. STRUCTURING is placed at 0.85-0.99 of it and the
# rule watches the same constant, so that recall is partly by construction.
STRUCTURING_THRESHOLD = 10_000_000

# Bank limits, not regulatory. Only LIMIT_DAILY is read (DAILY_LIMIT_BREACH).
LIMIT_DAILY = 100_000_000
LIMIT_PER_TRANSACTION = 30_000_000   # UNUSED
LIMIT_MONTHLY = 500_000_000          # UNUSED

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

    # Defaults reproduce the dataset of record draw for draw: every knob that adds
    # behaviour is off at 0. The realistic values: docs/generator-spec.md 10.
    profile: str = "baseline"
    active_call_base_rate: float = ACTIVE_CALL_BASE_RATE
    active_call_app_rate: float = ACTIVE_CALL_APP_RATE
    app_time_stretch: tuple = APP_TIME_STRETCH
    decision_time_sigma: float = DECISION_TIME_SIGMA
    app_moderate_share: float = 0.40
    aged_fraud_share: float = 0.30
    ato_stealth_share: float = 0.40
    mule_recruited_share: float = MULE_RECRUITED_SHARE
    mule_senders: tuple = (4, 9)             # integers(lo, hi): 4..8
    mule_gap_minutes: tuple = (1, 6)
    structuring_events: tuple = (5, 12)      # 5..11
    structuring_gap_minutes: tuple = (3, 15)
    structuring_fraction: tuple = (0.85, 0.99)
    phone_change_share: float = 0.0
    collector_share: float = 0.0
    split_payment_share: float = 0.0
    round_amount_share: float = 0.0
    unreported_fraud_share: float = 0.0


def realistic(**overrides):
    """The profile generator-spec.md 10 argues for: fraud at the rate real card
    traffic runs at, legitimate transfers that share fraud's shapes, fraud that
    shares legitimate ones, and labels as incomplete as real ones."""
    values = dict(
        profile="realistic",
        n_persons=50_000, n_transactions=500_000, fraud_rate=0.002,
        hard_negative_share=0.08,
        active_call_base_rate=0.10, active_call_app_rate=0.45,
        app_time_stretch=(1.0, 3.5), decision_time_sigma=0.75,
        app_moderate_share=0.60, aged_fraud_share=0.50, ato_stealth_share=0.60,
        mule_recruited_share=0.50, mule_senders=(3, 11), mule_gap_minutes=(5, 60),
        structuring_events=(3, 9), structuring_gap_minutes=(10, 90),
        structuring_fraction=(0.70, 0.99),
        phone_change_share=0.04, collector_share=0.01, split_payment_share=0.003,
        round_amount_share=0.90, unreported_fraud_share=0.10)
    values.update(overrides)
    return GeneratorConfig(**values)


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
