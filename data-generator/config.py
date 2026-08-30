"""
Uzbekistan-calibrated configuration for the synthetic P2P transaction generator.

Every value here is configurable. Where a parameter reflects a real-world figure
(card BIN ranges, Central Bank transfer limits), the default is a *reasonable
placeholder*. These should be confirmed against the authoritative source — the
UzCard / HUMO network specifications and Central Bank Regulation No. 3759.
"""

from dataclasses import dataclass
from collections import Counter
import csv
import os

# --- Card networks (BIN prefixes) -------------------------------------------
# UzCard cards historically begin with 8600, HUMO with 9860.
# CONFIRM against current network specifications.

# --- Behavioural session signals (#7) ---
DECISION_TIME_MEDIAN_SEC = 40.0       # популяционная медиана: логин → подтверждение
DECISION_TIME_SIGMA = 0.55            # lognormal sigma — разброс внутри клиента
DECISION_TIME_CLIENT_SPREAD = 0.35    # насколько медианы клиентов расходятся между собой

ACTIVE_CALL_BASE_RATE = 0.03          # обычные транзакции
ACTIVE_CALL_APP_RATE = 0.70           # жертва APP под звонком

APP_TIME_STRETCH = (2.5, 5.0)         # жертва слушает инструкции → медленнее
ATO_TIME_COMPRESS = (0.4, 0.7)        # злоумышленник спешит → быстрее
SECS_LOGIN_FLOOR  = 3.0                  # физический минимум

# --- Kinship (MyID-style verified family relationships) ----------------------
# Persons are generated in households; members of one household are treated as
# verified relatives, which is what a MyID lookup would return.
#
# These two constants exist because of a methodological failure worth recording.
# An earlier revision had *no* fraud going to relatives, so `is_family` separated
# fraud perfectly and ranked #1 in SHAP — an artefact of the generator, not a
# property of fraud. The feature was removed as unsound. It is back only because
# the data now models both sides of the relationship:
#
#   - a substantial share of LEGITIMATE transfers goes to relatives (people
#     genuinely send money to family more than to anyone else), and
#   - a realistic minority of FRAUD does too: mule networks recruit through
#     families, and relatives' accounts get used as drops.
#
# Kinship is therefore informative but not decisive — which is the honest shape
# of the signal. If either constant is set to 0 the feature becomes separating
# again by construction, and any SHAP importance it shows is meaningless.
FAMILY_PAYEE_SHARE = 0.35     # share of a person's frequent payees who are relatives
FAMILY_FRAUD_SHARE = 0.10     # share of eligible fraud legs routed to a relative
MULE_RECRUITED_SHARE = 0.30   # share of mules who are ordinary recruited people
                              # (the rest are purpose-made fraud accounts)

CARD_NETWORKS = {
    "UZCARD": "8600",
    "HUMO": "9860",
}
CARD_LENGTH = 16  # 16-digit PAN with a valid Luhn check digit



# --- Currency & amounts (UZS) -----------------------------------------------
# Each person also gets a personal "typical_amount" spend baseline.
# These global bounds clip extreme values.
AMOUNT_MIN = 1_000
AMOUNT_MAX = 50_000_000

# --- Central Bank transfer limits (UZS) -------------------------------------
# PLACEHOLDERS — set per Regulation No. 3759 / current CBU directives.
LIMIT_PER_TRANSACTION = 30_000_000
LIMIT_DAILY = 100_000_000
LIMIT_MONTHLY = 500_000_000
# Reporting / control threshold that "structuring" fraud tries to stay under.
STRUCTURING_THRESHOLD = 10_000_000

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
    days: int = 30                     # time span covered by the dataset
    seed: int = 42
    # --- realism / class-overlap knobs (so the benchmark isn't trivially separable) ---
    new_account_share: float = 0.12    # share of legit accounts opened recently (fresh)
    hard_negative_share: float = 0.03  # share of legit transfers that look suspicious
    start_date: str = "2025-01-01"


# --- Issuing banks (BIN -> bank -> MFO reference table) ----------------------
# REAL DATA GOES IN data-generator/banks.csv (columns: bin,code,name). Drop the
# authoritative UzCard/HUMO BIN registry there (you can pull it at Kapitalbank)
# and it is used automatically. UzCard PANs begin 8600, HUMO 9860; code = 5-digit
# MFO. The list below is a SYNTHETIC fallback used only when banks.csv is absent.
_SYNTHETIC_BANKS = [
    {"bin": "860002", "code": "00014", "name": "National Bank of Uzbekistan"},
    {"bin": "860006", "code": "00450", "name": "Uzpromstroybank"},
    {"bin": "860049", "code": "01041", "name": "Kapitalbank"},
    {"bin": "860014", "code": "00873", "name": "Ipoteka Bank"},
    {"bin": "860033", "code": "00982", "name": "Agrobank"},
    {"bin": "860055", "code": "01183", "name": "Hamkorbank"},
    {"bin": "986003", "code": "00491", "name": "Xalq Banki"},
    {"bin": "986012", "code": "00994", "name": "Aloqabank"},
    {"bin": "986027", "code": "01067", "name": "Ipak Yuli Bank"},
    {"bin": "986041", "code": "00206", "name": "Trustbank"},
    {"bin": "986008", "code": "01158", "name": "Davr Bank"},
    {"bin": "986055", "code": "01234", "name": "Mikrokreditbank"},
]


def _load_banks():
    """Bank BIN/MFO table: real values from banks.csv, else the synthetic fallback.

    banks.csv (next to this file) must have a header `bin,code,name,cards_mln`.
    Each `bin` is a 6-digit issuing BIN (8600.. UzCard / 9860.. HUMO); `code` is
    the 5-digit MFO; `name` is the bank; `cards_mln` is that bank's cards in
    circulation, in millions. A bank may have several BINs -> several rows, and
    `cards_mln` is repeated on each of them (it is a per-bank figure).
    """
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "banks.csv")
    if os.path.exists(path):
        with open(path, newline="", encoding="utf-8") as fh:
            rows = [{"bin": r["bin"].strip(), "code": r["code"].strip(),
                     "name": r["name"].strip(),
                     "cards_mln": float(r.get("cards_mln") or 0.0)}
                    for r in csv.DictReader(fh) if r.get("bin", "").strip()]
        if rows:
            return rows
    return _SYNTHETIC_BANKS


BANKS = _load_banks()

# --- Bank market share ------------------------------------------------------
# Which bank issued a person's card decides whether a transfer is on-us (both
# parties at the same bank) or inter-bank. That distinction matters because the
# receiver's account age is only knowable to the sending bank when the receiver
# is its own client — so the on-us rate bounds how much of the traffic the
# `receiver_age` feature can cover at all.
#
# Assigning banks uniformly makes on-us ~1/n_banks (~3%), which is an artefact of
# the generator, not of the market. Weighting by cards in circulation reproduces
# the real concentration instead: Xalq banki alone holds ~16% of the country's
# cards.
#
# Set to False to fall back to uniform assignment (useful as a control, or to
# sweep the on-us rate independently of real market structure).
WEIGHT_BANKS_BY_CARD_SHARE = True

# CAVEAT for interpretation: cards in circulation is a proxy for transfer
# volume, not a measurement of it. Xalq banki leads largely on state social
# payments, whose cards are low-activity, while digital banks such as Uzum see
# far more transactions per card. Card share therefore overstates the former and
# understates the latter for P2P specifically. No per-bank P2P volume statistics
# are published, so this is the closest available proxy — hence the toggle.


def _bank_weights():
    """Per-BIN sampling weights, proportional to each bank's cards in circulation.

    A bank's card base is split evenly across its BINs (most banks issue on both
    UzCard and HUMO, and the national split is close to even). Falls back to
    uniform weights when the table carries no card figures.
    """
    n = len(BANKS)
    if not WEIGHT_BANKS_BY_CARD_SHARE:
        return [1.0 / n] * n

    bins_per_bank = Counter(b["name"] for b in BANKS)
    raw = [float(b.get("cards_mln") or 0.0) / bins_per_bank[b["name"]] for b in BANKS]
    total = sum(raw)
    if total <= 0:                      # no card data (e.g. synthetic fallback)
        return [1.0 / n] * n
    return [w / total for w in raw]


BANK_WEIGHTS = _bank_weights()

# --- Name components (SYNTHETIC Uzbek full names: "Surname First Patronymic") --
# The male/female split exists only to build grammatically correct names
# (surname -ov/-ova, patronymic o'g'li/qizi); no gender is stored.
MALE_FIRST = ["Sardor", "Jasur", "Bekzod", "Aziz", "Otabek", "Sherzod", "Akmal",
              "Bobur", "Doniyor", "Ulugbek", "Farrux", "Javohir", "Sanjar",
              "Rustam", "Timur"]
FEMALE_FIRST = ["Nilufar", "Zilola", "Malika", "Dilnoza", "Sevara", "Gulnora",
                "Shahnoza", "Kamola", "Feruza", "Madina", "Charos", "Nigora",
                "Zarina", "Laylo", "Oysha"]
SURNAME_STEMS = ["Karim", "Rahim", "Yusup", "Tursun", "Umar", "Nazar", "Sulton",
                 "Xolmat", "Qodir", "Mirza", "Yormat", "Saidjon", "Ibragim",
                 "Bekmurod", "Toshpulat"]