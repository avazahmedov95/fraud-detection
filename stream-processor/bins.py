"""
Card issuer resolution from the PAN's BIN.

In production the sending bank does not receive the counterparty's institution
as a field: it receives the destination PAN and resolves the issuer itself from
the 6-digit BIN prefix, against a table it maintains. This module is that table
and that resolution, so the stream processor derives the issuer exactly the way
a deployment would rather than trusting a field the switch never sends.

Why this exists as its own module rather than as two extra fields on the wire:
`sender_bank_name` / `receiver_bank_name` used to travel in the Kafka message.
That made the message carry something UzCard/HUMO does not carry, and made the
on-us test - which gates the whole `receiver_age` capability - depend on a
convenience of the generator rather than on data a bank has. Deriving it here
removes both problems and shortens the wire format.

BANK IDENTITY. The identity compared by `is_on_us` is the bank's CODE, not its
display name: one bank owns several BINs (Ipotekabank has 860033 and 986001),
and names are free text that can differ by spelling between sources. While the
`code` column of banks.csv is unfilled the module falls back to the name and
says so once, out loud - see `_choose_identity`. That fallback is a migration
step, not a design: it is scheduled for removal once the column is filled.
"""

import csv
import os
import sys

import config as C

#: Where the BIN table lives, in resolution order:
#:   1. BANKS_CSV, for a deployment that keeps it elsewhere;
#:   2. beside this module - where `run.ps1 serve-prep` copies it, so the file
#:      ships to the Flink cluster with the job;
#:   3. the generator's copy, for local runs and tests.
def _resolve_path():
    """Where the BIN table is, per config.BANKS_CSV_PATH.

    Deliberately NOT derived from this module's __file__. Flink ships the job's
    modules with `--pyFiles`, which copies them into a Beam temp directory - so
    `os.path.dirname(__file__)` inside the worker is that temp directory, and a
    data file resolved against it is never found. config.py already owns this
    problem for model.onnx; the same resolver answers for banks.csv.
    """
    path = C.BANKS_CSV_PATH
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"banks.csv not found (looked for {path}). The stream processor "
            "resolves the card issuer from the PAN's BIN and cannot do so "
            "without the table. Run `run.ps1 serve-prep`, which copies it into "
            "the mounted job directory, or set BANKS_CSV.")
    return path


def _choose_identity(rows):
    """Pick the column that identifies a bank, and say which was picked.

    The code column is the right answer: it is stable, and it is what a BIN
    table in a bank actually keys on. It is only usable once it is filled -
    a column where every row holds the same placeholder would make `is_on_us`
    compare "00000" with "00000" and return True for EVERY transfer, silently
    turning the `on_us` receiver-age mode into `always` and inflating a measured
    result by a factor of fifteen. That is the exact class of silent, results-
    changing failure this project catalogues, so it is checked rather than
    assumed, and the choice is printed rather than made quietly.
    """
    codes = {r["code"] for r in rows if r.get("code")}
    if len(codes) > 1:
        return "code"
    print("[bins] banks.csv 'code' column is not filled "
          f"({len(codes)} distinct value(s)); falling back to bank NAME as the "
          "issuer identity. Fill the column to switch over.", file=sys.stderr)
    return "name"


def _load():
    path = _resolve_path()
    with open(path, newline="", encoding="utf-8") as f:
        rows = [r for r in csv.DictReader(f) if r.get("bin")]
    if not rows:
        raise ValueError(f"{path} contains no BIN rows")
    field = _choose_identity(rows)
    table = {}
    for r in rows:
        bin_prefix = r["bin"].strip()
        if len(bin_prefix) != 6 or not bin_prefix.isdigit():
            raise ValueError(
                f"{path}: BIN {bin_prefix!r} is not 6 digits. The lookup slices "
                "the PAN at a fixed width, so a short or non-numeric prefix "
                "would never match and the issuer would read as unknown.")
        table[bin_prefix] = (r.get(field) or "").strip()
    return table, field


BIN_TABLE, IDENTITY_FIELD = _load()


#: BINs that were in circulation when the population was generated and are no
#: longer in the table, with the reason. This is NOT consulted by issuer_of():
#: a closed bank is correctly an unknown issuer, and two parties cannot be
#: on-us at an institution that does not exist. It exists so that the
#: unresolvable slice of the traffic is a NAMED, reviewed list rather than
#: anonymous silence - test_bins.py asserts that every unresolved BIN in the
#: generated population appears here, so a row deleted by accident still fails
#: while a bank that genuinely closed does not.
#:
#: This is also the ordinary operating condition, not an artefact of synthetic
#: data. A real BIN table always lags the cards in circulation: institutions
#: close, merge and are licensed between refreshes, so some share of live
#: traffic is always unresolvable. The pipeline must therefore treat an
#: unresolved BIN as "unknown", never as a bank identity - which is what
#: features.is_on_us does by requiring both sides to be non-empty.
RETIRED_BINS = {
    # Licence withdrawn; the bank ceased operations after the transaction set
    # in data-generator/out was generated. 74 of the 5200 generated accounts
    # hold a card on this BIN, ~1.1% of card sides in the stream.
    "986040": "Yangi Bank (closed)",
}


def issuer_of(pan) -> str:
    """Issuer identity for a PAN, or "" when the BIN is not in the table.

    Empty is "unknown issuer", and callers must treat it as such rather than as
    a value: two unknown issuers are not evidence of a shared institution. See
    `features.is_on_us`, which requires both sides to be non-empty.
    """
    return BIN_TABLE.get(str(pan or "")[:6], "")


def describe() -> str:
    n_banks = len(set(BIN_TABLE.values()))
    return (f"{len(BIN_TABLE)} BINs -> {n_banks} banks, "
            f"identified by '{IDENTITY_FIELD}'"
            + (f"; {len(RETIRED_BINS)} retired BIN(s) tracked" if RETIRED_BINS else ""))


if __name__ == "__main__":
    print(describe())
