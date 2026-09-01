"""Resolves the card issuer from the PAN's BIN, the way a bank does.

The switch message carries the card, not the counterparty's institution.
`is_on_us` compares bank CODES: one bank owns several BINs, and names are free
text. Table path comes from config, never from __file__ - see BANKS_CSV_PATH.
"""

import csv
import os

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


def _bank_identity(rows):
    """The column that identifies a bank: the code.

    Checked, not assumed. banks.csv shipped with `code` unfilled - every row
    "00000" - and comparing that column would make is_on_us() true for EVERY
    transfer, turning the on_us receiver-age mode into `always` and inflating
    its measured coverage from 6.85% to 100%.
    """
    codes = {r["code"] for r in rows if r.get("code")}
    if len(codes) <= 1:
        raise ValueError(
            f"banks.csv 'code' column holds {len(codes)} distinct value(s); it "
            "identifies the bank and must be filled. An unfilled column would "
            "make every transfer look on-us.")
    return "code"


def _load():
    path = _resolve_path()
    with open(path, newline="", encoding="utf-8") as f:
        rows = [r for r in csv.DictReader(f) if r.get("bin")]
    if not rows:
        raise ValueError(f"{path} contains no BIN rows")
    field = _bank_identity(rows)
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


#: BINs held by generated accounts but no longer in the table. NOT consulted by
#: issuer_of(): a closed bank is correctly an unknown issuer. The list exists so
#: the unresolvable slice is reviewed rather than silent - test_bins.py requires
#: every unresolved BIN to appear here, so an accidental deletion still fails.
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
