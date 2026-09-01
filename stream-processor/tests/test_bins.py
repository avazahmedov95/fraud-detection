"""Tests for issuer resolution from the PAN's BIN.

The behaviour worth pinning is not the lookup - it is the two ways this can go
wrong quietly: a bank whose several BINs stop being recognised as one bank, and
a placeholder `code` column that would make every transfer look on-us.
"""

import pytest

import bins as B
import features as F
from conftest import bank_card


def test_known_bin_resolves():
    assert B.issuer_of(bank_card("BankA")) != ""


def test_unknown_bin_is_empty_not_a_value():
    """An unrecognised issuer must read as unknown, never as a bank identity."""
    assert B.issuer_of("9999990000000001") == ""
    assert B.issuer_of("") == ""
    assert B.issuer_of(None) == ""


def test_short_pan_does_not_partially_match():
    """A truncated PAN must not resolve: slicing 6 characters off a 4-digit
    string yields the whole string, which would match nothing - but only by
    luck. Pinned so a future change to the slice width fails here."""
    assert B.issuer_of("8600") == ""


def test_one_bank_owning_several_bins_is_one_issuer():
    """The reason the identity is the bank, not the BIN.

    Ipotekabank issues under both 860033 (UzCard) and 986001 (HUMO). A customer
    holding one of each banks at ONE institution, so a transfer between them is
    on-us and the receiver's account age is visible. Comparing BINs directly
    would call it inter-bank and silently discard the receiver_age capability
    for every dual-network customer of every multi-BIN bank.
    """
    uzcard, humo = "8600330000000001", "9860010000000002"
    assert B.issuer_of(uzcard) == B.issuer_of(humo) != ""
    assert F.is_on_us({"sender_card": uzcard, "receiver_card": humo}) is True


def test_is_on_us_across_different_banks():
    assert F.is_on_us({"sender_card": bank_card("BankA"),
                       "receiver_card": bank_card("BankB")}) is False


def test_unknown_issuer_is_never_on_us():
    """Two unresolvable BINs are not evidence of a shared institution."""
    unknown = bank_card("")
    assert F.is_on_us({"sender_card": unknown, "receiver_card": unknown}) is False


# --- the guard that stops a placeholder column inflating a measured result ---

def _unresolved_bins_in_population():
    """BINs held by generated accounts that the current table does not resolve.

    None when the population has not been generated.
    """
    import csv
    import os
    here = os.path.dirname(os.path.abspath(__file__))
    persons = os.path.join(here, "..", "..", "data-generator", "out", "persons.csv")
    if not os.path.exists(persons):
        return None
    with open(persons, newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    unresolved = sorted({r["card"][:6] for r in rows if not B.issuer_of(r["card"])})
    return unresolved, len(rows), sum(
        1 for r in rows if not B.issuer_of(r["card"]))


def test_unresolved_bins_are_all_accounted_for():
    """banks.csv tracks the market as it stands; persons.csv was generated
    against the market as it stood then. A bank that has since closed shows up
    as an unresolvable BIN, and that is correct. A row deleted by accident must
    not read as the same thing, so every unresolved BIN has to be named in
    bins.RETIRED_BINS with a reason."""
    found = _unresolved_bins_in_population()
    if found is None:
        pytest.skip("population not generated")
    unresolved, _, _ = found
    unaccounted = [b for b in unresolved if b not in B.RETIRED_BINS]
    assert not unaccounted, (
        f"BIN(s) {unaccounted} are held by generated accounts, are absent from "
        f"banks.csv, and are not in bins.RETIRED_BINS. Restore the row, or add "
        f"it there with the reason.")


def test_the_unresolvable_share_stays_small():
    """A bound, not an exactness. Some unresolvable traffic is normal; a large
    share means the table has come apart from the data and every is_on_us would
    answer False, withdrawing receiver_age across the board."""
    found = _unresolved_bins_in_population()
    if found is None:
        pytest.skip("population not generated")
    _, total, n_unresolved = found
    assert n_unresolved / total < 0.05, f"{n_unresolved}/{total}"


def test_a_retired_issuer_is_never_on_us():
    """Two parties at the same CLOSED bank are still not on-us: the institution
    that would perform the account lookup no longer exists. Works through the
    unknown-issuer rule, not a special case - pinned so a later fallback to
    RETIRED_BINS inside issuer_of does not break it."""
    retired = next(iter(B.RETIRED_BINS))
    a, b = retired + "0000000001", retired + "0000000002"
    assert B.issuer_of(a) == ""
    assert F.is_on_us({"sender_card": a, "receiver_card": b}) is False


def test_an_unfilled_code_column_is_refused():
    """banks.csv shipped with `code` unfilled - every row "00000". Comparing
    that column makes is_on_us() true for EVERY transfer, turning the on_us
    receiver-age mode into `always` and inflating its coverage from 6.85% to
    100%. Refused at load rather than worked around."""
    rows = [{"code": "00000", "name": "A"}, {"code": "00000", "name": "B"}]
    with pytest.raises(ValueError, match="must be filled"):
        B._bank_identity(rows)


def test_a_filled_code_column_identifies_the_bank():
    rows = [{"code": "00873", "name": "A"}, {"code": "00420", "name": "B"}]
    assert B._bank_identity(rows) == "code"


def test_malformed_bin_is_rejected_loudly(tmp_path, monkeypatch):
    """A short or non-numeric BIN can never match a 6-character slice, so it
    would read as 'unknown issuer' for every card of that bank. Fail at load
    rather than degrade silently.

    The path is patched on config, not on the environment: BANKS_CSV_PATH is
    resolved once at import, the same way MODEL_ONNX_PATH is, because in a
    deployment the artefact does not move while the job runs.
    """
    import config as C
    csv = tmp_path / "banks.csv"
    csv.write_text("bin,code,name,cards_mln\n8600,00937,Broken,1.0\n860033,00440,Fine,2.0\n",
                   encoding="utf-8")
    monkeypatch.setattr(C, "BANKS_CSV_PATH", str(csv))
    with pytest.raises(ValueError, match="6 digits"):
        B._load()


def test_a_missing_table_names_the_fix(tmp_path, monkeypatch):
    """The job dies at import if this file is absent, so the message has to say
    what to run - this is the failure an operator meets after a fresh clone."""
    import config as C
    monkeypatch.setattr(C, "BANKS_CSV_PATH", str(tmp_path / "nope.csv"))
    with pytest.raises(FileNotFoundError, match="serve-prep"):
        B._load()
