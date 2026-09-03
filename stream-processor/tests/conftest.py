"""Path shim and shared card helpers for the stream-processor tests.

sys.path: pytest inserts the test file's OWN directory, not its parent, so the
package dir is added here; in a conftest because the five packages deploy as
separate units with no shared tooling.

Bank cards: the issuer is not a field on the event, it is resolved from the PAN's
BIN (bins.py), so tests say same/different bank with CARDS and exercise the real
resolution path.
"""

import hashlib
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


#: Bank alias -> 16-digit PAN whose 6-digit BIN resolves to a real issuer in
#: data-generator/banks.csv. Only the prefix matters to bins.issuer_of(); the
#: rest is filler with no Luhn meaning.
_BINS = {
    "BankA": "860033",      # Ipotekabank
    "BankB": "860003",      # UzSQB
    "BankC": "986003",      # a third issuer, for tests needing more than two
    # Deliberately absent from banks.csv. "Unknown issuer" used to be an empty
    # bank_name field; now it is a PAN whose BIN the table does not resolve - the
    # condition a real deployment meets (a card from an issuer the BIN table has
    # not been updated for). Tests that mean "unknown" pass "" and get this.
    "": "999999",
}


def payee_card(payee: str, alias: str = "BankA") -> str:
    """A PAN for a named payee, at `alias`'s bank.

    Distinct payees MUST get distinct cards: receiver state is keyed by card, so
    a shared PAN collapses them - is_new_payee goes quiet after the first
    transfer and the fan-in window merges unrelated payees. The tail is derived
    from the name, not hash(), whose str value changes between processes.
    """
    digest = hashlib.md5(payee.encode("utf-8")).hexdigest()
    tail = f"{int(digest[:12], 16) % 10**10:010d}"
    return bank_card(alias, tail)


def bank_card(alias: str, tail: str = "0000000001") -> str:
    """A PAN that bins.issuer_of() resolves to `alias`'s issuer."""
    try:
        return _BINS[alias] + tail
    except KeyError:                                   # pragma: no cover
        raise AssertionError(
            f"unknown bank alias {alias!r} in a test; add its BIN to "
            f"conftest._BINS (known: {sorted(_BINS)})") from None
