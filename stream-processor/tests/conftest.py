"""Path shim and shared helpers for the stream-processor tests.

TWO JOBS, both of which have to be here.

Importable package. The tests live one directory below the modules they
exercise, so the package directory has to be on sys.path for `import features`
to resolve: pytest inserts the test file's OWN directory, not its parent. Kept
in a conftest rather than a pytest.ini or an __init__.py because these five
packages deploy as separate units with no shared tooling, and conftest is the
one file pytest loads with no configuration at all.

Bank cards. The issuer is no longer a field on the event - it is resolved from
the PAN's BIN (bins.py) - so a test that wants two parties at the same or at
different banks has to say so with CARDS, the way the live job sees it. Keeping
one card per bank alias here means the tests exercise the real resolution path
instead of a synthetic field the wire format no longer carries.
"""

import hashlib
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


#: Bank alias -> a 16-digit PAN whose 6-digit BIN resolves to a real issuer in
#: data-generator/banks.csv. Only the prefix matters to bins.issuer_of(); the
#: remaining digits are filler and carry no Luhn meaning.
_BINS = {
    "BankA": "860033",      # Ipotekabank
    "BankB": "860003",      # UzSQB
    "BankC": "986003",      # a third issuer, for tests needing more than two
    # Deliberately absent from banks.csv. Under the old design "unknown issuer"
    # was an empty bank_name field; now it is a PAN whose BIN the table does not
    # resolve, which is the condition a real deployment actually meets (a card
    # from an issuer the BIN table has not been updated for). Tests that mean
    # "unknown" pass "" and get this.
    "": "999999",
}


def payee_card(payee: str, alias: str = "BankA") -> str:
    """A PAN for a named payee, at `alias`'s bank.

    Distinct payee names MUST map to distinct cards. Receiver-side state is
    keyed by card under the default payee_identity mode, so handing every payee
    the same PAN would collapse them into one: is_new_payee would go quiet after
    the first transfer and the fan-in window would merge unrelated payees. The
    tail is derived from the name rather than hashed with hash(), whose value
    for a str changes between processes and would make tests flap.
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
