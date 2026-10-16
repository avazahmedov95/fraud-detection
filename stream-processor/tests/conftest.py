"""The package directory on sys.path (pytest adds only the test file's own), and a
card helper."""

import hashlib
import os
import sys

_PKG = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _PKG)


def payee_card(payee: str) -> str:
    """A 16-digit PAN for a named payee or sender: distinct names, distinct cards,
    since the payee's state is keyed by card. From the name, not hash(), whose value
    changes between processes."""
    digest = hashlib.md5(payee.encode("utf-8")).hexdigest()
    tail = f"{int(digest[:12], 16) % 10**10:010d}"
    return "860033" + tail
