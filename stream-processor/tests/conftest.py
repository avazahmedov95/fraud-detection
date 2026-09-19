"""Path shim and a shared card helper for the stream-processor tests.

sys.path: pytest inserts the test file's OWN directory, not its parent, so the
package dir is added here; in a conftest because the five packages deploy as
separate units with no shared tooling.
"""

import hashlib
import os
import sys

_PKG = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _PKG)
# The harnesses moved to experiments/ and one of them is unit-tested
# (test_dependency_loss). Added here rather than in that test so a second
# harness test does not have to rediscover the path.
sys.path.insert(0, os.path.join(_PKG, "experiments"))


def payee_card(payee: str) -> str:
    """A 16-digit PAN for a named payee (or sender).

    Distinct payees MUST get distinct cards: receiver state is keyed by card, so
    a shared PAN collapses them - is_new_payee goes quiet after the first
    transfer and the fan-in window merges unrelated payees. The tail is derived
    from the name, not hash(), whose str value changes between processes.
    """
    digest = hashlib.md5(payee.encode("utf-8")).hexdigest()
    tail = f"{int(digest[:12], 16) % 10**10:010d}"
    return "860033" + tail
