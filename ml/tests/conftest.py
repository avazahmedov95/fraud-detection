"""Path shim for the ml tests.

pytest inserts the test file's OWN directory, not its parent, so the package dir
is added here - in a conftest, as the other packages do, because the packages
deploy as separate units with no shared tooling.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
